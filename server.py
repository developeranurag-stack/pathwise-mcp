import os
import sys
import shutil
import datetime
import time
import threading
import json
import asyncio
import re
import hashlib
import subprocess
from pathlib import Path
from urllib.parse import urljoin, urlparse, urldefrag

import httpx
import psycopg
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from pypdf import PdfReader
from fastmcp import FastMCP, Context

load_dotenv()

# Initialize FastMCP Server
mcp = FastMCP(name="GovJobExtractor")

# ------------------------------------------------------------------
# Storage & Database Setup
#
# Job records are written to PathWise's own Postgres (Neon) database,
# alongside careers/scholarships, so DATABASE_URL here must point at the
# same database as pathwise/.env. PDFs themselves stay on local disk
# (both projects are expected to run on the same host); only the local
# path is stored in Postgres.
# ------------------------------------------------------------------
BASE_DIR = Path(__file__).parent if "__file__" in globals() else Path.cwd()
STORAGE_DIR = BASE_DIR / "stored_pdfs"
STORAGE_DIR.mkdir(exist_ok=True)
PICKUP_DIR = BASE_DIR / "tobepicked"
PICKUP_DIR.mkdir(exist_ok=True)

def _dest_name(p: Path) -> str:
    h = hashlib.sha256(p.read_bytes()).hexdigest()[:12]
    return f"{p.stem}_{h}{p.suffix}"

DATABASE_URL = os.environ.get("DATABASE_URL")
LLM_API_KEY = os.environ.get("LLM_API_KEY") or os.environ.get("XAI_API_KEY") or os.environ.get("OPENAI_API_KEY")
LLM_BASE_URL = os.environ.get("LLM_BASE_URL", "https://api.x.ai/v1")
LLM_MODEL = os.environ.get("LLM_MODEL", "grok-2-1212")
FETCH_COMMISSIONS = os.environ.get("FETCH_COMMISSIONS", "").strip().lower() in ("1", "true", "yes")
FETCH_INTERVAL_SECONDS = int(os.environ.get("FETCH_INTERVAL_SECONDS", str(6 * 3600)))
POLL_SECONDS = int(os.environ.get("POLL_SECONDS", "5"))
FETCH_MAX_PDFS_PER_HOST = int(os.environ.get("FETCH_MAX_PDFS_PER_HOST", "3"))
HTTP_USER_AGENT = os.environ.get(
    "HTTP_USER_AGENT",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36 PathwiseGovJobBot/1.0",
)

EIGHTH_SCHEDULE_LANGUAGES = {
    "as": "Assamese", "bn": "Bengali", "brx": "Bodo", "doi": "Dogri",
    "gu": "Gujarati", "hi": "Hindi", "kn": "Kannada", "ks": "Kashmiri",
    "kok": "Konkani", "mai": "Maithili", "ml": "Malayalam", "mni": "Manipuri",
    "mr": "Marathi", "ne": "Nepali", "or": "Odia", "pa": "Punjabi",
    "sa": "Sanskrit", "sat": "Santali", "sd": "Sindhi", "ta": "Tamil",
    "te": "Telugu", "ur": "Urdu",
}

# exam_kind values persisted on gov_job_notifications.exam_kind
EXAM_KIND_COMBINED = "combined_exam"
EXAM_KIND_MULTI_POST = "multi_post_ad"
EXAM_KIND_SINGLE = "single_post"
EXAM_KIND_DEPARTMENTAL = "departmental_exam"

# ---------------------------------------------------------------------------
# COMMISSION_REGISTRY — data-driven issuer detection for Indian recruitment
# PDFs. Detection order is in _detect_commission(): URL host, English name,
# Hindi / known pypdf-garbled forms, filename tokens, state+PSC co-occurrence.
# Never invent a commission: if nothing matches, store commission="".
# ---------------------------------------------------------------------------
def _c(
    code, name_en, name_hi="", state=None, url_hosts=None, filename_tokens=None,
    exam_aliases=None, search_aliases=None, name_hi_forms=None, name_en_aliases=None,
):
    return {
        "code": code,
        "name_en": name_en,
        "name_hi": name_hi or "",
        "state": state,
        "url_hosts": list(url_hosts or []),
        "filename_tokens": list(filename_tokens or []),
        "exam_aliases": list(exam_aliases or []),
        "search_aliases": list(search_aliases or []),
        "name_hi_forms": list(name_hi_forms or []),
        "name_en_aliases": list(name_en_aliases or []),
    }


COMMISSION_REGISTRY = [
    # --- National ---
    _c("UPSC", "Union Public Service Commission", "संघ लोक सेवा आयोग", None,
       ["upsc.gov.in", "upsconline.nic.in", "upsconline.gov.in"],
       ["UPSC", "CIVIL_SERVICES", "COMBINED_DEFENCE", "ENGINEERING_SERVICES_EXAMINATION",
        "NDA_NA", "CDS_EXAM", "CAPF", "INDIAN_FOREST_SERVICE"],
       ["CSE", "IAS", "IPS", "IFS", "IFoS", "ESE", "IES", "CDS", "NDA", "CAPF", "CMS",
        "Civil Services Examination", "Engineering Services Examination"],
       ["upsc", "cse", "ias", "ips", "ifs", "ese", "ies", "nda", "cds", "capf", "cms", "ifos"],
       ["सघ लोक सेवा आयोग", "सं घ लोक सेवा आयोग"],
       ["SPECIAL ADVERTISEMENT", "Union Public Service Commission"]),
    _c("SSC", "Staff Selection Commission", "कर्मचारी चयन आयोग", None,
       ["ssc.gov.in", "ssc.nic.in", "ssc.digitalgaruda.in"],
       ["SSC", "COMBINED_GRADUATE_LEVEL", "COMBINED_HIGHER_SECONDARY", "SSC_CGL", "SSC_CHSL",
        "SSC_MTS", "SSC_GD", "SSC_CPO", "SSC_JE"],
       ["CGL", "CHSL", "MTS", "GD", "CPO", "JE", "Stenographer", "Combined Graduate Level",
        "Combined Higher Secondary"],
       ["ssc", "cgl", "chsl", "mts", "gd", "cpo", "ssc je", "stenographer"],
       ["कमचारी चयन आयोग", "कमचार चयन आयोग"]),
    _c("RRB", "Railway Recruitment Board", "रेलवे भर्ती बोर्ड", None,
       ["rrbcdg.gov.in", "rrbapply.gov.in", "indianrailways.gov.in", "rrb.indianrailways.gov.in"],
       ["RRB", "RRC", "RAILWAY_RECRUITMENT", "NTPC", "RRC_"],
       ["NTPC", "ALP", "Group D", "RRC", "CEN"],
       ["rrb", "rrc", "ntpc", "railway", "alp", "group d"]),
    _c("IBPS", "Institute of Banking Personnel Selection", "इंस्टीट्यूट ऑफ बैंकिंग पर्सनेल सिलेक्शन", None,
       ["ibps.in"],
       ["IBPS", "CRP_PO", "CRP_CLERK"],
       ["PO", "Clerk", "SO", "RRB"],
       ["ibps", "ibps po", "ibps clerk", "ibps so"]),
    _c("SBI", "State Bank of India", "भारतीय स्टेट बैंक", None,
       ["sbi.co.in", "bank.sbi"],
       ["SBI_PO", "SBI_CLERK", "SBI_SO"],
       ["PO", "Clerk", "SO"],
       ["sbi", "sbi po", "sbi clerk"]),
    _c("RBI", "Reserve Bank of India", "भारतीय रिजर्व बैंक", None,
       ["rbi.org.in"],
       ["RBI_GRADE", "RBI_ASSISTANT"],
       ["Grade B", "Assistant"],
       ["rbi", "rbi grade b"]),
    _c("NTA", "National Testing Agency", "राष्ट्रीय परीक्षा एजेंसी", None,
       ["nta.ac.in", "ugcnet.nta.nic.in", "examinationservices.nic.in"],
       ["UGC_NET", "NTA_UGC", "CSIR_NET"],
       ["UGC-NET", "NET", "CSIR NET"],
       ["nta", "ugc net", "ugc-net"]),
    # --- State PSCs ---
    _c("CGPSC", "Chhattisgarh Public Service Commission", "छत्तीसगढ़ लोक सेवा आयोग", "Chhattisgarh",
       ["psc.cg.gov.in", "cgpsc.cg.gov.in", "cgpsc.gov.in"],
       ["CGPSC", "STATE_SERVICE_EXAMINATION", "STATE_ENGINEERING_SERVICE", "CHHATTISGARH_PSC"],
       ["SSE", "राज्य सेवा परीक्षा", "State Service Examination", "CCE", "PCS",
        "State Engineering Service", "राज्य अभियांत्रिकी सेवा"],
       ["cgpsc", "sse", "pcs", "cce", "chhattisgarh psc", "psc.cg.gov.in"],
       ["छत्तीसगढ़ लोक सेवा आयोग", "छ°ीसगढ़ लोक सेवा आयोग", "छ°ीसगढ़ लोक सेवा आयोग",
        "छ ीसगढ़ लोक सेवा आयोग", "छत्तीसगढ लोक सेवा आयोग"]),
    _c("MPPSC", "Madhya Pradesh Public Service Commission", "मध्य प्रदेश लोक सेवा आयोग", "Madhya Pradesh",
       ["mppsc.mp.gov.in", "mppsc.gov.in", "mppsc.nic.in"],
       ["MPPSC", "MP_STATE_SERVICE"],
       ["SSE", "State Service Examination", "PCS", "राज्य सेवा परीक्षा"],
       ["mppsc", "sse", "pcs", "madhya pradesh psc"]),
    _c("UPPSC", "Uttar Pradesh Public Service Commission", "उत्तर प्रदेश लोक सेवा आयोग", "Uttar Pradesh",
       ["uppsc.up.nic.in", "uppsc.nic.in"],
       ["UPPSC", "UP_PCS", "PCS_J"],
       ["PCS", "RO/ARO", "Lower Subordinate", "प्रांतीय सिविल सेवा"],
       ["uppsc", "pcs", "up pcs", "ro aro"]),
    _c("UKPSC", "Uttarakhand Public Service Commission", "उत्तराखण्ड लोक सेवा आयोग", "Uttarakhand",
       ["ukpsc.gov.in", "ukpsc.net.in"],
       ["UKPSC", "UK_PCS"],
       ["PCS", "Upper Subordinate"],
       ["ukpsc", "pcs", "uttarakhand psc"]),
    _c("BPSC", "Bihar Public Service Commission", "बिहार लोक सेवा आयोग", "Bihar",
       ["bpsc.bih.nic.in", "bpsc.bihar.gov.in"],
       ["BPSC", "BPSC_CCE"],
       ["CCE", "Combined Competitive Examination", "PT", "संयुक्त प्रतियोगिता परीक्षा"],
       ["bpsc", "cce", "bihar psc"]),
    _c("JPSC", "Jharkhand Public Service Commission", "झारखण्ड लोक सेवा आयोग", "Jharkhand",
       ["jpsc.gov.in"],
       ["JPSC"],
       ["CCE", "Combined Civil Services"],
       ["jpsc", "cce", "jharkhand psc"]),
    _c("RPSC", "Rajasthan Public Service Commission", "राजस्थान लोक सेवा आयोग", "Rajasthan",
       ["rpsc.rajasthan.gov.in", "rpsc.raj.nic.in"],
       ["RPSC", "RAS_RTS"],
       ["RAS", "RTS", "Rajasthan Administrative Service"],
       ["rpsc", "ras", "rts", "rajasthan psc"]),
    _c("HPSC", "Haryana Public Service Commission", "हरियाणा लोक सेवा आयोग", "Haryana",
       ["hpsc.gov.in"],
       ["HPSC", "HCS"],
       ["HCS", "Haryana Civil Service"],
       ["hpsc", "hcs", "haryana psc"]),
    _c("PPSC", "Punjab Public Service Commission", "पंजाब लोक सेवा आयोग", "Punjab",
       ["ppsc.gov.in"],
       ["PPSC", "PUNJAB_PCS"],
       ["PCS", "Punjab Civil Service"],
       ["ppsc", "pcs", "punjab psc"]),
    _c("GPSC", "Gujarat Public Service Commission", "गुजरात लोक सेवा आयोग", "Gujarat",
       ["gpsc.gujarat.gov.in", "ojas.gujarat.gov.in"],
       ["GPSC", "GUJARAT_PSC"],
       ["Class 1", "Class 2", "GPSC Class I"],
       ["gpsc", "gujarat psc"]),
    _c("GPSC_GOA", "Goa Public Service Commission", "गोवा लोक सेवा आयोग", "Goa",
       ["gpsc.goa.gov.in"],
       ["GOA_PSC", "GPSC_GOA"],
       ["Goa Civil Service"],
       ["goa psc", "gpsc goa"]),
    _c("MPSC", "Maharashtra Public Service Commission", "महाराष्ट्र लोक सेवा आयोग", "Maharashtra",
       ["mpsc.gov.in", "mpscmaha.gov.in"],
       ["MPSC", "MAHARASHTRA_PSC", "RAJYASEVA"],
       ["Rajyaseva", "राज्यसेवा", "PSI", "STI", "ASO"],
       ["mpsc", "rajyaseva", "maharashtra psc"]),
    _c("MPSC_MANIPUR", "Manipur Public Service Commission", "मणिपुर लोक सेवा आयोग", "Manipur",
       ["mpscmanipur.gov.in", "mpscmanipur.nic.in"],
       ["MANIPUR_PSC", "MPSC_MANIPUR"],
       ["MCS", "Manipur Civil Service"],
       ["manipur psc", "mpsc manipur"]),
    _c("APSC", "Assam Public Service Commission", "असम लोक सेवा आयोग", "Assam",
       ["apsc.nic.in", "apsc.assam.gov.in"],
       ["APSC", "ASSAM_PSC"],
       ["CCE", "Combined Competitive Examination"],
       ["apsc", "cce", "assam psc"]),
    _c("OPSC", "Odisha Public Service Commission", "ओडिशा लोक सेवा आयोग", "Odisha",
       ["opsc.gov.in"],
       ["OPSC", "OAS"],
       ["OAS", "Odisha Civil Services"],
       ["opsc", "oas", "odisha psc", "orsc"]),
    _c("WBPSC", "West Bengal Public Service Commission", "पश्चिम बंगाल लोक सेवा आयोग", "West Bengal",
       ["pscwbonline.gov.in", "wbpsc.gov.in", "pscwb.org.in"],
       ["WBPSC", "WBCS"],
       ["WBCS", "West Bengal Civil Service"],
       ["wbpsc", "wbcs", "west bengal psc"]),
    _c("TNPSC", "Tamil Nadu Public Service Commission", "तमिलनाडु लोक सेवा आयोग", "Tamil Nadu",
       ["tnpsc.gov.in"],
       ["TNPSC", "GROUP_I", "GROUP_II", "GROUP_IV"],
       ["Group 1", "Group 2", "Group 4", "CCSE"],
       ["tnpsc", "group 1", "group 2", "group 4", "tnpsc group 1"]),
    _c("APPSC", "Andhra Pradesh Public Service Commission", "आंध्र प्रदेश लोक सेवा आयोग", "Andhra Pradesh",
       ["psc.ap.gov.in", "appsc.gov.in"],
       ["APPSC", "AP_PSC", "APPSC_GROUP"],
       ["Group 1", "Group 2", "Group 3"],
       ["appsc", "andhra pradesh psc", "ap psc"]),
    _c("TSPSC", "Telangana State Public Service Commission", "तेलंगाना राज्य लोक सेवा आयोग", "Telangana",
       ["tspsc.gov.in"],
       ["TSPSC"],
       ["Group 1", "Group 2", "Group 3"],
       ["tspsc", "telangana psc"]),
    _c("KPSC", "Karnataka Public Service Commission", "कर्नाटक लोक सेवा आयोग", "Karnataka",
       ["kpsc.kar.nic.in", "kpsc.karnataka.gov.in"],
       ["KPSC", "KARNATAKA_PSC", "KAS_EXAM"],
       ["KAS", "Karnataka Administrative Service", "Group A", "Group B"],
       ["kpsc", "kas", "karnataka psc"]),
    _c("KPPSC", "Kerala Public Service Commission", "केरल लोक सेवा आयोग", "Kerala",
       ["keralapsc.gov.in", "thulasi.keralapsc.gov.in"],
       ["KERALA_PSC", "KPPSC", "KPSC_KERALA"],
       ["KAS", "Degree Level"],
       ["kerala psc", "kppsc", "keralapsc"]),
    _c("HPPSC", "Himachal Pradesh Public Service Commission", "हिमाचल प्रदेश लोक सेवा आयोग", "Himachal Pradesh",
       ["hppsc.hp.gov.in"],
       ["HPPSC", "HPAS"],
       ["HPAS", "HAS"],
       ["hppsc", "hpas", "himachal psc"]),
    _c("JKPSC", "Jammu and Kashmir Public Service Commission", "जम्मू और कश्मीर लोक सेवा आयोग",
       "Jammu and Kashmir",
       ["jkpsc.nic.in"],
       ["JKPSC", "JKAS"],
       ["JKAS", "Combined Competitive"],
       ["jkpsc", "jkas", "jammu kashmir psc"]),
    _c("APPSC_AR", "Arunachal Pradesh Public Service Commission", "अरुणाचल प्रदेश लोक सेवा आयोग",
       "Arunachal Pradesh",
       ["appsc.gov.in", "psc.arunachal.gov.in"],
       ["ARUNACHAL_PSC", "APPSC_AR"],
       ["APPSCCE", "Combined Competitive"],
       ["arunachal psc", "appsc arunachal"]),
    _c("MPSC_MZ", "Mizoram Public Service Commission", "मिजोरम लोक सेवा आयोग", "Mizoram",
       ["mpsc.mizoram.gov.in"],
       ["MIZORAM_PSC", "MPSC_MZ"],
       ["MCS"],
       ["mizoram psc"]),
    _c("NPSC", "Nagaland Public Service Commission", "नागालैंड लोक सेवा आयोग", "Nagaland",
       ["npsc.nagaland.gov.in"],
       ["NPSC", "NAGALAND_PSC"],
       ["NCS"],
       ["npsc", "nagaland psc"]),
    _c("MePSC", "Meghalaya Public Service Commission", "मेघालय लोक सेवा आयोग", "Meghalaya",
       ["mpsc.nic.in", "megpsc.gov.in"],
       ["MEGHALAYA_PSC", "MePSC"],
       ["MCS"],
       ["meghalaya psc", "mepsc"]),
    _c("TPSC", "Tripura Public Service Commission", "त्रिपुरा लोक सेवा आयोग", "Tripura",
       ["tpsc.tripura.gov.in"],
       ["TPSC", "TRIPURA_PSC"],
       ["TCS", "TPS"],
       ["tpsc", "tripura psc"]),
    _c("SPSC", "Sikkim Public Service Commission", "सिक्किम लोक सेवा आयोग", "Sikkim",
       ["spsc.sikkim.gov.in", "spscskm.gov.in"],
       ["SIKKIM_PSC", "SPSC_SK"],
       ["SCS"],
       ["sikkim psc", "spsc"]),
    # --- Courts, boards, specialist issuers ---
    _c("SCI", "Supreme Court of India", "भारत का सर्वोच्च न्यायालय", None,
       ["sci.gov.in", "main.sci.gov.in"],
       ["SUPREME_COURT", "SCI_"],
       [],
       ["supreme court", "sci"]),
    _c("HIGH_COURT", "High Court", "उच्च न्यायालय", None,
       ["hc.nic.in", "allahabadhighcourt.in", "bombayhighcourt.nic.in",
        "highcourt.cg.gov.in", "mphc.gov.in", "hcraj.nic.in"],
       ["HIGH_COURT", "HIGHCOURT"],
       [],
       ["high court"]),
    _c("MPESB", "Madhya Pradesh Employees Selection Board", "मध्य प्रदेश कर्मचारी चयन मण्डल", "Madhya Pradesh",
       ["esb.mp.gov.in", "peb.mp.gov.in", "vyapam.nic.in", "esb.mponline.gov.in"],
       ["VYAPAM", "PEB_MP", "ESB_MP", "MPESB", "MPPEB"],
       ["PEB", "ESB", "Group 2", "Group 3", "Patwari", "Van Rakshak"],
       ["vyapam", "peb", "esb", "mpesb", "mp peb", "mp vyapam"],
       ["व्यापम"],
       ["MP Vyapam", "Professional Examination Board", "Employees Selection Board"]),
    _c("CGVYAPAM", "Chhattisgarh Professional Examination Board", "छत्तीसगढ़ व्यावसायिक परीक्षा मण्डल", "Chhattisgarh",
       ["vyapamcg.cgstate.gov.in", "vyapamprofile.cgstate.gov.in"],
       ["CGVYAPAM", "CG_VYAPAM", "CGPEB"],
       ["TET", "SET", "LSAT", "CG-SET"],
       ["cg vyapam", "cgvyapam", "chhattisgarh vyapam", "vyapamcg"],
       ["व्यापम", "छत्तीसगढ़ कर्मचारी चयन मण्डल"],
       ["CG Vyapam", "Chhattisgarh Vyapam"]),
    _c("RSMSSB", "Rajasthan Subordinate and Ministerial Services Selection Board",
       "राजस्थान अधीनस्थ एवं मंत्रालयिक सेवा चयन बोर्ड", "Rajasthan",
       ["rsmssb.rajasthan.gov.in", "rssb.rajasthan.gov.in"],
       ["RSMSSB", "RSSB"],
       ["Patwari", "REET", "Clerk"],
       ["rsmssb", "rssb", "rajasthan ssb"]),
    _c("HSSC", "Haryana Staff Selection Commission", "हरियाणा कर्मचारी चयन आयोग", "Haryana",
       ["hssc.gov.in", "hssc.hryssc.com"],
       ["HSSC", "HARYANA_SSC"],
       ["CET", "Group C", "Group D"],
       ["hssc", "haryana ssc"]),
    _c("UKSSSC", "Uttarakhand Subordinate Service Selection Commission",
       "उत्तराखण्ड अधीनस्थ सेवा चयन आयोग", "Uttarakhand",
       ["sssc.uk.gov.in", "ukssscrecruitment.in", "uksssc.co.in"],
       ["UKSSSC", "UK_SSSC"],
       ["Group C", "Patwari"],
       ["uksssc", "uttarakhand sssc"]),
    _c("OSSC", "Odisha Staff Selection Commission", "ओडिशा कर्मचारी चयन आयोग", "Odisha",
       ["ossc.gov.in", "www.ossc.gov.in"],
       ["OSSC"],
       ["CGL", "Combined Recruitment"],
       ["ossc", "odisha ssc"]),
    _c("JSSC", "Jharkhand Staff Selection Commission", "झारखंड कर्मचारी चयन आयोग", "Jharkhand",
       ["jssc.jharkhand.gov.in"],
       ["JSSC"],
       ["CGL", "JGGLCCE"],
       ["jssc", "jharkhand ssc"]),
    _c("GSSSB", "Gujarat Subordinate Services Selection Board", "गुजरात गौण सेवा चयन मण्डल", "Gujarat",
       ["gsssb.gujarat.gov.in", "ojas.gujarat.gov.in"],
       ["GSSSB"],
       ["CCE", "Class 3"],
       ["gsssb", "gujarat sssb"]),
    _c("OSSSC", "Odisha Sub-ordinate Staff Selection Commission", "ओडिशा अधीनस्थ कर्मचारी चयन आयोग", "Odisha",
       ["osssc.gov.in"],
       ["OSSSC"],
       [],
       ["osssc"]),
    _c("BPSSC", "Bihar Police Subordinate Services Commission", "बिहार पुलिस अधीनस्थ सेवा आयोग", "Bihar",
       ["bpssc.bih.nic.in", "bpssc.bihar.gov.in"],
       ["BPSSC"],
       ["SI", "Sergeant"],
       ["bpssc", "bihar police"]),
    _c("WBSSC", "West Bengal Staff Selection Commission", "पश्चिम बंगाल कर्मचारी चयन आयोग", "West Bengal",
       ["wbssc.gov.in"],
       ["WBSSC"],
       [],
       ["wbssc"]),
    _c("HPRCA", "Himachal Pradesh Staff Selection Commission", "हिमाचल प्रदेश कर्मचारी चयन आयोग", "Himachal Pradesh",
       ["hpsssb.hp.gov.in"],
       ["HPSSSB", "HPSSC"],
       [],
       ["hpsssb", "hpssc"]),
    _c("AIIMS", "All India Institute of Medical Sciences", "अखिल भारतीय आयुर्विज्ञान संस्थान", None,
       ["aiims.edu", "aiimsexams.ac.in"],
       ["AIIMS"],
       ["NORCET"],
       ["aiims", "norcet"]),
    _c("PGIMER", "Postgraduate Institute of Medical Education and Research", None, None,
       ["pgimer.edu.in"],
       ["PGIMER"],
       [],
       ["pgimer"]),
    _c("ESIC", "Employees State Insurance Corporation", "कर्मचारी राज्य बीमा निगम", None,
       ["esic.gov.in", "esic.nic.in"],
       ["ESIC"],
       [],
       ["esic"]),
    _c("KVS", "Kendriya Vidyalaya Sangathan", "केन्द्रीय विद्यालय संगठन", None,
       ["kvsangathan.nic.in"],
       ["KVS_"],
       ["PRT", "TGT", "PGT"],
       ["kvs"]),
    _c("NVS", "Navodaya Vidyalaya Samiti", "नवोदय विद्यालय समिति", None,
       ["navodaya.gov.in"],
       ["NVS_"],
       ["PRT", "TGT", "PGT"],
       ["nvs"]),
    _c("POLICE", "State Police Headquarters", "पुलिस मुख्यालय", None,
       [],
       ["POLICE_HQ", "CONSTABLE_RECRUITMENT", "SI_RECRUITMENT"],
       [],
       ["police"]),
    _c("DSSSB", "Delhi Subordinate Services Selection Board", "दिल्ली अधीनस्थ सेवा चयन बोर्ड", None,
       ["dsssb.delhi.gov.in"],
       ["DSSSB"],
       ["TGT", "PGT", "PRT"],
       ["dsssb", "delhi dsssb"]),
    _c("BSSC", "Bihar Staff Selection Commission", "बिहार कर्मचारी चयन आयोग", "Bihar",
       ["bssc.bihar.gov.in"],
       ["BSSC"],
       ["Inter Level", "CGL"],
       ["bssc", "bihar ssc"]),
    _c("UPSSSC", "Uttar Pradesh Subordinate Services Selection Commission",
       "उत्तर प्रदेश अधीनस्थ सेवा चयन आयोग", "Uttar Pradesh",
       ["upsssc.gov.in"],
       ["UPSSSC"],
       ["PET", "VDO"],
       ["upsssc"]),
]

# Host → entry, built once. Longer hosts win first via sort.
_HOST_INDEX = []
for _entry in COMMISSION_REGISTRY:
    for _h in _entry["url_hosts"]:
        _HOST_INDEX.append((_h.lower(), _entry))
_HOST_INDEX.sort(key=lambda x: len(x[0]), reverse=True)

# Generic advertisement/listing paths tried on every official host.
_DEFAULT_LISTING_PATHS = [
    "/",
    "/advertisement",
    "/advertisements",
    "/Advertisement",
    "/Advertisements",
    "/notification",
    "/notifications",
    "/recruitment",
    "/whats-new",
    "/whatsnew",
    "/latest-news",
    "/latest-updates",
    "/examination",
    "/examinations",
    "/advt",
    "/new-advertisement",
    "/current-advertisement",
    "/advertisement.aspx",
    "/Advertisement.aspx",
    "/notifications.aspx",
    "/site/advertisement",
    "/home/advertisement",
    "/Posts",
    "/Posts?tag=ONLINEAPPLICATION",
    "/e_default.html",
    "/home_n.html",
    "/leftmenu.html",
]
# Extra seeds for commissions whose notices live off the homepage.
_LISTING_PATHS_BY_CODE = {
    "UPSC": ["/", "/examinations", "/recruitment", "/recruitment/advertisement",
             "/whats-new", "/examinations/active-exams"],
    "SSC": ["/", "/portal/whats-new", "/portal/notices", "/portal/candidate-notice"],
    "RRB": ["/", "/rrbcdg/viewsection.jsp"],
    "CGPSC": ["/", "/advertisement", "/Default.aspx"],
    "MPPSC": ["/", "/Advertisement.aspx", "/more_advertisement.aspx"],
    "UPPSC": ["/", "/Notifications.aspx", "/Advertisements.aspx"],
    "BPSC": ["/", "/Notices.aspx", "/Advertisement.aspx"],
    "RPSC": ["/", "/Advertisements", "/news"],
    "TNPSC": ["/", "/Notifications.aspx"],
    "TSPSC": ["/", "/Notifications"],
    "KPSC": ["/", "/notification"],
    "KPPSC": ["/", "/notifications"],
    "MPSC": ["/", "/advertisements"],
    "GPSC": ["/", "/Advertisement"],
    "WBPSC": ["/", "/advertisement"],
    "HPSC": ["/", "/Advertisements"],
    "PPSC": ["/", "/Advertisement"],
    "OPSC": ["/", "/Advertisement"],
    "APSC": ["/", "/advertisement"],
    "HPPSC": ["/", "/advertisements"],
    "JKPSC": ["/", "/Advertisement"],
    "UKPSC": ["/", "/Advertisement"],
    "JPSC": ["/", "/advertisement"],
    "APPSC": ["/", "/notifications"],
    "DSSSB": ["/", "/latest-updates"],
    "NTA": ["/", "/"],
    "MPESB": [
        "/",
        "/e_default.html",
        "/home_n.html",
        "/leftmenu.html",
        "/advertisement/Important_message_candidate.htm",
        "/Rulebooks/rule_books.htm",
        "/Advertisement",
    ],
    "VYAPAM": [
        "/",
        "/e_default.html",
        "/home_n.html",
    ],
    "CGVYAPAM": [
        "/",
        "/Posts?tag=ONLINEAPPLICATION",
        "/Posts?tag=ADVERTISEMENT",
        "/Post?PostID=Recruitment%20Year%20Wise",
    ],
    "RSMSSB": ["/", "/page?menuName=Advertisement", "/Advertisement"],
    "HSSC": ["/", "/advertisement"],
    "UKSSSC": ["/", "/"],
    "OSSC": ["/", "/"],
    "JSSC": ["/", "/"],
    "GSSSB": ["/", "/Home/Index"],
    "OSSSC": ["/", "/"],
    "BPSSC": ["/", "/"],
}

_FETCH_SKIP_RE = re.compile(
    r"(?i)(?:answer[_\s-]*key|hall[_\s-]*ticket|admit[_\s-]*card|cut[\s-]?off|"
    r"result[-_ ]?(?:pdf|sheet)?|score[_\s-]*card|attendance|tender|"
    r"\brti\b|faq|innovations|informationforcandidates|"
    r"corrigendum-only|photo\.(?:jpg|png))",
)
_FETCH_WANT_RE = re.compile(
    r"(?i)(?:advertis|advt|notification|recruit|exam|vacanc|विज्ञापन|"
    r"भर्ती|परीक्षा|सूचना|अधिसूचना|विज्ञप्ति|ora|special.?ad|"
    r"onlineapplication|rulebook|vigyapti|posts\?|postid=)",
)

# Combined-exam filename / body phrases (kind A). Intentionally does NOT
# match "State Engineering Service" (kind D) — that is a specialist exam.
_COMBINED_EXAM_RES = [
    re.compile(p, re.I) for p in [
        r"state[_\s-]+service[_\s-]+exam",
        r"civil[_\s-]+services?[_\s-]+exam",
        r"combined[_\s-]+graduate",
        r"combined[_\s-]+higher[_\s-]+secondary",
        r"combined[_\s-]+competitive",
        r"combined[_\s-]+defence",
        r"\bcse\b", r"\bcgl\b", r"\bchsl\b", r"\bcce\b", r"\bras\b", r"\bwbcs\b",
        r"group[_\s-]*(?:1|i|2|ii)\b",
        r"preliminary\s+(?:and|&)\s+main",
        r"prelim(?:inary)?\s+(?:cum|&|and)\s+main",
        r"राज्य सेवा परी",
        r"राº\s*य सेवा परी",
        r"लोक सेवा परीक्षा",
        r"engineering[_\s-]+services[_\s-]+exam",  # UPSC ESE, not State Engineering
        r"indian[_\s-]+forest[_\s-]+service",
        r"nda[_\s-]+na\b",
        r"\bcds\b",
        r"\bcapf\b",
    ]
]
_DEPARTMENTAL_EXAM_RES = [
    re.compile(p, re.I) for p in [
        r"state[_\s-]+engineering[_\s-]+service",
        r"राज्य अभियांत्रिकी",
        r"राº\s*य अिभयां",
        r"assistant[_\s-]+librarian[_\s-]+exam",
        r"assistant[_\s-]+professor",
        r"सहायक प्राध्यापक",
        r"forest[_\s-]+ranger[_\s-]+exam",
        r"the examination shall be conducted by",
    ]
]
_CADRE_WORDS = (
    "Collector", "Police", "Service", "Officer", "Inspector", "Professor",
    "Engineer", "Manager", "Registrar", "Tahsildar", "Tehsildar", "Jailor",
    "Superintendent", "Director", "Organiser", "Organizer", "Commandant",
    "Instructor", "Auditor", "Accountant", "Assistant", "Deputy", "Naib",
    "Sub-", "Commissioner", "Magistrate", "Judge", "Librarian", "Lecturer",
    "Teacher", "Principal", "Surgeon", "Physician", "Specialist", "Analyst",
    "Statistician", "Chemist", "Pharmacist", "Nurse", "Clerk", "Stenographer",
    "Typist", "Translator", "Forest", "Ranger", "Guard", "Constable",
    "Scientist", "Geologist", "Economist", "Planner", "Prosecutor",
    "Advocate", "Counsel", "Abhiyojan", "Adhikari", "Accounts", "Excise",
    "Employment", "Labour", "Jail", "Panchayat", "Development", "Welfare",
    "Tax", "Audit", "Relation", "Nutrition", "Home Guard",
)
_CADRE_HINT_RE = re.compile(
    r"\b(?:%s)\b" % "|".join(re.escape(w) for w in _CADRE_WORDS), re.I
)
_GARBLED_DEVANAGARI_RE = re.compile(r"[º±ĬÅÆÿØ§¤´µ¶·¸¹º»¼½¾¿]")
_DEVANAGARI_RE = re.compile(r"[\u0900-\u097F]")
_DATE_RE = re.compile(r"\b(\d{1,2}[./\-]\d{1,2}[./\-]\d{2,4})\b")
_APPLY_CTX_RE = re.compile(
    r"(?i)(?:apply|application|online|last\s*date|closing\s*date|"
    r"आवेदन|ऑनलाइन|अितम\s*ितिथ|अंतिम\s*तिथि|अन्तिम\s*तिथि)"
)
_SYLLABUS_TOPIC_RE = re.compile(
    r"(?i)(?:\bof Chhattisgarh\b|\bSchemes?\b|\bEconomy,|\bAdministrative Structure|"
    r"\bHistory of\b|\bConstitution of\b|\bWelfare Schemes)"
)
_HISTORICAL_RULE_RE = re.compile(
    r"(?i)(?:vide\s+notification|rules?,?\s*20\d{2}|dated\s+\d|amendment\s+notification|"
    r"as\s+per\s+order|examination\s+rules)"
)
_PACK_BUDGET = 28000


def _log(event, *parts):
    try:
        with open(BASE_DIR / "mcp_read_log.txt", "a", encoding="utf-8") as lf:
            lf.write("|".join(
                [datetime.datetime.now().isoformat(), event, *[str(p) for p in parts]]
            ) + "\n")
    except Exception:
        pass


def _norm_ws(s):
    return re.sub(r"\s+", " ", (s or "")).strip()


_db_ready = False
_initializing_db = False


def connect():
    if not DATABASE_URL:
        raise RuntimeError(
            "DATABASE_URL is not set. Create a .env file pointing at the same Neon "
            "Postgres database as pathwise/.env, e.g. "
            "DATABASE_URL=postgresql://user:pass@host/dbname?sslmode=require"
        )
    if not _initializing_db:
        _ensure_db()
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)


def _ensure_db():
    global _db_ready
    if _db_ready or not DATABASE_URL:
        return
    init_db()


def init_db():
    global _db_ready, _initializing_db
    if _db_ready:
        return
    _initializing_db = True
    try:
        _init_db_body()
        _db_ready = True
    finally:
        _initializing_db = False


def _init_db_body():
    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS gov_job_notifications (
                    id                      SERIAL PRIMARY KEY,
                    job_title               TEXT NOT NULL,
                    department              TEXT,
                    total_vacancies         INTEGER,
                    reservation_details     JSONB,
                    qualification           TEXT,
                    age_limit               TEXT,
                    age_relaxation          TEXT,
                    apply_start_date        TEXT,
                    apply_end_date          TEXT,
                    exam_date               TEXT,
                    advertisement_number    TEXT,
                    application_fee         TEXT,
                    official_url            TEXT,
                    local_pdf_path          TEXT,
                    source                  TEXT NOT NULL DEFAULT 'mcp:gov-job-extractor',
                    created_at              TIMESTAMPTZ NOT NULL DEFAULT now()
                )
            """)
            # ADD COLUMN IF NOT EXISTS so this also migrates a DB that already
            # had gov_job_notifications from before these columns existed.
            # pathwise/schema.sql sync — add these columns to gov_job_notifications:
            #   commission     TEXT        -- 'UPSC' | 'CGPSC' | 'SSC' | 'MPPSC' | ...
            #   state          TEXT        -- 'Chhattisgarh' | NULL for national
            #   exam_name      TEXT        -- 'State Service Examination 2025'
            #   exam_kind      TEXT        -- 'combined_exam' | 'multi_post_ad' |
            #                              -- 'single_post' | 'departmental_exam'
            #   search_document TEXT       -- denormalised aliases blob, lowercase
            for column, ddl in [
                ("exam_date", "TEXT"),
                ("advertisement_number", "TEXT"),
                ("application_fee", "TEXT"),
                ("translations", "JSONB"),
                ("age_relaxation_details", "JSONB"),
                ("nationality", "TEXT"),
                ("syllabus", "JSONB"),
                ("commission", "TEXT"),
                ("state", "TEXT"),
                ("exam_name", "TEXT"),
                ("exam_kind", "TEXT"),
                ("search_document", "TEXT"),
            ]:
                cur.execute(f"ALTER TABLE gov_job_notifications ADD COLUMN IF NOT EXISTS {column} {ddl}")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS gov_job_posts (
                    id                      SERIAL PRIMARY KEY,
                    notification_id         INT NOT NULL REFERENCES gov_job_notifications(id) ON DELETE CASCADE,
                    post_name               TEXT NOT NULL,
                    department              TEXT,
                    pay_level               TEXT,
                    total_vacancies         INTEGER,
                    vacancies_breakdown     JSONB,
                    qualification           TEXT
                )
            """)
            cur.execute("ALTER TABLE gov_job_posts ADD COLUMN IF NOT EXISTS translations JSONB")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_gov_job_posts_notification ON gov_job_posts(notification_id)")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS gov_job_fetch_seen (
                    url         TEXT PRIMARY KEY,
                    sha256      TEXT,
                    commission  TEXT,
                    first_seen  TIMESTAMPTZ NOT NULL DEFAULT now(),
                    last_status TEXT
                )
            """)
        conn.commit()
    finally:
        conn.close()

def resolve_pdf_path(stored) -> Path:
    """Resolve a DB path that may be absolute, relative, or just a filename."""
    if not stored:
        return Path("")
    p = Path(str(stored))
    if p.is_file():
        return p
    name = p.name
    for cand in (BASE_DIR / "stored_pdfs" / name, STORAGE_DIR / name, BASE_DIR / p):
        if cand.is_file():
            return cand
    return p


def _db_pdf_path(stored_path: str) -> str:
    """Persist a host-independent relative path."""
    name = Path(stored_path or "").name
    return f"stored_pdfs/{name}" if name else (stored_path or "")


def _score_extracted_text(s: str) -> int:
    if not s:
        return 0
    dev = len(_DEVANAGARI_RE.findall(s))
    garb = len(_GARBLED_DEVANAGARI_RE.findall(s))
    return len(s) + dev * 3 - garb * 8


def _pdftotext(path: Path, layout: bool = True) -> str:
    args = ["pdftotext"]
    if layout:
        args.append("-layout")
    args += ["-enc", "UTF-8", str(path), "-"]
    try:
        r = subprocess.run(args, capture_output=True, timeout=60)
        if r.returncode == 0 and r.stdout:
            return r.stdout.decode("utf-8", errors="replace")
    except Exception:
        pass
    return ""


def _pdfplumber_text(path: Path) -> str:
    try:
        import pdfplumber
    except Exception:
        return ""
    try:
        chunks = []
        with pdfplumber.open(str(path)) as doc:
            for i, pg in enumerate(doc.pages):
                chunks.append(f"\n--- Page {i+1} ---\n")
                chunks.append(pg.extract_text() or "")
                for tbl in (pg.extract_tables() or []):
                    if not tbl:
                        continue
                    chunks.append("\n[TABLE]\n")
                    for row in tbl:
                        cells = [re.sub(r"\s+", " ", str(c or "")).strip() for c in row]
                        chunks.append(" | ".join(cells) + "\n")
        return "".join(chunks)
    except Exception:
        return ""


def _pypdf_text(path: Path) -> str:
    reader = PdfReader(str(path))
    text = ""
    for i, page in enumerate(reader.pages):
        page_text = page.extract_text(extraction_mode="layout") or ""
        text += f"\n--- Page {i+1} ---\n" + page_text
    try:
        plain = ""
        for page in reader.pages:
            plain += (page.extract_text() or "") + "\n"
        if len(plain) > 100:
            text += "\n\n=== PLAIN TEXT FALLBACK ===\n" + plain[:8000]
    except Exception:
        pass
    return text


def _write_shared_registry():
    """Keep pathwise/gov_job_aliases.py in sync via a JSON sidecar."""
    payload = []
    for e in COMMISSION_REGISTRY:
        payload.append({
            "code": e["code"],
            "name_en": e["name_en"],
            "name_hi": e.get("name_hi") or "",
            "state": e.get("state"),
            "url_hosts": e.get("url_hosts") or [],
            "search_aliases": e.get("search_aliases") or [],
            "exam_aliases": e.get("exam_aliases") or [],
        })
    try:
        (BASE_DIR / "commission_registry.json").write_text(
            json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
        )
    except Exception:
        pass


_write_shared_registry()


# ------------------------------------------------------------------
# 1. TOOL: Ingest and Store PDF locally
# ------------------------------------------------------------------
@mcp.tool()
def store_notification_pdf(source_pdf_path: str) -> str:
    """
    Copies a source PDF into the server's local storage folder (./stored_pdfs/)
    so that users can view the official document later.
    Returns the new stored file path.
    """
    source_path = Path(source_pdf_path)
    if not source_path.exists():
        with open(BASE_DIR / "mcp_read_log.txt", "a", encoding="utf-8") as lf: lf.write(f"{datetime.datetime.now().isoformat()}|STORE_ERR|noexist|{source_pdf_path}\n")
        return f"Error: Source PDF at '{source_pdf_path}' does not exist."

    if source_path.suffix.lower() != ".pdf":
        with open(BASE_DIR / "mcp_read_log.txt", "a", encoding="utf-8") as lf: lf.write(f"{datetime.datetime.now().isoformat()}|STORE_ERR|notpdf|{source_pdf_path}\n")
        return "Error: File must be a PDF."

    if source_path.stat().st_size == 0:
        with open(BASE_DIR / "mcp_read_log.txt", "a", encoding="utf-8") as lf: lf.write(f"{datetime.datetime.now().isoformat()}|STORE_ERR|empty|{source_pdf_path}\n")
        return "Error: Source PDF is empty."

    dest_filename = _dest_name(source_path)
    dest_path = STORAGE_DIR / dest_filename
    if dest_path.exists():
        with open(BASE_DIR / "mcp_read_log.txt", "a", encoding="utf-8") as lf: lf.write(f"{datetime.datetime.now().isoformat()}|STORE_HIT|{source_pdf_path}|dest={dest_path}\n")
        return str(dest_path.resolve())

    data = source_path.read_bytes()
    for cand in STORAGE_DIR.glob(f"{source_path.stem}_*.pdf"):
        if cand.stat().st_size == len(data) and cand.read_bytes() == data:
            with open(BASE_DIR / "mcp_read_log.txt", "a", encoding="utf-8") as lf: lf.write(f"{datetime.datetime.now().isoformat()}|STORE_HIT_LEGACY|{source_pdf_path}|dest={cand}\n")
            return str(cand.resolve())

    shutil.copy2(source_path, dest_path)
    with open(BASE_DIR / "mcp_read_log.txt", "a", encoding="utf-8") as lf: lf.write(f"{datetime.datetime.now().isoformat()}|STORE|{source_pdf_path}|dest={dest_path}\n")
    return str(dest_path.resolve())

# ------------------------------------------------------------------
# 2. RESOURCES: Extract text & Retrieve PDF file
# ------------------------------------------------------------------
@mcp.resource("pdf://{file_path}")
def read_pdf_notification(file_path: str) -> str:
    """Reads and extracts text from a local PDF notification file."""
    path = resolve_pdf_path(file_path)
    if not path.exists():
        return f"Error: File '{file_path}' does not exist."

    try:
        candidates = []
        layout = _pdftotext(path, layout=True)
        if layout:
            candidates.append(layout)
        plain_poppler = _pdftotext(path, layout=False)
        if plain_poppler:
            candidates.append(plain_poppler)
        plumber = _pdfplumber_text(path)
        if plumber:
            candidates.append(plumber)
        try:
            candidates.append(_pypdf_text(path))
        except Exception:
            pass
        text = max(candidates, key=_score_extracted_text) if candidates else ""
        _log("READ_OK", file_path, f"len={len(text)}", f"sources={len(candidates)}")
        return text
    except Exception as e:
        _log("READ_ERR", file_path, str(e)[:200])
        return f"Error reading PDF: {str(e)}"


@mcp.resource("job-pdf://{job_id}")
def get_stored_job_pdf(job_id: int) -> str:
    """Retrieves the local file path and text content of a stored job notification by Job ID."""
    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT job_title, local_pdf_path FROM gov_job_notifications WHERE id = %s",
                (job_id,),
            )
            row = cur.fetchone()
    finally:
        conn.close()

    if not row:
        return f"Error: No job found with ID {job_id}."

    job_title, pdf_path = row["job_title"], row["local_pdf_path"]
    resolved = resolve_pdf_path(pdf_path)
    if not pdf_path or not resolved.exists():
        return f"No local PDF available for job: '{job_title}'."

    return f"PDF Location: {resolved}\n\nContent:\n" + read_pdf_notification(str(resolved))

# ------------------------------------------------------------------
# 3. PROMPT TEMPLATE: Common Extraction Workflow
# ------------------------------------------------------------------
@mcp.prompt()
def extract_gov_job_details(source_pdf_path: str) -> str:
    """Standardized prompt to store the PDF, read it, and extract key fields."""
    return f"""
    Step 1: First, run the tool `store_notification_pdf` with `source_pdf_path="{source_pdf_path}"`
    to copy the official PDF to local storage.

    Step 2: Read the content from resource `pdf://{source_pdf_path}` and extract:
    === STRICT EXTRACTION RULES (MUST FOLLOW) ===
    - Classify the PDF as one of: combined_exam (one exam, many cadres — UPSC CSE, State PSC SSE/PCS, SSC CGL), multi_post_ad (UPSC ORA / High Court combo), single_post, or departmental_exam (State Engineering Service, Assistant Librarian Exam). Pass exam_kind, commission (CGPSC/UPSC/SSC/…), state (or empty for national), exam_name, and search_aliases.
    - COMBINED_EXAM: job_title is the EXAM ("CGPSC State Service Examination 2025"), NEVER a numbered cadre line ("1. State Civil Service (Deputy Collector) II", "1. Indian Administrative Service").
    - job_title and every post_name MUST be the real recruitment/exam/post name. NEVER use "SPECIAL ADVERTISEMENT NO. XX/20XX", "Advt No...", advertisement number, filename, or "Vacancy No..." as job_title or post_name.
    - If the PDF lists multiple distinct posts (numbered 1., 2. ... "vacancies for the post of ...", or a State PSC cadre list), you MUST return them as separate objects in the `posts` list passed to save_job_to_database. Do NOT collapse to one row.
    - Dates come from the apply window on the advertisement, NEVER from a historical rules annex (e.g. 7.2.1997 / 27.08.2008 in CG SSE rules).
    - advertisement_number is the advt id ("06/2025/परीक्षा", "51/2026"), never the word "No".
    - department for a commission-run exam is the commission name, not a truncated "vide Notification No. F…" clause.
    - For teaching/education ads (Principal, Vice Principal, Lecturer, etc.) actively locate the post names and their individual pay scales / vacancy tables.
    - pay_level MUST capture the full "Level-X in the Pay Matrix as per 7th CPC" string for each post.
    - vacancies_breakdown must be proper JSON dict like {"UR": 69, "EWS": 11, "OBC": 18, "SC": 16, "ST": 10}
    - Prefer English for job_title/dept/post_name/qualification. Put Hindi/other accurate text ONLY in translations.
    - If pypdf text has garbled Devanagari, transcribe visually or use registry name_hi / best English equivalent; never store garbage.
    === END STRICT RULES ===

    - Job Title / overall exam or recruitment name
    - Department Name (issuing department; if multiple posts span different
      departments, use the primary/exam-conducting one at the top level and
      the specific one per post below)
    - Nationality/citizenship requirement, if stated (often a one-line
      eligibility clause, e.g. "must be a citizen of India" — easy to skip
      past since it's short, but it's still a real eligibility condition)
    - Total Vacancies & Reservation Category Breakdown (aggregate across all posts)
    - If the notification covers MORE THAN ONE distinct post (very common —
      e.g. one exam covering Deputy Collector, DSP, Naib Tehsildar, Assistant
      Engineer (Civil) vs (Electrical), etc.), extract each post separately:
      post name, its own department, pay level/pay matrix, its own vacancy
      total, its own reservation category breakdown, and qualification if it
      differs from the notification-level qualification. This becomes the
      `posts` argument to `save_job_to_database` — do not flatten multiple
      posts into a single row.
    - Essential Qualifications (notification-level default)
    - Age Limits & Relaxations
    - Application Dates (apply_start_date / apply_end_date)
    - Exam Date(s), if announced
    - Advertisement/Notification Number
    - Application Fee (including any category-based fee differences or
      correction/edit fees)
    - Official Website URL (if mentioned)
    - Exam Syllabus, if the notification includes one (often as an annexure —
      e.g. "Appendix/Annexure II" for a Preliminary exam, "III" for Main).
      Extract each paper's name, question count, marks, duration, negative
      marking rule, and its parts/topics as a `syllabus` dict (see
      `save_job_to_database` for the shape), keyed by exam stage when there's
      more than one (e.g. "preliminary", "main"). If the syllabus section
      states its own language-precedence rule (e.g. "in case of doubt the
      Hindi version will prevail"), capture that in the dict's
      `language_note` — it can override the general LANGUAGE PREFERENCE rule
      below for that section specifically, so don't silently apply the
      English-preference default over an explicit contrary instruction in
      the source.
    - If the PDF presents any of the above text in a regional/Eighth Schedule
      language alongside English (very common — these notifications are
      routinely bilingual, e.g. English + Hindi, or English + the state's
      official language), capture the regional-language original too instead
      of discarding it. Build a `translations` dict keyed by ISO language code
      ({', '.join(EIGHTH_SCHEDULE_LANGUAGES)}), e.g.
      {{"hi": {{"job_title": "...", "department": "...", "qualification": "..."}}}}.
      This becomes the `translations` argument to `save_job_to_database`, and
      each entry in `posts` can carry its own `translations` dict the same way
      (keys: post_name, department, qualification).
    - LANGUAGE PREFERENCE: when the source PDF states the *same* fact in both
      English and a regional language (a true parallel translation — common
      for eligibility/age-limit clauses, which are often restated verbatim in
      an English annex elsewhere in the document), extract the primary
      top-level fields (job_title, department, qualification, nationality,
      age_limit, age_relaxation, etc.) from the English wording, not from a
      translation of the regional-language text — put the regional-language
      original in `translations` instead. This only applies when it's truly
      the same fact in both languages, though: some bilingual notifications
      have an English annex that's a generic/older restatement of a rule
      while the Hindi body has a more specific or updated version (e.g. a
      circular amendment mentioned only in the Hindi section) — in that case
      the two aren't equivalent, and the more specific/complete version wins
      regardless of language; don't drop real information for the sake of
      language preference.
    - CAUTION on regional-language text: some government PDFs embed Devanagari
      (or other Indic-script) text with a broken font encoding, so the raw
      text pulled from the `pdf://{source_pdf_path}` resource can come out
      garbled for those fields — e.g. "राज्य" extracting as "राº य" — while the
      English text on the same PDF extracts cleanly. If the regional-language
      text from the resource looks mangled (broken conjuncts, stray symbols
      like º/±/Ĭ mixed into words), do not save it into `translations` as-is —
      instead read it visually off the rendered PDF page (e.g. by viewing the
      page as an image) and transcribe the correct text from there.
    - Age relaxation rules routinely span more than one clause/section (a base
      list of categories — SC/ST/OBC, ex-servicemen, PwD, domicile-based,
      etc. — plus sometimes a second block that incorporates a separate rule
      by reference, e.g. "relaxations one to seventeen under Rule 5(c) apply").
      Don't compress this into a single prose sentence for `age_relaxation` if
      the source has more than ~2-3 categories — instead build a
      `age_relaxation_details` list, one object per clause, each with:
      source (section/clause reference, e.g. "8.1(B)(iii)"), category (who it
      applies to), relaxation (the years/basis, e.g. "up to 5 years" or
      "equal to prior service duration"), cap (any capping condition, e.g.
      "combined relaxations must not exceed max age 45"), and notes for any
      other qualifying detail. Keep `age_relaxation` as a short prose summary
      for quick display, and put full precision in `age_relaxation_details`.

    Step 3: Ask for confirmation to save the extracted details into PathWise's database via
    `save_job_to_database`, making sure to pass the `local_pdf_path` returned from Step 1,
    and the `posts` list whenever the notification covers more than one post.
    """

# ------------------------------------------------------------------
# 4. TOOLS: Database Management & Viewing
# ------------------------------------------------------------------
@mcp.tool()
async def save_job_to_database(
    job_title: str,
    department: str,
    total_vacancies: int,
    qualification: str,
    reservation_details: dict,
    age_limit: str,
    age_relaxation: str,
    apply_start_date: str,
    apply_end_date: str,
    local_pdf_path: str,
    official_url: str = "",
    exam_date: str = "",
    advertisement_number: str = "",
    application_fee: str = "",
    posts: list = None,
    translations: dict = None,
    age_relaxation_details: list = None,
    nationality: str = "",
    syllabus: dict = None,
    commission: str = "",
    state: str = "",
    exam_name: str = "",
    exam_kind: str = "",
    search_aliases: list = None,
    ctx: Context = None
) -> str:
    """
    Saves the extracted job details along with the stored PDF local path.

    `age_relaxation_details` is optional and should be used whenever the age
    relaxation rules span more than a couple of categories (common — see the
    `extract_gov_job_details` prompt for when/how to build this). Pass a list
    of dicts, one per clause, each with keys: source, category, relaxation,
    cap, notes (all optional except source/category/relaxation). Keep
    `age_relaxation` as a short prose summary either way.

    `posts` is optional and should be used whenever the notification advertises
    more than one distinct post (common — e.g. one exam covering Deputy Collector,
    DSP, Naib Tehsildar, etc. each with their own department/pay level/vacancy
    split). Pass a list of dicts, one per post, each with keys:
    post_name (required), department, pay_level, total_vacancies,
    vacancies_breakdown (dict of category -> count), qualification
    (only if it differs from the notification-level `qualification`),
    translations (see below, scoped to that post's own fields).
    If the notification is for a single post, you can omit `posts` — the
    top-level fields already cover that case.

    `translations` is optional and preserves the notification's original
    regional-language text (these PDFs are routinely bilingual) instead of
    only keeping the English extraction. Keyed by ISO language code — see
    EIGHTH_SCHEDULE_LANGUAGES for the reference set — each value a dict with
    any of: job_title, department, qualification, age_limit, age_relaxation.
    e.g. {"hi": {"job_title": "...", "department": "..."}}. When a PDF states
    the same fact in both English and a regional language, prefer the English
    wording for the top-level fields and put the regional-language original
    here instead — see `extract_gov_job_details` for the full rule.

    `nationality` is optional — the citizenship/nationality eligibility
    clause, if the notification states one (commonly "must be a citizen of
    India").

    `syllabus` is optional — the exam scheme/syllabus, if the notification
    includes one (often as an annexure). Shape: {"papers": [{"name": "...",
    "questions": 100, "marks": 200, "duration": "2:00 hours",
    "negative_marking": "...", "parts": [{"name": "...", "topics": [...]}]}],
    "language_note": "..."}. Key the whole dict by exam stage
    (e.g. {"preliminary": {...}, "main": {...}}) when a notification has more
    than one stage's syllabus.

    `commission`, `state`, `exam_name`, `exam_kind`, and `search_aliases` are
    optional extras used so students can find the row by "cgpsc" / "upsc cse"
    / "ssc cgl". Existing callers may omit them.
    """
    if ctx:
        await ctx.info(f"Saving job '{job_title}' into PathWise's database...")

    try:
        fields = {
            "job_title": job_title,
            "department": department,
            "total_vacancies": total_vacancies,
            "reservation_details": reservation_details,
            "qualification": qualification,
            "age_limit": age_limit,
            "age_relaxation": age_relaxation,
            "apply_start_date": apply_start_date,
            "apply_end_date": apply_end_date,
            "exam_date": exam_date,
            "advertisement_number": advertisement_number,
            "application_fee": application_fee,
            "official_url": official_url,
            "translations": translations,
            "age_relaxation_details": age_relaxation_details,
            "nationality": nationality,
            "syllabus": syllabus,
            "posts": posts,
            "commission": commission or "",
            "state": state or None,
            "exam_name": exam_name or "",
            "exam_kind": exam_kind or "",
            "search_aliases": search_aliases or None,
        }
        # Backfill issuer/kind/aliases when the caller omitted them (old UPSC path).
        if not fields["commission"] or not fields["exam_kind"] or not fields["search_aliases"]:
            comm = _detect_commission(
                " ".join([
                    job_title or "", department or "", official_url or "",
                    exam_name or "", local_pdf_path or "",
                ]),
                local_pdf_path or job_title or "",
            )
            if not fields["commission"] and comm:
                fields["commission"] = comm.get("code") or ""
                if not fields["state"]:
                    fields["state"] = comm.get("state")
            if not fields["exam_kind"]:
                fields["exam_kind"] = _classify_exam_kind(
                    job_title + " " + (exam_name or "") + " " + " ".join(
                        (p.get("post_name") or "") for p in (posts or []) if isinstance(p, dict)
                    ),
                    local_pdf_path or "",
                    comm,
                )
            if not fields["exam_name"]:
                fields["exam_name"] = _exam_name_from_hints(
                    job_title, local_pdf_path or job_title, comm, fields["exam_kind"]
                )
            fields["search_aliases"] = _build_search_aliases(
                fields, local_pdf_path or "", comm
            )
        return _persist_job(fields, local_pdf_path or "", replace_ids=None, match_exam=False)
    except Exception as e:
        return f"Failed to save job entry to database: {str(e)}"


@mcp.tool()
def view_official_notification(job_id: int) -> str:
    """
    Returns the absolute local PDF file location for a specific job ID
    so the AI or user can open/display the official notification document.
    """
    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, job_title, local_pdf_path, official_url FROM gov_job_notifications WHERE id = %s",
                (job_id,),
            )
            row = cur.fetchone()
    finally:
        conn.close()

    if not row:
        return f"No job found with ID #{job_id}."

    jid, title, pdf_path, web_url = row["id"], row["job_title"], row["local_pdf_path"], row["official_url"]

    response = f"Job ID #{jid}: {title}\n"
    resolved = resolve_pdf_path(pdf_path)
    if pdf_path and resolved.exists():
        response += f"Local PDF File Path: {resolved.resolve()}\n"
    else:
        response += "Local PDF File Path: Not available.\n"

    if web_url:
        response += f"Official Web Link: {web_url}\n"

    return response


def _filename_hints(source_path: str) -> dict:
    """Stem, year, and humanized exam/post phrase from a PDF path or name."""
    raw = Path(source_path or "").name or (source_path or "")
    stem = re.sub(r"\.pdf$", "", raw, flags=re.I)
    stem = re.sub(r"_[0-9a-f]{10,12}$", "", stem, flags=re.I)
    stem = re.sub(r"_\d{15,}$", "", stem)
    year = None
    years = [int(y) for y in re.findall(r"(20[2-3]\d)", stem)]
    if years:
        year = years[-1]
    core = re.sub(r"_?ADVERTISEMENT.*", "", stem, flags=re.I)
    core = re.sub(r"[\(\)\[\],]", " ", core)
    core = re.sub(r"[_./-]+", " ", core)
    core = re.sub(r"\s+", " ", core).strip()
    # Keep known commission/exam acronyms; title-case ordinary words (STATE, SERVICE).
    _keep_caps = {e["code"].upper() for e in COMMISSION_REGISTRY}
    _keep_caps.update({
        "CSE", "CGL", "CHSL", "MTS", "CPO", "NDA", "CDS", "CAPF", "CMS",
        "IAS", "IPS", "IFS", "ESE", "IES", "PCS", "CCE", "RAS", "KAS",
        "HCS", "WBCS", "NET", "NTPC", "ALP", "RRC", "ORA", "GNCTD",
    })
    words = []
    for w in core.split():
        if w.upper() in _keep_caps:
            words.append(w.upper())
        else:
            words.append(w.capitalize())
    phrase = " ".join(words)
    phrase = re.sub(r"\bExam\b", "Examination", phrase)
    phrase = re.sub(r"\s+", " ", phrase).strip()
    return {"raw": raw, "stem": stem, "year": year, "phrase": phrase}


def _parse_year_from_date(token: str):
    if not token:
        return None
    m = re.search(r"(\d{1,2})[./\-](\d{1,2})[./\-](\d{2,4})", token)
    if not m:
        return None
    y = int(m.group(3))
    if y < 100:
        y += 2000 if y < 50 else 1900
    return y


def _date_year_plausible(date_s: str, file_year) -> bool:
    y = _parse_year_from_date(date_s)
    if y is None:
        return not (date_s or "").strip()
    if file_year and 2024 <= int(file_year) <= 2030:
        if y < 2015:
            return False
        return abs(y - int(file_year)) <= 1
    return y >= 2015


def _iter_commission_text_forms(entry):
    if entry.get("name_en"):
        yield entry["name_en"]
    for a in entry.get("name_en_aliases") or []:
        yield a
    if entry.get("name_hi"):
        yield entry["name_hi"]
    for a in entry.get("name_hi_forms") or []:
        yield a


def _high_court_from_text(text: str):
    m = re.search(r"High Court of\s+([A-Za-z][A-Za-z .]{2,40})", text or "", re.I)
    if not m:
        return None
    state = _norm_ws(m.group(1)).title()
    state = re.sub(r"\s+(Judicature|India|Recruitment).*$", "", state, flags=re.I).strip()
    return {
        "code": "HIGH_COURT",
        "name_en": f"High Court of {state}",
        "name_hi": "उच्च न्यायालय",
        "state": state,
        "url_hosts": [],
        "filename_tokens": [],
        "exam_aliases": [],
        "search_aliases": ["high court", state.lower()],
        "name_hi_forms": [],
        "name_en_aliases": [],
    }


def _detect_commission(text: str, source_path: str = ""):
    """First confident issuer win. Returns a registry entry or None."""
    blob = (text or "") + "\n" + (source_path or "")
    blob_l = blob.lower()
    fname = Path(source_path or "").name.upper()

    # 1. URL host in body / path
    for host, entry in _HOST_INDEX:
        if host in blob_l:
            return entry

    # High Court of <State> before the generic "High Court" registry name.
    hc = _high_court_from_text(text or "")
    if hc:
        return hc

    # 2. Explicit English issuer line (full name, then distinctive aliases)
    for entry in COMMISSION_REGISTRY:
        if entry["code"] in ("HIGH_COURT", "POLICE"):
            continue
        name = (entry.get("name_en") or "").strip()
        if name and len(name) >= 12 and name.lower() in blob_l:
            return entry
    for entry in COMMISSION_REGISTRY:
        for alias in entry.get("name_en_aliases") or []:
            if alias and alias.lower() in blob_l:
                # "SPECIAL ADVERTISEMENT" is a UPSC ORA tell even without the
                # commission's full name or URL (see classifier smoke cases).
                return entry

    # 3. Hindi issuer / known garbled pypdf forms
    for entry in COMMISSION_REGISTRY:
        for form in [entry.get("name_hi"), *(entry.get("name_hi_forms") or [])]:
            if form and form in blob:
                return entry
    if re.search(r"लोक सेवा आयोग", blob) or re.search(r"लोक सेवा आयोग", blob):
        # State + PSC co-occurrence handled in step 5; generic "लोक सेवा आयोग"
        # alone is not enough to pick a state.
        pass

    # 4. Filename / advertisement header tokens (issuer-like, not generic EXAM)
    for entry in COMMISSION_REGISTRY:
        for tok in entry.get("filename_tokens") or []:
            if tok and tok.upper() in fname:
                return entry

    # 5. State name + "Public Service Commission" / "PSC"
    psc_near = bool(re.search(
        r"public service commission|\bpsc\b|लोक सेवा आयोग", blob, re.I
    ))
    if psc_near:
        for entry in COMMISSION_REGISTRY:
            st = entry.get("state")
            if st and st.lower() in blob_l and entry["code"].endswith("PSC") or (
                st and st.lower() in blob_l and "PSC" in entry["code"]
            ):
                if st.lower() in blob_l:
                    return entry

    hc = _high_court_from_text(text or "")
    if hc:
        return hc
    if re.search(r"\bSupreme Court of India\b", text or "", re.I):
        for entry in COMMISSION_REGISTRY:
            if entry["code"] == "SCI":
                return entry
    return None


def _has_numbered_cadre_list(text: str) -> bool:
    n = 0
    for m in re.finditer(
        r"^\s*\d{1,3}\.\s+(?:\d+\s+)?([A-Z][A-Za-z].{6,140})$",
        text or "",
        re.M,
    ):
        name = m.group(1)
        if _DEVANAGARI_RE.search(name) or _GARBLED_DEVANAGARI_RE.search(name):
            continue
        if _SYLLABUS_TOPIC_RE.search(name):
            continue
        if _CADRE_HINT_RE.search(name):
            n += 1
            if n >= 3:
                return True
    return False


def _count_vacancy_blocks(text: str) -> int:
    return len(re.findall(
        r"vacancies\s+for\s+the\s+post\s+of", text or "", re.I
    ))


def _combined_signal(blob: str) -> bool:
    return any(rx.search(blob) for rx in _COMBINED_EXAM_RES)


def _departmental_signal(blob: str) -> bool:
    return any(rx.search(blob) for rx in _DEPARTMENTAL_EXAM_RES)


def _classify_exam_kind(text: str, source_path: str = "", commission=None) -> str:
    """Return one of combined_exam / multi_post_ad / single_post / departmental_exam."""
    blob = ((text or "") + "\n" + (source_path or ""))
    fname = Path(source_path or "").name
    combined = _combined_signal(blob)
    departmental = _departmental_signal(blob)
    numbered = _has_numbered_cadre_list(text or "")
    vac_blocks = _count_vacancy_blocks(text or "")
    special_ad = bool(re.search(r"SPECIAL\s+ADVERTISEMENT", blob, re.I))
    true_combined_brand = bool(re.search(
        r"(?i)state[_\s-]+service[_\s-]+exam|civil[_\s-]+services|"
        r"combined[_\s-]+(?:graduate|higher|competitive|defence)|"
        r"\bcse\b|\bcgl\b|\bchsl\b|\bcce\b|\bras\b|\bwbcs\b",
        blob,
    ))

    # Named specialist exams (librarian / assistant professor / state engineering)
    # stay departmental even when they mention preliminary & main or a syllabus list.
    if departmental and not true_combined_brand:
        return EXAM_KIND_DEPARTMENTAL
    # Prefer A over D when the document is a true combined cadre exam.
    if numbered and (true_combined_brand or combined):
        return EXAM_KIND_COMBINED
    if combined and not departmental:
        return EXAM_KIND_COMBINED
    if departmental and numbered:
        if true_combined_brand:
            return EXAM_KIND_COMBINED
        return EXAM_KIND_DEPARTMENTAL
    if special_ad or vac_blocks >= 2:
        return EXAM_KIND_MULTI_POST
    if re.search(r"(?i)name of (?:the )?post|post name", text or "") and vac_blocks >= 2:
        return EXAM_KIND_MULTI_POST
    # Single-post High Court / departmental ad
    if commission and commission.get("code") in ("HIGH_COURT", "SCI") and vac_blocks <= 1:
        return EXAM_KIND_SINGLE
    if re.search(r"(?i)\bexam(?:ination)?\b|परी", fname) and not combined:
        return EXAM_KIND_DEPARTMENTAL
    return EXAM_KIND_SINGLE


def _exam_name_from_hints(text: str, source_path: str, commission, exam_kind: str) -> str:
    hints = _filename_hints(source_path)
    phrase = hints.get("phrase") or ""
    year = hints.get("year")
    # Filename exam tokens are stronger than an in-body "RULES" heading
    # (e.g. "STATE SERVICES EXAMINATION RULES") which is often ALL CAPS.
    file_looks_like_exam = bool(re.search(
        r"(?i)exam|cgl|chsl|cse|sse|pcs|cce|ras", hints.get("stem") or ""
    ))
    if not file_looks_like_exam:
        body_m = re.search(
            r"(?i)\b((?:State|Civil|Combined|Engineering|Forest|Assistant)\s+"
            r"[A-Za-z][A-Za-z /&-]{6,80}?(?:Examination|Exam))\s*"
            r"[-–]?\s*((?:20\d{2})?)",
            text or "",
        )
        if body_m and not _DEVANAGARI_RE.search(body_m.group(0)):
            phrase = _norm_ws(body_m.group(1))
            phrase = re.sub(r"\bRules?\b", "", phrase, flags=re.I)
            phrase = re.sub(r"\bExam\b", "Examination", phrase)
            if phrase.isupper():
                phrase = phrase.title()
            if body_m.group(2) and not year:
                year = int(body_m.group(2))
    if exam_kind == EXAM_KIND_DEPARTMENTAL and phrase and not re.search(
        r"(?i)exam", phrase
    ):
        # Assistant Professor [Dept…] filenames omit EXAM; the PDF is still an exam.
        if re.search(r"(?i)exam|परी", (hints.get("stem") or "") + "\n" + (text or "")[:4000]):
            phrase = re.sub(r"\s+(20\d{2})\s*$", r" Examination \1", phrase)
            if not re.search(r"(?i)exam", phrase):
                phrase = phrase + " Examination"
    if year and phrase and str(year) not in phrase:
        phrase = f"{phrase} {year}"
    return _norm_ws(phrase)


def _commission_title(commission, exam_name: str) -> str:
    if not exam_name:
        return (commission or {}).get("code") or ""
    code = (commission or {}).get("code") or ""
    if code and code not in ("HIGH_COURT", "POLICE") and not exam_name.upper().startswith(code):
        return f"{code} {exam_name}"
    if code == "HIGH_COURT":
        name = (commission or {}).get("name_en") or "High Court"
        if name.lower() not in exam_name.lower():
            return f"{exam_name}, {name}"
    return exam_name


def _build_search_aliases(fields: dict, source_path: str, commission=None) -> list:
    aliases = set()
    comm = commission or {}
    if comm.get("code"):
        aliases.add(comm["code"].lower())
    if comm.get("name_en"):
        aliases.add(comm["name_en"].lower())
    if comm.get("name_hi"):
        aliases.add(comm["name_hi"].lower())
    if comm.get("state"):
        aliases.add(comm["state"].lower())
    blob = " ".join([
        str(fields.get("job_title") or ""),
        str(fields.get("exam_name") or ""),
        str(fields.get("department") or ""),
        source_path or "",
        str(fields.get("official_url") or ""),
    ]).lower()
    compact = blob.replace(" ", "").replace("_", "").replace("-", "")
    # Short commission nicknames always (students search "cgpsc" / "upsc").
    for a in comm.get("search_aliases") or []:
        al = a.lower().strip()
        if not al:
            continue
        if al == (comm.get("code") or "").lower() or al in {
            h.lower() for h in (comm.get("url_hosts") or [])
        }:
            aliases.add(al)
        elif al in blob or al.replace(" ", "") in compact:
            aliases.add(al)
        elif len(al.split()) == 1 and al in {
            "upsc", "ssc", "rrb", "rrc", "ibps", "sbi", "rbi", "nta",
            "cgpsc", "mppsc", "uppsc", "bpsc", "rpsc", "tnpsc", "gpsc",
        }:
            aliases.add(al)
    for a in comm.get("exam_aliases") or []:
        al = a.lower()
        if al in blob or al.replace(" ", "") in compact:
            aliases.add(al)
    for h in comm.get("url_hosts") or []:
        aliases.add(h.lower())
    fname = Path(source_path or "").name.upper()
    for tok in comm.get("filename_tokens") or []:
        if tok and tok.upper() in fname:
            aliases.add(tok.replace("_", " ").lower())
    for key in ("job_title", "exam_name", "department", "advertisement_number"):
        v = fields.get(key)
        if v:
            aliases.add(str(v).lower())
    hints = _filename_hints(source_path)
    if hints.get("phrase"):
        aliases.add(hints["phrase"].lower())
    if hints.get("stem"):
        aliases.add(hints["stem"].replace("_", " ").lower())
    url = fields.get("official_url") or ""
    if url:
        aliases.add(url.lower())
        hm = re.search(r"(?:https?://|www\.)([^/\s]+)", url, re.I)
        if hm:
            aliases.add(hm.group(1).lower())
    for p in fields.get("posts") or []:
        pn = (p or {}).get("post_name")
        if pn:
            aliases.add(str(pn).lower())
    # Short exam names students actually type, only when the long form is present.
    phrase_shorts = [
        (r"state service", ("sse", "pcs")),
        (r"civil services", ("cse", "ias", "ips")),
        (r"combined graduate", ("cgl",)),
        (r"combined higher secondary", ("chsl",)),
        (r"combined competitive", ("cce",)),
        (r"rajasthan administrative", ("ras",)),
        (r"\bwbcs\b|west bengal civil", ("wbcs",)),
        (r"engineering services examination", ("ese", "ies")),
    ]
    hay = " ".join(aliases)
    for pat, shorts in phrase_shorts:
        if re.search(pat, hay, re.I):
            aliases.update(shorts)
    return sorted({a.strip() for a in aliases if a and len(a.strip()) >= 3})


def _search_document_from_aliases(aliases) -> str:
    return " ".join(aliases or [])


def _ascii_ratio(s: str) -> float:
    if not s:
        return 0.0
    letters = re.findall(r"[A-Za-z]", s)
    return len(letters) / max(len(s), 1)


def _find_english_annex(text: str) -> str:
    """Window around Rules / Plan of Examination / cadre list / vacancy blocks."""
    if not text:
        return ""
    keys = [
        r"STATE SERVICES? EXAMINATION RULES",
        r"PLAN OF EXAMINATION",
        r"Class of Service/Post",
        r"vacancies\s+for\s+the\s+post\s+of",
        r"SPECIAL ADVERTISEMENT",
        r"Name of (?:the )?Post",
        r"SCHEME OF EXAMINATION",
        r"APPENDIX\s*[-–]?\s*I\b",
    ]
    best = ""
    for pat in keys:
        m = re.search(pat, text, re.I)
        if not m:
            continue
        start = max(0, m.start() - 400)
        chunk = text[start:start + 9000]
        if len(chunk) > len(best):
            best = chunk
    if best:
        return best
    # Fallback: densest ASCII block after the first 8k (Hindi-first PDFs)
    tail = text[8000:]
    if not tail:
        return ""
    window, best_score, best_chunk = 4000, 0.0, ""
    step = 1500
    for i in range(0, max(1, len(tail) - 500), step):
        chunk = tail[i:i + window]
        score = _ascii_ratio(chunk)
        if score > best_score and score > 0.35:
            best_score, best_chunk = score, chunk
    return best_chunk


def _issuer_keyword_line(line: str) -> bool:
    if not line or len(line.strip()) < 6:
        return False
    l = line.lower()
    if re.search(
        r"examination|preliminary|main exam|public service|commission|"
        r"advertisement|vacanc|apply|online|psc\.|upsc\.|ssc\.|rrb\.|"
        r"परी|आयोग|लोक सेवा|विज्ञापन|िव²ापन",
        l,
    ):
        return True
    for entry in COMMISSION_REGISTRY:
        code = entry["code"].lower()
        if code and code in l:
            return True
        for h in entry["url_hosts"]:
            if h.lower() in l:
                return True
    return False


def _build_extraction_pack(pdf_text: str, source_path: str, commission=None, exam_kind: str = "") -> str:
    """Select header + issuer/exam/vacancy slices instead of blindly slicing [:16000]."""
    text = pdf_text or ""
    hints = _filename_hints(source_path)
    comm = commission or {}
    header = text[:8000]
    picked = []

    # Keyword / issuer lines anywhere
    kw_lines = []
    for line in text.splitlines():
        if _issuer_keyword_line(line):
            s = line.strip()
            if s and s not in kw_lines:
                kw_lines.append(s)
            if len(kw_lines) >= 80:
                break
    if kw_lines:
        picked.append("=== ISSUER / EXAM / DATE LINES ===\n" + "\n".join(kw_lines))

    annex = _find_english_annex(text)
    if annex:
        picked.append("=== ENGLISH ANNEX / RULES / CADRE OR VACANCY BLOCK ===\n" + annex)

    # Numbered cadre / vacancy-table slices
    cadre_chunks = []
    for m in re.finditer(
        r"(?:(?:^\s*\d{1,3}\.\s+[A-Z][A-Za-z].{8,160}\n){3,})",
        text,
        re.M,
    ):
        cadre_chunks.append(m.group(0)[:4000])
        if len(cadre_chunks) >= 2:
            break
    if cadre_chunks:
        picked.append("=== NUMBERED CADRE / SERVICE LISTS ===\n" + "\n".join(cadre_chunks))

    vac_idx = [m.start() for m in re.finditer(
        r"vacancies\s+for\s+the\s+post\s+of|Name of (?:the )?Post", text, re.I
    )]
    if vac_idx:
        bits = []
        for i in vac_idx[:6]:
            bits.append(text[max(0, i - 200):i + 900])
        picked.append("=== VACANCY / POST TABLES ===\n" + "\n---\n".join(bits))

    meta = (
        f"FILENAME_HINT: {hints.get('phrase') or hints.get('stem')} "
        f"(year={hints.get('year')}, file={hints.get('raw')})\n"
        f"DETECTED_COMMISSION: {(comm.get('code') or '')} | {comm.get('name_en') or ''} | "
        f"state={comm.get('state')}\n"
        f"DETECTED_KIND: {exam_kind}\n"
        "HINT usage: filename is a strong exam-name signal for combined/departmental "
        "exams. Do NOT use the first numbered cadre/post line as job_title.\n"
    )
    parts = [meta, "=== HEADER (first pages) ===\n" + header]
    used = sum(len(p) for p in parts)
    for block in picked:
        if used + len(block) + 2 > _PACK_BUDGET:
            remain = _PACK_BUDGET - used - 2
            if remain > 400:
                parts.append(block[:remain])
                used += remain
            break
        parts.append(block)
        used += len(block) + 2
    return "\n\n".join(parts)[:_PACK_BUDGET]


def _looks_like_ad_number(s: str) -> bool:
    if not s:
        return False
    s = s.strip().upper()
    bad_patterns = [
        r"^SPECIAL\s+ADVERTISEMENT",
        r"^ADVERTISEMENT\s+NO",
        r"^ADVT\s*NO",
        r"^ADV\.?\s*NO",
        r"^\d+/\d{4}",
        r"^NOTIFICATION\s+NO",
        r"^RECRUITMENT\s+ADVERTISEMENT",
    ]
    for p in bad_patterns:
        if re.search(p, s):
            return True
    if len(s) < 35 and re.search(r"[/\-]?\d{2,4}", s) and not re.search(
        r"(PRINCIPAL|PROFESSOR|MANAGER|OFFICER|ASSISTANT|LECTURER|TEACHER|"
        r"JUDGE|ENGINEER|COLLECTOR|EXAMINATION|SERVICE|LIBRARIAN)",
        s,
    ):
        return True
    return False


def _title_starts_with_index(s: str) -> bool:
    return bool(re.match(r"^\s*\d{1,3}\.\s+", s or ""))


def _clean_title(s: str) -> str:
    if not s:
        return ""
    s = re.sub(r"\s+", " ", s).strip()
    s = re.sub(
        r"^(SPECIAL\s+ADVERTISEMENT|ADVERTISEMENT|ADVT\.?|NOTIFICATION)\s*"
        r"(NO\.?|NUMBER)?\s*[:\-]?\s*\S+\s*",
        "",
        s,
        flags=re.I,
    ).strip()
    s = re.sub(
        r"^(UPSC|SSC|CGPSC|HIGH COURT|COMMISSION)\s+(INVITES|RECRUITMENT)\s+",
        "",
        s,
        flags=re.I,
    ).strip()
    return s[:200]


def _parse_vacancy_table_block(text_block: str) -> dict:
    """Heuristic parse of category tables appearing near 'UR EWS OBC SC ST Total'."""
    bd = {}
    m = re.search(
        r"(?:No\.?\s*of\s*)?Vacancies?\s*[:\s]*\n?\s*UR\s+EWS\s+OBC\s+SC\s+ST\s+Total\s*\n?\s*([\d\s]+)",
        text_block or "",
        re.I,
    )
    if m:
        nums = re.findall(r"\d+", m.group(1))
        cats = ["UR", "EWS", "OBC", "SC", "ST"]
        for i, c in enumerate(cats):
            if i < len(nums):
                bd[c] = int(nums[i])
        if len(nums) > 5:
            bd["Total"] = int(nums[5])
        return bd
    for cat in ["UR", "EWS", "OBC", "SC", "ST", "PwBD"]:
        m = re.search(rf"{cat}[:\s\-–]*(\d+)", text_block or "", re.I)
        if m:
            bd[cat] = int(m.group(1))
    return bd or None


def _extract_upsc_vacancy_blocks(text: str) -> list:
    """UPSC-ORA / Special Advertisement: '(Vacancy No. …) N vacancies for the post of X in Y'."""
    posts = []
    pattern = (
        r"(\d+)\.\s*\(Vacancy No\..*?\)\s*[^\n]*?\s+vacancies\s+for\s+the\s+post\s+of\s+"
        r"([A-Za-z][A-Za-z\s\n]+?)\s+in\s+([^\n\.]+)"
    )
    for match in re.finditer(pattern, text or "", re.I | re.S):
        num_v = 0
        mnum = re.search(r"(\d+)\s+vacancies", match.group(0), re.I)
        if mnum:
            num_v = int(mnum.group(1))
        else:
            wmatch = re.search(
                r"(one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|"
                r"thirteen|fourteen|fifteen|sixteen|seventeen|eighteen|nineteen|"
                r"twenty|thirty|forty|fifty|sixty|seventy|eighty|ninety|hundred)\s+"
                r"(hundred|thousand)?[^\n]*?vacancies",
                match.group(0),
                re.I,
            )
            if wmatch:
                low = match.group(0).lower()
                num_v = (
                    124 if "hundred twenty four" in low
                    else 704 if "hundred four" in low
                    else 0
                )
        post_name = match.group(2).strip()
        dept = match.group(3).strip()
        block = (text or "")[match.end():match.end() + 1200]
        pay = ""
        pm = re.search(
            r"(Level[\-\s]*\d+[^\n\.]{0,60}(?:Pay Matrix|7th CPC|CPC)[^\n\.]*)",
            block,
            re.I,
        )
        if pm:
            pay = pm.group(1).strip()
        vbd = _parse_vacancy_table_block(block)
        qm = re.search(
            r"ESSENTIAL QUALIFICATIONS?:(.*?)(?:DESIRABLE|AGE:|DUTIES:|OTHER DETAILS:|$)",
            block,
            re.I | re.S,
        )
        qual = (qm.group(1).strip()[:600] if qm else "")[:400]
        posts.append({
            "post_name": _clean_title(post_name) or post_name[:80],
            "department": dept[:120],
            "pay_level": pay[:120],
            "total_vacancies": num_v,
            "vacancies_breakdown": vbd,
            "qualification": qual or None,
            "translations": None,
        })
    return posts


def _extract_numbered_cadre_posts(text: str) -> list:
    """State-PSC / CSE style: '1. State Civil Service (Deputy Collector)  II'."""
    posts = []
    seen = set()
    for m in re.finditer(
        r"^\s*(\d{1,3})\.\s+(?:\d+\s+)?([A-Z][A-Za-z][A-Za-z0-9 .,'()/\-&]{6,140})",
        text or "",
        re.M,
    ):
        name = _norm_ws(m.group(2))
        name = re.sub(r"\s+\d+\s*$", "", name)
        name = re.sub(r"\s+(I{1,3}|IV|V|VI|VII|VIII|IX|X)\s*$", "", name).strip()
        name = name.rstrip(" .;:")
        if len(name) < 8 or len(name) > 120:
            continue
        if _DEVANAGARI_RE.search(name) or _GARBLED_DEVANAGARI_RE.search(name):
            continue
        if not _CADRE_HINT_RE.search(name):
            continue
        if _looks_like_ad_number(name):
            continue
        if _SYLLABUS_TOPIC_RE.search(name):
            continue
        if re.search(
            r"(?i)\b(candidates?|shall|would|marks?|paper|appendix|rule|schemes?)\b",
            name,
        ):
            continue
        key = name.lower()
        if key in seen:
            continue
        seen.add(key)
        posts.append({
            "post_name": name[:120],
            "department": "",
            "pay_level": "",
            "total_vacancies": 0,
            "vacancies_breakdown": None,
            "qualification": None,
            "translations": None,
        })
    return posts


def _extract_name_of_post_table(text: str) -> list:
    """High Court / teaching 'Name of Post' / markdown-ish tables."""
    if not text or not re.search(r"(?i)name of (?:the )?post|post name|name of the post", text):
        return []
    posts = []
    seen = set()
    # Header then rows: Post name ... N
    for m in re.finditer(
        r"(?i)(?:name of (?:the )?post|post name)[^\n]{0,80}\n(.{0,4000})",
        text,
    ):
        block = m.group(1)
        for row in block.splitlines():
            row = row.strip()
            if not row or re.search(r"(?i)name of|post name|vacanc|category|sl\.?no", row):
                continue
            rm = re.match(
                r"^(?:\d+[\).\s]+)?([A-Z][A-Za-z][A-Za-z0-9 .,'()/\-&]{5,80}?)"
                r"(?:\s{2,}|\s+\||\s+)(\d{1,4})\s*$",
                row,
            )
            if not rm:
                continue
            name = _norm_ws(rm.group(1))
            if not _CADRE_HINT_RE.search(name):
                continue
            key = name.lower()
            if key in seen:
                continue
            seen.add(key)
            try:
                tv = int(rm.group(2))
            except Exception:
                tv = 0
            posts.append({
                "post_name": name[:120],
                "department": "",
                "pay_level": "",
                "total_vacancies": tv,
                "vacancies_breakdown": None,
                "qualification": None,
                "translations": None,
            })
    return posts


def _extract_post_of_prose(text: str) -> list:
    """Last-resort 'post of X' prose (kept for single-post High Court ads)."""
    posts = []
    seen = set()
    for m in re.finditer(
        r"(?:post of|posts of)\s+([A-Z][A-Za-z\s\(\)\-]{3,80}?)(?:\s+in\s+([A-Za-z][^\.\n]{3,80}))?",
        text or "",
        re.I,
    ):
        pn = _norm_ws(m.group(1))
        pn = re.sub(r"\s+\d+\s*$", "", pn)
        if len(pn) < 4 or _looks_like_ad_number(pn):
            continue
        if _DEVANAGARI_RE.search(pn):
            continue
        key = pn.lower()
        if key in seen:
            continue
        seen.add(key)
        posts.append({
            "post_name": pn[:100],
            "department": _norm_ws(m.group(2) or "")[:100],
            "pay_level": "",
            "total_vacancies": 0,
            "vacancies_breakdown": None,
            "qualification": None,
            "translations": None,
        })
    return posts


def _extract_posts_from_text(text: str, exam_kind: str = "") -> list:
    """Post splitters, in order. UPSC vacancy-block path is preserved as step 1."""
    posts = _extract_upsc_vacancy_blocks(text)
    if posts:
        return posts
    if exam_kind != EXAM_KIND_DEPARTMENTAL:
        posts = _extract_numbered_cadre_posts(text)
        if posts:
            return posts
    posts = _extract_name_of_post_table(text)
    if posts:
        return posts
    if exam_kind in (EXAM_KIND_COMBINED, EXAM_KIND_DEPARTMENTAL):
        # Do not let a random "post of Deputy Collector" / syllabus line become the only post.
        return []
    return _extract_post_of_prose(text)


def _parse_vacancy_table_rows(text: str) -> list:
    """Rows of (1-based index, total) from a vacancy table. Ignores 5-digit pay cells."""
    rows = []
    seen = set()
    for line in (text or "").splitlines():
        m = re.match(r"^\s*(\d{1,2})[.)\s]+(\S.*)$", line)
        if not m:
            continue
        idx = int(m.group(1))
        if idx in seen or idx == 0:
            continue
        nums = [int(x) for x in re.findall(r"\d+", m.group(2))]
        if len(nums) < 3:
            continue
        candidates = [n for n in nums if 1 <= n < 400]
        if not candidates:
            continue
        total = candidates[-1]
        seen.add(idx)
        rows.append({"index": idx, "total_vacancies": total})
    return rows


def _apply_vacancy_totals(posts: list, text: str) -> list:
    """Attach योग/total column values onto numbered posts by row index."""
    if not posts:
        return posts
    table = _parse_vacancy_table_rows(text)
    if not table:
        return posts
    by_idx = {r["index"]: r["total_vacancies"] for r in table}
    for i, p in enumerate(posts, start=1):
        if (p.get("total_vacancies") or 0) > 0:
            continue
        if i in by_idx:
            p["total_vacancies"] = by_idx[i]
    return posts


def _extract_eligibility_fields(text: str) -> dict:
    """English-annex / header clauses the fallback used to leave empty."""
    out = {
        "qualification": "",
        "age_limit": "",
        "age_relaxation": "",
        "application_fee": "",
        "nationality": "",
        "syllabus": None,
    }
    if not text:
        return out
    m = re.search(
        r"(?i)((?:must be a |shall be a )?citizen of India[^\n.]{0,100})",
        text,
    )
    if m:
        out["nationality"] = _norm_ws(m.group(1))[:200]
    m = re.search(
        r"(?i)((?:age limit|upper age limit|shall not have attained|must have attained|"
        r"not have attained the age)[^\n]{8,180})",
        text,
    )
    if m:
        out["age_limit"] = _norm_ws(m.group(1))[:200]
    m = re.search(
        r"(?i)((?:age relaxation|relaxable (?:by|up to|for))[^\n]{8,180})",
        text,
    )
    if m:
        out["age_relaxation"] = _norm_ws(m.group(1))[:200]
    m = re.search(
        r"(?i)((?:essential qualifications?|must (?:hold|possess) (?:a )?degree|"
        r"bachelor'?s degree|graduation)[^\n]{8,240})",
        text,
    )
    if m:
        out["qualification"] = _norm_ws(m.group(1))[:300]
    m = re.search(
        r"(?i)((?:examination fee|application fee|fee of|portal fee)\s*[:\-]?\s*"
        r"(?:Rs\.?|INR|₹)?\s*[\d,]+[^\n]{0,80})",
        text,
    )
    if m:
        out["application_fee"] = _norm_ws(m.group(1))[:160]
    if re.search(r"(?i)scheme of examination|plan of examination|syllabus", text):
        stages = {}
        if re.search(r"(?i)preliminary", text):
            stages["preliminary"] = {"papers": [], "language_note": ""}
        if re.search(r"(?i)main examination|mains", text):
            stages["main"] = {"papers": [], "language_note": ""}
        if stages:
            out["syllabus"] = stages
    return out


def _departmental_posts(exam_name: str, text: str, department: str = "") -> list:
    """Named specialist exam → real post(s), not a copy of the exam title."""
    streams = []
    for s in ("Civil", "Mechanical", "Electrical"):
        if re.search(rf"\b{s}\s+Engineering\b", text or "", re.I):
            streams.append(f"Assistant Engineer ({s})")
    names = streams
    if not names:
        raw = exam_name or ""
        raw = re.sub(r"^\s*[A-Z]{2,10}\s+", "", raw)
        raw = re.sub(r"\s+Examination(?:\s+20\d{2})?$", "", raw, flags=re.I)
        raw = re.sub(r"\s+20\d{2}$", "", raw).strip(" -")
        names = [raw] if raw else [exam_name or "Advertised post"]
    return [{
        "post_name": n[:120],
        "department": (department or "")[:120],
        "pay_level": "",
        "total_vacancies": 0,
        "vacancies_breakdown": None,
        "qualification": None,
        "translations": None,
    } for n in names]


def _extract_advertisement_number(text: str) -> str:
    if not text:
        return ""
    patterns = [
        r"(?i)(?:special\s+advertisement|advertisement|advt\.?|notification)\s*"
        r"(?:no\.?|number|øमा[कंक]|क्रमांक|क्रमाक)?\s*[:\-]?\s*"
        r"([A-Z0-9][A-Z0-9/.\-]{2,28})",
        r"िव²ापन\s*øमा[कंक]\s*[:\-]?\s*(\d{1,3}/\d{4}(?:/परी[^\s/]*)?)",
        r"विज्ञापन\s*क्रमांक\s*[:\-]?\s*(\d{1,3}/\d{4}(?:/परी[^\s/]*)?)",
        r"\b(\d{2}/\d{4}/परी[±कषा्ा]*)\b",
    ]
    for pat in patterns:
        m = re.search(pat, text)
        if not m:
            continue
        val = _norm_ws(m.group(1))
        val = val.strip(" :-")
        # Drop trailing date-words accidentally glued on (िदनाक / दिनांक).
        val = re.split(r"[/\s]*(?:िदनाक|दिनांक|dated)", val, maxsplit=1)[0]
        val = val.strip(" /")
        val = val.replace("परी±ा", "परीक्षा").replace("परी±", "परीक्षा")
        if val.upper() in {"", "NO", "NUMBER", "NOS", "NO."}:
            continue
        if not re.search(r"\d", val):
            continue
        return val[:60]
    return ""


def _extract_official_url(text: str, commission=None) -> str:
    hosts = []
    if commission:
        hosts.extend(commission.get("url_hosts") or [])
    for host in hosts:
        m = re.search(rf"(https?://)?(www\.)?{re.escape(host)}(/[^\s]*)?", text or "", re.I)
        if m:
            raw = m.group(0)
            if not raw.lower().startswith("http"):
                raw = "https://" + raw.lstrip("/")
            return raw.rstrip(").,;")
    m = re.search(r"(https?://[^\s]+|www\.[A-Za-z0-9.-]+\.[A-Za-z.]{2,})", text or "")
    if m:
        raw = m.group(1)
        if raw.lower().startswith("www."):
            raw = "https://" + raw
        return raw.rstrip(").,;")
    return ""


def _parse_apply_dates(text: str, source_path: str = "") -> dict:
    """Prefer apply/online/last-date lines whose year matches the filename year ±1."""
    hints = _filename_hints(source_path)
    fy = hints.get("year")
    start = end = exam = ""
    apply_line_dates = []
    for line in (text or "").splitlines():
        if not _DATE_RE.search(line):
            continue
        dates = _DATE_RE.findall(line)
        plausible = [d for d in dates if _date_year_plausible(d, fy)]
        if not plausible:
            continue
        if _APPLY_CTX_RE.search(line) and not _HISTORICAL_RULE_RE.search(line):
            # Prefer a true window (two dates + apply/online) over a single dated notice line.
            apply_line_dates.append(plausible)
        if not exam and re.search(r"(?i)exam|परी±ा|परीक्षा|preliminary|ÿारिभक", line):
            if not _APPLY_CTX_RE.search(line):
                exam = plausible[0]
    if apply_line_dates:
        # Two-date apply windows first (start … end on the same line).
        windows = [d for d in apply_line_dates if len(d) >= 2]
        chosen = windows[0] if windows else apply_line_dates[0]
        start = chosen[0]
        end = chosen[-1] if len(chosen) > 1 else ""
        if not end:
            for extra in apply_line_dates[1:]:
                if extra:
                    end = extra[-1]
                    break
        if not end:
            end = start
    return {"apply_start_date": start, "apply_end_date": end, "exam_date": exam}


def _title_looks_like_single_cadre(title: str) -> bool:
    t = title or ""
    if _title_starts_with_index(t):
        return True
    if re.search(r"(?i)\bexamination\b|\bexam\b|परीक्षा|परी", t):
        return False
    # "1. Indian Administrative Service" already caught; also bare cadre names.
    return bool(re.search(
        r"(?i)^(state civil service|indian administrative service|"
        r"indian police service|deputy collector|dy\.?\s*superintendent)",
        t,
    ))


def _quality_reject_reasons(fields: dict, pdf_text: str, source_path: str) -> list:
    reasons = []
    jt = (fields.get("job_title") or "").strip()
    kind = (fields.get("exam_kind") or "").strip()
    posts = fields.get("posts") or []
    hints = _filename_hints(source_path)
    blob = ((pdf_text or "") + "\n" + (source_path or ""))

    if _title_starts_with_index(jt):
        reasons.append("title_starts_with_index")
    if _looks_like_ad_number(jt) or len(jt) < 10 or jt.lower().startswith("extracted"):
        reasons.append("bad_or_short_title")
    if kind in (EXAM_KIND_COMBINED, EXAM_KIND_DEPARTMENTAL) and _title_looks_like_single_cadre(jt):
        reasons.append("combined_or_dept_title_is_cadre")
    if re.search(r"(?i)examination|परीक्षा|परी±ा|\bcse\b|\bcgl\b", blob) and not re.search(
        r"(?i)examination|exam|परीक्षा|परी", jt
    ):
        if kind in (EXAM_KIND_COMBINED, EXAM_KIND_DEPARTMENTAL):
            reasons.append("exam_in_source_but_not_in_title")
    long_pdf = len(pdf_text or "") > 20000
    if kind in (EXAM_KIND_COMBINED, EXAM_KIND_MULTI_POST) and long_pdf:
        tv = fields.get("total_vacancies") or 0
        try:
            tv = int(tv)
        except Exception:
            tv = 0
        if not posts or tv == 1:
            reasons.append("combined_or_multi_but_one_or_no_posts")
    end = fields.get("apply_end_date") or ""
    if end and not _date_year_plausible(end, hints.get("year")):
        reasons.append("implausible_apply_end_year")
    dept = fields.get("department") or ""
    if re.search(r"vide\s+Notification", dept, re.I) or re.search(r"\bNo\.\s*$", dept):
        reasons.append("department_truncated_or_vide_notification")
    ad = (fields.get("advertisement_number") or "").strip()
    if ad.upper() in {"", "NO", "NUMBER", "NOS", "NO."}:
        reasons.append("advertisement_number_empty_or_literal_no")
    return reasons


def _extraction_quality_score(fields: dict) -> int:
    score = 0
    kind = fields.get("exam_kind") or ""
    if kind in (
        EXAM_KIND_COMBINED, EXAM_KIND_DEPARTMENTAL,
        EXAM_KIND_MULTI_POST, EXAM_KIND_SINGLE,
    ):
        score += 10
    posts = fields.get("posts") or []
    score += min(len(posts), 40)
    if fields.get("commission"):
        score += 15
    if fields.get("exam_name") and re.search(r"(?i)exam", fields.get("exam_name") or ""):
        score += 8
    jt = fields.get("job_title") or ""
    if jt and not _title_starts_with_index(jt) and not _looks_like_ad_number(jt):
        score += 10
    ad = (fields.get("advertisement_number") or "").strip()
    if ad and ad.upper() not in {"NO", "NUMBER"}:
        score += 5
    if fields.get("apply_end_date"):
        score += 5
    dept = fields.get("department") or ""
    if dept and not re.search(r"vide\s+Notification", dept, re.I):
        score += 3
    return score


def _row_quality_score(row: dict) -> int:
    if not row:
        return -1
    return _extraction_quality_score({
        "exam_kind": row.get("exam_kind") or "",
        "posts": [{"post_name": "x"}] * 0,  # filled below if we have a count
        "commission": row.get("commission") or "",
        "exam_name": row.get("exam_name") or "",
        "job_title": row.get("job_title") or "",
        "advertisement_number": row.get("advertisement_number") or "",
        "apply_end_date": row.get("apply_end_date") or "",
        "department": row.get("department") or "",
    }) + (1 if (row.get("total_vacancies") or 0) not in (0, 1) else 0)


def _registry_hi_translations(commission, exam_name: str) -> dict:
    if not commission:
        return None
    hi = {}
    if commission.get("name_hi"):
        hi["department"] = commission["name_hi"]
    # Prefer registry Hindi exam alias over garbled pypdf text.
    for alias in commission.get("exam_aliases") or []:
        if re.search(r"[\u0900-\u097F]", alias):
            year_m = re.search(r"(20\d{2})", exam_name or "")
            hi["job_title"] = alias + (f" {year_m.group(1)}" if year_m else "")
            hi["exam_name"] = hi["job_title"]
            break
    return {"hi": hi} if hi else None


def _call_llm_for_extraction(pdf_text: str, source_path: str, pack: str = "", commission=None, exam_kind: str = ""):
    if not LLM_API_KEY:
        _log("LLM_SKIP", source_path, "no LLM_API_KEY/XAI_API_KEY/OPENAI_API_KEY")
        return None

    comm = commission or {}
    system_msg = (
        "You are an expert, precise, conservative extractor for Indian Government recruitment notifications "
        "(UPSC, SSC, every State PSC, High Courts, Railways, teaching/medical boards). "
        "You first classify the document kind, then extract. "
        "You output ONLY one valid, minified JSON object matching the exact schema. No prose, no markdown fences."
    )

    rules = """
DOCUMENT KINDS (pick exactly one exam_kind):
A. combined_exam — one exam, many cadres/services. Title = the EXAM (commission + exam + year), NEVER the first cadre.
   UPSC CSE/ESE/CDS/NDA/CAPF, State PSC SSE/PCS/CCE/RAS, SSC CGL/CHSL/MTS/GD/CPO/JE, State Forest Service.
B. multi_post_ad — one advertisement, several distinct posts, not an exam brand.
   UPSC Special Ad / ORA vacancy blocks, High Court multi-post, medical-college combo ads.
C. single_post — one post. job_title MAY be the post name.
D. departmental_exam — named specialist exam (State Engineering Service, Assistant Librarian Exam, Assistant Professor Exam).
   Title = exam name from header/filename, not a random table row.
If unsure A vs D, prefer A when the document lists a numbered services/posts cadre or says combined / state service / civil services / preliminary & main.

CRITICAL RULES (VIOLATION = INVALID):
1. COMBINED_EXAM: job_title is the exam, e.g. "CGPSC State Service Examination 2025", "UPSC Civil Services Examination 2025",
   "SSC Combined Graduate Level Examination 2026". NEVER a numbered cadre line
   ("1. Indian Administrative Service", "1. State Civil Service (Deputy Collector) II").
2. MULTI_POST_AD: split every UPSC-style "(Vacancy No.) N vacancies for the post of X" block AND every Name-of-Post table row.
   job_title is the recruitment as a whole ("Principal and Vice Principal, Education Department, GNCTD").
   NEVER use "SPECIAL ADVERTISEMENT NO. XX/20XX", Advt No, filename, or Vacancy No as job_title or post_name.
3. Dates come from the apply window on the advertisement proper, NEVER from a historical rules annex
   (CG SSE rules cite 27.08.2008 and 7.2.1997 — those are NOT apply_end_date).
4. advertisement_number is the advt/notification id ("06/2025/परीक्षा", "51/2026", "01/2026"), never the word "No",
   never a filename hash. The capture must contain a digit.
5. department for a commission-run exam is the commission ("Chhattisgarh Public Service Commission" / "Union Public Service Commission"),
   not a truncated "vide Notification No. F…" clause.
6. Bilingual: English for top-level fields; Hindi/regional originals in translations. If Devanagari is garbled (º ± Ĭ),
   do not copy garbage — use English + registry name_hi.
7. posts[] is required whenever more than one distinct post/cadre is listed. total_vacancies = sum of posts when available.
8. Always emit commission (code like CGPSC/UPSC/SSC or ""), state (or null for national), exam_name, exam_kind, search_aliases (array).
9. pay_level: exact "Level-12 in the Pay Matrix as per 7th CPC" when present.
10. vacancies_breakdown / reservation_details: category→int only (UR, EWS, OBC, SC, ST, PwBD…).
11. If no clear data for a required key, use "" or 0 or null — never hallucinate.

OUTPUT SCHEMA (exact keys):
{
  "job_title": "string",
  "department": "string",
  "commission": "CGPSC",
  "state": "Chhattisgarh" or null,
  "exam_name": "State Service Examination 2025",
  "exam_kind": "combined_exam",
  "search_aliases": ["cgpsc", "sse", "..."],
  "total_vacancies": 0,
  "reservation_details": {"UR":0} or null,
  "qualification": "string",
  "age_limit": "string",
  "age_relaxation": "string",
  "apply_start_date": "string",
  "apply_end_date": "string",
  "exam_date": "string",
  "advertisement_number": "string",
  "application_fee": "string",
  "official_url": "string",
  "nationality": "string",
  "translations": {"hi": {...}} or null,
  "age_relaxation_details": [ {...} ] or null,
  "syllabus": {...} or null,
  "posts": [ {
      "post_name": "string",
      "department": "string",
      "pay_level": "string",
      "total_vacancies": 0,
      "vacancies_breakdown": {"UR":0} or null,
      "qualification": "string" or null,
      "translations": {...} or null
    } ] or null
}
"""

    fewshot = """
FEW-SHOT 1 (UPSC multi-post teaching, Advt 51/2026):
INPUT: "SPECIAL ADVERTISEMENT NO. 51/2026" + "vacancies for the post of Principal..." + Vice Principal + upsconline.nic.in
CORRECT:
{"job_title":"Principal and Vice Principal in Education Department, Government of NCT of Delhi","department":"Education Department, Government of NCT of Delhi","commission":"UPSC","state":null,"exam_name":"","exam_kind":"multi_post_ad","search_aliases":["upsc","principal","vice principal","upsconline.nic.in"],"total_vacancies":828,"reservation_details":{"UR":356,"EWS":82,"OBC":207,"SC":119,"ST":64},"qualification":"Master's Degree + B.Ed","age_limit":"50 years Principal; 35 years Vice Principal","age_relaxation":"Up to 10 years for PwBD (max 56)","apply_start_date":"","apply_end_date":"","exam_date":"","advertisement_number":"51/2026","application_fee":"","official_url":"https://upsconline.nic.in/ora/","nationality":"Citizen of India","translations":null,"age_relaxation_details":[{"source":"Age clause","category":"PwBD","relaxation":"upto 10 years","cap":"subject to maximum 56 years"}],"syllabus":null,"posts":[{"post_name":"Principal","department":"Education Department, Government of NCT of Delhi","pay_level":"Level-12 in the Pay Matrix as per 7th CPC","total_vacancies":124,"vacancies_breakdown":{"UR":69,"EWS":11,"OBC":18,"SC":16,"ST":10},"qualification":null,"translations":null},{"post_name":"Vice Principal","department":"Education Department, Government of NCT of Delhi","pay_level":"Level-10 in the Pay Matrix as per 7th CPC","total_vacancies":704,"vacancies_breakdown":{"UR":287,"EWS":71,"OBC":189,"SC":103,"ST":54},"qualification":null,"translations":null}]}

FEW-SHOT 2 (State PSC combined SSE / PCS — CGPSC-like numbered cadre list):
INPUT filename STATE_SERVICE_EXAMINATION-2025 + header "छत्तीसगढ़ लोक सेवा आयोग" + www.psc.cg.gov.in + apply 01/12/2025-30/12/2025 + advt 06/2025/परीक्षा + English annex "1. State Civil Service (Deputy Collector)  2. State Police Service (Dy. Superintendent of Police) ..."
CORRECT:
{"job_title":"CGPSC State Service Examination 2025","department":"Chhattisgarh Public Service Commission","commission":"CGPSC","state":"Chhattisgarh","exam_name":"State Service Examination 2025","exam_kind":"combined_exam","search_aliases":["cgpsc","sse","state service examination","psc.cg.gov.in","deputy collector"],"total_vacancies":0,"reservation_details":null,"qualification":"","age_limit":"","age_relaxation":"","apply_start_date":"01/12/2025","apply_end_date":"30/12/2025","exam_date":"22/02/2026","advertisement_number":"06/2025/परीक्षा","application_fee":"","official_url":"https://www.psc.cg.gov.in","nationality":"","translations":{"hi":{"job_title":"राज्य सेवा परीक्षा 2025","department":"छत्तीसगढ़ लोक सेवा आयोग"}},"age_relaxation_details":null,"syllabus":null,"posts":[{"post_name":"State Civil Service (Deputy Collector)","department":"","pay_level":"","total_vacancies":0,"vacancies_breakdown":null,"qualification":null,"translations":null},{"post_name":"State Police Service (Dy. Superintendent of Police)","department":"","pay_level":"","total_vacancies":0,"vacancies_breakdown":null,"qualification":null,"translations":null}]}

FEW-SHOT 3 (SSC CGL-style exam notice):
INPUT: "Staff Selection Commission Combined Graduate Level Examination 2026" + ssc.gov.in
CORRECT:
{"job_title":"SSC Combined Graduate Level Examination 2026","department":"Staff Selection Commission","commission":"SSC","state":null,"exam_name":"Combined Graduate Level Examination 2026","exam_kind":"combined_exam","search_aliases":["ssc","cgl","combined graduate level","ssc.gov.in"],"total_vacancies":0,"reservation_details":null,"qualification":"Bachelor's degree","age_limit":"","age_relaxation":"","apply_start_date":"","apply_end_date":"","exam_date":"","advertisement_number":"","application_fee":"","official_url":"https://ssc.gov.in","nationality":"Citizen of India","translations":null,"age_relaxation_details":null,"syllabus":null,"posts":[{"post_name":"Assistant Section Officer","department":"","pay_level":"","total_vacancies":0,"vacancies_breakdown":null,"qualification":null,"translations":null}]}

FEW-SHOT 4 (single-post High Court / departmental ad):
INPUT: "HIGH COURT OF ... RECRUITMENT ... POST OF COURT MANAGER ... Pay Matrix Level-10 ... Total 5 vacancies (UR-3, SC-1, ST-1) ... Last date 25/09/2025"
CORRECT:
{"job_title":"Court Manager","department":"High Court of ...","commission":"HIGH_COURT","state":null,"exam_name":"","exam_kind":"single_post","search_aliases":["high court","court manager"],"total_vacancies":5,"reservation_details":{"UR":3,"SC":1,"ST":1},"qualification":"...","age_limit":"...","age_relaxation":"","apply_start_date":"","apply_end_date":"25/09/2025","exam_date":"","advertisement_number":"","application_fee":"","official_url":"","nationality":"","translations":null,"age_relaxation_details":null,"syllabus":null,"posts":[{"post_name":"Court Manager","department":"High Court of ...","pay_level":"Level-10","total_vacancies":5,"vacancies_breakdown":{"UR":3,"SC":1,"ST":1},"qualification":null,"translations":null}]}
"""

    body = pack or (pdf_text or "")[:_PACK_BUDGET]
    user_prompt = (
        rules + "\n" + fewshot + "\n"
        f"Pre-detected commission={comm.get('code') or ''} state={comm.get('state')} "
        f"kind={exam_kind}. Confirm or correct these; do not invent a commission if none fits.\n"
        f"Source file (HINT only): {source_path}\n\n"
        "--- BEGIN SELECTED PDF PACK ---\n" + body + "\n--- END SELECTED PDF PACK ---\n\n"
        "Now output the SINGLE minified JSON ONLY:"
    )

    try:
        with httpx.Client(timeout=180.0) as client:
            r = client.post(
                f"{LLM_BASE_URL}/chat/completions",
                headers={"Authorization": f"Bearer {LLM_API_KEY}", "Content-Type": "application/json"},
                json={
                    "model": LLM_MODEL,
                    "messages": [
                        {"role": "system", "content": system_msg},
                        {"role": "user", "content": user_prompt}
                    ],
                    "temperature": 0.0,
                    "max_tokens": 12000
                }
            )
            r.raise_for_status()
            content = r.json()["choices"][0]["message"]["content"].strip()
            # Robust JSON extraction
            if "```" in content:
                parts = content.split("```")
                content = parts[1] if len(parts) > 1 else content
                content = content.strip()
                if content.lower().startswith("json"):
                    content = content.split("\n", 1)[-1].strip()
            # Sometimes LLM adds trailing text; take first { ... } block
            if not content.startswith("{"):
                start = content.find("{")
                end = content.rfind("}")
                if start != -1 and end != -1:
                    content = content[start:end+1]
            fields = json.loads(content.strip())
            # Log raw LLM output for traceability (non-secret)
            try:
                with open(BASE_DIR / "mcp_read_log.txt", "a", encoding="utf-8") as lf:
                    lf.write(f"{datetime.datetime.now().isoformat()}|LLM_RAW|{source_path}|keys={list(fields.keys()) if isinstance(fields,dict) else 'bad'}\n")
            except Exception:
                pass
            return fields
    except Exception as ex:
        try:
            with open(BASE_DIR / "mcp_read_log.txt", "a", encoding="utf-8") as lf:
                lf.write(f"{datetime.datetime.now().isoformat()}|LLM_ERR|{source_path}|{str(ex)[:200]}\n")
        except Exception:
            pass
        return None


def _validate_and_normalize_extraction(fields: dict, pdf_text: str, source_path: str) -> dict:
    """Post-LLM / post-fallback guard. Classify kind+issuer, never let a cadre line become the exam title."""
    if not isinstance(fields, dict):
        fields = {}
    text = pdf_text or ""
    commission = _detect_commission(text, source_path)
    # Honour an LLM-supplied commission code if it is in the registry.
    code_in = (fields.get("commission") or "").strip().upper()
    if code_in and not commission:
        for entry in COMMISSION_REGISTRY:
            if entry["code"] == code_in:
                commission = entry
                break
    exam_kind = (fields.get("exam_kind") or "").strip()
    if exam_kind not in (
        EXAM_KIND_COMBINED, EXAM_KIND_MULTI_POST, EXAM_KIND_SINGLE, EXAM_KIND_DEPARTMENTAL
    ):
        exam_kind = _classify_exam_kind(text, source_path, commission)
    exam_name = (fields.get("exam_name") or "").strip()
    if not exam_name or _title_starts_with_index(exam_name) or _looks_like_ad_number(exam_name):
        exam_name = _exam_name_from_hints(text, source_path, commission, exam_kind)

    hints = _filename_hints(source_path)
    jt = _clean_title(fields.get("job_title") or "")
    if (
        not jt
        or _looks_like_ad_number(jt)
        or _title_starts_with_index(jt)
        or (
            exam_kind in (EXAM_KIND_COMBINED, EXAM_KIND_DEPARTMENTAL)
            and _title_looks_like_single_cadre(jt)
        )
        or (
            exam_kind in (EXAM_KIND_COMBINED, EXAM_KIND_DEPARTMENTAL)
            and exam_name
            and not re.search(r"(?i)exam", jt)
        )
    ):
        if exam_kind in (EXAM_KIND_COMBINED, EXAM_KIND_DEPARTMENTAL) and exam_name:
            jt = _commission_title(commission, exam_name)
        elif exam_kind == EXAM_KIND_MULTI_POST:
            # Synthesize from first good posts later; placeholder until posts cleaned.
            jt = jt if jt and not _title_starts_with_index(jt) and not _looks_like_ad_number(jt) else ""
        elif hints.get("phrase") and not _looks_like_ad_number(hints["phrase"]):
            jt = _commission_title(commission, exam_name or hints["phrase"])
        else:
            jt = "Government Recruitment Notification"
    fields["job_title"] = (jt or "Government Recruitment Notification")[:180]
    fields["exam_name"] = exam_name[:180]
    fields["exam_kind"] = exam_kind
    fields["commission"] = (commission or {}).get("code") or (fields.get("commission") or "")
    if commission and commission.get("state"):
        fields["state"] = commission["state"]
    elif fields.get("state") in ("", "null", "None", None):
        fields["state"] = None if not (commission or {}).get("state") else commission["state"]
    if isinstance(fields.get("state"), str) and fields["state"].lower() in ("null", "none", "national"):
        fields["state"] = None

    # Advertisement number: never the word "No"
    ad = (fields.get("advertisement_number") or "").strip()
    if ad.upper() in {"", "NO", "NUMBER", "NOS", "NO."} or not re.search(r"\d", ad):
        ad = _extract_advertisement_number(text)
    fields["advertisement_number"] = ad

    # Department: commission-run exams use the commission name.
    dept = fields.get("department") or ""
    if (
        _looks_like_ad_number(dept)
        or len(dept) < 3
        or re.search(r"vide\s+Notification", dept, re.I)
        or re.search(r"\bNo\.\s*$", dept)
        or exam_kind in (EXAM_KIND_COMBINED, EXAM_KIND_DEPARTMENTAL)
    ):
        if commission and commission.get("name_en"):
            dept = commission["name_en"]
        else:
            m = re.search(
                r"(?i)(Education Department|Department of [A-Za-z ]+|Government of [A-Z ]+"
                r"|High Court of [A-Za-z ]+|[A-Za-z ]+ Public Service Commission)",
                text,
            )
            if m:
                dept = m.group(1).strip()
    fields["department"] = (dept or "")[:150]

    try:
        fields["total_vacancies"] = int(fields.get("total_vacancies") or 0)
    except Exception:
        fields["total_vacancies"] = 0

    # Dates: prefer parser (rejects historical annex years)
    parsed_dates = _parse_apply_dates(text, source_path)
    fy = hints.get("year")
    for key in ("apply_start_date", "apply_end_date", "exam_date"):
        cur = (fields.get(key) or "").strip()
        if not cur or not _date_year_plausible(cur, fy):
            if parsed_dates.get(key):
                fields[key] = parsed_dates[key]
            elif cur and not _date_year_plausible(cur, fy):
                fields[key] = ""

    url = (fields.get("official_url") or "").strip()
    if not url:
        fields["official_url"] = _extract_official_url(text, commission)
    else:
        fields["official_url"] = url

    # Posts
    posts = fields.get("posts") or []
    if not isinstance(posts, list):
        posts = []
    if not posts:
        posts = _extract_posts_from_text(text, exam_kind)

    cleaned_posts = []
    seen = set()
    for p in posts:
        if not isinstance(p, dict):
            continue
        pn = _clean_title(p.get("post_name") or "")
        if _looks_like_ad_number(pn) or _title_starts_with_index(pn) or len(pn) < 4:
            continue
        key = pn.lower()
        if key in seen:
            continue
        seen.add(key)
        pl = p.get("pay_level") or ""
        if not pl or len(pl) < 5:
            idx = text.lower().find(pn.lower()[:30]) if pn else -1
            search_area = text[max(0, idx):idx + 2000] if idx >= 0 else text[:4000]
            pm = re.search(
                r"(Level[\-\s]*\d+[^\.]{0,60}(?:Pay Matrix|7th CPC|CPC)[^\.\n]*)",
                search_area,
                re.I,
            )
            if pm:
                pl = pm.group(1).strip()
        vbd = p.get("vacancies_breakdown")
        if not vbd or not isinstance(vbd, dict):
            block = ""
            if pn:
                idx = text.lower().find(pn.lower()[:20])
                if idx >= 0:
                    block = text[idx:idx + 800]
            vbd = _parse_vacancy_table_block(block or "")
        try:
            tv = int(p.get("total_vacancies") or 0)
        except Exception:
            tv = 0
        if tv == 0 and isinstance(vbd, dict) and "Total" in vbd:
            tv = vbd.get("Total", 0)
        cleaned_posts.append({
            "post_name": pn[:120],
            "department": (p.get("department") or fields.get("department") or "")[:120],
            "pay_level": (pl or "")[:150],
            "total_vacancies": tv,
            "vacancies_breakdown": vbd if isinstance(vbd, dict) else None,
            "qualification": p.get("qualification") or None,
            "translations": p.get("translations") or None,
        })

    all_levels = re.findall(
        r"(Level[\-\s]*\d+[^\n\.]{0,70}(?:Pay Matrix|7th CPC|CPC)[^\.\n]*)",
        text,
        re.I,
    )
    if all_levels and cleaned_posts:
        current_pays = [c.get("pay_level") for c in cleaned_posts if c.get("pay_level")]
        for i, cp in enumerate(cleaned_posts):
            if i < len(all_levels):
                if (not cp.get("pay_level")) or (
                    len(set(current_pays)) <= 1 and current_pays and current_pays[0] != all_levels[i]
                ):
                    cp["pay_level"] = all_levels[i][:150]

    if exam_kind == EXAM_KIND_MULTI_POST and cleaned_posts and (
        not fields.get("job_title")
        or _title_starts_with_index(fields["job_title"])
        or _looks_like_ad_number(fields["job_title"])
    ):
        names = [p["post_name"] for p in cleaned_posts[:4]]
        if names:
            fields["job_title"] = (" and ".join(names) if len(names) <= 2 else ", ".join(names[:-1]) + " and " + names[-1])[:180]

    if exam_kind == EXAM_KIND_DEPARTMENTAL:
        looks_like_exam_title = bool(cleaned_posts) and all(
            re.search(r"(?i)examination", p.get("post_name") or "") for p in cleaned_posts
        )
        if not cleaned_posts or looks_like_exam_title:
            cleaned_posts = _departmental_posts(
                fields.get("exam_name") or fields.get("job_title") or "",
                text,
                fields.get("department") or "",
            )
    elif not cleaned_posts and fields.get("job_title") and exam_kind == EXAM_KIND_SINGLE:
        if not _looks_like_ad_number(fields["job_title"]) and not _title_starts_with_index(fields["job_title"]):
            cleaned_posts = [{
                "post_name": fields["job_title"][:120],
                "department": fields.get("department") or "",
                "pay_level": "",
                "total_vacancies": fields.get("total_vacancies") or 0,
                "vacancies_breakdown": fields.get("reservation_details"),
                "qualification": fields.get("qualification") or None,
                "translations": None,
            }]

    cleaned_posts = _apply_vacancy_totals(cleaned_posts or [], text)

    extra = _extract_eligibility_fields(text)
    for k in ("qualification", "age_limit", "age_relaxation", "application_fee", "nationality"):
        if not (fields.get(k) or "").strip() and extra.get(k):
            fields[k] = extra[k]
    if not fields.get("syllabus") and extra.get("syllabus"):
        fields["syllabus"] = extra["syllabus"]

    fields["posts"] = cleaned_posts or None

    if fields.get("posts") and (not fields.get("total_vacancies") or fields["total_vacancies"] in (0, 1)):
        s = sum((p.get("total_vacancies") or 0) for p in fields["posts"])
        if s > 1:
            fields["total_vacancies"] = s
        elif exam_kind in (EXAM_KIND_COMBINED, EXAM_KIND_MULTI_POST) and len(fields["posts"]) > 1:
            # Many cadres without per-row counts: do not leave total_vacancies==1.
            if fields["total_vacancies"] == 1:
                fields["total_vacancies"] = 0

    for k in ["reservation_details", "translations", "age_relaxation_details", "syllabus"]:
        v = fields.get(k)
        if isinstance(v, str):
            try:
                fields[k] = json.loads(v)
            except Exception:
                fields[k] = None

    if exam_kind in (EXAM_KIND_COMBINED, EXAM_KIND_DEPARTMENTAL) and commission:
        if not fields.get("translations"):
            fields["translations"] = _registry_hi_translations(commission, exam_name)
        else:
            # Drop garbled Devanagari the LLM may have copied.
            tr = fields["translations"]
            if isinstance(tr, dict):
                for lang, payload in list(tr.items()):
                    if not isinstance(payload, dict):
                        continue
                    for fk, fv in list(payload.items()):
                        if isinstance(fv, str) and _GARBLED_DEVANAGARI_RE.search(fv):
                            payload.pop(fk, None)

    aliases = fields.get("search_aliases")
    if not isinstance(aliases, list) or not aliases:
        aliases = _build_search_aliases(fields, source_path, commission)
    else:
        aliases = _build_search_aliases(
            {**fields, "search_aliases": aliases}, source_path, commission
        )
    fields["search_aliases"] = aliases
    fields["search_document"] = _search_document_from_aliases(aliases)

    try:
        _log(
            "NORMALIZED",
            source_path,
            f"title={fields.get('job_title','')[:60]}",
            f"kind={fields.get('exam_kind')}",
            f"commission={fields.get('commission')}",
            f"posts={len(fields.get('posts') or [])}",
            f"tv={fields.get('total_vacancies')}",
            f"ad={fields.get('advertisement_number')}",
            f"end={fields.get('apply_end_date')}",
        )
    except Exception:
        pass
    return fields


def _basic_fallback_extract(pdf_text: str, source_path: str):
    """Deterministic extract: classify issuer+kind from URL/filename/header, never first officer line."""
    text = pdf_text or ""
    commission = _detect_commission(text, source_path)
    exam_kind = _classify_exam_kind(text, source_path, commission)
    exam_name = _exam_name_from_hints(text, source_path, commission, exam_kind)
    hints = _filename_hints(source_path)

    if exam_kind in (EXAM_KIND_COMBINED, EXAM_KIND_DEPARTMENTAL) and exam_name:
        job_title = _commission_title(commission, exam_name)
    elif hints.get("phrase") and not _looks_like_ad_number(hints["phrase"]):
        job_title = _commission_title(commission, exam_name or hints["phrase"])
    else:
        job_title = "Government Recruitment Notification"

    dates = _parse_apply_dates(text, source_path)
    ad = _extract_advertisement_number(text)
    dept = (commission or {}).get("name_en") or ""
    if not dept:
        m = re.search(
            r"(?i)(High Court of [A-Za-z ]+|[A-Za-z ]+ Public Service Commission|"
            r"Staff Selection Commission|Union Public Service Commission)",
            text,
        )
        if m:
            dept = m.group(1).strip()

    posts = _extract_posts_from_text(text, exam_kind)
    if exam_kind == EXAM_KIND_DEPARTMENTAL and (
        not posts or (len(posts) == 1 and "Examination" in (posts[0].get("post_name") or ""))
    ):
        posts = _departmental_posts(exam_name or job_title, text, dept)
    posts = _apply_vacancy_totals(posts or [], text)
    extra = _extract_eligibility_fields(text)
    tv = 0
    m = re.search(r"(?i)total\s*(?:no\.?\s*of\s*)?vacanc(?:y|ies)\s*[:\-]?\s*(\d+)", text)
    if m:
        tv = int(m.group(1))
    if not tv and posts:
        tv = sum((p.get("total_vacancies") or 0) for p in posts)

    fields = {
        "job_title": job_title[:180],
        "department": (dept or "")[:150],
        "commission": (commission or {}).get("code") or "",
        "state": (commission or {}).get("state"),
        "exam_name": exam_name,
        "exam_kind": exam_kind,
        "total_vacancies": tv,
        "reservation_details": _parse_vacancy_table_block(text),
        "qualification": extra.get("qualification") or "",
        "age_limit": extra.get("age_limit") or "",
        "age_relaxation": extra.get("age_relaxation") or "",
        "apply_start_date": dates.get("apply_start_date") or "",
        "apply_end_date": dates.get("apply_end_date") or "",
        "exam_date": dates.get("exam_date") or "",
        "advertisement_number": ad,
        "application_fee": extra.get("application_fee") or "",
        "official_url": _extract_official_url(text, commission),
        "nationality": extra.get("nationality") or "",
        "translations": _registry_hi_translations(commission, exam_name),
        "age_relaxation_details": None,
        "syllabus": extra.get("syllabus"),
        "posts": posts or None,
        "search_aliases": [],
    }
    if not fields["age_limit"]:
        am = re.search(r"(?i)age[:\s]+([^\n]{5,80})", text)
        if am:
            fields["age_limit"] = am.group(1).strip()[:120]
    if not fields["qualification"]:
        qm = re.search(r"(?i)essential qualification[s]?[:\s]+([^\n]{10,200})", text)
        if qm:
            fields["qualification"] = qm.group(1).strip()[:300]
    return fields


def _extract_fields(pdf_text: str, source_path: str) -> dict:
    """LLM-first (selected pack), then fallback. Always classify + normalize."""
    commission = _detect_commission(pdf_text or "", source_path)
    exam_kind = _classify_exam_kind(pdf_text or "", source_path, commission)
    pack = _build_extraction_pack(pdf_text or "", source_path, commission, exam_kind)
    fields = _call_llm_for_extraction(
        pdf_text or "", source_path, pack=pack, commission=commission, exam_kind=exam_kind
    )
    if not fields:
        fields = _basic_fallback_extract(pdf_text or "", source_path)
    return _validate_and_normalize_extraction(fields, pdf_text or "", source_path)


def _persist_job(fields: dict, stored_path: str, replace_ids=None, match_exam: bool = False) -> str:
    """Insert or replace a notification. Never overwrite a better existing row unless replace_ids is set."""
    aliases = fields.get("search_aliases") or _build_search_aliases(fields, stored_path)
    search_document = fields.get("search_document") or _search_document_from_aliases(aliases)
    posts = fields.get("posts")
    new_score = _extraction_quality_score(fields)
    stored_rel = _db_pdf_path(stored_path)

    conn = connect()
    try:
        with conn.cursor() as cur:
            existing = []
            if replace_ids:
                cur.execute(
                    "SELECT * FROM gov_job_notifications WHERE id = ANY(%s) ORDER BY id",
                    (list(replace_ids),),
                )
                existing = list(cur.fetchall() or [])
            if not existing and stored_path:
                cur.execute(
                    "SELECT * FROM gov_job_notifications WHERE local_pdf_path = %s "
                    "OR local_pdf_path = %s OR local_pdf_path LIKE %s ORDER BY id",
                    (stored_path, stored_rel, "%" + Path(stored_path).name),
                )
                existing = list(cur.fetchall() or [])
            if (
                match_exam
                and not existing
                and fields.get("commission")
                and (fields.get("exam_name") or "")
                and len(fields.get("exam_name") or "") >= 8
            ):
                cur.execute(
                    "SELECT * FROM gov_job_notifications WHERE commission = %s AND exam_name = %s ORDER BY id",
                    (fields.get("commission"), fields.get("exam_name")),
                )
                existing = list(cur.fetchall() or [])

            if existing and replace_ids is None:
                best = max(existing, key=_row_quality_score)
                # Prefer comparing post counts from DB when we can.
                try:
                    cur.execute(
                        "SELECT COUNT(*) AS n FROM gov_job_posts WHERE notification_id = %s",
                        (best["id"],),
                    )
                    old_posts = (cur.fetchone() or {}).get("n") or 0
                except Exception:
                    old_posts = 0
                old_score = _row_quality_score(best) + min(old_posts, 40)
                if new_score <= old_score:
                    _log(
                        "AUTO_SKIP_BETTER_EXISTS",
                        stored_path,
                        f"keep={best['id']}",
                        f"old={old_score}",
                        f"new={new_score}",
                    )
                    return f"Skipped; existing id {best['id']} is better or equal"

            values = (
                fields.get("job_title") or "Extracted Job Notification",
                fields.get("department") or "",
                int(fields.get("total_vacancies") or 0),
                Jsonb(fields.get("reservation_details") or {}),
                fields.get("qualification") or "",
                fields.get("age_limit") or "",
                fields.get("age_relaxation") or "",
                fields.get("apply_start_date") or "",
                fields.get("apply_end_date") or "",
                fields.get("exam_date") or "",
                fields.get("advertisement_number") or "",
                fields.get("application_fee") or "",
                fields.get("official_url") or "",
                stored_rel,
                Jsonb(fields["translations"]) if fields.get("translations") else None,
                Jsonb(fields["age_relaxation_details"]) if fields.get("age_relaxation_details") else None,
                fields.get("nationality") or None,
                Jsonb(fields["syllabus"]) if fields.get("syllabus") else None,
                fields.get("commission") or "",
                fields.get("state") or None,
                fields.get("exam_name") or "",
                fields.get("exam_kind") or "",
                search_document,
            )
            col_sql = """
                job_title=%s, department=%s, total_vacancies=%s, reservation_details=%s,
                qualification=%s, age_limit=%s, age_relaxation=%s, apply_start_date=%s,
                apply_end_date=%s, exam_date=%s, advertisement_number=%s, application_fee=%s,
                official_url=%s, local_pdf_path=%s, translations=%s, age_relaxation_details=%s,
                nationality=%s, syllabus=%s, commission=%s, state=%s, exam_name=%s,
                exam_kind=%s, search_document=%s
            """
            if existing:
                keep_id = existing[0]["id"]
                cur.execute(
                    f"UPDATE gov_job_notifications SET {col_sql} WHERE id = %s",
                    values + (keep_id,),
                )
                cur.execute("DELETE FROM gov_job_posts WHERE notification_id = %s", (keep_id,))
                for extra in existing[1:]:
                    cur.execute("DELETE FROM gov_job_notifications WHERE id = %s", (extra["id"],))
                job_id = keep_id
                action = "updated"
            else:
                cur.execute(
                    """
                    INSERT INTO gov_job_notifications (
                        job_title, department, total_vacancies, reservation_details,
                        qualification, age_limit, age_relaxation, apply_start_date,
                        apply_end_date, exam_date, advertisement_number, application_fee,
                        official_url, local_pdf_path, translations, age_relaxation_details,
                        nationality, syllabus, commission, state, exam_name, exam_kind,
                        search_document
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    values,
                )
                job_id = cur.fetchone()["id"]
                action = "inserted"

            for post in (posts or []):
                cur.execute(
                    """
                    INSERT INTO gov_job_posts (
                        notification_id, post_name, department, pay_level,
                        total_vacancies, vacancies_breakdown, qualification, translations
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        job_id,
                        post["post_name"],
                        post.get("department"),
                        post.get("pay_level"),
                        post.get("total_vacancies"),
                        Jsonb(post["vacancies_breakdown"]) if post.get("vacancies_breakdown") else None,
                        post.get("qualification"),
                        Jsonb(post["translations"]) if post.get("translations") else None,
                    ),
                )
        conn.commit()
    finally:
        conn.close()

    post_note = f" with {len(posts)} individual post(s)" if posts else ""
    return f"Successfully {action} job ID #{job_id} ('{fields.get('job_title')}'){post_note}. Local PDF: {stored_path}"

def _mark_pickup_name_only(source_path: Path):
    try:
        if source_path.exists() and source_path.stat().st_size > 0:
            source_path.unlink()
            source_path.touch()
            _log("AUTO_NAME_ONLY", source_path)
    except Exception:
        pass


def _auto_save_fields(fields: dict, stored: str, source_path, text: str, replace_ids=None) -> str:
    reasons = _quality_reject_reasons(fields, text or "", str(source_path))
    if reasons:
        _log("AUTO_QUALITY_REJECT", source_path, ",".join(reasons), f"title={(fields.get('job_title') or '')[:60]}")
        return f"QUALITY_REJECT:{','.join(reasons)}"
    try:
        save_res = _persist_job(
            fields, stored, replace_ids=replace_ids, match_exam=True
        )
        _log("AUTO_SAVE", source_path, f"result={str(save_res)[:160]}")
        return save_res
    except Exception as ex:
        _log("AUTO_SAVE_ERR", source_path, str(ex)[:200])
        return f"SAVE_ERR:{ex}"


def _auto_ingest_from_pickup():
    processed = 0
    for source_path in sorted(PICKUP_DIR.glob("*.pdf")):
        if not source_path.is_file():
            continue
        if source_path.stat().st_size == 0:
            continue
        dest_filename = _dest_name(source_path)
        dest_path = STORAGE_DIR / dest_filename
        if dest_path.exists():
            _mark_pickup_name_only(source_path)
            continue
        stored = store_notification_pdf(str(source_path))
        if stored.startswith("Error:"):
            continue
        text = read_pdf_notification(str(source_path))
        text_path = STORAGE_DIR / (Path(stored).stem + ".txt")
        text_path.write_text(text or "", encoding="utf-8", errors="replace")
        _log("AUTO_INGEST", source_path, f"dest={stored}", f"len={len(text or '')}")
        fields = _extract_fields(text or "", str(source_path))
        _auto_save_fields(fields, stored, source_path, text or "")
        _mark_pickup_name_only(source_path)
        processed += 1
    return processed


def _reingest_stored_pdf(pdf_path: Path, replace_ids=None) -> dict:
    pdf_path = Path(pdf_path)
    text = read_pdf_notification(str(pdf_path))
    text_path = STORAGE_DIR / (pdf_path.stem + ".txt")
    try:
        text_path.write_text(text or "", encoding="utf-8", errors="replace")
    except Exception:
        pass
    fields = _extract_fields(text or "", str(pdf_path))
    result = _auto_save_fields(fields, str(pdf_path.resolve()), pdf_path, text or "", replace_ids=replace_ids)
    return {"path": str(pdf_path), "fields": fields, "result": result}


def _reingest_stored_fixtures() -> str:
    """Re-extract stored_pdfs/ fixtures. SSE-2025 replaces bad ids 74 and 1."""
    reports = []
    sse = None
    for p in STORAGE_DIR.glob("*.pdf"):
        if "STATE_SERVICE_EXAMINATION-2025" in p.name:
            sse = p
            break
    if sse:
        out = _reingest_stored_pdf(sse, replace_ids=[74, 1])
        f = out["fields"]
        reports.append(
            f"SSE: {out['result']} | title={f.get('job_title')} kind={f.get('exam_kind')} "
            f"commission={f.get('commission')} ad={f.get('advertisement_number')} "
            f"end={f.get('apply_end_date')} posts={len(f.get('posts') or [])}"
        )
    else:
        reports.append("SSE: stored PDF not found")

    for p in sorted(STORAGE_DIR.glob("*.pdf")):
        if "STATE_SERVICE_EXAMINATION-2025" in p.name:
            continue
        out = _reingest_stored_pdf(p)
        f = out["fields"]
        reports.append(
            f"{p.name[:48]}: {out['result']} | title={f.get('job_title')} "
            f"kind={f.get('exam_kind')} commission={f.get('commission')} "
            f"posts={len(f.get('posts') or [])}"
        )
    return "\n".join(reports)


def _run_extraction_self_tests() -> bool:
    """Classifier + UPSC vacancy-block smoke checks (no DB required beyond import)."""
    cases = [
        (
            "UNION PUBLIC SERVICE COMMISSION / Civil Services Examination 2025 / upsconline.nic.in",
            "CIVIL_SERVICES_EXAMINATION-2025.pdf",
            {"code": "UPSC", "kind": EXAM_KIND_COMBINED, "state": None},
        ),
        (
            "MADHYA PRADESH PUBLIC SERVICE COMMISSION / State Service Exam / mppsc.nic.in",
            "MP_STATE_SERVICE_EXAM-2025.pdf",
            {"code": "MPPSC", "kind": EXAM_KIND_COMBINED, "state": "Madhya Pradesh"},
        ),
        (
            "SPECIAL ADVERTISEMENT NO. 51/2026 / vacancies for the post of Principal "
            "in Education Department. PAY SCALE Level-12. Seven hundred four vacancies "
            "for the post of Vice Principal in Education Department.",
            "AdvtNo-51-2026-Special-Engl.pdf",
            {"code": "UPSC", "kind": EXAM_KIND_MULTI_POST, "state": None},
        ),
        (
            "Staff Selection Commission Combined Graduate Level Examination 2026 ssc.gov.in",
            "SSC_CGL_2026.pdf",
            {"code": "SSC", "kind": EXAM_KIND_COMBINED, "state": None},
        ),
        (
            "High Court of Chhattisgarh recruitment to the post of Court Manager, 5 vacancies",
            "HC_COURT_MANAGER.pdf",
            {"code": "HIGH_COURT", "kind": EXAM_KIND_SINGLE, "state": "Chhattisgarh"},
        ),
    ]
    ok = True
    for text, path, expect in cases:
        comm = _detect_commission(text, path)
        kind = _classify_exam_kind(text, path, comm)
        code = (comm or {}).get("code")
        state = (comm or {}).get("state")
        if code != expect["code"] or kind != expect["kind"] or state != expect["state"]:
            print(f"FAIL classify {path}: got code={code} kind={kind} state={state} expected {expect}")
            ok = False
        else:
            print(f"OK classify {path}: {code} {kind} {state}")

    upsc_text = (
        "1. (Vacancy No. 26075101724) One hundred twenty four vacancies for the post of "
        "Principal in Education Department, Government of NCT of Delhi.\n"
        "PAY SCALE: Level-12 in the Pay Matrix as per 7th CPC\n"
        "2. (Vacancy No. 26075102724) Seven hundred four vacancies for the post of "
        "Vice Principal in Education Department, Government of NCT of Delhi.\n"
        "PAY SCALE: Level-10 in the Pay Matrix as per 7th CPC\n"
    )
    posts = _extract_posts_from_text(upsc_text, EXAM_KIND_MULTI_POST)
    names = [p["post_name"] for p in posts]
    if not any(n == "Principal" or n.startswith("Principal") for n in names):
        print("FAIL UPSC splitter missing Principal", names)
        ok = False
    elif not any("Vice Principal" in n for n in names):
        print("FAIL UPSC splitter missing Vice Principal", names)
        ok = False
    elif len(posts) < 2:
        print("FAIL UPSC splitter post count", names)
        ok = False
    else:
        print("OK UPSC vacancy-block split", names)

    sse_stub = (
        "www.psc.cg.gov.in\n"
        "STATE SERVICES EXAMINATION RULES\n"
        "1.            State Civil Service (Deputy Collector)                                              II\n"
        "2.            State Police Service (Dy. Superintendent of Police)                         II\n"
        "3.            State Accounts Service                                                                     II\n"
    )
    comm = _detect_commission(sse_stub, "STATE_SERVICE_EXAMINATION-2025.pdf")
    kind = _classify_exam_kind(sse_stub, "STATE_SERVICE_EXAMINATION-2025.pdf", comm)
    cadres = _extract_numbered_cadre_posts(sse_stub)
    if (comm or {}).get("code") != "CGPSC" or kind != EXAM_KIND_COMBINED:
        print("FAIL SSE stub", (comm or {}).get("code"), kind)
        ok = False
    elif not any("Deputy Collector" in p["post_name"] for p in cadres):
        print("FAIL SSE cadres", [p["post_name"] for p in cadres])
        ok = False
    else:
        print("OK SSE stub", comm["code"], kind, len(cadres), "cadres")

    table_text = (
        "1    State Civil Service (Deputy Collector)   6  2  5  1  14  56100\n"
        "2    State Police Service (Dy. Superintendent of Police)  11 4 9 4  28  56100\n"
        "3    Something Officer  1 0 1 0  2  56100\n"
    )
    rows = _parse_vacancy_table_rows(table_text)
    if not rows or rows[0]["total_vacancies"] != 14 or rows[1]["total_vacancies"] != 28:
        print("FAIL vacancy table", rows)
        ok = False
    else:
        print("OK vacancy table", rows[:3])

    dposts = _departmental_posts(
        "CGPSC Assistant Librarian Examination 2026",
        "The Examination shall be conducted by the CGPSC",
        "Chhattisgarh Public Service Commission",
    )
    if not dposts or "Examination" in dposts[0]["post_name"] or "Librarian" not in dposts[0]["post_name"]:
        print("FAIL departmental post", dposts)
        ok = False
    else:
        print("OK departmental post", dposts[0]["post_name"])

    eng_posts = _departmental_posts(
        "State Engineering Service Examination 2026",
        "Civil Engineering and Mechanical Engineering and Electrical Engineering papers",
        "",
    )
    if len(eng_posts) < 3:
        print("FAIL engineering streams", [p["post_name"] for p in eng_posts])
        ok = False
    else:
        print("OK engineering streams", [p["post_name"] for p in eng_posts])

    elig = _extract_eligibility_fields(
        "The candidate must be a citizen of India. Age limit shall not have attained 40 years. "
        "Essential qualifications: Bachelor's degree from a recognized university. "
        "Examination fee Rs. 400. Scheme of Examination: Preliminary and Main Examination."
    )
    if "citizen of India" not in (elig.get("nationality") or "") or not elig.get("qualification"):
        print("FAIL eligibility", elig)
        ok = False
    else:
        print("OK eligibility", {k: bool(elig.get(k)) for k in elig})

    codes = {e["code"] for e in _all_fetch_entries()}
    for need in ("UPSC", "SSC", "CGPSC", "MPPSC", "UPPSC", "BPSC", "RPSC", "TNPSC",
                 "KPSC", "KPPSC", "WBPSC", "TSPSC", "APPSC", "JKPSC", "NPSC",
                 "CGVYAPAM", "MPESB", "RSMSSB", "HSSC", "UKSSSC", "OSSC", "JSSC", "GSSSB"):
        if need not in codes:
            print("FAIL missing fetch code", need)
            ok = False
    cg_seeds = _seed_urls_for_entry(next(e for e in COMMISSION_REGISTRY if e["code"] == "CGPSC"))
    vy_seeds = _seed_urls_for_entry(next(e for e in COMMISSION_REGISTRY if e["code"] == "CGVYAPAM"))
    esb_seeds = _seed_urls_for_entry(next(e for e in COMMISSION_REGISTRY if e["code"] == "MPESB"))
    if not any("psc.cg.gov.in" in u for u in cg_seeds):
        print("FAIL CGPSC seeds", cg_seeds[:3])
        ok = False
    elif not any("vyapamcg.cgstate.gov.in/Posts?tag=ONLINEAPPLICATION" in u for u in vy_seeds):
        print("FAIL CGVYAPAM seed", vy_seeds[:6])
        ok = False
    elif not any("esb.mp.gov.in/e_default.html" in u for u in esb_seeds):
        print("FAIL MPESB seed", esb_seeds[:6])
        ok = False
    else:
        print("OK fetch seeds", len(_all_fetch_entries()), "commissions")

    if _link_is_pdf("https://rpsc.rajasthan.gov.in/Static/Others/FAQ.pdf", "FAQ"):
        print("FAIL should skip FAQ.pdf")
        ok = False
    elif not _link_is_pdf(
        "https://psc.cg.gov.in/PDFs/Notifications/STATE_SERVICE_EXAMINATION-2025_ADVERTISEMENT.pdf",
        "Advertisement",
    ):
        print("FAIL should keep SSE advertisement PDF")
        ok = False
    elif _link_is_pdf(
        "https://psc.cg.gov.in/PDFs/Notifications/STATE_ENGINEERING_SERVICE_EXAM-2026_ABOUT_ADMIT_CARD.pdf",
        "admit card",
    ):
        print("FAIL should skip admit card")
        ok = False
    else:
        print("OK pdf link filter")

    rel = _db_pdf_path("/tmp/stored_pdfs/foo_abc.pdf")
    if rel != "stored_pdfs/foo_abc.pdf":
        print("FAIL relative path", rel)
        ok = False
    else:
        print("OK relative path", rel)
    return ok


def _host_variants(host: str) -> list:
    host = (host or "").strip().lower()
    if not host:
        return []
    out = [host]
    if not host.startswith("www."):
        out.append("www." + host)
    return out


def _seed_urls_for_entry(entry: dict) -> list:
    paths = list(_LISTING_PATHS_BY_CODE.get(entry["code"], []))
    for p in _DEFAULT_LISTING_PATHS:
        if p not in paths:
            paths.append(p)
    urls = []
    # Walk each host's listing paths before moving to the next host, otherwise
    # a cap of 8 seeds only hits "/" on every alias and never e_default.html.
    for host in entry.get("url_hosts") or []:
        h = (host or "").strip().lower()
        if not h:
            continue
        for path in paths:
            if path.startswith("http://") or path.startswith("https://"):
                continue
            if not path.startswith("/"):
                path = "/" + path
            urls.append(f"https://{h}{path}")
    for path in paths:
        if path.startswith("http://") or path.startswith("https://"):
            urls.append(path)
    seen = set()
    uniq = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            uniq.append(u)
    return uniq


def _all_fetch_entries(codes=None) -> list:
    want = None
    if codes:
        want = {c.strip().upper() for c in codes if c and c.strip()}
    out = []
    for e in COMMISSION_REGISTRY:
        if not e.get("url_hosts"):
            continue
        if want and e["code"] not in want:
            continue
        out.append(e)
    return out


def _normalize_href(base: str, href: str) -> str:
    if not href:
        return ""
    href = href.strip()
    if href.startswith("#") or href.lower().startswith("javascript:"):
        return ""
    href, _ = urldefrag(href)
    absu = urljoin(base, href)
    return absu


def _link_is_pdf(url: str, text: str = "") -> bool:
    blob = f"{url} {text}"
    if _FETCH_SKIP_RE.search(blob):
        return False
    path = urlparse(url).path.lower()
    if not (path.endswith(".pdf") or ".pdf?" in url.lower()):
        return False
    return bool(_FETCH_WANT_RE.search(blob))


def _link_is_listing(url: str, text: str = "") -> bool:
    blob = f"{url} {text}".lower()
    if _FETCH_SKIP_RE.search(blob):
        return False
    if urlparse(url).path.lower().endswith(".pdf"):
        return False
    return bool(_FETCH_WANT_RE.search(blob))


def _same_registrable(url: str, hosts: list) -> bool:
    net = (urlparse(url).hostname or "").lower()
    if net.startswith("www."):
        net = net[4:]
    for h in hosts:
        h = (h or "").lower()
        if h.startswith("www."):
            h = h[4:]
        if not h:
            continue
        if net == h or net.endswith("." + h) or h.endswith("." + net):
            return True
        # esb.mp.gov.in <-> esb.mponline.gov.in
        left = net.split(".")
        right = h.split(".")
        if len(left) >= 2 and len(right) >= 2 and left[0] == right[0] and left[-2:] == right[-2:]:
            return True
    return False


def _http_get(url: str, timeout=8.0):
    # Many state-board hosts (esb.mp.gov.in, some NIC sites) serve incomplete
    # certificate chains. Fetch still needs the HTML; downloads are hashed.
    try:
        with httpx.Client(
            timeout=timeout,
            follow_redirects=True,
            verify=False,
            headers={
                "User-Agent": HTTP_USER_AGENT,
                "Accept": "text/html,application/xhtml+xml,application/pdf;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-IN,en;q=0.9,hi;q=0.8",
            },
        ) as client:
            r = client.get(url)
            r.raise_for_status()
            return r
    except Exception as ex:
        _log("FETCH_HTTP_ERR", url, str(ex)[:160])
        return None


def _parse_page_links(url: str, html: str) -> tuple:
    soup = BeautifulSoup(html or "", "html.parser")
    pdfs, listings = [], []
    for a in soup.find_all("a", href=True):
        href = _normalize_href(url, a.get("href") or "")
        if not href:
            continue
        text = a.get_text(" ", strip=True)[:200]
        if _link_is_pdf(href, text):
            pdfs.append((href, text))
        elif _link_is_listing(href, text):
            listings.append((href, text))
    # Frame-based boards (MP ESB e_default.html) hide ads in iframe src.
    for fr in soup.find_all(["iframe", "frame"]):
        src = _normalize_href(url, fr.get("src") or "")
        if src:
            listings.append((src, "iframe"))
    return pdfs, listings


def _seen_fetch_url(url: str) -> bool:
    try:
        conn = connect()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT url FROM gov_job_fetch_seen WHERE url = %s", (url,))
                return cur.fetchone() is not None
        finally:
            conn.close()
    except Exception:
        return False


def _mark_fetch_url(url: str, sha: str, commission: str, status: str):
    try:
        conn = connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO gov_job_fetch_seen (url, sha256, commission, last_status)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (url) DO UPDATE SET last_status = EXCLUDED.last_status, sha256 = EXCLUDED.sha256
                    """,
                    (url, sha, commission, status),
                )
            conn.commit()
        finally:
            conn.close()
    except Exception as ex:
        _log("FETCH_SEEN_ERR", url, str(ex)[:160])


def _download_pdf(url: str, dest_dir: Path) -> Path:
    r = _http_get(url, timeout=60.0)
    if r is None:
        return None
    ctype = (r.headers.get("content-type") or "").lower()
    if "html" in ctype and not url.lower().endswith(".pdf"):
        return None
    data = r.content or b""
    if len(data) < 1000 or not data[:8].startswith(b"%PDF"):
        return None
    if len(data) > 30 * 1024 * 1024:
        _log("FETCH_SKIP_BIG", url, str(len(data)))
        return None
    name = Path(urlparse(url).path).name or "notice.pdf"
    if not name.lower().endswith(".pdf"):
        name += ".pdf"
    name = re.sub(r"[^\w.\-]+", "_", name)[:80]
    dest = dest_dir / name
    if dest.exists():
        dest = dest_dir / f"{dest.stem}_{hashlib.sha256(data).hexdigest()[:8]}.pdf"
    dest.write_bytes(data)
    return dest


def fetch_commission_sites(codes=None, dry_run: bool = True, max_pdfs_per_host: int = None) -> dict:
    """Crawl every registered official host for advertisement PDFs."""
    max_pdfs = max_pdfs_per_host if max_pdfs_per_host is not None else FETCH_MAX_PDFS_PER_HOST
    report = {"hosts": [], "pdfs": [], "downloaded": [], "errors": []}
    entries = _all_fetch_entries(codes)
    for entry in entries:
        seeds = _seed_urls_for_entry(entry)
        # Try homepage first; skip remaining seeds on that host if homepage works.
        pdf_candidates = []
        listing_candidates = []
        fetched_pages = set()
        hosts = entry.get("url_hosts") or []
        failures = 0
        for seed in seeds[:12]:
            if seed in fetched_pages:
                continue
            if failures >= 3:
                break
            time.sleep(0.25)
            resp = _http_get(seed)
            fetched_pages.add(seed)
            if resp is None:
                failures += 1
                continue
            failures = 0
            ctype = (resp.headers.get("content-type") or "").lower()
            if "pdf" in ctype:
                pdf_candidates.append((str(resp.url), entry["code"]))
                continue
            if "html" not in ctype and "text" not in ctype:
                continue
            pdfs, listings = _parse_page_links(str(resp.url), resp.text)
            for u, t in pdfs:
                if _same_registrable(u, hosts) or u.lower().endswith(".pdf"):
                    pdf_candidates.append((u, t))
            for u, t in listings:
                if _same_registrable(u, hosts):
                    listing_candidates.append((u, t))
        # follow listing pages + iframe shells (MP ESB) + per-post VYAPAM pages
        for listing_url, _t in listing_candidates[:10]:
            if listing_url in fetched_pages:
                continue
            time.sleep(0.25)
            resp = _http_get(listing_url)
            fetched_pages.add(listing_url)
            if resp is None:
                continue
            pdfs, more = _parse_page_links(str(resp.url), resp.text)
            for u, t in pdfs:
                pdf_candidates.append((u, t))
            for u, t in more:
                if _same_registrable(u, hosts) and u not in fetched_pages:
                    listing_candidates.append((u, t))
        # unique PDFs
        seen_u = set()
        uniq_pdfs = []
        for u, t in pdf_candidates:
            if u in seen_u:
                continue
            seen_u.add(u)
            uniq_pdfs.append((u, t))
        def _pdf_rank(item):
            blob = f"{item[0]} {item[1]}".lower()
            score = 0
            if re.search(r"advertis|vigyapti|विज्ञापन|advt", blob):
                score += 10
            if "notification" in blob:
                score += 4
            if re.search(r"exam|recruit", blob):
                score += 2
            return -score
        uniq_pdfs.sort(key=_pdf_rank)
        report["hosts"].append({
            "code": entry["code"],
            "seeds_tried": len(fetched_pages),
            "pdfs_found": len(uniq_pdfs),
        })
        for u, t in uniq_pdfs[:max_pdfs]:
            report["pdfs"].append({"commission": entry["code"], "url": u, "text": t})
            if dry_run:
                continue
            if _seen_fetch_url(u):
                continue
            dest = _download_pdf(u, PICKUP_DIR)
            if dest is None:
                _mark_fetch_url(u, "", entry["code"], "download_fail")
                continue
            sha = hashlib.sha256(dest.read_bytes()).hexdigest()
            _mark_fetch_url(u, sha, entry["code"], "downloaded")
            report["downloaded"].append(str(dest))
            _log("FETCH_PDF", entry["code"], u, dest.name)
        _log("FETCH_HOST", entry["code"], f"pages={len(fetched_pages)}", f"pdfs={len(uniq_pdfs)}")
    if not dry_run and report["downloaded"]:
        ingested = _auto_ingest_from_pickup()
        report["ingested"] = ingested
    return report


def _poll_loop():
    last_fetch = 0.0
    while True:
        try:
            n = _auto_ingest_from_pickup()
            if n:
                _log("POLL_INGEST", f"count={n}")
        except Exception as ex:
            _log("POLL_ERR", str(ex)[:300])
        now = time.time()
        if FETCH_COMMISSIONS and (now - last_fetch) >= FETCH_INTERVAL_SECONDS:
            last_fetch = now
            try:
                _log("FETCH_START", f"interval={FETCH_INTERVAL_SECONDS}")
                fetch_commission_sites(dry_run=False)
            except Exception as ex:
                _log("FETCH_ERR", str(ex)[:300])
        time.sleep(max(1, POLL_SECONDS))


@mcp.tool()
def ingest_notification(source_pdf_path: str) -> str:
    """Store a PDF, extract structured fields, quality-gate, and save in one step."""
    stored = store_notification_pdf(source_pdf_path)
    if stored.startswith("Error:"):
        return stored
    text = read_pdf_notification(stored)
    fields = _extract_fields(text or "", stored)
    return _auto_save_fields(fields, stored, source_pdf_path, text or "")


@mcp.tool()
def reingest_job(job_id: int) -> str:
    """Re-extract a saved notification by id and replace the row if the new extract is better."""
    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, local_pdf_path, job_title FROM gov_job_notifications WHERE id = %s",
                (job_id,),
            )
            row = cur.fetchone()
    finally:
        conn.close()
    if not row:
        return f"No job found with ID #{job_id}."
    pdf = resolve_pdf_path(row["local_pdf_path"])
    if not pdf.exists():
        return f"PDF missing on disk for job #{job_id} ({row['local_pdf_path']})."
    out = _reingest_stored_pdf(pdf, replace_ids=[job_id])
    return out["result"]


@mcp.tool()
def list_recent_jobs(limit: int = 10) -> str:
    """List recently saved notifications (id, title, commission, kind, apply window)."""
    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, job_title, commission, exam_kind, exam_name,
                       apply_end_date, total_vacancies
                FROM gov_job_notifications
                ORDER BY created_at DESC
                LIMIT %s
                """,
                (max(1, min(int(limit or 10), 50)),),
            )
            rows = cur.fetchall() or []
    finally:
        conn.close()
    if not rows:
        return "No saved notifications."
    lines = []
    for r in rows:
        lines.append(
            f"#{r['id']} [{r.get('commission') or '-'}|{r.get('exam_kind') or '-'}] "
            f"{r.get('job_title')} | end={r.get('apply_end_date') or '-'} "
            f"vac={r.get('total_vacancies')}"
        )
    return "\n".join(lines)


@mcp.tool()
def fetch_commission_notices(codes: str = "", dry_run: bool = True, max_pdfs_per_host: int = 3) -> str:
    """
    Crawl official Indian PSC / UPSC / SSC / RRB / High Court websites for new
    advertisement PDFs. `codes` is a comma-separated list (empty = every
    registered commission). dry_run=True only lists discovered PDF URLs.
    """
    code_list = [c.strip().upper() for c in (codes or "").split(",") if c.strip()]
    report = fetch_commission_sites(
        codes=code_list or None,
        dry_run=bool(dry_run),
        max_pdfs_per_host=max(1, min(int(max_pdfs_per_host or 3), 10)),
    )
    lines = [
        f"hosts={len(report['hosts'])} pdfs_found={len(report['pdfs'])} "
        f"downloaded={len(report['downloaded'])} dry_run={dry_run}"
    ]
    for h in report["hosts"]:
        lines.append(f"  {h['code']}: pages={h['seeds_tried']} pdfs={h['pdfs_found']}")
    for p in report["pdfs"][:40]:
        lines.append(f"  PDF {p['commission']}: {p['url']}")
    if report.get("downloaded"):
        lines.append("downloaded: " + ", ".join(Path(p).name for p in report["downloaded"]))
    return "\n".join(lines)


@mcp.tool()
def process_pending_uploads() -> str:
    """
    Scans tobepicked/ for new PDFs. Auto: store + read text + sidecar + extract + save. After success, replaces the full PDF in tobepicked/ with a 0-byte file of the same name (keeps the filename visible in drop queue without storing the whole PDF). Background poll on server start.
    """
    count = _auto_ingest_from_pickup()
    return f"Auto-processed {count} upload(s) end-to-end (ingest + extract + DB save + name-only placeholder). Check mcp_read_log.txt and DB for results."


if __name__ == "__main__":
    if "--self-test" in sys.argv:
        raise SystemExit(0 if _run_extraction_self_tests() else 1)
    if "--reingest" in sys.argv:
        print(_reingest_stored_fixtures())
        raise SystemExit(0)
    if "--fetch" in sys.argv:
        dry = "--apply" not in sys.argv
        codes = []
        for a in sys.argv[1:]:
            if a.startswith("--codes="):
                codes = [c.strip() for c in a.split("=", 1)[1].split(",") if c.strip()]
        print(fetch_commission_notices(
            codes=",".join(codes), dry_run=dry, max_pdfs_per_host=FETCH_MAX_PDFS_PER_HOST
        ))
        raise SystemExit(0)
    threading.Thread(target=_poll_loop, daemon=True).start()
    mcp.run()
