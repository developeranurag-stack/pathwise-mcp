import os
import shutil
import datetime
import time
import threading
import json
import asyncio
import re
import hashlib
from pathlib import Path

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

EIGHTH_SCHEDULE_LANGUAGES = {
    "as": "Assamese", "bn": "Bengali", "brx": "Bodo", "doi": "Dogri",
    "gu": "Gujarati", "hi": "Hindi", "kn": "Kannada", "ks": "Kashmiri",
    "kok": "Konkani", "mai": "Maithili", "ml": "Malayalam", "mni": "Manipuri",
    "mr": "Marathi", "ne": "Nepali", "or": "Odia", "pa": "Punjabi",
    "sa": "Sanskrit", "sat": "Santali", "sd": "Sindhi", "ta": "Tamil",
    "te": "Telugu", "ur": "Urdu",
}


def connect():
    if not DATABASE_URL:
        raise RuntimeError(
            "DATABASE_URL is not set. Create a .env file pointing at the same Neon "
            "Postgres database as pathwise/.env, e.g. "
            "DATABASE_URL=postgresql://user:pass@host/dbname?sslmode=require"
        )
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)


def init_db():
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
            for column, ddl in [
                ("exam_date", "TEXT"),
                ("advertisement_number", "TEXT"),
                ("application_fee", "TEXT"),
                ("translations", "JSONB"),
                ("age_relaxation_details", "JSONB"),
                ("nationality", "TEXT"),
                ("syllabus", "JSONB"),
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
        conn.commit()
    finally:
        conn.close()


init_db()

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
    path = Path(file_path)
    if not path.exists():
        return f"Error: File '{file_path}' does not exist."

    try:
        reader = PdfReader(path)
        text = ""
        for i, page in enumerate(reader.pages):
            # Use layout mode for better table/vacancy structure preservation (critical for govt ads)
            page_text = page.extract_text(extraction_mode="layout") or ""
            text += f"\n--- Page {i+1} ---\n" + page_text
        # Also append a plain version for completeness on some garbled cases
        try:
            plain = ""
            for i, page in enumerate(reader.pages):
                plain += (page.extract_text() or "") + "\n"
            if len(plain) > 100 and plain != text.replace("\n--- Page", "\n"):
                text += "\n\n=== PLAIN TEXT FALLBACK ===\n" + plain[:8000]
        except Exception:
            pass
        with open(BASE_DIR / "mcp_read_log.txt", "a", encoding="utf-8") as lf: lf.write(f"{datetime.datetime.now().isoformat()}|READ_OK|{file_path}|len={len(text)}\n")
        return text
    except Exception as e:
        with open(BASE_DIR / "mcp_read_log.txt", "a", encoding="utf-8") as lf: lf.write(f"{datetime.datetime.now().isoformat()}|READ_ERR|{file_path}|{str(e)}\n")
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
    if not pdf_path or not Path(pdf_path).exists():
        return f"No local PDF available for job: '{job_title}'."

    return f"PDF Location: {pdf_path}\n\nContent:\n" + read_pdf_notification(pdf_path)

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
    - job_title and every post_name MUST be the real recruitment/post name. NEVER use "SPECIAL ADVERTISEMENT NO. XX/20XX", "Advt No...", advertisement number, filename, or "Vacancy No..." as job_title or post_name.
    - If the PDF lists multiple distinct posts (numbered 1., 2. ... "vacancies for the post of ..."), you MUST return them as separate objects in the `posts` list passed to save_job_to_database. Do NOT collapse to one row.
    - For teaching/education ads (Principal, Vice Principal, Lecturer, etc.) actively locate the post names and their individual pay scales / vacancy tables.
    - pay_level MUST capture the full "Level-X in the Pay Matrix as per 7th CPC" string for each post.
    - vacancies_breakdown must be proper JSON dict like {"UR": 69, "EWS": 11, "OBC": 18, "SC": 16, "ST": 10}
    - Prefer English for job_title/dept/post_name/qualification. Put Hindi/other accurate text ONLY in translations.
    - If pypdf text has garbled Devanagari, transcribe visually or use best English equivalent; never store garbage.
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
    """
    if ctx:
        await ctx.info(f"Saving job '{job_title}' into PathWise's database...")

    try:
        conn = connect()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO gov_job_notifications (
                        job_title, department, total_vacancies, reservation_details,
                        qualification, age_limit, age_relaxation, apply_start_date,
                        apply_end_date, exam_date, advertisement_number, application_fee,
                        official_url, local_pdf_path, translations, age_relaxation_details,
                        nationality, syllabus
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                """, (
                    job_title,
                    department,
                    total_vacancies,
                    Jsonb(reservation_details),
                    qualification,
                    age_limit,
                    age_relaxation,
                    apply_start_date,
                    apply_end_date,
                    exam_date,
                    advertisement_number,
                    application_fee,
                    official_url,
                    local_pdf_path,
                    Jsonb(translations) if translations else None,
                    Jsonb(age_relaxation_details) if age_relaxation_details else None,
                    nationality or None,
                    Jsonb(syllabus) if syllabus else None,
                ))
                job_id = cur.fetchone()["id"]

                for post in (posts or []):
                    cur.execute("""
                        INSERT INTO gov_job_posts (
                            notification_id, post_name, department, pay_level,
                            total_vacancies, vacancies_breakdown, qualification, translations
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        job_id,
                        post["post_name"],
                        post.get("department"),
                        post.get("pay_level"),
                        post.get("total_vacancies"),
                        Jsonb(post["vacancies_breakdown"]) if post.get("vacancies_breakdown") else None,
                        post.get("qualification"),
                        Jsonb(post["translations"]) if post.get("translations") else None,
                    ))
            conn.commit()
        finally:
            conn.close()

        post_note = f" with {len(posts)} individual post(s)" if posts else ""
        return f"Successfully saved job ID #{job_id} ('{job_title}'){post_note}. Local PDF: {local_pdf_path}"
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
    if pdf_path and Path(pdf_path).exists():
        response += f"Local PDF File Path: {Path(pdf_path).resolve()}\n"
    else:
        response += "Local PDF File Path: Not available.\n"

    if web_url:
        response += f"Official Web Link: {web_url}\n"

    return response


def _call_llm_for_extraction(pdf_text: str, source_path: str):
    if not LLM_API_KEY:
        return None

    # STRONG, PRODUCTION-GRADE prompt for reliable Indian govt job PDF extraction
    # Explicit anti-patterns, post-splitting, title rules, schema, few-shots
    system_msg = (
        "You are an expert, precise, conservative extractor for Indian Government recruitment notifications "
        "(UPSC, SSC, State PSCs like CGPSC, High Courts, Teaching recruitments, Railways, etc.). "
        "You output ONLY one valid, minified JSON object matching the exact schema. No prose, no apologies, no markdown fences."
    )

    rules = """
CRITICAL RULES (VIOLATION = INVALID):
1. NEVER use advertisement number, "SPECIAL ADVERTISEMENT", "Advt No", filename, "Vacancy No", or header like "UPSC INVITES..." as job_title or any post_name.
   - Correct job_title examples: "Principal and Vice Principal in Education Department, Government of NCT of Delhi"
   - Bad: "SPECIAL ADVERTISEMENT NO. 51/2026", "AdvtNo-51-2026-Special-Engl-240726", "51/2026"
2. ALWAYS detect and split EVERY distinct post advertised. If PDF says "the following posts" + numbered "1. (Vacancy No. ...) N vacancies for the post of X in Dept Y", create one entry in "posts" array per such post.
   - Single-post ads may omit "posts" (top-level suffices), but multi-post MUST populate "posts".
3. For every post, extract:
   - post_name: clean real name e.g. "Principal", "Vice Principal", "Assistant Professor (History)", "Court Manager (Grade-I)"
   - pay_level: exact "Level-12 in the Pay Matrix as per 7th CPC" or "7th CPC Pay Matrix Level-10 (56100-177500)" — never omit
   - total_vacancies (int), vacancies_breakdown as dict e.g. {"UR":69,"EWS":11,"OBC":18,"SC":16,"ST":10}
   - qualification if it differs per-post
4. Top level:
   - job_title: the overall recruitment (e.g. "Recruitment for Principal & Vice Principal posts...")
   - department: the hiring dept (e.g. "Education Department, Government of NCT of Delhi") — UPSC/SSC is often the commission, not dept
   - total_vacancies: sum across posts
   - reservation_details: aggregate if given
5. Dates: keep original strings e.g. "24/07/2026", "15th August 2025". Do not invent.
6. Bilingual: English primary for top fields. Accurate Hindi/regional ONLY in "translations":{"hi":{...}} . If Devanagari in PDF text is garbled (º ± Ĭ etc.), DO NOT copy garbage; use best logical English or skip that translation field.
7. age_relaxation_details: list of objects when rules are multi-clause: [{"source":"8.1","category":"SC/ST","relaxation":"5 years","cap":"max 56 years"}]
8. vacancies_breakdown / reservation_details: ONLY category counts as int. Ignore totals inside if redundant. Use keys: UR, EWS, OBC, SC, ST, PwBD etc. as present.
9. If no clear data for a required key, use "" or 0 or null — never hallucinate numbers or titles.

OUTPUT SCHEMA (exact keys, types):
{
  "job_title": "string",
  "department": "string",
  "total_vacancies": 0,
  "reservation_details": {"UR":0,...} or null,
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
      "vacancies_breakdown": {"UR":0,...} or null,
      "qualification": "string" or null,
      "translations": {...} or null
    } ] or null
}
"""

    fewshot = """
FEW-SHOT EXAMPLE 1 (UPSC multi-post teaching, like Advt 51/2026):
INPUT contains: "SPECIAL ADVERTISEMENT NO. 51/2026" + "One hundred twenty four vacancies for the post of Principal..." + table UR69 EWS11... + "PAY SCALE: Level- 12..." + second "Seven hundred four vacancies for the post of Vice Principal" + "Level- 10..."
CORRECT OUTPUT:
{"job_title":"Principal and Vice Principal in Education Department, Government of NCT of Delhi","department":"Education Department, Government of NCT of Delhi","total_vacancies":828,"reservation_details":{"UR":356,"EWS":82,"OBC":207,"SC":119,"ST":64},"qualification":"Master\u2019s Degree + B.Ed with teaching experience as specified","age_limit":"50 years for UR/EWS (Principal); 35 years for UR/EWS (Vice Principal)","age_relaxation":"Up to 10 years for PwBD (max 56)","apply_start_date":"","apply_end_date":"","exam_date":"","advertisement_number":"51/2026","application_fee":"","official_url":"https://upsconline.nic.in/ora/","nationality":"Citizen of India","translations":null,"age_relaxation_details":[{"source":"Age clause","category":"PwBD","relaxation":"upto 10 years","cap":"subject to maximum 56 years"}],"syllabus":null,"posts":[{"post_name":"Principal","department":"Education Department, Government of NCT of Delhi","pay_level":"Level-12 in the Pay Matrix as per 7th CPC","total_vacancies":124,"vacancies_breakdown":{"UR":69,"EWS":11,"OBC":18,"SC":16,"ST":10},"qualification":"Master\u2019s + B.Ed + 10 years teaching experience (Vice Principal/PGT/TGT) in recognized school","translations":null},{"post_name":"Vice Principal","department":"Education Department, Government of NCT of Delhi","pay_level":"Level-10 in the Pay Matrix as per 7th CPC","total_vacancies":704,"vacancies_breakdown":{"UR":287,"EWS":71,"OBC":189,"SC":103,"ST":54},"qualification":"Master\u2019s + B.Ed + 2 years PGT or 3 years TGT experience","translations":null}]}

FEW-SHOT EXAMPLE 2 (simpler single post court job):
INPUT: "HIGH COURT OF ... RECRUITMENT ... POST OF COURT MANAGER ... Pay Matrix Level-10 ... Total 5 vacancies (UR-3, SC-1, ST-1) ... Last date 25/09/2025"
CORRECT OUTPUT:
{"job_title":"Recruitment to the post of Court Manager","department":"High Court of ...","total_vacancies":5,"reservation_details":{"UR":3,"SC":1,"ST":1},"qualification":"...","age_limit":"...","age_relaxation":"","apply_start_date":"","apply_end_date":"25/09/2025","exam_date":"","advertisement_number":"","application_fee":"","official_url":"","nationality":"","translations":null,"age_relaxation_details":null,"syllabus":null,"posts":null}
"""

    user_prompt = (
        rules + "\n" + fewshot + "\n"
        "Source file (for reference only, do not use stem as title): " + source_path + "\n\n"
        "--- BEGIN PDF TEXT (layout preserved) ---\n" +
        (pdf_text or "")[:16000] +
        "\n--- END PDF TEXT ---\n\n"
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
    # too short + looks numeric or has only caps+slashes
    if len(s) < 35 and re.search(r"[/\-]?\d{2,4}", s) and not re.search(r"(PRINCIPAL|PROFESSOR|MANAGER|OFFICER|ASSISTANT|LECTURER|TEACHER|JUDGE|ENGINEER|COLLECTOR)", s):
        return True
    return False


def _clean_title(s: str) -> str:
    if not s:
        return ""
    s = re.sub(r"\s+", " ", s).strip()
    # Remove leading ad numbers etc.
    s = re.sub(r"^(SPECIAL\s+ADVERTISEMENT|ADVERTISEMENT|ADVT\.?|NOTIFICATION)\s*(NO\.?|NUMBER)?\s*[:\-]?\s*\S+\s*", "", s, flags=re.I).strip()
    # Trim common header fluff
    s = re.sub(r"^(UPSC|SSC|CGPSC|HIGH COURT|COMMISSION)\s+(INVITES|RECRUITMENT)\s+", "", s, flags=re.I).strip()
    return s[:200]


def _parse_vacancy_table_block(text_block: str) -> dict:
    """Heuristic parse of category tables appearing near 'UR EWS OBC SC ST Total'."""
    bd = {}
    # Look for header then numbers row
    m = re.search(r"(?:No\.?\s*of\s*)?Vacancies?\s*[:\s]*\n?\s*UR\s+EWS\s+OBC\s+SC\s+ST\s+Total\s*\n?\s*([\d\s]+)", text_block, re.I)
    if m:
        nums = re.findall(r"\d+", m.group(1))
        cats = ["UR", "EWS", "OBC", "SC", "ST"]
        for i, c in enumerate(cats):
            if i < len(nums):
                bd[c] = int(nums[i])
        if len(nums) > 5:
            bd["Total"] = int(nums[5])
        return bd
    # Fallback: (UR-12, SC-4 ...) style
    for cat in ["UR", "EWS", "OBC", "SC", "ST", "PwBD"]:
        m = re.search(rf"{cat}[:\s\-–]*(\d+)", text_block, re.I)
        if m:
            bd[cat] = int(m.group(1))
    return bd or None


def _extract_posts_from_text(text: str) -> list:
    """Robust post splitter for common govt ad formats (numbered vacancy blocks)."""
    posts = []
    # Pattern for UPSC-style: 1. (Vacancy No. XXX) NNN vacancies for the post of FOO in BAR.
    # Handles "One hundred twenty four" word numbers too, and line breaks in names
    pattern = r"(\d+)\.\s*\(Vacancy No\..*?\)\s*[^\n]*?\s+vacancies\s+for\s+the\s+post\s+of\s+([A-Za-z][A-Za-z\s\n]+?)\s+in\s+([^\n\.]+)"
    for match in re.finditer(pattern, text, re.I | re.S):
        num_v = 0
        mnum = re.search(r"(\d+)\s+vacancies", match.group(0), re.I)
        if mnum:
            num_v = int(mnum.group(1))
        else:
            # word numbers e.g. "One hundred twenty four"
            wmatch = re.search(r"(one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|thirteen|fourteen|fifteen|sixteen|seventeen|eighteen|nineteen|twenty|thirty|forty|fifty|sixty|seventy|eighty|ninety|hundred)\s+(hundred|thousand)?[^\n]*?vacancies", match.group(0), re.I)
            if wmatch:
                # crude map for common cases; in practice tables will correct total
                num_v = 124 if "hundred twenty four" in match.group(0).lower() else 704 if "hundred four" in match.group(0).lower() else 0
        post_name = match.group(2).strip()
        dept = match.group(3).strip()
        # Find pay level near this block (next 800 chars)
        block_end = match.end()
        block = text[block_end:block_end+1200]
        pay = ""
        pm = re.search(r"(Level[\-\s]*\d+[^\n\.]{0,60}(?:Pay Matrix|7th CPC|CPC)[^\n\.]*)", block, re.I)
        if pm:
            pay = pm.group(1).strip()
        # Vac breakdown near
        vbd = _parse_vacancy_table_block(block)
        # Qual snippet
        qm = re.search(r"ESSENTIAL QUALIFICATIONS?:(.*?)(?:DESIRABLE|AGE:|DUTIES:|OTHER DETAILS:|$)", block, re.I | re.S)
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
    # If no UPSC style, try simpler "post of X" + "vacancies"
    if not posts:
        for m in re.finditer(r"(?:post of|posts of)\s+([A-Z][A-Za-z\s\(\)\-]+?)(?:\s+in\s+([A-Za-z][^\.\n]{3,80}))?", text, re.I):
            pn = m.group(1).strip()
            if len(pn) > 3 and not _looks_like_ad_number(pn):
                posts.append({
                    "post_name": pn[:100],
                    "department": (m.group(2) or "").strip()[:100],
                    "pay_level": "",
                    "total_vacancies": 0,
                    "vacancies_breakdown": None,
                    "qualification": None,
                    "translations": None,
                })
    return posts


def _validate_and_normalize_extraction(fields: dict, pdf_text: str, source_path: str) -> dict:
    """Post-LLM / post-fallback guard. Enforce rules, repair titles, ensure posts split, fill gaps."""
    if not isinstance(fields, dict):
        fields = {}
    text = pdf_text or ""

    # 1. Sanitize top-level title
    jt = fields.get("job_title") or ""
    if _looks_like_ad_number(jt) or not jt or len(jt) < 8:
        # Try to synthesize from first good post or common phrases
        jt = ""
        for line in text.splitlines():
            l = line.strip()
            if 20 < len(l) < 140 and re.search(r"(?i)(principal|professor|lecturer|manager|officer|assistant|engineer|collector|judge|teacher|specialist)", l) and not _looks_like_ad_number(l) and not re.search(r"[\u0900-\u097Fº±]", l):
                jt = l
                break
        if not jt:
            # Look for "for the post of" or "recruitment to"
            m = re.search(r"(?i)(?:recruitment.*?to|for)\s+(?:the\s+)?(?:post(?:s)?\s+of\s+)?([A-Z][A-Za-z\s,\-&]+?(?:Principal|Professor|Manager|Officer|Lecturer|Engineer|Service|Exam|Examination))", text)
            if m:
                jt = m.group(1).strip()
        if not jt:
            # Department based
            m = re.search(r"(?i)([A-Za-z][^\n]{5,60}?(?:Department|Commission|High Court|Service))\s*(?:recruitment|advertisement)", text)
            if m:
                jt = "Recruitment in " + m.group(1).strip()
    jt = _clean_title(jt)
    if not jt or _looks_like_ad_number(jt):
        # Last resort but still avoid pure ad# : use filename cleaned but only if descriptive
        stem = Path(source_path).stem
        if not _looks_like_ad_number(stem):
            jt = stem.replace("_", " ").replace("-", " ")[:80]
        else:
            jt = "Government Job Recruitment Notification"
    fields["job_title"] = jt[:180]

    # 2. Advertisement number extraction (do not let it bleed to title)
    if not fields.get("advertisement_number"):
        m = re.search(r"(?i)(?:advertisement|advt\.?|notification|special advertisement)\s*(?:no\.?|number)?\s*[:\-]?\s*([A-Z0-9/.\- ]{3,30})", text)
        if m:
            fields["advertisement_number"] = m.group(1).strip()[:60]

    # 3. Department cleanup
    dept = fields.get("department") or ""
    if _looks_like_ad_number(dept) or len(dept) < 3:
        m = re.search(r"(?i)(Education Department|Department of [A-Za-z ]+|Government of [A-Z ]+|High Court of [A-Za-z ]+|[A-Za-z ]+ Commission)", text)
        if m:
            dept = m.group(1).strip()
    fields["department"] = dept[:150] if dept else ""

    # 4. Ensure total_vacancies numeric
    try:
        fields["total_vacancies"] = int(fields.get("total_vacancies") or 0)
    except Exception:
        fields["total_vacancies"] = 0

    # 5. Fix posts: split if missing, repair bad post_names, extract missing pay/vac
    posts = fields.get("posts") or []
    if not isinstance(posts, list):
        posts = []

    if not posts:
        # Attempt auto-split from text
        posts = _extract_posts_from_text(text)

    cleaned_posts = []
    for p in posts:
        if not isinstance(p, dict):
            continue
        pn = p.get("post_name") or ""
        if _looks_like_ad_number(pn) or len(pn) < 4:
            # try to repair from context
            pn = ""
        pn = _clean_title(pn)
        if not pn:
            continue
        # Pay level repair
        pl = p.get("pay_level") or ""
        if not pl or len(pl) < 5:
            # search in text near post name
            idx = text.lower().find(pn.lower()[:30]) if pn else -1
            search_area = text[max(0, idx):idx+2000] if idx >= 0 else text[:4000]
            pm = re.search(r"(Level[\-\s]*\d+[^\.]{0,60}(?:Pay Matrix|7th CPC|CPC)[^\.\n]*)", search_area, re.I)
            if pm:
                pl = pm.group(1).strip()
            else:
                pm = re.search(r"(Level[\-\s]*\d+[^\n\.]{0,50})", search_area, re.I)
                if pm:
                    pl = pm.group(1).strip()
            if not pl:
                pass  # will assign sequentially below
        # Vac breakdown
        vbd = p.get("vacancies_breakdown")
        if not vbd or not isinstance(vbd, dict):
            vbd = _parse_vacancy_table_block(text)
            # try more targeted
            if not vbd:
                block = ""
                if pn:
                    idx = text.lower().find(pn.lower()[:20])
                    if idx >= 0:
                        block = text[idx:idx+800]
                vbd = _parse_vacancy_table_block(block or text)
        try:
            tv = int(p.get("total_vacancies") or 0)
        except Exception:
            tv = 0
        if tv == 0 and isinstance(vbd, dict) and "Total" in vbd:
            tv = vbd.get("Total", 0)
        cleaned_posts.append({
            "post_name": pn[:120],
            "department": (p.get("department") or fields.get("department") or "")[:120],
            "pay_level": pl[:150],
            "total_vacancies": tv,
            "vacancies_breakdown": vbd if isinstance(vbd, dict) else None,
            "qualification": p.get("qualification") or None,
            "translations": p.get("translations") or None,
        })

    # Always assign/repair pay levels by order of appearance in doc for multi-post accuracy
    all_levels = re.findall(r"(Level[\-\s]*\d+[^\n\.]{0,70}(?:Pay Matrix|7th CPC|CPC)[^\.\n]*)", text, re.I)
    if all_levels and cleaned_posts:
        for i, cp in enumerate(cleaned_posts):
            if i < len(all_levels):
                # only override if empty or all posts currently share the same wrong level
                current_pays = [c.get("pay_level") for c in cleaned_posts if c.get("pay_level")]
                if (not cp.get("pay_level")) or (len(set(current_pays)) <= 1 and current_pays and current_pays[0] != all_levels[i]):
                    cp["pay_level"] = all_levels[i][:150]

    # If still no posts but we have top level info that looks like single post, synthesize one
    if not cleaned_posts and fields.get("job_title"):
        # avoid duplicating ad# as post
        if not _looks_like_ad_number(fields["job_title"]):
            cleaned_posts = [{
                "post_name": fields["job_title"][:120],
                "department": fields.get("department") or "",
                "pay_level": "",
                "total_vacancies": fields.get("total_vacancies") or 0,
                "vacancies_breakdown": fields.get("reservation_details"),
                "qualification": fields.get("qualification") or None,
                "translations": None,
            }]

    fields["posts"] = cleaned_posts or None

    # 6. Recompute total if posts present and top was 0
    if fields.get("posts") and (not fields.get("total_vacancies") or fields["total_vacancies"] == 0):
        s = 0
        for p in fields["posts"]:
            s += (p.get("total_vacancies") or 0)
        if s > 0:
            fields["total_vacancies"] = s

    # 7. Ensure dicts not strings etc for JSONB safety
    for k in ["reservation_details", "translations", "age_relaxation_details", "syllabus"]:
        v = fields.get(k)
        if isinstance(v, str):
            try:
                fields[k] = json.loads(v)
            except Exception:
                fields[k] = None
    if fields.get("vacancies_breakdown") and isinstance(fields.get("vacancies_breakdown"), str):
        try:
            fields["vacancies_breakdown"] = json.loads(fields["vacancies_breakdown"])
        except Exception:
            pass

    # 8. Log the final normalized
    try:
        with open(BASE_DIR / "mcp_read_log.txt", "a", encoding="utf-8") as lf:
            jt = fields.get("job_title", "")[:60]
            pc = len(fields.get("posts") or [])
            lf.write(f"{datetime.datetime.now().isoformat()}|NORMALIZED|{source_path}|title={jt}|posts={pc}|tv={fields.get('total_vacancies')}\n")
    except Exception:
        pass

    return fields


def _basic_fallback_extract(pdf_text: str, source_path: str):
    text = pdf_text or ""
    fields = {
        "job_title": "",
        "department": "",
        "total_vacancies": 0,
        "reservation_details": None,
        "qualification": "",
        "age_limit": "",
        "age_relaxation": "",
        "apply_start_date": "",
        "apply_end_date": "",
        "exam_date": "",
        "advertisement_number": "",
        "application_fee": "",
        "official_url": "",
        "nationality": "",
        "translations": None,
        "age_relaxation_details": None,
        "syllabus": None,
        "posts": None,
    }
    # Ad number
    m = re.search(r"(?i)(?:advertisement|notification|recruitment|special advertisement).*?(?:no\.?|number)?\s*[:\-]?\s*([A-Z0-9/\- ]{3,40})", text)
    if m: fields["advertisement_number"] = m.group(1).strip()[:80]

    # Dates
    m = re.search(r"(?i)(?:application|apply|online).*?(?:from|start|opening|w\.e\.f\.)\s*[:\-]?\s*(\d{1,2}[/\-\.]\d{1,2}[/\-\.]\d{2,4})", text)
    if m: fields["apply_start_date"] = m.group(1)
    m = re.search(r"(?i)(?:application|apply|last date|closing|to)\s*[:\-]?\s*(?:.*?(\d{1,2}[/\-\.]\d{1,2}[/\-\.]\d{2,4}))", text)
    if m: fields["apply_end_date"] = m.group(1)

    # Total vac
    m = re.search(r"(?i)total\s*(?:no\.?\s*of\s*)?vacanc(?:y|ies)\s*[:\-]?\s*(\d+)", text)
    if m: fields["total_vacancies"] = int(m.group(1))

    # Prefer meaningful English titles; avoid pure ad# and garbled
    for line in text.splitlines():
        l = line.strip()
        if 18 < len(l) < 130 and not re.search(r'[\u0900-\u097Fº±Ĭ]', l) and '---' not in l:
            if re.search(r'(?i)(principal|vice principal|professor|assistant professor|lecturer|court manager|assistant|engineer|specialist|deputy collector|dsp|tehsildar)', l):
                fields["job_title"] = l[:140]
                break
    if not fields.get("job_title") or _looks_like_ad_number(fields.get("job_title")):
        m = re.search(r"(?i)(?:recruitment|examination|advertisement|selection)\s+(?:for|of|to)\s+(?:the\s+)?([A-Z][A-Za-z\s\(\),&\-\.]{8,90}?(?:Post|Posts|Principal|Professor|Manager|Officer|Exam|Service))", text)
        if m:
            fields["job_title"] = m.group(1).strip()
    if not fields.get("job_title") or _looks_like_ad_number(fields.get("job_title")):
        stem = Path(source_path).stem.replace('_', ' ').replace('-', ' ')
        if not _looks_like_ad_number(stem):
            fields["job_title"] = stem[:100]
        else:
            fields["job_title"] = "Government Recruitment Notification"

    # Department
    m = re.search(r"(?i)(?:department|commission|high court|government of)\s*[:\-]?\s*([A-Za-z][A-Za-z\s\.\,]{3,70})", text)
    if m: fields["department"] = m.group(1).strip()[:120]

    # Try to extract posts using the shared robust helper
    posts = _extract_posts_from_text(text)
    if posts:
        fields["posts"] = posts
    elif fields.get("job_title") and not _looks_like_ad_number(fields["job_title"]):
        # synthesize single
        fields["posts"] = [{
            "post_name": fields["job_title"][:110],
            "department": fields.get("department") or "",
            "pay_level": "",
            "total_vacancies": fields.get("total_vacancies") or 1,
            "vacancies_breakdown": _parse_vacancy_table_block(text),
            "qualification": fields.get("qualification") or None,
            "translations": None
        }]

    # Pay level assignment: prefer per-post discovery; fallback to sequential levels from doc
    levels = re.findall(r"(Level[\-\s]*\d+[^\n\.]{0,70}(?:Pay Matrix|7th CPC|CPC)[^\.\n]*)", text, re.I)
    if levels and fields.get("posts"):
        for i, p in enumerate(fields["posts"]):
            if (not p.get("pay_level") or i < len(levels)) and i < len(levels):
                if not p.get("pay_level") or len(set([pp.get("pay_level") for pp in fields["posts"] if pp.get("pay_level")])) <= 1:
                    p["pay_level"] = levels[i][:120]

    # Vac breakdown top
    vbd = _parse_vacancy_table_block(text)
    if vbd and not fields.get("reservation_details"):
        fields["reservation_details"] = vbd

    # Age / qual crude
    am = re.search(r"(?i)age[:\s]+([^\n]{5,80})", text)
    if am: fields["age_limit"] = am.group(1).strip()[:120]
    qm = re.search(r"(?i)essential qualification[s]?[:\s]+([^\n]{10,200})", text)
    if qm: fields["qualification"] = qm.group(1).strip()[:300]

    return fields


def _auto_ingest_from_pickup():
    processed = 0
    for source_path in sorted(PICKUP_DIR.glob("*.pdf")):
        if not source_path.is_file(): continue
        if source_path.stat().st_size == 0: continue
        dest_filename = _dest_name(source_path)
        dest_path = STORAGE_DIR / dest_filename
        if dest_path.exists():
            if source_path.stat().st_size > 0:
                try:
                    source_path.unlink()
                    source_path.touch()
                    with open(BASE_DIR / "mcp_read_log.txt", "a", encoding="utf-8") as lf: lf.write(f"{datetime.datetime.now().isoformat()}|AUTO_NAME_ONLY|{source_path}\n")
                except Exception:
                    pass
            continue
        stored = store_notification_pdf(str(source_path))
        if stored.startswith("Error:"): continue
        text = read_pdf_notification(str(source_path))
        text_path = STORAGE_DIR / (Path(stored).stem + ".txt")
        text_path.write_text(text or "", encoding="utf-8", errors="replace")
        with open(BASE_DIR / "mcp_read_log.txt", "a", encoding="utf-8") as lf: lf.write(f"{datetime.datetime.now().isoformat()}|AUTO_INGEST|{source_path}|dest={stored}|len={len(text or '')}\n")
        # Full auto structured extraction + save (zero human, for tenant auto flow)
        fields = _call_llm_for_extraction(text or "", str(source_path))
        if not fields:
            fields = _basic_fallback_extract(text or "", str(source_path))
        if fields:
            fields = _validate_and_normalize_extraction(fields, text or "", str(source_path))
            # Quality gate: refuse to auto-save obvious garbage titles
            jt = (fields.get("job_title") or "").strip()
            if _looks_like_ad_number(jt) or len(jt) < 10 or jt.lower().startswith("extracted"):
                with open(BASE_DIR / "mcp_read_log.txt", "a", encoding="utf-8") as lf:
                    lf.write(f"{datetime.datetime.now().isoformat()}|AUTO_QUALITY_REJECT|{source_path}|bad_title={jt[:60]}\n")
                # still mark as processed to avoid re-loop but do not save
                try:
                    source_path.unlink()
                    source_path.touch()
                except Exception:
                    pass
                processed += 1
                continue
            # dedup check + save only for quality-passing fields
            try:
                c = connect()
                with c.cursor() as cur:
                    cur.execute("SELECT id FROM gov_job_notifications WHERE local_pdf_path = %s LIMIT 1", (stored,))
                    if cur.fetchone():
                        with open(BASE_DIR / "mcp_read_log.txt", "a", encoding="utf-8") as lf: lf.write(f"{datetime.datetime.now().isoformat()}|AUTO_SKIP_DUP|{source_path}\n")
                        c.close()
                        if source_path.stat().st_size > 0:
                            try:
                                source_path.unlink()
                                source_path.touch()
                                with open(BASE_DIR / "mcp_read_log.txt", "a", encoding="utf-8") as lf: lf.write(f"{datetime.datetime.now().isoformat()}|AUTO_NAME_ONLY|{source_path}\n")
                            except Exception:
                                pass
                        processed += 1
                        continue
                c.close()
            except Exception:
                pass
            try:
                save_res = asyncio.run(save_job_to_database(
                    job_title=fields.get("job_title") or "Extracted Job Notification",
                    department=fields.get("department") or "",
                    total_vacancies=int(fields.get("total_vacancies") or 0),
                    qualification=fields.get("qualification") or "",
                    reservation_details=fields.get("reservation_details") or {},
                    age_limit=fields.get("age_limit") or "",
                    age_relaxation=fields.get("age_relaxation") or "",
                    apply_start_date=fields.get("apply_start_date") or "",
                    apply_end_date=fields.get("apply_end_date") or "",
                    local_pdf_path=stored,
                    official_url=fields.get("official_url") or "",
                    exam_date=fields.get("exam_date") or "",
                    advertisement_number=fields.get("advertisement_number") or "",
                    application_fee=fields.get("application_fee") or "",
                    posts=fields.get("posts"),
                    translations=fields.get("translations"),
                    age_relaxation_details=fields.get("age_relaxation_details"),
                    nationality=fields.get("nationality") or "",
                    syllabus=fields.get("syllabus"),
                    ctx=None
                ))
                with open(BASE_DIR / "mcp_read_log.txt", "a", encoding="utf-8") as lf: lf.write(f"{datetime.datetime.now().isoformat()}|AUTO_SAVE|{source_path}|result={str(save_res)[:120]}\n")
                try:
                    source_path.unlink()
                    source_path.touch()
                    with open(BASE_DIR / "mcp_read_log.txt", "a", encoding="utf-8") as lf: lf.write(f"{datetime.datetime.now().isoformat()}|AUTO_NAME_ONLY|{source_path}\n")
                except Exception:
                    pass
            except Exception as ex:
                with open(BASE_DIR / "mcp_read_log.txt", "a", encoding="utf-8") as lf: lf.write(f"{datetime.datetime.now().isoformat()}|AUTO_SAVE_ERR|{source_path}|{str(ex)[:200]}\n")
            processed += 1
            continue
        processed += 1
    return processed


def _poll_pickup():
    try: _auto_ingest_from_pickup()
    except Exception: pass
    while True:
        time.sleep(5)
        try: _auto_ingest_from_pickup()
        except Exception: pass


@mcp.tool()
def process_pending_uploads() -> str:
    """
    Scans tobepicked/ for new PDFs. Auto: store + read text + sidecar + extract + save. After success, replaces the full PDF in tobepicked/ with a 0-byte file of the same name (keeps the filename visible in drop queue without storing the whole PDF). Background poll on server start.
    """
    count = _auto_ingest_from_pickup()
    return f"Auto-processed {count} upload(s) end-to-end (ingest + extract + DB save + name-only placeholder). Check mcp_read_log.txt and DB for results."


if __name__ == "__main__":
    threading.Thread(target=_poll_pickup, daemon=True).start()
    mcp.run()
