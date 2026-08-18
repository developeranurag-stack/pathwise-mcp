# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

This repository is an MCP (Model Context Protocol) server, built with `fastmcp`, that ingests Indian government recruitment notifications: copy or fetch a PDF, classify issuer + exam kind, extract structured fields, and save them into PathWise’s shared Postgres.

Almost all application code lives in [server.py](server.py). Import also writes [commission_registry.json](commission_registry.json) for the sibling PathWise search aliases. Smoke checks: `python server.py --self-test`.

## Relationship to the `pathwise` project

This server is a companion tool to the sibling `pathwise` Flask app (`../pathwise`). It writes job records into `pathwise`'s own Postgres (Neon) database — the `gov_job_notifications` table, defined in both `pathwise/schema.sql` and (redundantly, `CREATE TABLE IF NOT EXISTS`) in this server's `init_db()` — rather than a local SQLite file. `pathwise`'s Flask app then serves that data read-only at `/gov-jobs` (see `pathwise/main.py` routes `gov_jobs_list`/`gov_job_detail`/`gov_job_pdf` and the matching templates).

This means:
- `.env` here **must** hold the same `DATABASE_URL` as `pathwise/.env` (see `.env.example`) — they are two processes sharing one database, not two separate databases.
- PDFs stay on local disk (`stored_pdfs/`). New rows store a **relative** `local_pdf_path` (`stored_pdfs/<file>.pdf`). PathWise resolves that via `../pathwise-mcp/stored_pdfs` or `GOV_JOB_PDF_DIR`. Both processes should still run on the same host (or share that directory).
- Do not reintroduce a local SQLite DB for this data — that would silently fork it from what `pathwise` displays.
- `init_db()` is lazy (first `connect()`). It runs `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` and `CREATE TABLE IF NOT EXISTS` (including `gov_job_posts` and `gov_job_fetch_seen`). Safe against an already-seeded database. Keep `pathwise/schema.sql` in sync by hand.

## Running the server

```bash
pip install -r requirements.txt
python server.py
```

`mcp.run()` starts the FastMCP server over stdio, the transport MCP clients expect. To use it from Claude Desktop or Claude Code, register it as an MCP server (e.g. in `claude_desktop_config.json`, or via `claude mcp add`) pointing at this `server.py` with this directory's Python environment — see your MCP client's docs for the exact config location.

Directories `stored_pdfs/` and `tobepicked/` are created next to `server.py`. Tables are ensured on first database use, not at import (so `--self-test` works without Neon).

Other entry points: `python server.py --self-test`, `--reingest`, `--fetch` / `--fetch --apply [--codes=CGVYAPAM,MPESB]`.

## Architecture

The server is organized around the FastMCP decorator pattern (`@mcp.tool()`, `@mcp.resource()`, `@mcp.prompt()`) with no separate modules — everything registers against the single `mcp = FastMCP(name="GovJobExtractor")` instance in [server.py](server.py).

**Ingest paths** (any one is enough):
1. Admin drop: PathWise uploads into `tobepicked/`; `_poll_loop` stores, extracts, quality-gates, saves.
2. Tool `ingest_notification(source_pdf_path)` — same pipeline in one call.
3. Website fetch: `fetch_commission_notices` / `python server.py --fetch --apply` crawls `COMMISSION_REGISTRY` hosts, downloads new advertisement PDFs into `tobepicked/`, then the same pipeline. Deduped in `gov_job_fetch_seen`.
4. Manual MCP flow still works: `store_notification_pdf` → `pdf://` → `extract_gov_job_details` → `save_job_to_database`.

**Classification** (do not treat every file as “a job post”):
- `combined_exam` — title is the exam (`CGPSC State Service Examination 2025`), never `1. State Civil Service (Deputy Collector)`.
- `multi_post_ad` — UPSC ORA / High Court multi-post ads; keep the vacancy-block splitter.
- `single_post` — one post; title may be the post name.
- `departmental_exam` — named specialist exam (State Engineering Service, Assistant Librarian). Posts should be the post/stream (`Assistant Librarian`, `Assistant Engineer (Civil)`), not a copy of the exam title.

Issuer detection is data-driven (`COMMISSION_REGISTRY`): URL host → English name → Hindi / known pypdf-garbled forms → filename tokens → state + “PSC”. Never invent a commission. Boards such as CG VYAPAM (`vyapamcg.cgstate.gov.in`) and MP ESB (`esb.mp.gov.in`) are first-class issuers.

**PDF text**: `read_pdf_notification` prefers `pdftotext -layout`, then pdfplumber (including tables), then pypdf. The LLM (when a key is set) receives a *selected pack* (header + issuer/exam/date lines + English annex + vacancy lists), not `text[:16000]`. Missing key → `LLM_SKIP` + deterministic fallback.

**Retrieval**:
- Resource `job-pdf://{job_id}` — stored path + extracted text.
- Tool `view_official_notification` — path and/or official URL (no post rows; PathWise `/gov-jobs/<id>` queries those).
- Tool `list_recent_jobs`.

**Database schema** (`init_db()` and `pathwise/schema.sql`):
- `gov_job_notifications`: existing fields plus `commission`, `state`, `exam_name`, `exam_kind`, `search_document` (lowercase aliases for ILIKE: `cgpsc`, `ssc cgl`, `vyapam`, hosts, post names).
- `gov_job_posts`: `notification_id` (FK, `ON DELETE CASCADE`), `post_name`, `department`, `pay_level`, `total_vacancies`, `vacancies_breakdown` (JSONB), `qualification`, `translations` (JSONB).
- `gov_job_fetch_seen`: crawler URL / hash de-dupe.

**Regional-language support**: these notification PDFs are routinely bilingual (English + a state's official language), and `pathwise` needs to serve all 22 Eighth Schedule languages, not just English. Rather than a column per language, `translations` on both tables is a single JSONB map keyed by ISO language code (see `EIGHTH_SCHEDULE_LANGUAGES` in [server.py](server.py) for the reference code list), each value holding the original-language text for that row's translatable fields — `job_title`/`department`/`qualification`/`age_limit`/`age_relaxation` at the notification level, `post_name`/`department`/`qualification` per post. `extract_gov_job_details` instructs the extracting model to populate this whenever the source PDF has regional-language text, rather than discarding it during English extraction. It's optional/nullable — omit it for English-only sources.

**Age relaxation structure**: these notifications routinely spread age-relaxation rules across multiple clauses/sections — a base list of categories (SC/ST/OBC, ex-servicemen, PwD, domicile-based, etc.), sometimes plus a second block that incorporates a wholly separate numbered rule by reference (e.g. "relaxations one through seventeen under Rule 5(c) apply"). `age_relaxation` stays a short prose summary for display; `age_relaxation_details` (JSONB array, nullable) holds one object per clause — `source` (section/clause reference), `category`, `relaxation`, `cap`, `notes` — for when precision matters (e.g. a candidate needs to know exactly which clause and cap applies to them). `extract_gov_job_details` instructs the model to populate this whenever a notification's relaxation rules exceed ~2-3 simple categories rather than compressing everything into one sentence, since that compression previously dropped clause-specific conditions (e.g. "ex-servicemen get relaxation equal to prior service duration, capped at 3 years above the upper limit" got flattened to just "standard...relaxations apply").

**Language preference when the same fact appears in both languages**: when a bilingual PDF states a fact in both English and a regional language as a true parallel translation (common for eligibility/age-limit clauses — many of these notifications carry an English annex restating the state's exam rules), the primary top-level fields (`job_title`, `department`, `nationality`, `qualification`, `age_limit`, `age_relaxation`, etc.) should be extracted from the English wording, with the regional-language original going into `translations` — not the reverse. `extract_gov_job_details` states this rule explicitly. The exception: if the two language versions aren't actually equivalent (e.g. the Hindi body includes a newer circular amendment the English annex doesn't), the more complete/specific version wins regardless of language — this happened on the CGPSC State Service Exam 2025 notification, where a 2024 circular's extra 5-year age relaxation for CG local residents appears only in the Hindi section 8.2(A), not in the older English Rule-5(c) annex.

**Syllabus**: `syllabus` (JSONB, nullable) holds the exam scheme, when a notification includes one — commonly as an annexure (e.g. "Annexure-II" for a Preliminary exam, "III" for Main). Keyed by exam stage (`"preliminary"`, `"main"`, ...) since a notification can have more than one; each stage holds `papers` (name, questions, marks, duration, negative-marking rule, `parts` → topics) plus a `language_note`. That last field matters: a syllabus section can carry its own explicit language-precedence rule (e.g. "in case of doubt the Hindi version will prevail") that overrides the general English-preference default above for that section specifically — `extract_gov_job_details` tells the model not to silently apply the general rule over an explicit contrary instruction in the source. On the CGPSC State Service Exam 2025 notification, both the Preliminary syllabus (Annexure-II, pages 29-30, structured in English and Hindi) and the Main exam syllabus (Annexure-III, pages 31-39, 7 papers) are backfilled — but the Main syllabus was extracted from the PDF's English text only (per the language-preference rule) and was not independently cross-checked against/transcribed from the Hindi original given its size; the Hindi source remains available on disk at `stored_pdfs/` if a parallel structured Hindi version is ever needed.

The CG State Engineering Service Exam 2026 notification (id 3) has a structurally different `syllabus`: a single written-exam stage (no Prelim/Main split) with two papers, so it isn't keyed by exam stage — `papers` is a flat list. Paper 2 (Engineering) adds a `subjects` level not otherwise part of the documented shape, since the candidate answers only one of Civil/Mechanical/Electrical Engineering depending on the post applied for; each subject's `parts` hold `units` (this PDF's own terminology) rather than being topics directly. This notification's main body (pages 1-7, eligibility/age/nationality) is Hindi-only with no parallel English annexure — unlike the State Service Exam PDF — so the language-preference rule didn't apply there; those fields were translated from Hindi and the originals kept in `translations.hi`. The Engineering syllabus itself (pages 8-13) is English-only and was extracted verbatim from the PDF's own extracted text rather than paraphrased, given how easy it is to introduce errors summarizing precise technical terminology (formulas, named theorems, standards).

**Known caveat — garbled Devanagari**: some source PDFs (confirmed on the CGPSC State Service Examination 2025 notification) embed Devanagari with a font whose glyphs are not mapped back to Unicode. `pypdf` then returns mangled Hindi (`राज्य` → `राº य`) while English on the same page is clean. `read_pdf_notification` prefers `pdftotext -layout` when it scores cleaner; if the remaining text still has `º/±/Ĭ`, do not copy it into `translations` — use English + registry `name_hi`. The prompt still tells a calling model to transcribe visually when the resource text looks mangled. The notification id 2 / `gov_job_posts` Hindi backfill was done from rendered page images for this reason.

**Website fetch**: `httpx` + BeautifulSoup crawl every `url_hosts` entry (PSCs, UPSC/SSC/RRB, exam boards). Extra listing paths live in `_LISTING_PATHS_BY_CODE` (query strings and `.html` shells allowed). Iframe srcs are followed (MP ESB’s `e_default.html`). TLS verify is off in `_http_get` because some board hosts (notably `esb.mp.gov.in`) present incomplete certificate chains. Skip admit cards / results / FAQ / RTI. Prefer filenames containing Advertisement / Vigyapti / परीक्षा. UPSC and some national WAFs return 403 from datacenter IPs — log `FETCH_HTTP_ERR` and continue. Background crawl is off until `FETCH_COMMISSIONS=true`.

**Quality gate**: refuse auto-save when the title is a numbered cadre, the ad number is the word “No”, dates look like historical annex years (e.g. 1997/2008 on a 2025 file), or a combined exam has no posts. Logs `AUTO_QUALITY_REJECT`. Never replace a better existing row with a worse extract unless `reingest_job` / `--reingest` names the ids.
