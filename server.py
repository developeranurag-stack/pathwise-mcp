import os
import shutil
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
STORAGE_DIR.mkdir(exist_ok=True)  # Directory to store local PDF files

DATABASE_URL = os.environ.get("DATABASE_URL")

# Eighth Schedule languages (ISO 639 codes) that `translations` fields may be
# keyed by. Not DB-enforced (JSONB accepts any key) — this is the reference
# list handed to the extraction prompt so it uses consistent codes across PDFs.
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
        return f"Error: Source PDF at '{source_pdf_path}' does not exist."

    if source_path.suffix.lower() != ".pdf":
        return "Error: File must be a PDF."

    # Sanitize and create destination path
    dest_filename = f"{source_path.stem}_{source_path.stat().st_mtime_ns}{source_path.suffix}"
    dest_path = STORAGE_DIR / dest_filename

    # Copy file to local storage
    shutil.copy2(source_path, dest_path)

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
            text += f"\n--- Page {i+1} ---\n" + (page.extract_text() or "")
        return text
    except Exception as e:
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

# ------------------------------------------------------------------
# Entry Point
# ------------------------------------------------------------------
if __name__ == "__main__":
    mcp.run()
