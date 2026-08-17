# AGENTS.md

All code lives in `server.py` (single FastMCP instance). No packages, no tests, no lint/typecheck config.

## Commands
```bash
pip install -r requirements.txt
cp .env.example .env   # then set DATABASE_URL
python server.py
```

Docker alternative (see README and .mcp.json):
```bash
docker build -t pathwise-mcp .
cp .env.example .env   # real shared DATABASE_URL
docker run -i --rm --env-file .env -v "$(pwd)/stored_pdfs:/app/stored_pdfs" pathwise-mcp
```

- `python server.py` (or docker equiv) is the only way to start; `mcp.run()` uses stdio.
- `init_db()` + `STORAGE_DIR.mkdir(exist_ok=True)` run at module import time.
- `.mcp.json` now uses `docker run` (update host paths + ensure image built first).
- Smoke check (native): `python -c "import server"` (after .env); for docker use `docker run --rm --entrypoint python ... -c "import os,ast; ...; ast.parse(open('server.py').read())"` or full after valid DB.

## Critical shared state with sibling `pathwise` project
- `.env` **MUST** contain the exact same `DATABASE_URL` (shared Neon Postgres; this writes `gov_job_notifications` + `gov_job_posts`).
- PDFs are copied to `./stored_pdfs/` (absolute path recorded in DB). The sibling serves them by direct filesystem read — both must run on the **same host**.
- Never introduce a separate SQLite or different DB for this data.

## Schema / migrations
- Schema lives in `init_db()` (CREATE TABLE IF NOT EXISTS + ALTER ... ADD COLUMN IF NOT EXISTS).
- On any schema change, update `init_db()` here **and** manually sync `pathwise/schema.sql` (no codegen).
- Safe to restart against seeded DB.

## Workflow (do not deviate)
1. `store_notification_pdf(source_pdf_path)`
2. Read the `pdf://{file_path}` resource (use the path passed to `extract_gov_job_details`)
3. Use `extract_gov_job_details` prompt to drive structured extraction (handles multi-post, translations, syllabus, age_relaxation_details).
4. `save_job_to_database(...)` — pass `posts=[...]` for notifications with >1 distinct post; use `translations` for bilingual content.

See `CLAUDE.md` for:
- full schema
- `EIGHTH_SCHEDULE_LANGUAGES`
- language-preference rules
- pypdf Devanagari garbling caveat (transcribe visually when mangled)
- `age_relaxation_details` and per-post `posts` shape
- why `httpx`/`beautifulsoup4` are imported but unused

## Other
- `stored_pdfs/`, `.env`, `tobeextracted/`, and `tobepicked/` (the admin drop queue from sibling pathwise) are gitignored.
- Edit only `server.py`. Changes to decorators immediately affect exposed tools/resources/prompts.
- No CI, no pre-commit, no release process defined in repo.
