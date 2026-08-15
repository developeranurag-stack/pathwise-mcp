# pathwise-mcp — Gov Job Extractor MCP Server

A single-file [MCP](https://modelcontextprotocol.io) server, built with [`fastmcp`](https://github.com/jlowin/fastmcp), for ingesting government job notification PDFs: copy a source PDF into local storage, extract its text, and save structured job details into a shared Postgres database.

All server code lives in [server.py](server.py) — there is no other application code or test suite in this repo.

## Relationship to `pathwise`

This server is a companion tool to the sibling `pathwise` Flask app. It writes job records into `pathwise`'s own Postgres (Neon) database — the `gov_job_notifications` / `gov_job_posts` tables — rather than a local SQLite file, and `pathwise` serves that data read-only at `/gov-jobs`.

This means:
- `.env` here **must** hold the same `DATABASE_URL` as `pathwise/.env` — the two apps share one database, not two separate ones.
- PDFs stay on local disk (`stored_pdfs/`); this only works cleanly when both projects run on the same host, since `pathwise` reads the stored path directly off disk to serve PDFs.
- `init_db()` runs `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` / `CREATE TABLE IF NOT EXISTS` on every startup, so it's safe to run against an already-seeded database.

See [CLAUDE.md](CLAUDE.md) for the full architecture writeup, including the database schema, regional-language (`translations`) handling, and known caveats around PDF text extraction.

## Setup

```bash
pip install -r requirements.txt
cp .env.example .env   # then fill in DATABASE_URL — same value as pathwise/.env
python server.py
```

`mcp.run()` starts the server over stdio, the transport MCP clients expect. On startup it creates `stored_pdfs/` (for locally stored PDF copies) and ensures the required Postgres tables/columns exist.

## Using it from an MCP client

Register `server.py` with your MCP client (Claude Desktop, Claude Code, etc.), pointing at this repo's Python environment. Example `.mcp.json` entry:

```json
{
  "mcpServers": {
    "gov-job-extractor": {
      "command": "/path/to/pathwise-mcp/.venv/bin/python",
      "args": ["/path/to/pathwise-mcp/server.py"]
    }
  }
}
```

## What it exposes

- **Tool** `store_notification_pdf` — copies a source PDF into `stored_pdfs/`, returning the resolved local path.
- **Resource** `pdf://{file_path}` — extracts and returns text from a stored PDF.
- **Resource** `job-pdf://{job_id}` — looks up a saved job by ID and returns its stored PDF path plus extracted text.
- **Prompt** `extract_gov_job_details` — guides the calling model through extracting structured fields (title, department, vacancies, qualification, age limit/relaxation, dates, fee, per-post breakdowns, regional-language translations, syllabus, etc.) from notification text.
- **Tool** `save_job_to_database` — persists the extracted notification-level fields into `gov_job_notifications`, plus one row per post into `gov_job_posts` when a `posts` list is provided.
- **Tool** `view_official_notification` — returns a saved job's local PDF path and/or official URL without extracting text.

## Workflow

1. Call `store_notification_pdf` with a source PDF path.
2. Read the `pdf://{file_path}` resource to get the extracted text.
3. Follow the `extract_gov_job_details` prompt to pull structured fields out of that text.
4. Call `save_job_to_database` to persist the result.

Retrieval later goes through `job-pdf://{job_id}` or `view_official_notification`, independent of ingestion.
