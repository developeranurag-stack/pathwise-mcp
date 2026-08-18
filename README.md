# pathwise-mcp — Gov Job Extractor MCP Server

A [MCP](https://modelcontextprotocol.io) server, built with [`fastmcp`](https://github.com/jlowin/fastmcp), that ingests Indian government recruitment notifications: copy or fetch a PDF, classify the *kind* of notice and the issuing body, extract structured fields, and write them into the shared PathWise Postgres database.

Almost all server logic lives in [server.py](server.py). A generated [commission_registry.json](commission_registry.json) is written on import so the sibling `pathwise` app can reuse the same issuer list.

## Relationship to `pathwise`

This server is a companion to the sibling `pathwise` Flask app. It **writes** `gov_job_notifications` / `gov_job_posts`; PathWise **reads** that data at `/gov-jobs`.

- `.env` here **must** hold the same `DATABASE_URL` as `pathwise/.env`.
- PDFs live in `stored_pdfs/`. The database stores a **relative** path (`stored_pdfs/<file>.pdf`). PathWise resolves that against `../pathwise-mcp/stored_pdfs` (or `GOV_JOB_PDF_DIR`). Both processes should still run on the same host.
- Do not introduce a second database or a local SQLite copy of this data.

- **Connecting an app** (PathWise or anything like it): **[INTEGRATING.md](INTEGRATING.md)** — shared DB, drop folder, search SQL, MCP tools, how to render `exam_kind`.
- Schema, language rules, and extraction caveats: [CLAUDE.md](CLAUDE.md).

## Setup

```bash
pip install -r requirements.txt
cp .env.example .env   # set DATABASE_URL to the same Neon URL as pathwise
python server.py
```

`mcp.run()` speaks stdio (what MCP clients expect). On first DB use, `init_db()` creates/migrates tables. `stored_pdfs/` and `tobepicked/` are created next to `server.py`.

Optional env (see [.env.example](.env.example)):

| Variable | Role |
|----------|------|
| `LLM_API_KEY` / `XAI_API_KEY` | If unset, auto-extract logs `LLM_SKIP` and uses the deterministic fallback |
| `FETCH_COMMISSIONS` | `true` to crawl official sites in the background (off by default) |
| `FETCH_INTERVAL_SECONDS` | Default `21600` (6 hours) |
| `FETCH_MAX_PDFS_PER_HOST` | Default `3` new PDFs per host per run |
| `POLL_SECONDS` | How often `tobepicked/` is scanned (default `5`) |

## How notifications get in

Three equivalent ingest paths:

1. **Admin drop folder** — PathWise admin uploads a PDF into `tobepicked/`. The poller stores it, extracts, quality-gates, and saves.
2. **One MCP call** — `ingest_notification(path)` does store + extract + save.
3. **Website fetch** — `fetch_commission_notices` (or `python server.py --fetch --apply`) crawls every registered official host, downloads new advertisement PDFs into `tobepicked/`, then the same extract/save path runs.

The extractor classifies each PDF as one of:

- `combined_exam` — one exam, many cadres (UPSC CSE, State PSC SSE/PCS, SSC CGL). Title is the **exam**, never the first cadre line.
- `multi_post_ad` — one advertisement, several posts (UPSC ORA / High Court combo).
- `single_post` — one post.
- `departmental_exam` — named specialist exam (State Engineering Service, Assistant Librarian Exam).

It also fills `commission`, `state`, `exam_name`, and `search_document` so students can find rows with queries like `cgpsc`, `ssc cgl`, `mppsc`, `vyapam`.

## Using it from an MCP client

Point the client at this repo’s Python environment. Example `.mcp.json`:

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

Docker alternative (adjust host paths; image must be built first) is in this repo’s `.mcp.json`.

## What it exposes

**Ingest / extract**

- `ingest_notification(source_pdf_path)` — store, extract, quality-gate, save in one step.
- `store_notification_pdf` — copy a PDF into `stored_pdfs/` (still used by the multi-step flow).
- `save_job_to_database` — persist fields; new args `commission`, `state`, `exam_name`, `exam_kind`, `search_aliases` default empty so old callers keep working.
- `process_pending_uploads` — scan `tobepicked/` end-to-end.
- `reingest_job(job_id)` — re-extract a saved row from its stored PDF.
- Prompt `extract_gov_job_details` — kind-aware instructions for a calling model.

**Fetch**

- `fetch_commission_notices(codes="", dry_run=true, max_pdfs_per_host=3)` — crawl official PSC / SSC / UPSC / RRB / exam-board sites. Empty `codes` means every registered issuer. `dry_run=true` only lists PDF URLs.

**Read**

- Resource `pdf://{file_path}` — extract text (`pdftotext -layout`, then pdfplumber, then pypdf).
- Resource `job-pdf://{job_id}` — stored path + text for a saved job.
- `view_official_notification(job_id)` — local PDF path and/or official URL.
- `list_recent_jobs(limit)` — id, commission, kind, title, apply window.

## Website fetch (all registered Indian issuers)

The crawler is data-driven from `COMMISSION_REGISTRY` in `server.py` — not a hard-coded CG/UPSC/SSC list. It includes:

- National: UPSC, SSC, RRB, IBPS, SBI, RBI, NTA
- Every State PSC in the registry (CGPSC, MPPSC, UPPSC, BPSC, RPSC, TNPSC, …)
- Exam boards: **CG VYAPAM** (`vyapamcg.cgstate.gov.in`, including `/Posts?tag=ONLINEAPPLICATION`), **MP ESB / Vyapam** (`esb.mp.gov.in/e_default.html`), RSMSSB, HSSC, UKSSSC, OSSC, JSSC, GSSSB, DSSSB, BSSC, UPSSSC, and similar
- High Courts / SCI, AIIMS, ESIC, KVS, NVS when hosts are listed

It follows iframe shells (MP ESB), query-string listing pages (CG VYAPAM), and skips admit cards, results, FAQ, and RTI PDFs. Some national sites (notably UPSC) return HTTP 403 from datacenter IPs; those hosts are logged and skipped.

One-shot from the shell:

```bash
# list PDFs only
python server.py --fetch --codes=CGVYAPAM,MPESB

# download + extract
python server.py --fetch --apply --codes=CGVYAPAM,MPESB
```

Background fetch (every 6 hours once the server is running):

```
FETCH_COMMISSIONS=true
```

## Commands

```bash
python server.py                 # MCP stdio + pickup poller
python server.py --self-test     # classifier / splitter / fetch-seed checks (no DB required)
python server.py --reingest      # re-extract PDFs already in stored_pdfs/
python server.py --fetch         # dry-run crawl
python server.py --fetch --apply --codes=CGPSC,MPESB
```

## Running with Docker

```bash
docker build -t pathwise-mcp .
cp .env.example .env   # real shared DATABASE_URL
docker run -i --rm \
  --env-file .env \
  -v "$(pwd)/stored_pdfs:/app/stored_pdfs" \
  -v "$(pwd)/tobepicked:/app/tobepicked" \
  pathwise-mcp
```

Relative `stored_pdfs/<name>` paths work across container and host as long as PathWise can see the same files (shared volume or `GOV_JOB_PDF_DIR`).
