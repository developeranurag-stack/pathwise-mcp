# AGENTS.md

Almost all code lives in `server.py` (single FastMCP instance). `commission_registry.json` is generated on import for the sibling PathWise search aliases. No packages, no lint/typecheck config. Smoke checks: `python server.py --self-test`.

## Commands
```bash
pip install -r requirements.txt
cp .env.example .env   # then set DATABASE_URL (same Neon URL as pathwise)
python server.py
```

```bash
python server.py --self-test
python server.py --reingest
python server.py --fetch                         # dry-run all registered hosts
python server.py --fetch --apply --codes=CGVYAPAM,MPESB
```

Docker (see README and `.mcp.json`):
```bash
docker build -t pathwise-mcp .
docker run -i --rm --env-file .env \
  -v "$(pwd)/stored_pdfs:/app/stored_pdfs" \
  -v "$(pwd)/tobepicked:/app/tobepicked" \
  pathwise-mcp
```

- `python server.py` starts `mcp.run()` on stdio **and** a background poller (`_poll_loop`).
- `init_db()` is **lazy** (first `connect()`), so `--self-test` does not need Neon.
- Pickup scan: `tobepicked/*.pdf` every `POLL_SECONDS` (default 5).
- Website crawl only if `FETCH_COMMISSIONS=true` (interval `FETCH_INTERVAL_SECONDS`, default 6h).

## Critical shared state with sibling `pathwise`
- `.env` **MUST** contain the exact same `DATABASE_URL`.
- Persist `local_pdf_path` as `stored_pdfs/<filename>.pdf` (relative). PathWise resolves via `../pathwise-mcp/stored_pdfs` or `GOV_JOB_PDF_DIR`.
- Never introduce a separate SQLite or different DB for this data.
- Keep `COMMISSION_REGISTRY` here as source of truth. Import writes `commission_registry.json`; `pathwise/gov_job_aliases.py` merges it.

## Schema / migrations
- Schema lives in `init_db()` (`CREATE TABLE IF NOT EXISTS` + `ALTER ... ADD COLUMN IF NOT EXISTS`).
- Extra table: `gov_job_fetch_seen` (URL de-dupe for the crawler).
- Notification extras: `commission`, `state`, `exam_name`, `exam_kind`, `search_document`.
- On any schema change, update `init_db()` **and** `pathwise/schema.sql` by hand.

## Ingest paths (any one is enough)
1. Drop PDF in `tobepicked/` (PathWise admin upload) — poller runs extract + save.
2. Tool `ingest_notification(source_pdf_path)` — one shot.
3. Tool `fetch_commission_notices` / CLI `--fetch --apply` — download from official sites, then the same pipeline.
4. Manual MCP flow still works: `store_notification_pdf` → `pdf://` → `extract_gov_job_details` → `save_job_to_database`.

Extraction always classifies `exam_kind` + issuer from `COMMISSION_REGISTRY`. Combined exams must be titled as the **exam** (e.g. `CGPSC State Service Examination 2025`), never the first numbered cadre.

## Registry / fetch
Add issuers in `COMMISSION_REGISTRY` (`_c(...)`), not `if cgpsc` branches. Listing extras go in `_LISTING_PATHS_BY_CODE` (query strings and `.html` shells are allowed). Boards such as CG VYAPAM and MP ESB are first-class issuers, not PSC special cases.

Fetch skips admit cards / results / FAQ / RTI. Some hosts (UPSC) 403; log and continue. `esb.mp.gov.in` needs TLS verify disabled (incomplete chain) — already handled in `_http_get`.

## Other
- `stored_pdfs/`, `.env`, `tobeextracted/`, `tobepicked/` are gitignored.
- Prefer editing `server.py`. Decorator changes immediately change the MCP surface.
- No CI / pre-commit / release process defined.

See `INTEGRATING.md` for the app-facing contract (how PathWise-like clients should connect). See `CLAUDE.md` for schema shape, Eighth Schedule `translations`, language-preference rules, Devanagari extraction caveats, and `age_relaxation_details` / `posts` / `syllabus` shapes.
