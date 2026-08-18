# Integrating with pathwise-mcp

This guide is for applications like [PathWise](https://github.com/) — student career apps, counselling UIs, or admin consoles — that need official Indian government job notifications without re-implementing PDF extraction.

**pathwise-mcp writes. Your app reads.**  
The MCP server is the ingest pipeline. Your application should almost never parse recruitment PDFs itself.

```
Official PSC / exam-board sites          Admin upload
            \                               /
             \                             /
              v                           v
         pathwise-mcp  (stdio MCP + poller)
              |  extract + classify
              v
     Shared Postgres  +  stored_pdfs/
              |
              v
         Your app  (list / search / detail / PDF)
```

---

## 1. Choose an integration pattern

### Pattern A — Shared database (recommended for web apps)

What PathWise does. Your app **does not speak MCP**.

1. Run pathwise-mcp as a long-lived process on the same host (or with a shared volume).
2. Point both `.env` files at the **same** `DATABASE_URL`.
3. Your admin UI writes uploaded PDFs into this repo’s `tobepicked/` directory.
4. Your student UI `SELECT`s from `gov_job_notifications` / `gov_job_posts`.
5. Serve PDFs by resolving `local_pdf_path` against `stored_pdfs/`.

This is the most efficient setup: no MCP protocol in the request path, no extra hop per page view.

### Pattern B — MCP client (agents / Claude Desktop / Claude Code)

Use this when a model should ingest or fetch on demand.

Register the server (stdio). Then call tools. Do **not** use MCP as your public HTTP API.

```json
{
  "mcpServers": {
    "gov-job-extractor": {
      "command": "/abs/path/to/pathwise-mcp/.venv/bin/python",
      "args": ["/abs/path/to/pathwise-mcp/server.py"]
    }
  }
}
```

Docker equivalent: see `.mcp.json` in this repo (mount `stored_pdfs` and `tobepicked`, pass `--env-file`).

### Pattern C — Hybrid

Web app uses Pattern A for students. Admins or an assistant use Pattern B (`ingest_notification`, `fetch_commission_notices`, `reingest_job`) when a human is in the loop.

---

## 2. Shared contract (do this first)

| Shared thing | Rule |
|--------------|------|
| Postgres | One Neon/Postgres URL. Same `DATABASE_URL` in both `.env` files. |
| Tables | `gov_job_notifications`, `gov_job_posts`. MCP creates/migrates them on first connect. Also copy columns into your `schema.sql` by hand. |
| PDFs | Files in `<mcp-root>/stored_pdfs/`. DB column `local_pdf_path` is **relative**: `stored_pdfs/<filename>.pdf`. |
| Drop queue | Your upload endpoint writes into `<mcp-root>/tobepicked/<original-name>.pdf`. |
| Issuer list | Import or copy `commission_registry.json` (written whenever the MCP process starts). |

Never give the MCP a second database. Never parse the PDF again in the web app if the row already exists.

### Resolve a PDF in your app

```python
import os

def resolve_gov_job_pdf(local_pdf_path, mcp_root=None):
    if not local_pdf_path:
        return None
    if os.path.isfile(local_pdf_path):
        return local_pdf_path
    name = os.path.basename(local_pdf_path)
    roots = [
        os.environ.get("GOV_JOB_PDF_DIR") or "",
        os.path.join(mcp_root or "", "stored_pdfs"),
        os.path.join(os.path.dirname(__file__), "..", "pathwise-mcp", "stored_pdfs"),
    ]
    for root in roots:
        if not root:
            continue
        cand = os.path.abspath(os.path.join(root, name))
        if os.path.isfile(cand):
            return cand
    return None
```

Older rows may still hold an absolute path. Always try the stored value first, then the basename under `stored_pdfs/`.

### Drop-folder upload

```python
# After validating the upload is a non-empty .pdf:
dest = os.path.join(MCP_ROOT, "tobepicked", secure_filename)
save(upload, dest)
# Do not extract here. The MCP poller (every POLL_SECONDS, default 5s)
# copies to stored_pdfs/, extracts, quality-gates, and INSERTs.
# On success it replaces tobepicked/<name>.pdf with a 0-byte placeholder
# so the filename stays visible without keeping the full file twice.
```

If the MCP process is not running, files sit in `tobepicked/` until it starts (or someone calls `process_pending_uploads`).

---

## 3. Data model your UI should use

### Notification (`gov_job_notifications`)

| Column | Use it for |
|--------|------------|
| `id` | Detail URL, PDF URL |
| `job_title` | Heading. For combined exams this is the **exam** (`CGPSC State Service Examination 2025`), not a cadre. |
| `exam_name` | Same exam phrase without the commission prefix when present |
| `exam_kind` | How to render the page (see below) |
| `commission` | Badge / filter: `UPSC`, `CGPSC`, `SSC`, `MPESB`, `CGVYAPAM`, … |
| `state` | Filter. `NULL` for national bodies |
| `department` | Issuing body or hiring department |
| `advertisement_number` | e.g. `06/2025/परीक्षा` |
| `apply_start_date`, `apply_end_date`, `exam_date` | Display as stored strings (not ISO). Year should match the file. |
| `total_vacancies` | Badge. May be `0` if the table could not be parsed — still show posts. |
| `qualification`, `age_limit`, `age_relaxation`, `nationality`, `application_fee` | Eligibility block |
| `reservation_details` | JSON object `{"UR": n, "OBC": n, …}` |
| `age_relaxation_details` | JSON array of `{source, category, relaxation, cap, notes}` |
| `syllabus` | JSON exam scheme (often keyed `preliminary` / `main`) |
| `translations` | JSON `{"hi": {"job_title": "…", "department": "…"}}` — Eighth Schedule codes |
| `official_url` | Link to the commission site |
| `local_pdf_path` | Resolve and stream; do not store a second copy |
| `search_document` | Lowercase alias blob for `ILIKE` |
| `source` | Provenance (`mcp:gov-job-extractor`, etc.) |
| `created_at` | Recency. **Do not hide** rows whose apply window has passed — vacancies recur. |

### Posts (`gov_job_posts`)

One row per cadre/post inside the notification. Always load these on the detail page.

| Column | Notes |
|--------|--------|
| `post_name` | `State Civil Service (Deputy Collector)`, `Assistant Librarian`, … |
| `department`, `pay_level`, `qualification` | Per-post when they differ |
| `total_vacancies`, `vacancies_breakdown` | Per-post counts |
| `translations` | Same shape as the parent, scoped to the post |

```sql
SELECT * FROM gov_job_posts
WHERE notification_id = %s
ORDER BY id;
```

### `exam_kind` — render rules

| `exam_kind` | Title means | Detail page |
|-------------|-------------|-------------|
| `combined_exam` | The exam brand | Lead with commission + exam + year, then list **all** `gov_job_posts` as cadres. Never treat the first post as “the job”. |
| `multi_post_ad` | The recruitment as a whole | List each post with its own pay / vacancies (UPSC ORA style). |
| `single_post` | Usually the post name | One post is enough; still show commission + dates. |
| `departmental_exam` | The named specialist exam | Title is the exam; posts are streams (`Assistant Engineer (Civil)`). |

If `job_title` starts with `1.` it is a bad extract. You may still show the row, but prefer `exam_name` / `commission` and say details may be incomplete.

---

## 4. Search efficiently (what students type)

Students search `cgpsc`, `upsc cse`, `ssc cgl`, `mppsc`, `vyapam`, `pcs`, `ras` — strings that often **do not** appear in `job_title`.

**Do this:**

```sql
SELECT n.id, n.job_title, n.commission, n.state, n.exam_kind, n.exam_name,
       n.apply_end_date, n.total_vacancies, n.advertisement_number
FROM gov_job_notifications n
WHERE
      LOWER(COALESCE(n.search_document, '')) LIKE '%' || $q || '%'
   OR LOWER(COALESCE(n.commission, ''))      LIKE '%' || $q || '%'
   OR LOWER(COALESCE(n.exam_name, ''))       LIKE '%' || $q || '%'
   OR LOWER(n.job_title)                     LIKE '%' || $q || '%'
   OR EXISTS (
        SELECT 1 FROM gov_job_posts p
        WHERE p.notification_id = n.id
          AND LOWER(p.post_name) LIKE '%' || $q || '%'
      )
ORDER BY n.created_at DESC
LIMIT 20;
```

Normalize the query to lowercase. Expand acronyms using `commission_registry.json` (or PathWise’s `gov_job_aliases.py`, which merges that file). Example: `cgl` → also match `ssc` and `combined graduate`.

**Do not** search only `job_title` / `department`. That is why “cgpsc” used to miss a row titled `1. State Civil Service (Deputy Collector)`.

Optional filters that map 1:1 to columns: `commission`, `state`, `exam_kind`.

---

## 5. Connecting as an MCP client

Use this when your process *is* an agent, not a page renderer.

### Efficient tool sequence

| Goal | Call |
|------|------|
| User uploaded / pasted a PDF path | **`ingest_notification(source_pdf_path)`** only |
| Drain the admin drop folder now | `process_pending_uploads` |
| Look at what was saved | `list_recent_jobs` |
| Open the official file | `view_official_notification(job_id)` |
| Bad extract | `reingest_job(job_id)` |
| Pull new ads from the web | `fetch_commission_notices(codes, dry_run=false)` — start with `dry_run=true` |
| Human-in-the-loop extract | `store_notification_pdf` → read `pdf://{path}` → follow prompt `extract_gov_job_details` → `save_job_to_database` |

Prefer `ingest_notification` over the four-step flow. It classifies kind, builds the extraction pack, falls back if there is no LLM key, quality-gates, and upserts.

### `save_job_to_database` (if you extract yourself)

Required-style fields: `job_title`, `department`, `total_vacancies`, `qualification`, `reservation_details`, `age_limit`, `age_relaxation`, `apply_start_date`, `apply_end_date`, `local_pdf_path`.

Always pass when you have them:

- `posts`: list of `{post_name, department, pay_level, total_vacancies, vacancies_breakdown, qualification, translations}`
- `commission`, `state`, `exam_name`, `exam_kind`
- `search_aliases`: list of lowercase strings (the server will also rebuild `search_document`)
- `translations`, `syllabus`, `age_relaxation_details`, `nationality`, `advertisement_number`, `official_url`

Existing callers that omit the new fields still work; the server backfills issuer/kind when it can.

### Fetch

`codes` is a comma-separated list of registry codes (`CGPSC`, `CGVYAPAM`, `MPESB`, `UPSC`, …). Empty = every registered host.

```
fetch_commission_notices(codes="CGVYAPAM,MPESB", dry_run=true)
fetch_commission_notices(codes="CGVYAPAM,MPESB", dry_run=false, max_pdfs_per_host=3)
```

Or without MCP:

```bash
python server.py --fetch --codes=CGVYAPAM,MPESB          # list URLs
python server.py --fetch --apply --codes=CGVYAPAM,MPESB  # download + extract
```

Background: set `FETCH_COMMISSIONS=true` in the MCP `.env`. The poller then crawls on `FETCH_INTERVAL_SECONDS` (default 6 hours).

Some sites (UPSC) return 403 from datacenter IPs. That is logged (`FETCH_HTTP_ERR`); other hosts still run.

---

## 6. Commission codes and registry

The MCP is the source of truth: `COMMISSION_REGISTRY` in `server.py`. On import it writes `commission_registry.json`:

```json
{
  "code": "CGVYAPAM",
  "name_en": "Chhattisgarh Professional Examination Board",
  "name_hi": "छत्तीसगढ़ व्यावसायिक परीक्षा मण्डल",
  "state": "Chhattisgarh",
  "url_hosts": ["vyapamcg.cgstate.gov.in", "vyapamprofile.cgstate.gov.in"],
  "search_aliases": ["cg vyapam", "cgvyapam", "..."],
  "exam_aliases": ["TET", "SET", "LSAT", "CG-SET"]
}
```

Load this file in your app for filter dropdowns and query expansion. If you add an issuer, add it in the MCP registry — do not maintain a parallel list that can drift.

`state` is `null` for national bodies (UPSC, SSC, RRB, IBPS, …).

---

## 7. Running the MCP next to your app

```bash
cd pathwise-mcp
pip install -r requirements.txt
cp .env.example .env
# DATABASE_URL = the same value as your app
python server.py          # stdio MCP + tobepicked poller
```

Keep this process up whenever you expect uploads or fetch. It is not an HTTP server.

Optional `.env`:

```
DATABASE_URL=postgresql://...
LLM_API_KEY=                 # optional; without it, fallback extract still classifies issuer/kind
FETCH_COMMISSIONS=false
FETCH_INTERVAL_SECONDS=21600
FETCH_MAX_PDFS_PER_HOST=3
POLL_SECONDS=5
```

Health / debug: `mcp_read_log.txt` in the MCP root (`AUTO_SAVE`, `AUTO_QUALITY_REJECT`, `LLM_SKIP`, `FETCH_HOST`). `python server.py --self-test` does not need the database.

---

## 8. Display checklist (student-facing)

- [ ] List page shows `job_title`, `commission`, apply-by date, vacancy count (if > 0).
- [ ] Combined exams are not summarized as “1 vacancy for Deputy Collector”.
- [ ] Detail page lists every `gov_job_posts` row.
- [ ] Search hits `search_document` + `commission` + `exam_name` + post names.
- [ ] Closed apply windows stay listed (historical records are intentional).
- [ ] “View official notification” streams the resolved PDF, not a 404 on a relative path.
- [ ] Hindi (or other) strings come from `translations`, not garbled `º/±` text in the English columns.

---

## 9. What not to do

- Do not implement a second extractor in the web app.
- Do not `ILIKE` only `job_title`.
- Do not delete rows because `apply_end_date` is in the past.
- Do not treat `1. Indian Administrative Service` as a valid combined-exam title.
- Do not invent vacancies or a commission that the row does not store.
- Do not call MCP tools on every page view — query Postgres.

PathWise’s reference implementation: admin drop → `../pathwise-mcp/tobepicked/`, list/detail/PDF in `main.py`, alias expansion in `gov_job_aliases.py`. Internal MCP architecture is in [CLAUDE.md](CLAUDE.md); operator commands are in [README.md](README.md).
