# Contributing

Thanks for helping improve pathwise-mcp. This is a small repo: almost all runtime code is in [server.py](server.py).

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

`.env` is gitignored. Never commit a real `DATABASE_URL` or LLM key.

`python server.py --self-test` does not need Postgres. Use that as the default smoke check.

## What belongs in a PR

Useful changes:

- A new issuer in `COMMISSION_REGISTRY` (`_c(...)` in `server.py`), plus listing paths in `_LISTING_PATHS_BY_CODE` when the board does not publish ads on `/`.
- Better classification of `exam_kind` (combined exam vs multi-post ad vs single post vs departmental exam).
- Quality-gate cases that stop a bad auto-save.
- Clearer extraction of posts, dates, or `search_document` aliases.
- Docs that match the code.

Please do **not**:

- Commit PDFs, `.env`, `mcp_read_log.txt`, or anything under `stored_pdfs/` / `tobepicked/`.
- Add a second database (SQLite or otherwise). This server writes `gov_job_notifications` / `gov_job_posts` in the caller’s Postgres.
- Invent a commission that is not in the registry.
- Treat a numbered cadre line as the exam title on a combined exam.

If you change the schema, update `init_db()` **and** mention it in the PR so consumers can update their `schema.sql`.

## Adding an issuer

1. Add a `_c(...)` entry to `COMMISSION_REGISTRY` with official `url_hosts`, English name, Hindi name if known, and search aliases.
2. Add extra listing paths in `_LISTING_PATHS_BY_CODE` when the default crawl would miss advertisement pages.
3. Import once (or run `--self-test`) so `commission_registry.json` regenerates.
4. Dry-run fetch: `python server.py --fetch --codes=YOURCODE`.

## Pull requests

1. Keep the change scoped. One issuer or one bug is easier to review than a rewrite.
2. Run `python server.py --self-test`.
3. Describe the official source you used (URL, PDF, or board name).
4. Open the PR against `main`.

By contributing, you agree that your work is licensed under the [MIT License](LICENSE).
