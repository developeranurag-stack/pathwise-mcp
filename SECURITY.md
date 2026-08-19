# Security

## Reporting a vulnerability

Please **do not** open a public GitHub issue for a security problem.

Email **developeranurag2108@gmail.com** with:

- a description of the issue
- steps to reproduce
- impact (data exposure, overwrite, SSRF, etc.)
- a suggested fix if you have one

You should hear back within a few days.

## Please do not report

- Missing LLM keys or `LLM_SKIP` when no API key is set
- HTTP 403 from UPSC or other sites that block datacenter IPs
- Incomplete TLS chains on official board hosts (the crawler disables verify for known broken chains such as `esb.mp.gov.in`)
- Quality-gate rejections (`AUTO_QUALITY_REJECT`) of a bad extract

## Operational notes for operators

- Treat `.env` as secret. It is gitignored and was never meant to be published.
- `DATABASE_URL` is the same database your student-facing app reads. A leaked URL is a leaked production database.
- The fetch crawler hits public government sites. Keep `FETCH_MAX_PDFS_PER_HOST` small and do not point it at private networks.
- Stored PDFs live on disk under `stored_pdfs/`. Anyone with filesystem or `job-pdf://` access can read them.
