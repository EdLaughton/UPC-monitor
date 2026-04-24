# UPC Decisions Ingester

A small Dockerised Python service that polls the official UPC Decisions and Orders page, follows detail pages, mirrors newly published official PDFs, stores rich metadata in SQLite under `/data`, and serves a very simple public mirror.

## Run

```powershell
docker compose up --build
```

The mirror is served at:

- `http://localhost:8000/`
- `http://localhost:8000/stats.html`
- `http://localhost:8000/latest.json`
- `http://localhost:8000/stats.json`
- `http://localhost:8000/status.json`
- `http://localhost:8000/all.ndjson`
- mirrored PDFs under `http://localhost:8000/pdfs/...`

## Publish the Image

This repo includes a GitHub Actions workflow at `.github/workflows/docker-publish.yml`.
After the repo is pushed to GitHub, the workflow builds the Docker image and publishes:

```text
ghcr.io/edlaughton/upc-monitor:latest
```

For a public Unraid install, make the GHCR package public in GitHub:

1. Open the GitHub repo.
2. Go to Packages, then `upc-monitor`.
3. Package settings, then change visibility to public.

Private packages also work, but Unraid must be logged in to GHCR with a GitHub token.

## Unraid

Recommended Unraid setup uses the GHCR image built by GitHub Actions.

1. Push this repo to GitHub and wait for the `Build and publish Docker image` workflow to finish.
2. On Unraid, create `/mnt/user/appdata/upc-monitor`.
3. Docker tab, Add Container.
4. Set Repository to `ghcr.io/edlaughton/upc-monitor:latest`.
5. Set WebUI to `http://[IP]:[PORT:8000]/`.
6. Add path mapping: host `/mnt/user/appdata/upc-monitor` to container `/data`, read/write.
7. Add port mapping: host `8000` to container `8000`.
8. Add variables:
   - `POLL_CRON=0 * * * *`
   - `TIMEZONE=Europe/London`
   - `RUN_ON_START=true`
   - `LOG_LEVEL=INFO`

An Unraid template starter is included at `unraid/upc-monitor.xml`.

First-run bootstrap, if you want to ignore everything already on the UPC site and only ingest future publications:

1. Set `RUN_ON_START=false` for the first container start.
2. Start the container.
3. Run:

```bash
docker exec -it upc-monitor python -m upc_ingester bootstrap
```

4. Change `RUN_ON_START=true` and restart the container.

After the container starts, open:

```text
http://<unraid-ip>:8000/
http://<unraid-ip>:8000/stats.html
http://<unraid-ip>:8000/latest.json
http://<unraid-ip>:8000/stats.json
http://<unraid-ip>:8000/status.json
```

## Commands

Inside the image:

```bash
python -m upc_ingester serve
python -m upc_ingester run-once
python -m upc_ingester backfill
python -m upc_ingester backfill --index-only
python -m upc_ingester bootstrap
```

`bootstrap` crawls the current UPC index and marks visible items as seen without downloading PDFs. Use it once if you only want future UPC publications mirrored.

`backfill --index-only` crawls the UPC index and stores one SQLite decision row per index item using only index-table metadata. It does not open detail pages and does not download PDFs. New rows are left with `last_error="index-only backfill; detail not yet fetched"` so they remain eligible for later enrichment. Existing complete rows are skipped, and existing incomplete rows only have safe index metadata refreshed; mirrored PDF, hash, document, headnote, keyword, language, panel, and specific error fields are preserved.

Example full catalogue index-only backfill:

```bash
python -m upc_ingester backfill --index-only --date-from 2024-01-01 --date-window-days 7 --max-items 2200
```

Date-window mode is preferred for historical catalogue backfill because it avoids deep pagination. It requests shallow UPC index ranges by decision date and recursively splits a range if the first page still has pagination.

Resume from a later date:

```bash
python -m upc_ingester backfill --index-only --date-from 2025-01-01 --date-to 2025-12-31 --date-window-days 7 --max-items 2200
```

Simple live smoke check:

```bash
python -m upc_ingester backfill --index-only --max-pages 1 --max-items 10 --index-page-retry-delay-seconds 30 --index-page-max-retries 3
```

Docker example against the persistent `/data` volume:

```bash
docker exec -it upc-monitor python -m upc_ingester backfill --index-only --date-from 2024-01-01 --date-window-days 7 --max-items 2200
```

Docker date-range resume example:

```bash
docker exec -it upc-monitor python -m upc_ingester backfill --index-only --date-from 2025-01-01 --date-to 2025-12-31 --date-window-days 7 --max-items 2200
```

For local development without installing the package:

```powershell
$env:PYTHONPATH="src"
python -m upc_ingester run-once
```

For live local scraping outside Docker, install the browser once:

```powershell
python -m playwright install chromium
```

## Configuration

Environment variables:

- `POLL_CRON`: cron schedule, default `0 * * * *`
- `TIMEZONE`: scheduler timezone, default `Europe/London`
- `RUN_ON_START`: run immediately on service start, default `true`
- `PORT`: static mirror port, default `8000`
- `DATA_DIR`: persistent data directory, default `/data`
- `LOG_LEVEL`: Python log level, default `INFO`
- `SOURCE_URL`: primary UPC source URL, default `https://www.unifiedpatentcourt.org/en/decisions-and-orders`
- `FALLBACK_SOURCE_URL`: fallback official alias, default `https://www.unified-patent-court.org/en/decisions-and-orders`
- `MAX_PAGES`: index page discovery cap, default `1` for gentle hourly polling; `backfill` is uncapped
- `MAX_ITEMS`: item discovery cap, default `10` for gentle hourly polling; `backfill` is uncapped
- `DATE_FROM`: optional oldest decision date for date-window backfill, default `2024-01-01` when date-window mode is enabled
- `DATE_TO`: optional newest decision date for date-window backfill, default today when date-window mode is enabled
- `DATE_WINDOW_DAYS`: date-window size for shallow historical discovery, default `0` disabled; usually set via `backfill --date-window-days`
- `INDEX_PAGE_RETRY_DELAY_SECONDS`: seconds to wait before retrying a UPC HTML page that appears unavailable or challenged, default `30`
- `INDEX_PAGE_MAX_RETRIES`: retry count for the same UPC HTML page URL before failing or returning partial index results, default `3`

## Unraid Compose Alternative

If you use the Docker Compose Manager plugin on Unraid, copy `docker-compose.unraid.yml`, set:

```bash
UPC_MONITOR_IMAGE=ghcr.io/edlaughton/upc-monitor:latest
```

and deploy it. The compose file persists all app data under `/mnt/user/appdata/upc-monitor`.

## Data Layout

```text
/data/upc.sqlite3
/data/public/index.html
/data/public/stats.html
/data/public/latest.json
/data/public/stats.json
/data/public/status.json
/data/public/all.ndjson
/data/public/all.json        # only when WRITE_ALL_JSON=true or --write-all-json is used
/data/public/pdfs/YYYY/node-NNNNN/<stable-name>.pdf
/data/debug/<run-id>/
```

Debug directories contain saved HTML, screenshots, and small diagnostic notes when a page fails to parse or a PDF cannot be downloaded.

## Notes

- The ingester uses Playwright Chromium for both page access and PDF downloads so cookies and headers match the browser session.
- The index table is used for discovery; UPC detail pages are the primary source for rich metadata such as headnotes, keywords, panel, language, and official PDF links.
- Use `python -m upc_ingester backfill --index-only --date-from 2024-01-01 --date-window-days 7 --max-items 2200` when Cloudflare challenges make historical detail-page enrichment impractical. This stores index metadata only, deliberately avoids detail-page and PDF network requests, and is intended to be followed by slower detail/PDF enrichment later. Date-window mode is preferred for large historical runs because it avoids deep pagination.
- Full headnotes and keywords are stored in SQLite and emitted in `/latest.json`. The HTML table intentionally shows short previews only.
- `/stats.html` and `/stats.json` are generated from already-ingested SQLite records only. The statistics are descriptive and separate UPC decision/order statistics from scraper/data-quality health.
- `/status.json` is operational run status. `/all.ndjson` is a full machine-readable export, while `/latest.json` is capped by `LATEST_EXPORT_LIMIT`.
- PDF bytes are validated as PDFs and hashed with SHA-256 before an item is marked seen.

## Tests

```powershell
pip install -r requirements.txt
pytest
```

The unit tests use local HTML fixtures and do not require a live browser.


## Private Alerts + Airtable Review Queue

This project now supports private watch-profile matching and optional Airtable sync for **matched alerts only**.

Important design rules:

- SQLite and local files under `/data` remain the full UPC mirror and source of truth.
- Airtable is a private review queue for matched items only (not a full mirror).
- Low-confidence matches are not synced by default.
- No PDFs are uploaded to Airtable; only URLs/metadata are synced.
- No Airtable AI/Automations are required.
- Private alerts/watch profiles must stay private and are never written to `/data/public`.

Airtable Free warning:

- Airtable Free limits a base to roughly 1,000 records.
- UPC catalog is larger than this, so sync intentionally only writes matched items.
- A run-time safety cap defaults to `--airtable-max-sync-records 100` and counts estimated UPC Item + Match records before writing.

### Airtable setup

Base ID default:

- `AIRTABLE_BASE_ID=appzaT3sgr7AfBKkn`

Auth:

- `AIRTABLE_TOKEN` (preferred) or `AIRTABLE_API_KEY`

Watch profiles source:

- Primary: Airtable `Watch Profiles` table (active profiles only)
- Fallback: `/data/private/watch_profiles.yml`

### Alerts commands

```bash
python -m upc_ingester alerts --dry-run
python -m upc_ingester alerts --write-json
python -m upc_ingester alerts --sync-airtable
python -m upc_ingester alerts --since-days 7
python -m upc_ingester alerts --include-low-confidence
python -m upc_ingester alerts --airtable-max-sync-records 100
```

First run sequence:

```bash
python -m upc_ingester alerts --dry-run --since-days 30
python -m upc_ingester alerts --write-json --since-days 30
python -m upc_ingester alerts --write-json --sync-airtable --since-days 7 --airtable-max-sync-records 100
```

Inside a running container:

```bash
docker exec -it upc-monitor python -m upc_ingester alerts --dry-run --since-days 30
docker exec -it upc-monitor python -m upc_ingester alerts --write-json --since-days 30
docker exec -it upc-monitor python -m upc_ingester alerts --write-json --sync-airtable --since-days 7 --airtable-max-sync-records 100
```

### Private outputs

Alerts write only under `/data/private`:

- `/data/private/alerts.json`
- `/data/private/alerts-digest-source.json`

### Optional daily scheduled alerts (same container)

Existing hourly UPC monitor remains unchanged.

Default alert scheduler config (disabled by default):

- `ALERTS_ENABLED=false`
- `ALERTS_SYNC_AIRTABLE=false`
- `ALERTS_SCHEDULE_HOUR=10`
- `ALERTS_SCHEDULE_MINUTE=5`
- `ALERTS_SINCE_DAYS=7`
- `ALERTS_INCLUDE_LOW_CONFIDENCE=false`
- `ALERTS_AIRTABLE_MAX_SYNC_RECORDS=100`

When enabled:

- Alerts job writes private JSON each run.
- Airtable sync is optional (`ALERTS_SYNC_AIRTABLE=true`).
- Alerts failures are logged and do not stop hourly ingestion.
- Overlapping alerts runs are skipped safely.

### Unraid / Docker operational examples

Manual one-off sync:

```bash
docker run --rm \
  --name upc-monitor-airtable-alerts \
  -v /mnt/user/appdata/upc-monitor:/data \
  --env-file /mnt/user/appdata/upc-monitor/private/airtable.env \
  ghcr.io/edlaughton/upc-monitor:latest \
  python -m upc_ingester alerts --write-json --sync-airtable --since-days 7 --airtable-max-sync-records 100
```

Long-running monitor with scheduled alerts:

```bash
docker run -d \
  --name upc-monitor \
  -v /mnt/user/appdata/upc-monitor:/data \
  --env-file /mnt/user/appdata/upc-monitor/private/airtable.env \
  -e ALERTS_ENABLED=true \
  -e ALERTS_SYNC_AIRTABLE=true \
  -e ALERTS_SCHEDULE_HOUR=10 \
  -e ALERTS_SCHEDULE_MINUTE=5 \
  -e ALERTS_SINCE_DAYS=7 \
  -e ALERTS_AIRTABLE_MAX_SYNC_RECORDS=100 \
  ghcr.io/edlaughton/upc-monitor:latest
```
