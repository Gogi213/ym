# YM Ingest Pipeline

Pipeline for ingesting Gmail report attachments into Supabase, normalizing the extracted data, and syncing operator-facing views to Google Sheets.

## What Lives Here

- [Code.js](./Code.js): Apps Script intake from Gmail to Supabase
- [supabase/functions/mail-ingest/index.ts](./supabase/functions/mail-ingest/index.ts): ingest Edge Function
- [supabase/migrations](./supabase/migrations): database schema
- [scripts/normalize_supabase.py](./scripts/normalize_supabase.py): raw -> normalized loader
- [scripts/sync_goal_mapping_sheet.py](./scripts/sync_goal_mapping_sheet.py): goal slot mapping sync to sheet `отчеты`
- [scripts/sync_export_rows_wide_sheet.py](./scripts/sync_export_rows_wide_sheet.py): operator union sync to sheet `union`
- [scripts/sync_pipeline_status_sheet.py](./scripts/sync_pipeline_status_sheet.py): pipeline status sync to sheet `pipeline_status`
- [scripts/run_pipeline.py](./scripts/run_pipeline.py): one-command orchestrator for `normalize + sync`
- [docs](./docs): business, technical, and deployment notes

## Runtime Shape

1. Apps Script reads topics from spreadsheet `17izchH29LyxuTCNWJ0SThSXmuubMnNFCjtPJiWtcxFA`, sheet `отчеты`, column `A` starting from `A2`.
2. Apps Script finds matching Gmail messages and uploads `xlsx/csv` attachments to the Supabase Edge Function.
3. Supabase stores raw files and extracted raw rows.
4. Python normalizer builds canonical fact tables and `export_rows_wide`.
5. Python sync scripts write operator views back to Google Sheets.
6. `union` is not a raw wide dump. It is an operator-facing export:
   - `utm_term` is fully collapsed to `aggregated`
   - additive metrics are precomputed for aggregation
   - dates and numbers are written as typed sheet values

## Local Commands

Node / Apps Script helpers:

```powershell
npm test
node --check Code.js
```

Python:

```powershell
python -m pip install -r requirements.txt
python -m unittest discover -s tests -p "test_*.py" -v
```

Normalizer:

```powershell
python scripts\normalize_supabase.py --run-date 2026-04-11
```

Sheets sync:

```powershell
python scripts\sync_goal_mapping_sheet.py --spreadsheet-id 17izchH29LyxuTCNWJ0SThSXmuubMnNFCjtPJiWtcxFA --service-account-json key\service-account.json
python scripts\sync_export_rows_wide_sheet.py --spreadsheet-id 17izchH29LyxuTCNWJ0SThSXmuubMnNFCjtPJiWtcxFA --service-account-json key\service-account.json
python scripts\sync_pipeline_status_sheet.py --spreadsheet-id 17izchH29LyxuTCNWJ0SThSXmuubMnNFCjtPJiWtcxFA --service-account-json key\service-account.json
```

One-command post-ingest pipeline:

```powershell
python scripts\run_pipeline.py --service-account-json key\service-account.json
```

## Required Configuration

Apps Script properties:

- `SUPABASE_FUNCTION_URL`
- `SUPABASE_INGEST_TOKEN`
- optional `SUPABASE_REST_URL`
- required for `runMonthBackfill()`: `SUPABASE_SERVICE_ROLE_KEY`
- optional debug switch: `VERBOSE_LOGGING=true`

Python environment:

- `SUPABASE_DB_URL`
or
- `SUPABASE_POOLER_URL`
- `SUPABASE_DB_PASSWORD`

Google Sheets sync:

- service account JSON file with editor access to the spreadsheet

## Main Entry Points

Apps Script:

- `run()`: daily ingest for a single target date
- `runMonthBackfill()`: backfill from month start to today, skipping dates already present in `public.ingest_files`

Python:

- `normalize_supabase.py`: rebuild normalized layer for a specific `run_date`
- `sync_goal_mapping_sheet.py`: write goal slot labels to sheet `отчеты`
- `sync_export_rows_wide_sheet.py`: write operator-facing `union`
- `sync_pipeline_status_sheet.py`: write run-level operational status to `pipeline_status`
- `run_pipeline.py`: detect pending raw `run_date`, run normalization, then sync all operator sheets

## Operator Union Semantics

`union` is intentionally optimized for operators, not for raw audit.

- `utm_term` is always collapsed to `aggregated`
- grouping happens by all exported dimensions except `utm_term`
- `bounce_visits = visits * bounce_rate`
- `pageviews = visits * page_depth`
- `time_on_site_total = visits * avg_time_on_site_seconds`
- `robot_visits = visits * robot_rate`
- `goal_1 ... goal_25` are additive in the export and remain topic-specific
- raw detail remains in Supabase; only the sheet layer is collapsed

## Pipeline Status Sheet

`pipeline_status` is the operator/run-control layer.

It shows per `run_date`:

- `pipeline_status`
- raw file counts
- raw row counts
- normalized file counts
- normalized row counts
- first/last message timestamps
- latest normalization timestamp

Current statuses:

- `ready`
- `pending_normalize`
- `raw_only`

## Repository Notes

- `key/`, `node_modules/`, generated CSV exports, and Supabase local temp files are intentionally ignored.
- Historical task/spec material is archived under [docs/archive](./docs/archive).
