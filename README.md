# YM Ingest Pipeline

Pipeline for ingesting Gmail report attachments into Supabase, normalizing the extracted data, and syncing operator-facing views to Google Sheets.

## What Lives Here

- [Code.js](./Code.js): Apps Script intake from Gmail to Supabase
- [supabase/functions/mail-ingest/index.ts](./supabase/functions/mail-ingest/index.ts): ingest Edge Function
- [supabase/migrations](./supabase/migrations): database schema
- [scripts/normalize_supabase.py](./scripts/normalize_supabase.py): raw -> normalized loader
- [scripts/sync_goal_mapping_sheet.py](./scripts/sync_goal_mapping_sheet.py): goal slot mapping sync to sheet `отчеты`
- [scripts/sync_export_rows_wide_sheet.py](./scripts/sync_export_rows_wide_sheet.py): wide union sync to sheet `union`
- [docs](./docs): business, technical, and deployment notes

## Runtime Shape

1. Apps Script reads topics from spreadsheet `17izchH29LyxuTCNWJ0SThSXmuubMnNFCjtPJiWtcxFA`, sheet `отчеты`, column `A` starting from `A2`.
2. Apps Script finds matching Gmail messages and uploads `xlsx/csv` attachments to the Supabase Edge Function.
3. Supabase stores raw files and extracted raw rows.
4. Python normalizer builds canonical fact tables and `export_rows_wide`.
5. Python sync scripts write operator views back to Google Sheets.

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

## Repository Notes

- `key/`, `node_modules/`, generated CSV exports, and Supabase local temp files are intentionally ignored.
- Historical task/spec material is archived under [docs/archive](./docs/archive).
