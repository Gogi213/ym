# YM Ingest Pipeline

Pipeline for loading Gmail report attachments from Apps Script into a cloud database, then running normalization and Google Sheets sync from a local Python environment.

## Supported Contour

Current supported operating model:

- `Apps Script -> configured ingest endpoint -> Turso/libSQL`
- `local Python -> normalize + operator sheet sync`

What this repo supports directly:

- Apps Script source and bundle generation
- local Python ingest service
- Turso/libSQL bootstrap and runtime
- local Python normalize/sync scripts

What this repo does **not** maintain anymore:

- hosted deployment target for the ingest service
- Docker/container deployment path
- abandoned hosted-runtime experiments

## Repository Layout

- [Code.js](./Code.js): deployable Apps Script bundle
- [appsscript-src](./appsscript-src): source of truth for Apps Script code
- [ingest_service](./ingest_service): local Python HTTP ingest service
- [scripts/normalize_supabase.py](./scripts/normalize_supabase.py): normalizer CLI facade
- [scripts/normalize](./scripts/normalize): modular normalizer package
- [scripts/run_pipeline.py](./scripts/run_pipeline.py): local orchestration entrypoint
- [scripts/sync_goal_mapping_sheet.py](./scripts/sync_goal_mapping_sheet.py): sync `отчеты`
- [scripts/sync_export_rows_wide_sheet.py](./scripts/sync_export_rows_wide_sheet.py): sync `union`
- [scripts/sync_pipeline_status_sheet.py](./scripts/sync_pipeline_status_sheet.py): sync `pipeline_status`
- [scripts/turso_runtime.py](./scripts/turso_runtime.py): shared Turso/libSQL connection bootstrap
- [scripts/bootstrap_turso.py](./scripts/bootstrap_turso.py): apply Turso bootstrap schema
- [turso/bootstrap_schema.sql](./turso/bootstrap_schema.sql): Turso DDL
- [docs](./docs): business and technical notes

## Current Data Flow

1. Apps Script reads topic bindings from spreadsheet `17izchH29LyxuTCNWJ0SThSXmuubMnNFCjtPJiWtcxFA`, sheet `отчеты`.
2. Apps Script finds matching Gmail messages and uploads `xlsx/csv` attachments to the configured ingest endpoint.
3. Raw layer is written into Turso/libSQL.
4. Local Python normalizer builds canonical fact tables and operator cache.
5. Local Python sync scripts write `отчеты`, `union`, and `pipeline_status` back to Google Sheets.

## Apps Script

Source of truth:
- edit files in [appsscript-src](./appsscript-src)
- rebuild bundle with:

```powershell
python scripts\build_appsscript_bundle.py
```

Transport config used by Apps Script:
- `INGEST_BASE_URL`
- `INGEST_TOKEN`
- optional `INGEST_STATUS_URL`

Apps Script logic remains generic. This repo no longer documents a built-in hosted runtime target. If Apps Script must reach an external endpoint, that exposure is handled outside the repo.

## Local Python Commands

Install dependencies:

```powershell
python -m pip install -r requirements.txt
```

Run all tests:

```powershell
npm test
python -m unittest discover -s tests -p "test_*.py" -v
```

Check Apps Script bundle syntax:

```powershell
node --check Code.js
```

Bootstrap Turso schema:

```powershell
$env:TURSO_DATABASE_URL='libsql://<db-name>-<org>.turso.io'
$env:TURSO_AUTH_TOKEN='<db-token>'
python scripts\bootstrap_turso.py
```

Run local ingest service:

```powershell
$env:INGEST_TOKEN='<ingest-token>'
$env:TURSO_DATABASE_URL='libsql://<db-name>-<org>.turso.io'
$env:TURSO_AUTH_TOKEN='<db-token>'
uvicorn ingest_service.main:app --host 0.0.0.0 --port 8000
```

Run one-day normalize:

```powershell
python scripts\normalize_supabase.py --run-date 2026-04-11
```

Run full local post-ingest pipeline:

```powershell
python scripts\run_pipeline.py --service-account-json key\service-account.json
```

## Performance Notes

- Full month rebuild is expensive by design: raw ingest can contain tens of thousands of rows and each dirty `run_date` is normalized separately.
- Empty-state rebuilds use bootstrap fast path inside `run_pipeline.py`.
- `run_pipeline.py` is the supported operator entrypoint for local Python execution.
- If there are no pending `run_date`, it only syncs `pipeline_status`.

## Validation State

The repo already contains validation work proving:
- `visits` remain consistent across `raw -> export_rows_wide -> union`
- `goal_N` remain consistent across `raw -> export_rows_wide -> union`
- `union` is an operator-facing aggregated export, not a raw row dump

## Docs

Main technical note:
- [2026-04-07-ym-mail-ingest-technical.md](./docs/2026-04-07-ym-mail-ingest-technical.md)

Business note:
- [2026-04-07-ym-mail-ingest-business.md](./docs/2026-04-07-ym-mail-ingest-business.md)
