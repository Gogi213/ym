# YM Ingest Pipeline

Pipeline for loading Gmail report attachments from Apps Script into a cloud database, then running normalization and Google Sheets sync from a local Python environment.

## Supported Contour

Current supported operating model:

- `Apps Script -> Turso/libSQL raw layer`
- `local Python -> normalize + operator sheet sync`

Business-critical ingest semantics:

- Apps Script scans the full sync window, not just the latest message per topic.
- raw files are deduped by stable content-based identity before they hit Turso.
- default ingest no longer does destructive day reset.
- `pipeline_runs` is a day summary, not the primary raw identity.

What this repo supports directly:

- Apps Script source and bundle generation
- direct Apps Script raw writes into Turso/libSQL
- Turso/libSQL bootstrap and runtime
- local Python normalize/sync scripts

What this repo does **not** maintain anymore:

- hosted HTTP ingest target
- Docker/container deployment path
- legacy fallback runtimes

## Repository Layout

- [Code.js](./Code.js): deployable Apps Script bundle
- [appsscript-src](./appsscript-src): source of truth for Apps Script code
- [scripts/normalize_one_run.py](./scripts/normalize_one_run.py): one-run local normalize CLI
- [scripts/normalize](./scripts/normalize): modular normalizer package
- [scripts/run_pipeline.py](./scripts/run_pipeline.py): local orchestration entrypoint
- [scripts/doctor_direct_turso.py](./scripts/doctor_direct_turso.py): read-only doctor/smoke for the direct Turso contour
- [scripts/sync_goal_mapping_sheet.py](./scripts/sync_goal_mapping_sheet.py): sync `отчеты`
- [scripts/sync_export_rows_wide_sheet.py](./scripts/sync_export_rows_wide_sheet.py): sync `union`
- [scripts/sync_pipeline_status_sheet.py](./scripts/sync_pipeline_status_sheet.py): sync `pipeline_status`
- [scripts/turso_runtime.py](./scripts/turso_runtime.py): shared Turso/libSQL connection bootstrap
- [scripts/bootstrap_turso.py](./scripts/bootstrap_turso.py): apply Turso bootstrap schema
- [turso/bootstrap_schema.sql](./turso/bootstrap_schema.sql): Turso DDL
- [docs](./docs): business and technical notes

## Current Data Flow

1. Apps Script reads topic bindings from spreadsheet `17izchH29LyxuTCNWJ0SThSXmuubMnNFCjtPJiWtcxFA`, sheet `отчеты`.
2. Apps Script scans matching Gmail messages in the sync window and writes idempotent raw `xlsx/csv` attachments into Turso/libSQL.
3. Raw layer is written into Turso/libSQL.
4. Local Python parses raw payloads locally, updates compact normalized state in Turso, and refreshes operator export rows.
5. Local Python sync scripts write `отчеты`, `union`, and `pipeline_status` back to Google Sheets.

## Apps Script

Source of truth:
- edit files in [appsscript-src](./appsscript-src)
- rebuild bundle with:

```powershell
python scripts\build_appsscript_bundle.py
```

Transport config used by Apps Script:
- `TURSO_DATABASE_URL`
- `TURSO_AUTH_TOKEN`

Apps Script now uses only direct SQL-over-HTTP writes into Turso.

Direct raw ingest contract:

- one raw row per stable `raw_file_key`
- content hash stored in `file_hash`
- duplicate payloads within the same topic/day stream are upserted, not duplicated
- `runMonthBackfill()` defaults to re-scanning the month window instead of skipping already-ready days

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

Run read-only doctor for the direct Turso contour:

```powershell
npm run doctor:turso -- --run-date YYYY-MM-DD --validate-payloads
```

Bootstrap Turso schema:

```powershell
python scripts\bootstrap_turso.py
```

If `TURSO_DATABASE_URL` and `TURSO_AUTH_TOKEN` are not set, the Turso runtime will also try to load the active database URL and DB token from the local Turso CLI cache at `%APPDATA%\turso\settings.json`.

Run one-day normalize:

```powershell
python scripts\normalize_one_run.py --run-date 2026-04-11
```

Run full local post-ingest pipeline:

```powershell
python scripts\run_pipeline.py --service-account-json key\service-account.json
```

This is now the primary local step after direct Apps Script -> Turso ingest.

Current supported hot path:

- `ingest_files`
- `ingest_file_payloads`
- `pipeline_runs`
- `topic_goal_slots`
- `operator_export_rows`

Legacy compatibility tables like `ingest_rows` / `fact_*` may still exist in schema history, but they are not part of the supported operator flow anymore.

## Performance Notes

- Full month rebuild is still batch work: raw ingest can contain hundreds of files and each dirty `run_date` is normalized separately.
- The expensive part is now read/parse/build of compact export rows, not `fact_*` remote write amplification.
- `run_pipeline.py` is the supported operator entrypoint for local Python execution.
- If there are no pending `run_date`, it only syncs `pipeline_status`.

## Validation State

The repo already contains validation work proving:
- `visits` remain consistent across `raw -> export_rows_wide -> union`
- `goal_N` remain consistent across `raw -> export_rows_wide -> union`
- `union` is an operator-facing aggregated export, not a raw row dump

## Docs

Primary explaining doc:
- [2026-04-22-system-overview.md](./docs/2026-04-22-system-overview.md)

Main technical note:
- [2026-04-07-ym-mail-ingest-technical.md](./docs/2026-04-07-ym-mail-ingest-technical.md)
- [2026-04-17-local-python-runbook.md](./docs/2026-04-17-local-python-runbook.md)
- [2026-04-17-chat-transition.md](./docs/2026-04-17-chat-transition.md)
- [2026-04-22-system-audit.md](./docs/2026-04-22-system-audit.md)

Business note:
- [2026-04-07-ym-mail-ingest-business.md](./docs/2026-04-07-ym-mail-ingest-business.md)
