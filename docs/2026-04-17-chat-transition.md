# Chat Transition

## Current Supported Contour

The repo is now locked to this operating model:

1. `Apps Script` collects Gmail attachments and pushes raw ingest.
2. Raw data lands in `Turso/libSQL`.
3. Local Python runs normalize + Google Sheets sync.

What is intentionally **not** in scope anymore:

- hosted runtime migration
- Docker/container deployment
- Cloudflare Worker / R2 / Render / Northflank branches

The point of the cleanup was to stop carrying abandoned runtime experiments and return to one maintainable contour.

## Current Runtime Split

### Apps Script

Files:

- [Code.js](/C:/visual%20projects/ym/Code.js)
- [appsscript-src](/C:/visual%20projects/ym/appsscript-src)

Role:

- reads topic bindings from spreadsheet `отчеты`
- scans Gmail
- uploads matched `xlsx/csv` files to the configured ingest endpoint
- does **not** normalize, aggregate, or sync operator views

### Local Python ingest service

Files:

- [ingest_service/main.py](/C:/visual%20projects/ym/ingest_service/main.py)
- [ingest_service/handlers.py](/C:/visual%20projects/ym/ingest_service/handlers.py)
- [ingest_service/storage.py](/C:/visual%20projects/ym/ingest_service/storage.py)
- [ingest_service/parse.py](/C:/visual%20projects/ym/ingest_service/parse.py)

Role:

- exposes:
  - `POST /reset`
  - `POST /ingest`
  - `GET /pipeline-runs/{run_date}`
- parses incoming `xlsx/csv`
- writes raw layer + `pipeline_runs` to Turso

### Turso/libSQL

Primary working database right now:

- `ym-migration-20260414`

Key schema file:

- [bootstrap_schema.sql](/C:/visual%20projects/ym/turso/bootstrap_schema.sql)

Important current semantics:

- raw payload remains DB-backed in `ingest_file_payloads`
- `pipeline_runs` is the execution truth for `run_date`
- `ingest_files.status` is intentionally simple again:
  - `ingested`
  - `skipped`
  - `error`

### Local Python post-processing

Files:

- [run_pipeline.py](/C:/visual%20projects/ym/scripts/run_pipeline.py)
- [normalize_supabase.py](/C:/visual%20projects/ym/scripts/normalize_supabase.py)
- [sync_goal_mapping_sheet.py](/C:/visual%20projects/ym/scripts/sync_goal_mapping_sheet.py)
- [sync_export_rows_wide_sheet.py](/C:/visual%20projects/ym/scripts/sync_export_rows_wide_sheet.py)
- [sync_pipeline_status_sheet.py](/C:/visual%20projects/ym/scripts/sync_pipeline_status_sheet.py)

Role:

- normalize dirty days from `pipeline_runs`
- refresh operator/export layer
- sync `отчеты`, `union`, `pipeline_status`

## What Was Just Cleaned Up

The repo was deliberately cleaned back down to the supported contour.

Removed:

- Docker / compose / deploy scaffolding
- Cloudflare Worker branch
- Render / Northflank / Cloudflare runtime docs
- R2 branch
- abandoned deployment tests

Recent cleanup also simplified ingest status semantics:

- removed stale `uploaded / parsed / failed` file lifecycle statuses from the supported model
- status endpoint now exposes only the day-level fields needed by the actual workflow
- added a regression test for `skipped-only day -> raw_only`

## Current Operational Reality

### Local ingest service

The local ingest service can be started with:

```powershell
$env:INGEST_TOKEN='<ingest-token>'
$env:TURSO_DATABASE_URL='libsql://<db-name>-<org>.turso.io'
$env:TURSO_AUTH_TOKEN='<db-token>'
uvicorn ingest_service.main:app --host 0.0.0.0 --port 8000
```

### Apps Script cannot reach localhost directly

This remains the hard boundary:

- Google Apps Script cannot call `127.0.0.1`
- if Apps Script must hit the local ingest service, you need a public bridge

Current practical bridge:

```powershell
cloudflared tunnel --url http://127.0.0.1:8000 --no-autoupdate
```

That yields a temporary URL like:

- `https://<random>.trycloudflare.com`

Use that URL for:

- `INGEST_BASE_URL`
- `INGEST_STATUS_URL`

This is an operational tunnel, not a supported hosted deployment target.

## Current Working Config Conventions

### Apps Script properties

Used today:

- `INGEST_BASE_URL`
- `INGEST_TOKEN`
- optional `INGEST_STATUS_URL`

Legacy fallback still exists in code, but is no longer the preferred contour:

- `SUPABASE_FUNCTION_URL`
- `SUPABASE_INGEST_TOKEN`
- `SUPABASE_REST_URL`
- `SUPABASE_SERVICE_ROLE_KEY`

### Local ingest token

The current local working token used in session was:

- `local-ingest-token-ym`

That is an operational local token, not a repo-level secret-management system.

## Turso Gotcha

One real operational issue already hit in this session:

- the cached Turso **database token** in `%APPDATA%\\turso\\settings.json` can expire
- when it expires, the ingest service fails on startup with `401 Unauthorized` from `connection.sync()`

What is still usable there:

- the account token in the same file can be used to mint a fresh DB token

That issue is runtime/ops, not repo architecture.

## Current Validation State

Latest verified state after cleanup:

- Python tests: `91/91`
- JS tests: `39/39`
- `node --check Code.js` passes

Existing validation already established:

- `visits` are consistent across `raw -> export_rows_wide -> union`
- `goal_N` are consistent across `raw -> export_rows_wide -> union`

## Most Important Files For The Next Chat

If a new chat needs to continue productively, start from these:

- [README.md](/C:/visual%20projects/ym/README.md)
- [2026-04-17-local-python-runbook.md](/C:/visual%20projects/ym/docs/2026-04-17-local-python-runbook.md)
- [2026-04-07-ym-mail-ingest-technical.md](/C:/visual%20projects/ym/docs/2026-04-07-ym-mail-ingest-technical.md)
- [2026-04-17-local-python-cloud-db-pipeline-design.md](/C:/visual%20projects/ym/docs/superpowers/specs/2026-04-17-local-python-cloud-db-pipeline-design.md)
- [2026-04-17-local-python-cloud-db-pipeline.md](/C:/visual%20projects/ym/docs/superpowers/plans/2026-04-17-local-python-cloud-db-pipeline.md)

## What Should Happen Next

The next chat should **not** restart architecture churn.

Reasonable next work:

1. operate the current contour
2. ingest real data through Apps Script
3. run local Python post-processing
4. validate `pipeline_runs`, `union`, and goal mappings
5. only then optimize specific bottlenecks if they are real

Unreasonable next work:

- another hosted runtime migration branch
- another storage redesign branch
- reintroducing Docker/runtime scaffolding without a concrete deployment decision
