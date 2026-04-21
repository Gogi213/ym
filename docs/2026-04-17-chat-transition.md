# Chat Transition

## Current Supported Contour

The repo is now locked to this operating model:

1. `Apps Script` collects Gmail attachments and pushes raw ingest.
2. Raw data lands in `Turso/libSQL`.
3. Local Python runs normalize + Google Sheets sync.

What is intentionally **not** in scope anymore:

- hosted runtime migration
- Docker/container deployment
- abandoned hosting branches

The point of the cleanup was to stop carrying abandoned runtime experiments and return to one maintainable contour.

## Current Runtime Split

### Apps Script

Files:

- [Code.js](/C:/visual%20projects/ym/Code.js)
- [appsscript-src](/C:/visual%20projects/ym/appsscript-src)

Role:

- reads topic bindings from spreadsheet `отчеты`
- scans Gmail
- writes matched `xlsx/csv` raw payloads directly into Turso when `TURSO_*` script properties are set
- does **not** normalize, aggregate, or sync operator views

### Turso/libSQL

Key schema file:

- [bootstrap_schema.sql](/C:/visual%20projects/ym/turso/bootstrap_schema.sql)

Important current semantics:

- raw payload remains DB-backed in `ingest_file_payloads`
- `pipeline_runs` is now a day-level summary, not the primary raw identity
- `ingest_files.raw_file_key` is the stable file identity used by Apps Script ingest
- `ingest_files.file_hash` is the content hash used for dedupe
- `ingest_files.status` now supports the direct-write handoff state:
  - `raw_only`
- parsed/raw status set remains:
  - `ingested`
  - `skipped`
  - `error`

### Local Python post-processing

Files:

- [run_pipeline.py](/C:/visual%20projects/ym/scripts/run_pipeline.py)
- [normalize_one_run.py](/C:/visual%20projects/ym/scripts/normalize_one_run.py)
- [sync_goal_mapping_sheet.py](/C:/visual%20projects/ym/scripts/sync_goal_mapping_sheet.py)
- [sync_export_rows_wide_sheet.py](/C:/visual%20projects/ym/scripts/sync_export_rows_wide_sheet.py)
- [sync_pipeline_status_sheet.py](/C:/visual%20projects/ym/scripts/sync_pipeline_status_sheet.py)

Role:

- normalize dirty days selected from `pipeline_runs`
- refresh operator/export layer
- sync `отчеты`, `union`, `pipeline_status`

## What Was Just Cleaned Up

The repo was deliberately cleaned back down to the supported contour.

Removed:

- Docker / compose / deploy scaffolding
- abandoned hosting/runtime docs
- R2 branch
- abandoned deployment tests

Recent cleanup also simplified ingest status semantics:

- removed stale `uploaded / parsed / failed` file lifecycle statuses from the supported model
- status endpoint now exposes only the day-level fields needed by the actual workflow
- added a regression test for `skipped-only day -> raw_only`
- added direct `Apps Script -> Turso` raw-write path with local Python parsing `raw_only` payloads later

## Current Operational Reality

### Primary path: direct Apps Script -> Turso

Apps Script now prefers:

- `TURSO_DATABASE_URL`
- `TURSO_AUTH_TOKEN`

In that mode:

- Apps Script scans the sync window and writes raw files/payloads straight into Turso
- raw ingest is idempotent on stable file identity
- default ingest no longer does destructive day reset
- no local HTTP ingest is needed
- local Python is run manually afterwards with `scripts/run_pipeline.py`
- skipped raw files are purged after normalize, so the DB keeps only raw that was actually used

## Current Working Config Conventions

### Apps Script properties

- `TURSO_DATABASE_URL`
- `TURSO_AUTH_TOKEN`

## Turso Gotcha

One real operational issue already hit in this session:

- the cached Turso **database token** in `%APPDATA%\\turso\\settings.json` can expire
- when it expires, the ingest service fails on startup with `401 Unauthorized` from `connection.sync()`

What is still usable there:

- the account token in the same file can be used to mint a fresh DB token

That issue is runtime/ops, not repo architecture.

## Current Validation State

Latest verified state after runtime v2 cleanup:

- direct `Apps Script -> Turso -> Python -> Sheets` runs complete end-to-end;
- `visits` are consistent across `raw -> operator_export_rows -> union`;
- `goal_N` are consistent across `raw -> operator_export_rows -> union`;
- skipped raw payloads are purged after normalize;
- the primary operator runtime no longer depends on `ingest_rows`.

## Most Important Files For The Next Chat

If a new chat needs to continue productively, start from these:

- [README.md](/C:/visual%20projects/ym/README.md)
- [2026-04-17-local-python-runbook.md](/C:/visual%20projects/ym/docs/2026-04-17-local-python-runbook.md)
- [2026-04-07-ym-mail-ingest-technical.md](/C:/visual%20projects/ym/docs/2026-04-07-ym-mail-ingest-technical.md)
- [2026-04-22-system-audit.md](/C:/visual%20projects/ym/docs/2026-04-22-system-audit.md)

## What Should Happen Next

The next chat should **not** restart architecture churn.

Reasonable next work:

1. configure Apps Script with `TURSO_DATABASE_URL` and `TURSO_AUTH_TOKEN`
2. ingest real data through Apps Script directly into Turso
3. run local Python post-processing
4. validate `raw_file_key/file_hash`, `pipeline_runs`, `union`, and goal mappings
5. keep docs and launcher aligned with the current operator flow

Unreasonable next work:

- another hosted runtime migration branch
- another storage redesign branch
- reintroducing Docker/runtime scaffolding without a concrete deployment decision
