# YM Mail Ingest Technical Design

## Scope

Current supported contour in this repo:

- `Apps Script`
- `configured ingest endpoint`
- `Turso/libSQL`
- `local Python normalizer + sheet sync`

This document intentionally describes the working data model and local Python processing path. It does **not** document a supported hosted deployment target anymore.

## Runtime Components

### Apps Script

Files:

- [appsscript-src](/C:/visual%20projects/ym/appsscript-src)
- [Code.js](/C:/visual%20projects/ym/Code.js)

Responsibility:

- read primary and optional secondary topics from spreadsheet `17izchH29LyxuTCNWJ0SThSXmuubMnNFCjtPJiWtcxFA`, sheet `отчеты`;
- search Gmail mailbox `ya-stats@solta.io`;
- collect `xlsx/csv` attachments;
- send attachments and metadata to the configured ingest endpoint;
- avoid business normalization.

Transport settings:

- preferred:
  - `INGEST_BASE_URL`
  - `INGEST_TOKEN`
  - optional `INGEST_STATUS_URL`
- legacy fallback still exists in code:
  - `SUPABASE_FUNCTION_URL`
  - `SUPABASE_INGEST_TOKEN`
  - optional `SUPABASE_REST_URL`
  - optional `SUPABASE_SERVICE_ROLE_KEY`

Apps Script source of truth is `appsscript-src/`. `Code.js` is a generated deployable bundle.

### Local Python ingest service

Files:

- [ingest_service/app.py](/C:/visual%20projects/ym/ingest_service/app.py)
- [ingest_service/runtime.py](/C:/visual%20projects/ym/ingest_service/runtime.py)
- [ingest_service/handlers.py](/C:/visual%20projects/ym/ingest_service/handlers.py)
- [ingest_service/storage.py](/C:/visual%20projects/ym/ingest_service/storage.py)
- [ingest_service/parse.py](/C:/visual%20projects/ym/ingest_service/parse.py)
- [ingest_service/main.py](/C:/visual%20projects/ym/ingest_service/main.py)

Responsibility:

- expose `POST /reset`, `POST /ingest`, `GET /pipeline-runs/{run_date}`;
- accept Apps Script payloads;
- parse `xlsx/csv` table blocks;
- write raw files, raw rows, and `pipeline_runs` state into Turso/libSQL.

This repo keeps the ingest service as a local Python runtime. External exposure, if needed for Apps Script, is operational and outside the repo scope.

### Turso/libSQL

Files:

- [turso/bootstrap_schema.sql](/C:/visual%20projects/ym/turso/bootstrap_schema.sql)
- [scripts/bootstrap_turso.py](/C:/visual%20projects/ym/scripts/bootstrap_turso.py)
- [scripts/turso_runtime.py](/C:/visual%20projects/ym/scripts/turso_runtime.py)

Stores:

- raw:
  - `ingest_files`
  - `ingest_rows`
  - `ingest_file_payloads`
- normalized:
  - `fact_rows`
  - `fact_dimensions`
  - `fact_metrics`
  - `topic_goal_slots`
- state/cache:
  - `pipeline_runs`
  - `operator_export_rows`
- views:
  - `export_rows_wide`
  - `goal_mapping_wide`

## Python normalizer

Files:

- [scripts/normalize_supabase.py](/C:/visual%20projects/ym/scripts/normalize_supabase.py)
- [scripts/normalize](/C:/visual%20projects/ym/scripts/normalize)

Responsibility:

- normalize raw rows into canonical sparse facts;
- preserve current-row identity and operator-facing metrics;
- merge `secondary` topics into `primary_topic` only on exact grain match;
- refresh `operator_export_rows` and downstream sheet-facing views.

Backend selection:

- Turso is auto-selected when `TURSO_DATABASE_URL` and `TURSO_AUTH_TOKEN` are set;
- explicit override via `NORMALIZE_DB_BACKEND=turso` remains available.

## Local orchestration

Files:

- [scripts/run_pipeline.py](/C:/visual%20projects/ym/scripts/run_pipeline.py)
- [scripts/sync_goal_mapping_sheet.py](/C:/visual%20projects/ym/scripts/sync_goal_mapping_sheet.py)
- [scripts/sync_export_rows_wide_sheet.py](/C:/visual%20projects/ym/scripts/sync_export_rows_wide_sheet.py)
- [scripts/sync_pipeline_status_sheet.py](/C:/visual%20projects/ym/scripts/sync_pipeline_status_sheet.py)

Responsibility:

- run local normalize + sheet sync after raw ingest;
- update `отчеты`, `union`, and `pipeline_status`;
- expose one local operator entrypoint for post-ingest processing.

## Data Semantics

### Topics

- `primary_topic` is the business topic.
- `secondary` topics are optional conversion reports tied to a primary topic.
- `secondary` data is only attached to the primary topic when the exact grain matches.

### Operator export

`union` is an operator-facing export, not a raw dump.

Current semantics:

- `utm_term` is collapsed to `aggregated`
- `utm_content` is collapsed to `aggregated`
- higher-grain UTM dimensions remain when they differ
- additive metrics are aggregation-ready

### Validation rule

The pipeline is considered correct only when sums remain consistent across:

- raw extracted rows
- `export_rows_wide`
- Google Sheets `union`

This has already been validated for both `visits` and `goal_N` metrics on the working contour.

## Local Commands

Install dependencies:

```powershell
python -m pip install -r requirements.txt
```

Bootstrap Turso:

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

Run local pipeline:

```powershell
python scripts\run_pipeline.py --service-account-json key\service-account.json
```

## Out of Scope

This repo no longer treats the following as supported targets:

- Render runtime
- Northflank runtime
- Cloudflare Worker runtime
- Docker/container deployment scaffolds

Those paths were experiments and are not part of the supported operating model anymore.
