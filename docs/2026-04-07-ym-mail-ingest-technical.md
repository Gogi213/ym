# YM Mail Ingest Technical Design

## Scope

Current supported contour in this repo:

- `Apps Script`
- `Turso/libSQL`
- `local Python normalizer + sheet sync`

Primary ingress is now direct `Apps Script -> Turso`.

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
- scan the sync window instead of only the latest matched message per topic;
- write raw payloads and metadata directly to Turso over SQL-over-HTTP;
- dedupe raw files by stable content-based identity before insert/upsert;
- avoid business normalization.

Transport settings:

- `TURSO_DATABASE_URL`
- `TURSO_AUTH_TOKEN`

Apps Script source of truth is `appsscript-src/`. `Code.js` is a generated deployable bundle.

### Turso/libSQL

Files:

- [turso/bootstrap_schema.sql](/C:/visual%20projects/ym/turso/bootstrap_schema.sql)
- [scripts/bootstrap_turso.py](/C:/visual%20projects/ym/scripts/bootstrap_turso.py)
- [scripts/turso_runtime.py](/C:/visual%20projects/ym/scripts/turso_runtime.py)

Stores:

- raw:
  - `ingest_files`
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

- [scripts/normalize_one_run.py](/C:/visual%20projects/ym/scripts/normalize_one_run.py)
- [scripts/normalize](/C:/visual%20projects/ym/scripts/normalize)

Responsibility:

- normalize raw rows into canonical sparse facts;
- preserve current-row identity and operator-facing metrics;
- merge `secondary` topics into `primary_topic` only on exact grain match;
- refresh `operator_export_rows` and downstream sheet-facing views.

Backend selection:

- the runtime is Turso-only;
- if `TURSO_DATABASE_URL` and `TURSO_AUTH_TOKEN` are missing, the shared Turso runtime also tries the local Turso CLI cache at `%APPDATA%\\turso\\settings.json`;
- when Apps Script writes direct raw payloads, local Python now preprocesses `ingest_files.status = 'raw_only'` into parsed `ingested/skipped/error` rows before normalization.
- `ingest_rows` is still present as a compatibility layer for the current local parser path, but it is not part of the target prod ingest contract.

## Local orchestration

Files:

- [scripts/run_pipeline.py](/C:/visual%20projects/ym/scripts/run_pipeline.py)
- [scripts/doctor_direct_turso.py](/C:/visual%20projects/ym/scripts/doctor_direct_turso.py)
- [scripts/sync_goal_mapping_sheet.py](/C:/visual%20projects/ym/scripts/sync_goal_mapping_sheet.py)
- [scripts/sync_export_rows_wide_sheet.py](/C:/visual%20projects/ym/scripts/sync_export_rows_wide_sheet.py)
- [scripts/sync_pipeline_status_sheet.py](/C:/visual%20projects/ym/scripts/sync_pipeline_status_sheet.py)

Responsibility:

- run local normalize + sheet sync after raw ingest;
- update `отчеты`, `union`, and `pipeline_status`;
- expose one local operator entrypoint for post-ingest processing.
- expose one read-only doctor path for inspecting `pipeline_runs` and raw payload readiness before processing.

## Data Semantics

### Topics

- `primary_topic` is the business topic.
- `secondary` topics are optional conversion reports tied to a primary topic.
- `secondary` data is only attached to the primary topic when the exact grain matches.

### Raw ingest

- raw registry is file-centric, not day-reset-centric;
- `ingest_files.raw_file_key` is the stable raw identity used by Apps Script upsert;
- `ingest_files.file_hash` is the content hash used for business dedupe;
- `pipeline_runs` is a touched-day summary, not proof that the day was rebuilt from scratch.

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
python scripts\bootstrap_turso.py
```

Run local pipeline:

```powershell
python scripts\run_pipeline.py --service-account-json key\service-account.json
```

This is the primary local step after direct Apps Script -> Turso ingest.

Optional doctor:

```powershell
python scripts\doctor_direct_turso.py --run-date YYYY-MM-DD --validate-payloads
```

## Out of Scope

This repo no longer treats the following as supported targets:

- hosted runtime experiments
- Docker/container deployment scaffolds
- HTTP ingest fallback

Those paths were experiments and are not part of the supported operating model anymore.
