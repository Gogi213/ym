# YM Ingest Pipeline

Pipeline for ingesting Gmail report attachments from Apps Script into a Python ingest service, storing raw and normalized data in Turso/libSQL, and syncing operator-facing views to Google Sheets.

## What Lives Here

- [Code.js](./Code.js): Apps Script intake from Gmail to the configured ingest endpoint
- [appsscript-src](./appsscript-src): modular Apps Script source used to generate `Code.js`
- [supabase/functions/mail-ingest/index.ts](./supabase/functions/mail-ingest/index.ts): thin ingest Edge Function entrypoint
- [supabase/functions/mail-ingest](./supabase/functions/mail-ingest): split Edge Function modules (`auth / handlers / parse / shared / supabase`)
- [supabase/migrations](./supabase/migrations): database schema
- [scripts/normalize_supabase.py](./scripts/normalize_supabase.py): thin CLI/public facade for the normalizer
- [scripts/normalize](./scripts/normalize): modular normalizer package
- [scripts/sync_goal_mapping_sheet.py](./scripts/sync_goal_mapping_sheet.py): goal slot mapping sync to sheet `отчеты`
- [scripts/sync_export_rows_wide_sheet.py](./scripts/sync_export_rows_wide_sheet.py): operator union sync to sheet `union`
- [scripts/sync_pipeline_status_sheet.py](./scripts/sync_pipeline_status_sheet.py): pipeline status sync to sheet `pipeline_status`
- [scripts/run_pipeline.py](./scripts/run_pipeline.py): one-command orchestrator for `normalize + sync`
- [scripts/turso_runtime.py](./scripts/turso_runtime.py): libSQL/Turso Python connection bootstrap
- [scripts/bootstrap_turso.py](./scripts/bootstrap_turso.py): apply Turso-compatible schema bootstrap
- [turso/bootstrap_schema.sql](./turso/bootstrap_schema.sql): Turso/libSQL bootstrap DDL
- [ingest_service](./ingest_service): new Python HTTP ingest service scaffold for Turso cutover
- [Dockerfile.ingest-service](./Dockerfile.ingest-service): generic container runtime for the new Python ingest service
- [docker-compose.ingest-service.yml](./docker-compose.ingest-service.yml): local/prod-like compose scaffold for the ingest service
- [.env.ingest-service.example](./.env.ingest-service.example): required env template for the ingest service container
- [docs](./docs): business, technical, and deployment notes

## Current Storage State

Current target runtime is:

- `Apps Script -> Python ingest service -> Turso/libSQL -> Python normalize/sync -> Google Sheets`
- live ingest runtime: `Render`

Turso migration work has started:

- Turso-compatible bootstrap schema exists in [turso/bootstrap_schema.sql](./turso/bootstrap_schema.sql)
- Python runtime can connect to Turso via `libsql`
- bootstrap can be applied from Python via [bootstrap_turso.py](./scripts/bootstrap_turso.py)
- Python ingest service exists in [ingest_service](./ingest_service) and already writes raw data to Turso
- normalizer auto-detects Turso when `TURSO_DATABASE_URL` and `TURSO_AUTH_TOKEN` are present
- explicit override via `NORMALIZE_DB_BACKEND=turso` is still supported
- Turso-compatible read/write/operator modules exist under [scripts/normalize](./scripts/normalize)
- operator read-path for `goal_mapping / union / pipeline_status` already works against Turso-backed tables/views
- runtime wiring and live smoke against `ym-migration-20260414` are already verified for:
  - `reset + ingest`
  - `normalize`
  - `pipeline_status / operator_export_rows / goal_mapping_wide` reads

What is not cut over yet:

- production env is not switched to Turso by default
- Apps Script is not yet pointed at the live Render URL
- the old Supabase runtime still exists in repo as legacy fallback/reference during migration

## Runtime Shape

1. Apps Script reads topic bindings from spreadsheet `17izchH29LyxuTCNWJ0SThSXmuubMnNFCjtPJiWtcxFA`, sheet `отчеты`:
   - column `A` = primary topic
   - column `B` = optional secondary topic with conversions
2. Apps Script finds matching Gmail messages and uploads `xlsx/csv` attachments to the configured ingest endpoint.
3. Legacy ingest path:
   - Supabase Edge Function stores raw files and extracted raw rows.
   - package boundaries:
     - `auth.ts`: token auth
     - `handlers.ts`: reset/ingest request handlers
     - `parse.ts`: `csv/xlsx` table detection and parsing
     - `shared.ts`: shared types and HTTP helpers
     - `supabase.ts`: raw writes and `pipeline_runs` updates
4. New ingest path:
   - Python FastAPI ingest service writes the same raw layer into Turso/libSQL.
   - exposes:
     - `POST /reset`
     - `POST /ingest`
     - `GET /pipeline-runs/{run_date}`
   - live runtime: `Render`
5. Python normalizer builds canonical fact tables and `export_rows_wide`.
   - secondary topics do not become standalone operator topics
   - they are attached to their `primary_topic` only when the exact grain matches
6. Python normalizer also refreshes `operator_export_rows` only for dirty `run_date`.
   - package boundaries:
     - `scripts/normalize/fields.py`: header parsing, row parsing, row identity
     - `scripts/normalize/transform.py`: goal-slot collection, secondary merge, fact payload assembly
     - `scripts/normalize/db*.py`: Postgres backend
     - `scripts/normalize/turso_*.py`: Turso/libSQL backend
     - `scripts/normalize/db.py`: backend selector facade
     - `scripts/normalize/pipeline.py`: normalize/finalize orchestration
7. Python sync scripts write operator views back to Google Sheets.
8. `union` is not a raw wide dump. It is an operator-facing export:
   - `utm_term` is fully collapsed to `aggregated`
   - additive metrics are precomputed for aggregation
   - dates and numbers are written as typed sheet values

## Local Commands

Node / Apps Script helpers:

```powershell
python scripts\build_appsscript_bundle.py
npm test
node --check Code.js
```

Apps Script source of truth:

- edit files in `appsscript-src/`
- rebuild `Code.js` with:

```powershell
python scripts\build_appsscript_bundle.py
```

- `Code.js` remains the deployable single-file artifact for manual Apps Script copy/paste

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

Turso bootstrap:

```powershell
$env:TURSO_DATABASE_URL='libsql://<db-name>-<org>.turso.io'
$env:TURSO_AUTH_TOKEN='<db-token>'
python scripts\bootstrap_turso.py
```

Python ingest service:

```powershell
$env:INGEST_TOKEN='<ingest-token>'
$env:TURSO_DATABASE_URL='libsql://<db-name>-<org>.turso.io'
$env:TURSO_AUTH_TOKEN='<db-token>'
uvicorn ingest_service.main:app --host 0.0.0.0 --port 8000
```

Containerized ingest service:

```powershell
docker build -f Dockerfile.ingest-service -t ym-ingest-service .
docker run --rm -p 8000:8000 `
  -e INGEST_TOKEN='<ingest-token>' `
  -e TURSO_DATABASE_URL='libsql://<db-name>-<org>.turso.io' `
  -e TURSO_AUTH_TOKEN='<db-token>' `
  ym-ingest-service
```

Compose-based ingest service:

```powershell
Copy-Item .env.ingest-service.example .env.ingest-service
docker compose -f docker-compose.ingest-service.yml up --build
```

Render deployment:

- see [docs/2026-04-16-turso-render-deploy.md](./docs/2026-04-16-turso-render-deploy.md)

Turso-backed normalizer smoke:

```powershell
$env:TURSO_DATABASE_URL='libsql://<db-name>-<org>.turso.io'
$env:TURSO_AUTH_TOKEN='<db-token>'
python -m scripts.normalize_supabase --run-date 2026-04-14
```

`run_pipeline.py` now prints phase logs and timings during execution:

- `pipeline_started`
- `normalize_*`
- `sheet_sync_*`
- `pipeline_finished`

If there are no pending `run_date`, it only syncs `pipeline_status` and skips `отчеты` / `union`.

Cold-start behavior:

- if the normalized layer is empty, `run_pipeline.py` automatically switches to bootstrap mode;
- in bootstrap mode dirty `run_date` values are still processed sequentially, but:
  - existing normalized rows are not deleted per day;
  - per-day `is_current` refresh is deferred;
  - per-day `operator_export_rows` refresh is deferred;
- after all pending days are loaded, one final reconcile pass refreshes `is_current`, `operator_export_rows`, and `pipeline_runs`.

## Performance Notes

- Full month rebuild is intentionally expensive:
  - raw ingest can contain tens of thousands of rows
  - each pending `run_date` is normalized separately
  - after normalization, `отчеты`, `union`, and `pipeline_status` are synced back to Google Sheets
- Shipped optimization for empty-state rebuilds:
  - bootstrap mode removes the most expensive per-day cleanup/finalize passes
  - this is the current production path for `empty normalized layer -> rebuild everything`
- Investigated but not shipped:
  - two-worker parallel `run_date` rebuild
  - real measurement on live data regressed versus sequential execution because Postgres contention on `fact_*` outweighed worker overlap
- For targeted debugging or validation, prefer a single-day run:

```powershell
python scripts\normalize_supabase.py --run-date 2026-04-11
```

- `run_pipeline.py` is the correct operator entrypoint when the goal is to fully refresh pending dates and then refresh sheets.
- `union` sync no longer pulls the full `export_rows_wide` into Python.
  - Python reads pre-aggregated rows from `public.operator_export_rows`
  - the expensive operator aggregation is cached per `run_date` during normalization
- current-state refresh no longer scans all historical rows for an entire topic.
  - normalization refreshes `is_current` only for affected `(topic, row_hash)` keys of the dirty `run_date`
- latest cold-start measurement after bootstrap fast path:
  - `2026-04-01 .. 2026-04-12` rebuild from empty normalized layer completed in about `606882ms`
- normalizer entrypoint is no longer a 1000+ line god object:
  - `scripts/normalize_supabase.py` is now a thin facade
  - heavy logic lives in the `scripts/normalize/` package by responsibility

## Validation Snapshot

Сквозная сверка `raw -> export_rows_wide -> union` выполнена на `2026-04-13` после свежего month-backfill и полного cold rebuild normalized-слоя.

Проверка включала:

- все темы, которые дошли до `public.ingest_files` со статусом `ingested`;
- все дни `2026-04-01 .. 2026-04-12`;
- суммы `visits` по дням;
- суммы `goal_1 ... goal_N` по дням для всех тем, у которых есть goal-слоты;
- сравнение между:
  - raw строками, извлечёнными из исходных файлов;
  - `public.export_rows_wide`;
  - листом `union` в Google Sheets.

Семантика проверки:

- `visits` проверяются по `primary` raw-файлам;
- `goal_*` проверяются по effective topic:
  - `primary` goal-значения;
  - плюс `secondary`, приклеенный к `primary_topic`.

Итог проверки:

- `topics_total = 23`
- `visit_days_total = 223`
- `goal_points_total = 403`
- `visit_mismatches = 0`
- `goal_mismatches = 0`

Причина последнего критичного фикса:

- старая dedup-логика строила `row_hash` только по каноническим dimensions и `report_date`;
- это схлопывало строки, которые различались по неканоническим текстовым dimensions;
- в некоторых темах goal-значения оказывались только в не-current версии строки.

Что изменено:

- `row_hash` теперь учитывает unmapped text dimensions, если они реально влияют на идентичность строки;
- при этом он по-прежнему игнорирует метрики, durations, даты и goal-like числовые поля;
- после фикса выполнен полный rebuild и повторная валидация всей цепочки.

## Required Configuration

Apps Script properties:

- preferred for the new Python ingest service:
  - `INGEST_BASE_URL`
  - `INGEST_TOKEN`
  - optional `INGEST_STATUS_URL`
- legacy Supabase fallback:
  - `SUPABASE_FUNCTION_URL`
  - `SUPABASE_INGEST_TOKEN`
  - optional `SUPABASE_REST_URL`
  - required for `runMonthBackfill()` on legacy Supabase status checks: `SUPABASE_SERVICE_ROLE_KEY`
- optional debug switch: `VERBOSE_LOGGING=true`

Python environment:

- `SUPABASE_DB_URL`
or
- `SUPABASE_POOLER_URL`
- `SUPABASE_DB_PASSWORD`
- normalizer backend selection:
  - auto-switches to Turso when both `TURSO_DATABASE_URL` and `TURSO_AUTH_TOKEN` are present
  - optional explicit override: `NORMALIZE_DB_BACKEND=turso`

Turso bootstrap environment:

- `TURSO_DATABASE_URL`
- `TURSO_AUTH_TOKEN`
- optional `TURSO_LOCAL_REPLICA_PATH`

Python ingest service environment:

- `INGEST_TOKEN`
- `TURSO_DATABASE_URL`
- `TURSO_AUTH_TOKEN`
- optional `TURSO_LOCAL_REPLICA_PATH`

Google Sheets sync:

- service account JSON file with editor access to the spreadsheet

## Main Entry Points

Apps Script:

- `run()`: daily ingest for a single target date
- `runMonthBackfill()`: backfill from month start to today, skipping dates already present in ingest status
  - if `INGEST_STATUS_URL` or `INGEST_BASE_URL` is configured, it uses `GET /pipeline-runs/{run_date}`
  - otherwise it falls back to legacy Supabase REST checks against `ingest_files`

Python:

- `normalize_supabase.py`: rebuild normalized layer for a specific `run_date`
- the same entrypoint auto-runs on Turso/libSQL when Turso env is present
- explicit override with `NORMALIZE_DB_BACKEND=turso` is still supported
- secondary topic rows are merged into primary topic rows only on exact grain:
  - `report_date`
  - `report_date_from`
  - `report_date_to`
  - `utm_source`
  - `utm_medium`
  - `utm_campaign`
  - `utm_content`
  - `utm_term`
- `sync_goal_mapping_sheet.py`: write goal slot labels to sheet `отчеты`
- `sync_export_rows_wide_sheet.py`: write operator-facing `union`
- `sync_pipeline_status_sheet.py`: write run-level operational status to `pipeline_status`
- `run_pipeline.py`: detect pending raw `run_date`, run normalization, then sync all operator sheets
- if normalized tables are empty, it automatically uses bootstrap fast path before final reconcile
- `public.operator_export_rows`: incremental operator cache table for sheet `union`

## Operator Union Semantics

`union` is intentionally optimized for operators, not for raw audit.

- `utm_term` is always collapsed to `aggregated`
- grouping happens by all exported dimensions except `utm_term`
- `bounce_visits = visits * bounce_rate`
- `pageviews = visits * page_depth`
- `time_on_site_total = visits * avg_time_on_site_seconds`
- `robot_visits = visits * robot_rate`
- `goal_1 ... goal_25` are additive in the export and remain topic-specific
- raw detail remains in the DB; only the sheet layer is collapsed

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

- `key/`, `node_modules/`, generated CSV exports, local ingest logs, and Supabase local temp files are intentionally ignored.
- Historical task/spec material is archived under [docs/archive](./docs/archive).
