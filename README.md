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

1. Apps Script reads topic bindings from spreadsheet `17izchH29LyxuTCNWJ0SThSXmuubMnNFCjtPJiWtcxFA`, sheet `отчеты`:
   - column `A` = primary topic
   - column `B` = optional secondary topic with conversions
2. Apps Script finds matching Gmail messages and uploads `xlsx/csv` attachments to the Supabase Edge Function.
3. Supabase stores raw files and extracted raw rows.
4. Python normalizer builds canonical fact tables and `export_rows_wide`.
   - secondary topics do not become standalone operator topics
   - they are attached to their `primary_topic` only when the exact grain matches
5. Python normalizer also refreshes `public.operator_export_rows` only for dirty `run_date`.
6. Python sync scripts write operator views back to Google Sheets.
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

`run_pipeline.py` now prints phase logs and timings during execution:

- `pipeline_started`
- `normalize_*`
- `sheet_sync_*`
- `pipeline_finished`

If there are no pending `run_date`, it only syncs `pipeline_status` and skips `отчеты` / `union`.

## Performance Notes

- Full month rebuild is intentionally expensive:
  - raw ingest can contain tens of thousands of rows
  - each pending `run_date` is normalized separately
  - after normalization, `отчеты`, `union`, and `pipeline_status` are synced back to Google Sheets
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

## Validation Snapshot

Сквозная сверка `raw -> export_rows_wide -> union` выполнена на `2026-04-13` после свежего month-backfill и полного rebuild normalized-слоя.

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
- `goal_points_total = 1784`
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
