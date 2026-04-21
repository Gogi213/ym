# YM System Audit

Дата: `2026-04-22`

## Current Supported Contour

- `Apps Script -> Turso raw ingest`
- `local Python -> parse + compact normalize`
- `Google Sheets -> operator publish`

Поддерживаемый hot path сейчас такой:

- `ingest_files`
- `ingest_file_payloads`
- `pipeline_runs`
- `topic_goal_slots`
- `operator_export_rows`

## What Was Removed From The Supported Path

После аудита из активного operator path больше не считаются supported:

- `ingest_rows` как parse handoff;
- `fact_rows / fact_dimensions / fact_metrics` как основной write path;
- `refresh_current_flags_for_row_keys`;
- `refresh_operator_export_rows_for_run`;
- fake `bootstrap_mode / skip_delete_existing` в `run_pipeline.py`;
- stale references to `ingest_rows` in doctor/reset/docs.

Эти таблицы могут ещё существовать в исторической схеме и старых design docs, но они не являются частью текущего рабочего контура.

## Main Findings

### 1. Главный архитектурный drift был между runtime v2 и repo surface

Реальный pipeline уже давно жил на compact path, но код и дока всё ещё держали в голове старую модель:

- `ingest_rows`
- `fact_*`
- `bootstrap_mode`
- refresh текущих флагов

Это не ломало happy path напрямую, но резко поднимало когнитивную нагрузку и делало ревью/поддержку намного дороже.

### 2. Самые опасные stale places были не в core runtime, а на границах

- operator docs;
- doctor script;
- sync/status summary;
- compatibility facades and tests.

Именно они продолжали врать о том, как проект реально работает.

### 3. У проекта осталась batch-heavy природа, но bottleneck уже другой

После runtime v2 bottleneck сместился:

- не в `fact_*` remote writes;
- а в `fetch_run_files`, `prepare_raw_files`, `build_operator_export_rows`.

Это уже честный read/parse/build cost, а не сломанная granularность durable writes.

## Current Operational Truth

- Apps Script пишет raw file registry в Turso.
- Local Python сам парсит payload локально.
- `skipped` raw автоматически purge-ятся после normalize.
- `pipeline_runs` — day-level operational truth.
- `operator_export_rows` — основной normalized operator layer.
- `union` и `отчеты` — publish layers, не source of truth.

## What Still Matters

- file-centric raw identity;
- content-hash dedupe;
- exact-grain secondary merge;
- stable `goal_1 ... goal_N`;
- validation `raw -> operator_export_rows -> union`.
