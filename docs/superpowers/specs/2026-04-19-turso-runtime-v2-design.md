# Turso Runtime V2 Design

## Goal

Ускорить локальный post-ingest runtime минимум на порядок, с целевым ориентиром `50x+` против текущего baseline, **не меняя бизнес-задачу продукта**:

- raw file registry остается source of truth;
- dedupe остается по `file_hash`;
- `report_date` продолжает извлекаться из содержимого файла;
- `goal_1 ... goal_N` остаются stable per-topic mapping;
- `union` остается operator-facing publish layer;
- Google Sheets sync остается на выходе пайплайна.

## Root Cause

Текущий Turso runtime медленный не из-за Apps Script и не из-за Gmail.

Главный bottleneck:

1. Python читает raw payload.
2. Python взрывает каждый день в row-level normalized state:
   - `fact_rows`
   - `fact_dimensions`
   - `fact_metrics`
3. Затем из этого слоя пересчитывается `operator_export_rows`.

На одном live дне это даёт сотни тысяч HTTP write операций в Turso.

Текущая модель подходит для Postgres лучше, чем для Turso SQL-over-HTTP. После миграции backend изменился, а expensive intermediate storage model осталась прежней.

## Design Decision

### Что меняется

`fact_rows` / `fact_dimensions` / `fact_metrics` **перестают быть основным durable hot path**.

Новый hot path:

1. Apps Script пишет raw file registry в Turso.
2. Local Python читает raw payload из Turso.
3. Local Python парсит, нормализует и агрегирует данные **локально в памяти**.
4. В Turso записывается **сразу compact publish-ready слой**.
5. Sheets sync читает именно этот compact слой.

### Что остается durable в Turso

- `ingest_files`
- `ingest_file_payloads`
- `pipeline_runs`
- `topic_goal_slots`
- `operator_export_rows`

### Что уходит из основного runtime path

- `fact_rows`
- `fact_dimensions`
- `fact_metrics`

Эти таблицы могут быть оставлены временно для совместимости/миграции, но новый runtime не должен зависеть от них как от основного execution path.

## New Runtime Shape

### Source of Truth

`raw` остается единственным обязательным full-fidelity source of truth:

- file identity: `raw_file_key`
- dedupe key: `file_hash`
- payload: `ingest_file_payloads.file_base64`

Если нужно восстановить день или перестроить publish layer, это делается из raw.

### Compute Model

Python runtime для `run_date` делает только это:

1. выбрать raw files для дня;
2. распарсить payload;
3. применить goal-slot mapping;
4. восстановить business rows в памяти;
5. сделать merge secondary -> primary;
6. сделать current-version resolution;
7. агрегировать прямо в publish grain;
8. записать compact rows в `operator_export_rows`.

### Publish Grain

`operator_export_rows` становится основным durable output runtime.

Grain сохраняет текущую бизнес-семантику `union`:

- `run_date`
- `topic`
- `report_date`
- `report_date_from`
- `report_date_to`
- `utm_source`
- `utm_medium`
- `utm_campaign`
- `utm_content`
- `utm_term`
- base metrics
- `goal_1 ... goal_25`

Именно этот слой нужен operator-facing output и именно его нужно синкать в Google Sheets.

## Why This Preserves Business Semantics

Business logic продукта живет не в `fact_dimensions/fact_metrics` как таковых.

Она живет в:

- raw registry;
- content-based dedupe;
- per-topic goal-slot mapping;
- `report_date` extraction;
- merge and current-version rules;
- final `union` semantics.

Все эти правила могут быть сохранены без хранения row explosion в Turso.

Иными словами:

- продуктовая семантика остается;
- меняется только технический shape durable normalized state.

## Why This Is Faster

Текущий день создает:

- `fact_rows`
- `fact_dimensions`
- `fact_metrics`

То есть один логический ряд превращается в много DB rows.

Runtime V2 пишет:

- один delete/replace publish layer for target `run_date`
- один compact set of `operator_export_rows`
- один status update в `pipeline_runs`

Это сокращает число remote writes на порядок или больше.

Ожидаемый источник ускорения:

1. Нет `fact_dimensions` writes.
2. Нет `fact_metrics` writes.
3. Нет expensive row-level current-flag refresh на large fact tables.
4. Нет rebuild `operator_export_rows` из huge normalized storage.
5. Основной durable layer уже wide and publish-ready.

Именно это дает реалистичный шанс на `50x+` относительно текущего baseline.

## Operational Model

### Pipeline Phases

`run_pipeline.py` остается entrypoint, но фактическая работа меняется:

1. `prepare_raw`
2. `normalize_to_operator_export`
3. `mark_ready`
4. `sync_sheets`

### Status Model

`pipeline_runs` остается day-level control plane:

- `raw_only`
- `pending_normalize`
- `ready`
- `normalize_error`

### Concurrency

Одновременно допускается только один `run_pipeline.py`.

Single-run lock уже обязателен: повторные локальные прогоны не должны пересекаться и портить Turso state.

## Migration Strategy

### Step 1

Добавить новый runtime path, который строит `operator_export_rows` напрямую из raw, не трогая `fact_*`.

### Step 2

Оставить Sheets sync прежним: он уже читает `operator_export_rows`.

### Step 3

Переключить `run_pipeline.py` на новый runtime path.

### Step 4

После валидации live run:

- перестать писать `fact_rows`
- перестать писать `fact_dimensions`
- перестать писать `fact_metrics`

### Step 5

Позже:

- удалить legacy reads/writes;
- решить, оставлять ли `fact_*` таблицы как архивный/compat слой или выпилить их совсем.

## Non-Goals

Этот redesign не меняет:

- Apps Script raw ingest contract;
- file-centric ingest identity;
- `topic_goal_slots` business mapping;
- Google Sheets output semantics.

Этот redesign также не пытается:

- оптимизировать Gmail search;
- переносить runtime на другой язык;
- вводить новый внешний сервис.

## Summary

Runtime V2:

- сохраняет рабочую file-centric бизнес-модель;
- убирает самый дорогой технический дефект миграции в Turso;
- делает `operator_export_rows` основным durable normalized output;
- перестает хранить expensive row explosion как обязательный execution path.
