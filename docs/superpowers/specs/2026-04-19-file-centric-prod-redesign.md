# File-Centric Prod Redesign

## Goal

Вернуть после Turso migration правильную бизнес-модель Supabase-era:

- file-centric raw registry;
- dedupe по содержимому файла;
- `report_date` из содержимого файла;
- canonical sparse normalized layer;
- stable per-topic `goal_1 ... goal_N`;
- `union` как operator-facing publish layer.

Backend меняется, бизнес-семантика не меняется.

## Business Invariants

Продовый контур обязан сохранять:

1. `proxy spreadsheet` остается business-registry тем и goal-дешифровки.
2. файл определяется по содержимому, не по имени вложения.
3. ingest работает по реестру файлов в окне синхронизации, а не по модели `delete day -> reload day`.
4. `message_date` и `report_date` не смешиваются.
5. `fact_rows` / `fact_dimensions` / `fact_metrics` остаются canonical source of truth.
6. `row_hash + is_current` остаются механизмом выбора актуальной версии.
7. `operator_export_rows` / `union` остаются publish-layer, а не source of truth.

## What Went Wrong After Migration

После migration основной контур уехал в неверную operational model:

- `run_date` стал primary identity вместо файла;
- Apps Script начал делать destructive reset дня;
- raw ingest перестал быть idempotent;
- `ingest_files.status` начал врать о parse readiness;
- `ingest_rows` разросся до облачного scratch-space, хотя это не business asset.

Это нужно исправить, не ломая normalized/product semantics.

## Target Architecture

Целевой продовый контур:

- `Gmail`
- `Apps Script`
- `Turso raw registry`
- `local Python normalize`
- `Turso canonical facts`
- `publish layer`
- `Google Sheets`

### Responsibilities

`Apps Script` отвечает только за:

- чтение тем из proxy sheet;
- поиск писем в окне синхронизации;
- выбор релевантных вложений;
- вычисление stable raw-file identity;
- idempotent write raw metadata + payload в Turso.

`local Python` отвечает за:

- чтение dirty raw files;
- parse/normalize локально в памяти;
- запись только durable normalized state;
- refresh `is_current`;
- refresh publish layer;
- optional Sheets sync.

## Execution Unit

Primary execution unit = `raw file`, not `run_date`.

`run_date` остается только:

- summary/status layer;
- operator-facing progress slice;
- удобным batch selector для manual runs и backfill.

## Data Model

### Raw file identity

Каждый raw файл должен иметь стабильную identity:

- `file_hash = sha256(file_bytes)`
- `raw_file_key = run_date + '|' + primary_topic + '|' + file_hash`

То есть business dedupe идет по content hash, а raw registry key привязывает этот payload к конкретному topic/day stream.

Дополнительно хранить:

- `message_id`
- `thread_id`
- `attachment_index`
- `attachment_name`
- `message_subject`
- `message_date`
- `run_date`
- `primary_topic`
- `matched_topic`
- `topic_role`

### Raw state

`ingest_files` должен стать честным raw registry. Минимальный смысл статусов:

- `raw_loaded`
- `parse_ok`
- `parse_skipped`
- `parse_error`

`ingested` как переходное “и да, и нет” состояние убрать из основного контура.

### Day summary state

`pipeline_runs` остается day-level summary.

Recommended statuses:

- `raw_loaded`
- `normalizing`
- `normalized`
- `publish_error`
- `ready`
- `error`

`pipeline_runs` не должен подменять собой file registry.

### Canonical normalized state

Оставить:

- `topic_goal_slots`
- `fact_rows`
- `fact_dimensions`
- `fact_metrics`
- `operator_export_rows`

Убрать из primary cloud path:

- `ingest_rows`

`ingest_rows` может остаться только как temporary local/debug helper во время migration, но не как обязательный durable слой продового контура.

## Ingest Semantics

Apps Script больше не делает destructive reset всего дня перед upload.

Нормальный ingest flow:

1. найти кандидаты в окне синхронизации;
2. для каждого вложения вычислить `raw_file_key` / `file_hash`;
3. upsert raw registry:
   - новый файл вставляется;
   - уже известный файл не дублируется;
   - metadata/timestamps можно обновить без создания дубликата;
4. `pipeline_runs` для затронутого `run_date` переводится в dirty/raw-loaded summary.

Day-reset допустим только как explicit operator repair command, а не как default ingest behavior.

## Normalize Semantics

Python flow:

1. читать dirty raw files;
2. decode payload;
3. parse локально в memory;
4. извлекать `report_date` / `report_date_from` / `report_date_to`;
5. map headers -> canonical dimensions/metrics;
6. upsert goal slots per topic;
7. rebuild facts only for touched files;
8. refresh `is_current` по affected `(topic, row_hash)`;
9. refresh `operator_export_rows`;
10. обновить `pipeline_runs` summary.

Python не должен писать parsed scratch rows обратно в облако как обязательную промежуточную фазу.

## Publish Semantics

Sheets sync должен быть отдельной фазой после successful normalize.

Нормальный порядок:

- raw ingest
- normalize
- publish DB layer
- sync sheets

Если Sheets sync падает, normalized data в Turso не должны считаться потерянными.

## Idempotency Rules

Система должна корректно переживать:

- повторный Apps Script run на том же окне;
- досылку новых файлов за старый день;
- повторную отправку того же файла с тем же содержимым;
- несколько писем по одной теме;
- backfill диапазона.

Главные правила:

- dedupe по content hash;
- rerun не разрушает день целиком;
- normalize пересчитывает только затронутые файлы;
- `is_current` выбирает победителя по `message_date`, затем по load timestamp, затем по deterministic tie-breaker.

## Non-Goals

В этой redesign-фазе не нужно:

- переносить parse в Apps Script;
- добавлять scheduler/daemon;
- менять business contract Google Sheets;
- перепридумывать sparse canonical model.

## Success Criteria

Redesign считается успешным, когда:

- Apps Script перестает делать default day reset;
- raw ingest становится idempotent на уровне файла;
- новые/повторные ingest не дублируют известный payload;
- local Python может нормализовать без обязательного `ingest_rows` cloud scratch layer;
- business semantics Supabase-era полностью сохраняются на Turso backend.
