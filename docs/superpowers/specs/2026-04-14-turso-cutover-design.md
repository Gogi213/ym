# Turso Cutover Design

**Goal:** Replace the current `Supabase Edge Function + Postgres` backend with a `Python ingest service + Turso/libSQL` backend while keeping `Apps Script -> HTTP ingest -> Python normalize -> Google Sheets` as the external operating model.

## Scope

Полный cutover включает:

- замену Supabase Edge Function на Python HTTP ingest service;
- замену Postgres storage на Turso/libSQL;
- перенос raw-слоя, normalized-слоя, operator cache и pipeline state в Turso;
- сохранение Apps Script как тупого транспорта из Gmail;
- сохранение Google Sheets sync как внешнего operator layer.

Вне scope первого этапа:

- очереди, фоновые воркеры, job orchestration;
- object storage для payload-файлов;
- автоматическая миграция исторических данных из Supabase.

## Target Architecture

Целевой контур:

- `Apps Script`
- `Python ingest service`
- `Turso raw tables`
- `Python normalizer`
- `Turso normalized tables/views`
- `Python Google Sheets sync`

Ключевое решение:

- `Apps Script` не пишет SQL напрямую и не знает про структуру Turso;
- Python ingest service принимает тот же тип запроса, что и текущий Supabase ingest;
- Turso становится единственным backend storage, без гибрида с Supabase.

## Component Boundaries

### Apps Script

Остаётся тупым транспортом:

- читает `A:B` из листа `отчеты`;
- ищет письма;
- собирает `xlsx/csv`;
- шлёт `reset` и `multipart/form-data` в ingest service.

Apps Script не делает:

- SQL;
- parsing таблиц;
- row normalization;
- operator sync.

### Python ingest service

Новый backend ingress boundary.

Ответственность:

- auth по ingest token;
- `reset` для `run_date`;
- multipart ingest для одного файла;
- parsing `csv/xlsx`;
- запись raw-данных в Turso;
- обновление `pipeline_runs`.

Новый сервис должен сохранить текущий внешний контракт максимально близким к Supabase ingest:

- `POST application/json` для reset;
- `POST multipart/form-data` для ingest;
- похожие статусы/JSON ответы;
- те же поля метаданных:
  - `run_date`
  - `primary_topic`
  - `matched_topic`
  - `topic_role`
  - `message_*`
  - `attachment_*`

### Turso/libSQL

Единый backend storage.

Будет хранить:

- raw:
  - `ingest_files`
  - `ingest_rows`
  - `ingest_file_payloads`
- normalized:
  - `topic_goal_slots`
  - `fact_rows`
  - `fact_dimensions`
  - `fact_metrics`
- state/cache:
  - `pipeline_runs`
  - `operator_export_rows`
- views:
  - `export_rows_wide`
  - `goal_mapping_wide`

Адаптация под libSQL/SQLite:

- `uuid` -> `text`;
- `jsonb` -> `text` с JSON serialization;
- `timestamptz` -> `text` ISO timestamps;
- без Postgres functions/extensions;
- без `COPY`, `RETURNING`, temp table contracts в их текущем виде.

### Python normalizer

Остаётся отдельным слоем.

Новый responsibility split не меняется концептуально:

- `fields.py`
- `transform.py`
- `db_*`
- `pipeline.py`

Но storage backend меняется:

- с `psycopg/Postgres` на `libsql/Turso`;
- запись raw/normalized/state должна стать libSQL-compatible;
- логика secondary merge сохраняется.

### Google Sheets sync

Остаётся Python-side.

Изменения:

- источник данных переключается на Turso вместо Supabase/Postgres;
- semantics листов не меняются:
  - `отчеты`
  - `union`
  - `pipeline_status`

## Data Model Strategy

Turso получает те же logical layers, но не тот же literal SQL dialect.

### Raw layer

Нужно сохранить:

- file-level audit trail;
- row-level raw JSON;
- raw payload file body;
- `primary_topic/topic_role`.

### Normalized layer

Нужно сохранить:

- current-row identity через `row_hash`;
- sparse dimensions;
- sparse metrics;
- `goal_1 ... goal_25`;
- exact-grain secondary merge.

### Operator cache

Нужно сохранить:

- `operator_export_rows` как pre-aggregated cache;
- `union` semantics:
  - `utm_content = aggregated`
  - `utm_term = aggregated`
  - additive metrics already aggregation-ready.

## Migration Strategy

Переезд делать поэтапно, но без долгого hybrid runtime.

### Phase 1: Turso bootstrap

Сделать:

- рабочую Turso DB;
- schema bootstrap;
- Python connectivity;
- smoke CRUD validation.

### Phase 2: Python-side storage abstraction

Сделать:

- общий connection/config слой для Turso;
- минимальный DB access layer для ingest/normalize/sync;
- не держать новый storage код вперемешку с Supabase-specific кодом.

### Phase 3: Python ingest service

Сделать:

- `FastAPI`/ASGI service;
- reset handler;
- multipart ingest handler;
- raw writes + pipeline state writes.

### Phase 4: Normalizer/storage cutover

Сделать:

- перевод normalizer на Turso;
- перевод operator cache refresh на Turso;
- перевод orchestrator на Turso.

### Phase 5: Sheets cutover

Сделать:

- sync scripts читают из Turso;
- сквозная валидация:
  - raw
  - wide
  - union

### Phase 6: Apps Script cutover

Сделать:

- заменить `SUPABASE_FUNCTION_URL` на новый ingest endpoint;
- сохранить ingest token model;
- прогнать fresh ingest на новый backend.

## Risks

### 1. SQL dialect mismatch

Это главный технический риск.

Причина:

- текущий проект глубоко использует Postgres-specific SQL.

Ответ:

- не пытаться тащить Postgres DDL буквально;
- адаптировать storage/model слой под libSQL explicitly.

### 2. Payload size / ingress behavior

Нужно проверить:

- как новый Python ingest service держит multipart payload объёма реальных `xlsx`.

Ответ:

- отдельные smoke tests на реальные payload size bounds;
- при необходимости ограничение и явные ошибки на oversized uploads.

### 3. Current-row logic

`is_current` нельзя сломать при переносе SQL.

Ответ:

- сохранить exact current-row validation;
- повторить сквозную сверку `raw -> wide -> union`.

### 4. Cold rebuild performance

Turso даст другой профиль производительности, чем Postgres.

Ответ:

- сначала correctness;
- потом performance tuning уже на libSQL path.

## Success Criteria

Cutover считается успешным, если:

- новый ingest service принимает те же Apps Script payload;
- raw слой пишется в Turso без потерь;
- normalizer на Turso даёт те же суммы `visits` и `goal_N`, что raw;
- `union` и `отчеты` совпадают по семантике с текущим production behavior;
- Supabase больше не нужен как runtime dependency.
