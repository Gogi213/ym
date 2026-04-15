# YM Mail Ingest Technical Design

## Scope

Текущий production-контур больше не использует Google Drive staging.

Идёт полный переезд storage/runtime слоя с Supabase/Postgres на Turso/libSQL.
На текущий момент Turso bootstrap, raw ingest, normalizer backend и operator read-path уже проверены живым smoke.
Apps Script transport уже умеет работать не только с Supabase Function, но и с новым Python ingest service.

Рабочая цепочка сейчас такая:

- `Gmail`
- `Apps Script`
- `Supabase Edge Function`
- `Supabase raw tables`
- `Python normalizer`
- `Supabase export view`
- `Supabase operator export cache`
- `Python operator sheet sync`

Миграционный контур сейчас такой:

- `Turso bootstrap schema`
- `Python libsql runtime`
- `Python HTTP ingest service`
- `Turso raw + normalized + operator cache`
- backend-switchable `Python normalizer`
- backend-aware operator sheet sync read-path

Что уже реализовано:

- чтение связок `primary -> secondary` из proxy spreadsheet;
- поиск писем в Gmail по неполному вхождению темы;
- отправка `xlsx/csv` вложений в Supabase;
- сохранение raw-слоя файлов и строк;
- нормализация в канонический sparse-слой;
- построение wide-export view в Supabase.
- raw ingest в Turso/libSQL;
- normalize/write/operator refresh в Turso/libSQL;
- чтение `goal_mapping_wide`, `operator_export_rows`, `pipeline_runs` из Turso/libSQL.

## Runtime Components

### Apps Script

Файлы:

- [appsscript-src](/C:/visual%20projects/ym/appsscript-src)
- [Code.js](/C:/visual%20projects/ym/Code.js)

Ответственность:

- читать связки тем из spreadsheet `17izchH29LyxuTCNWJ0SThSXmuubMnNFCjtPJiWtcxFA`:
  - `A` = primary topic
  - `B` = optional secondary topic с конверсиями;
- искать письма в ящике `ya-stats@solta.io`;
- матчить письма по теме внутри тела заголовка;
- брать `xlsx/csv` вложения;
- слать вложения и метаданные в configured ingest endpoint;
- не делать бизнес-нормализацию.

Важно:

- текущий код работает с `runDayOffset = -1`, то есть по умолчанию грузит вчерашнюю дату по часовому поясу скрипта;
- dedup и классификация файла происходят уже после Apps Script;
- Apps Script отправляет в ingest метаданные:
  - `matched_topic`
  - `primary_topic`
  - `topic_role = primary | secondary`
- transport settings теперь двухрежимные:
  - preferred:
    - `INGEST_BASE_URL`
    - `INGEST_TOKEN`
    - optional `INGEST_STATUS_URL`
  - legacy fallback:
    - `SUPABASE_FUNCTION_URL`
    - `SUPABASE_INGEST_TOKEN`
    - optional `SUPABASE_REST_URL`
    - optional `SUPABASE_SERVICE_ROLE_KEY`
- для `runMonthBackfill()` при новом transport Apps Script проверяет уже не Supabase REST, а:
  - `GET /pipeline-runs/{run_date}`

Структура исходников:

- `appsscript-src/00_config_and_topics.js`
  - config;
  - topic loading;
  - topic matching;
- `appsscript-src/10_dates_and_matching.js`
  - date helpers;
  - Gmail search;
  - candidate collection;
- `appsscript-src/20_transport_and_runtime.js`
  - HTTP request shaping;
  - runtime/settings;
  - response/assert helpers;
- `appsscript-src/30_entrypoints.js`
  - `runForDate_`;
  - `run()`;
  - `runMonthBackfill()`.

Важно:

- `Code.js` больше не является source of truth;
- deployable single-file artifact собирается из `appsscript-src/` через:
  - [build_appsscript_bundle.py](/C:/visual%20projects/ym/scripts/build_appsscript_bundle.py)

### Supabase Edge Function

Файлы:

- [index.ts](/C:/visual%20projects/ym/supabase/functions/mail-ingest/index.ts)
- [auth.ts](/C:/visual%20projects/ym/supabase/functions/mail-ingest/auth.ts)
- [handlers.ts](/C:/visual%20projects/ym/supabase/functions/mail-ingest/handlers.ts)
- [parse.ts](/C:/visual%20projects/ym/supabase/functions/mail-ingest/parse.ts)
- [shared.ts](/C:/visual%20projects/ym/supabase/functions/mail-ingest/shared.ts)
- [supabase.ts](/C:/visual%20projects/ym/supabase/functions/mail-ingest/supabase.ts)

Ответственность:

- `index.ts`:
  - thin entrypoint;
  - routing `reset` vs `multipart/form-data`;
- `auth.ts`:
  - ingest token auth;
- `handlers.ts`:
  - request validation;
  - ingest/reset orchestration;
  - error handling;
- `parse.ts`:
  - `csv/xlsx` table detection;
  - parsing debug payload;
- `shared.ts`:
  - shared types;
  - header helpers;
  - common HTTP helpers;
- `supabase.ts`:
  - admin client bootstrap;
  - raw writes;
  - `pipeline_runs` state updates.

### Python normalizer

Файлы:

- [normalize_supabase.py](/C:/visual%20projects/ym/scripts/normalize_supabase.py)
- [common.py](/C:/visual%20projects/ym/scripts/normalize/common.py)
- [fields.py](/C:/visual%20projects/ym/scripts/normalize/fields.py)
- [transform.py](/C:/visual%20projects/ym/scripts/normalize/transform.py)
- [db_connection.py](/C:/visual%20projects/ym/scripts/normalize/db_connection.py)
- [db_reads.py](/C:/visual%20projects/ym/scripts/normalize/db_reads.py)
- [db_writes.py](/C:/visual%20projects/ym/scripts/normalize/db_writes.py)
- [db_operator_flags.py](/C:/visual%20projects/ym/scripts/normalize/db_operator_flags.py)
- [db_operator_export.py](/C:/visual%20projects/ym/scripts/normalize/db_operator_export.py)
- [db_operator.py](/C:/visual%20projects/ym/scripts/normalize/db_operator.py)
- [db.py](/C:/visual%20projects/ym/scripts/normalize/db.py)
- [pipeline.py](/C:/visual%20projects/ym/scripts/normalize/pipeline.py)
- [turso_connection.py](/C:/visual%20projects/ym/scripts/normalize/turso_connection.py)
- [turso_reads.py](/C:/visual%20projects/ym/scripts/normalize/turso_reads.py)
- [turso_writes.py](/C:/visual%20projects/ym/scripts/normalize/turso_writes.py)
- [turso_operator_flags.py](/C:/visual%20projects/ym/scripts/normalize/turso_operator_flags.py)
- [turso_operator_export.py](/C:/visual%20projects/ym/scripts/normalize/turso_operator_export.py)
- [query_utils.py](/C:/visual%20projects/ym/scripts/normalize/query_utils.py)

Ответственность:

- `normalize_supabase.py`:
  - thin CLI/public facade для тестов и внешних скриптов;
- `fields.py`:
  - нормализация заголовков;
  - parsing metrics/durations;
  - row identity;
  - сборка row-level payload;
- `transform.py`:
  - стабильное назначение `goal_N`;
  - secondary merge;
  - сборка `fact_rows / fact_dimensions / fact_metrics`;
- `db_connection.py`:
  - DB connection/bootstrap;
- `db_reads.py`:
  - raw/state reads;
- `db_writes.py`:
  - fact/state writes;
- `db_operator_flags.py`:
  - `is_current` refresh;
- `db_operator_export.py`:
  - `operator_export_rows` refresh;
- `db_operator.py`:
  - compatibility facade over operator DB submodules;
- `db.py`:
  - backend selector facade;
  - автоматически выбирает Turso, если заданы `TURSO_DATABASE_URL` и `TURSO_AUTH_TOKEN`;
  - explicit override через `NORMALIZE_DB_BACKEND` остаётся доступным;
- `pipeline.py`:
  - `normalize_run`;
  - `finalize_normalized_runs`.

Turso-специфичные модули:

- `turso_connection.py`:
  - подключение к libSQL/Turso через shared runtime;
- `turso_reads.py`:
  - raw/state reads;
  - декодирование `header_json` и `row_json` из text storage обратно в Python structures;
- `turso_writes.py`:
  - fact/state writes без Postgres-only SQL;
- `turso_operator_flags.py`:
  - `is_current` refresh без temp table dependency;
- `turso_operator_export.py`:
  - operator export refresh на SQLite/libSQL-compatible SQL;
- `query_utils.py`:
  - общая row-normalization для Postgres cursor rows и libSQL tuple rows.

Правило merge secondary в primary:

- `secondary` topic не уходит отдельной операторской темой;
- бизнес-topic для него всегда `primary_topic`;
- merge допускается только при полном совпадении:
  - `report_date`
  - `report_date_from`
  - `report_date_to`
  - `utm_source`
  - `utm_medium`
  - `utm_campaign`
  - `utm_content`
  - `utm_term`
- если exact grain не совпал, строка secondary не приклеивается автоматически.

### Python sheet sync

Файлы:

- [sync_goal_mapping_sheet.py](/C:/visual%20projects/ym/scripts/sync_goal_mapping_sheet.py)
- [sync_export_rows_wide_sheet.py](/C:/visual%20projects/ym/scripts/sync_export_rows_wide_sheet.py)
- [sync_pipeline_status_sheet.py](/C:/visual%20projects/ym/scripts/sync_pipeline_status_sheet.py)
- [run_pipeline.py](/C:/visual%20projects/ym/scripts/run_pipeline.py)

Ответственность:

- писать goal mapping обратно в лист `отчеты`;
- писать operator-facing `union` в отдельный лист;
- писать operational status в лист `pipeline_status`;
- оркестрировать `normalize + sheet sync` одной командой;
- не трогать raw или normalized данные в БД.

Важно:

- текущий production entrypoint для post-processing — `run_pipeline.py`;
- если normalized-слой пустой, orchestrator автоматически включает bootstrap fast path;
- bootstrap fast path не меняет бизнес-логику, но убирает самые дорогие per-day finalize шаги:
  - без per-day delete existing normalized rows;
  - без per-day `is_current` refresh;
  - без per-day `operator_export_rows` refresh;
- после загрузки всех dirty дней делается один общий finalize-pass.

## Raw Layer

Текущие raw-таблицы:

- `public.ingest_files`
- `public.ingest_rows`
- `public.ingest_file_payloads`

Назначение:

- `ingest_files` хранит метаданные файла, статус ingest и `header_json`;
- `ingest_files.primary_topic` хранит основной бизнес-topic, к которому относится secondary-файл;
- `ingest_files.topic_role` различает primary и secondary ingest;
- `ingest_rows` хранит распарсенные строки только для валидных таблиц;
- `ingest_file_payloads` хранит сырой файл в base64, чтобы файл не терялся даже если parsing failed.

## Normalized Layer

Новая схема:

- `public.topic_goal_slots`
- `public.goal_mapping_wide`
- `public.fact_rows`
- `public.fact_dimensions`
- `public.fact_metrics`
- `public.export_rows_wide`
- `public.operator_export_rows`

### topic_goal_slots

Хранит стабильное соответствие:

- `topic`
- `goal_slot`
- `source_header`
- `goal_label`

Правило:

- внутри одной темы goal-слоты назначаются слева направо по исходным goal-колонкам;
- новые goal-колонки получают следующий свободный слот;
- уже назначенные goal-слоты не перенумеровываются задним числом.

### goal_mapping_wide

Wide-view для операционного листа с расшифровкой goal-слотов:

- `topic`
- `goal_1 ... goal_25`

Источник данных:

- все темы, которые реально дошли до `public.ingest_files` со статусом `ingested`;
- значения goal-слотов из `public.topic_goal_slots`.

Назначение:

- это отдельный менеджерский слой;
- wide-union по строкам отчётов не меняет;
- нужен, чтобы оператор видел, что означает `goal_N` внутри конкретной темы.

### fact_rows

Одна запись = одна нормализованная строка источника.

Основные поля:

- `fact_row_id`
- `topic`
- `source_file_id`
- `source_row_index`
- `report_date`
- `report_date_from`
- `report_date_to`
- `message_date`
- `layout_signature`
- `row_hash`
- `is_current`
- `source_row_json`

Важно:

- `row_hash` больше не строится только по каноническим UTM-dimensions;
- в identity строки также попадают unmapped text dimensions, если они различают реальные строки отчёта;
- metric-like, duration-like, date-like и goal-like поля в identity не участвуют;
- это нужно, чтобы `is_current` не схлопывал разные строки в одну только потому, что различие сидит в неканоническом текстовом поле.

### fact_dimensions

Sparse-слой измерений:

- `fact_row_id`
- `dimension_key`
- `dimension_value`

Сейчас покрываются:

- `utm_source`
- `utm_medium`
- `utm_campaign`
- `utm_content`
- `utm_term`

### fact_metrics

Sparse-слой метрик:

- `fact_row_id`
- `metric_key`
- `metric_value`

Сейчас покрываются:

- `visits`
- `users`
- `bounce_rate`
- `page_depth`
- `time_on_site_seconds`
- `robot_rate`
- `goal_1 ... goal_25`

## Wide Export View

Витрина: `public.export_rows_wide`

Она:

- берёт только `is_current = true`;
- пивотит dimensions в фиксированные колонки;
- пивотит метрики в фиксированные колонки;
- выводит `goal_1 ... goal_25`;
- сохраняет `source_row_json` для кросс-проверки назад до исходной строки.

Важно:

- `public.export_rows_wide` остаётся полным wide-слоем из БД;
- операторский лист `union` строится поверх него отдельным Python-экспортом;
- `union` не является 1:1 копией `export_rows_wide`.

## Operator Export Cache

Таблица: `public.operator_export_rows`

Назначение:

- хранить уже агрегированный операторский слой для `union`;
- не пересчитывать operator aggregation на каждый sheet sync с нуля;
- обновляться только для dirty `run_date` внутри normalizer.

Это отдельный cache-layer:

- source of truth остаётся в `fact_*` и `export_rows_wide`;
- `operator_export_rows` нужен только для быстрого operator sync;
- Python больше не тянет десятки тысяч wide-строк в память ради одного листа.

## Current-State Refresh Strategy

После deep review на `2026-04-13` current-state refresh больше не работает на всём historical topic scope.

Теперь normalizer:

- собирает affected key set только для затронутого `run_date`;
- включает туда:
  - старые `(topic, row_hash)`, которые удаляются при re-normalize этого дня;
  - новые `(topic, row_hash)`, которые вставляются этим же прогоном;
- пересчитывает `is_current` только для этого набора.

Это важно, потому что именно старый refresh-by-topic был главным performance bottleneck incremental pipeline.

## Validation Status

После фикса `row_hash` / `is_current` и полного rebuild на `2026-04-13` выполнена полная валидация:

- `raw ingest_rows` против `public.export_rows_wide`
- `public.export_rows_wide` против листа `union`
- по всем темам
- по `visits`
- по всем существующим `goal_N`

Результат:

- `visit_mismatches = 0`
- `goal_mismatches = 0`

Важно по семантике:

- `visits` в export не суммируют secondary-строки поверх primary;
- `goal_*` в export могут приходить из `secondary`, если exact-grain merge совпал;
- поэтому корректная validation model такая:
  - `visits`: raw `primary` -> `export_rows_wide` -> `union`
  - `goal_*`: raw effective topic (`primary + attached secondary`) -> `export_rows_wide` -> `union`

## Operator Union Export

Лист: `union`

Экспорт строится Python-скриптом и отличается от DB-wide слоя:

- `utm_term` всегда схлопывается в `aggregated`;
- `utm_content` всегда схлопывается в `aggregated`;
- grouping идёт по всем остальным экспортируемым dimensions;
- `bounce_rate` превращается в `bounce_visits`;
- `page_depth` превращается в `pageviews`;
- `time_on_site_seconds` превращается в `time_on_site_total`;
- `robot_rate` превращается в `robot_visits`;
- `goal_1 ... goal_25` суммируются как additive metrics;
- даты и числа пишутся в Google Sheets типизированно, а не текстом.

## Pipeline Status Export

Лист: `pipeline_status`

Назначение:

- показывать, какие `run_date` уже дошли до raw ingest;
- показывать, какие `run_date` уже нормализованы;
- давать оператору короткий статус без чтения SQL.

Ключевые поля:

- `run_date`
- `pipeline_status`
- `total_files`
- `ingested_files`
- `skipped_files`
- `error_files`
- `raw_rows`
- `normalized_files`
- `normalized_rows`
- `first_message_at`
- `last_message_at`
- `normalized_at`

## Current Canonical Header Mapping

Нормализатор уже маппит:

- `UTM Source` -> `utm_source`
- `UTM Medium` -> `utm_medium`
- `UTM Campaign` -> `utm_campaign`
- `UTM Content` -> `utm_content`
- `UTM Term` -> `utm_term`
- `Визиты` -> `visits`
- `Посетители` -> `users`
- `Отказы` -> `bounce_rate`
- `Глубина просмотра` -> `page_depth`
- `Время на сайте` -> `time_on_site_seconds`
- `Роботность` -> `robot_rate`
- любой заголовок с вхождением `Роботность` -> `robot_rate`
- любой заголовок с вхождением `Конверсия` -> игнорируется
- любой заголовок с вхождением `Доход` -> `goal_N` по topic-slot mapping

`Дата визита` больше не хранится отдельной dimension-колонкой. Если она есть в исходной строке, нормализатор использует её только для расчёта `report_date`.
- `Товаров куплено (...)` -> `goal_N` по topic-slot mapping
- `Посетители, купившие товар (...)` -> `goal_N` по topic-slot mapping
- `Достижения избранных целей` -> `goal_N` по topic-slot mapping
- `Достижения цели (...)` -> `goal_N` по topic-slot mapping

## Run Model

Рекомендуемый порядок прогона:

1. Apps Script грузит raw-слой за целевую дату.
2. `python scripts\run_pipeline.py --service-account-json ...` находит pending `run_date`.
3. Если normalized-слой пустой, orchestrator включает bootstrap fast path.
4. Orchestrator прогоняет `normalize_supabase.py` по нужным датам.
5. Orchestrator синкает:
   - `отчеты`
   - `union`
   - `pipeline_status`

`run_pipeline.py` и `normalize_supabase.py` теперь печатают фазовые JSON-логи с `elapsed_ms`.

Если pending `run_date` нет, orchestrator не гоняет `отчеты` и `union`, а синкает только `pipeline_status`.

### Performance Boundary

Полный rebuild месяца не является дешёвой операцией.

Причины:

- raw ingest уже сейчас может давать десятки тысяч строк;
- normalizer пересобирает каждый pending `run_date` отдельно;
- `is_current` теперь пересчитывается только по темам dirty-дня, а не по всей таблице;
- operator cache пересчитывается только по dirty `run_date`;
- после нормализации Python ещё синкает три листа:
  - `отчеты`
  - `union`
  - `pipeline_status`

Практический вывод:

- для операционного прогона это нормальный путь;
- для точечной валидации конкретной темы или дня не надо гонять весь `run_pipeline.py`;
- правильнее запускать точечно `normalize_supabase.py --run-date ...` и проверять нужный срез отдельно.

Что уже проверено на живой базе:

- двухворкерный parallel-by-day rebuild был измерен и отвергнут;
- на реальных данных он оказался медленнее последовательного режима из-за DB contention на `fact_*`;
- текущий shipped path для empty-state rebuild — bootstrap fast path, а не two-worker execution.

Это не считается багом пайплайна само по себе.
Это текущая стоимость инкрементального rebuild без полного пересчёта уже готовых дней.

### Goal Mapping Spreadsheet Sync

Goal mapping синхронизируется Python-скриптом, не Apps Script.

Текущие CLI-запуски:

```powershell
python scripts\normalize_supabase.py --run-date 2026-04-06
python scripts\sync_goal_mapping_sheet.py --spreadsheet-id 17izchH29LyxuTCNWJ0SThSXmuubMnNFCjtPJiWtcxFA --service-account-json key\service-account.json
python scripts\sync_export_rows_wide_sheet.py --spreadsheet-id 17izchH29LyxuTCNWJ0SThSXmuubMnNFCjtPJiWtcxFA --service-account-json key\service-account.json
python scripts\sync_pipeline_status_sheet.py --spreadsheet-id 17izchH29LyxuTCNWJ0SThSXmuubMnNFCjtPJiWtcxFA --service-account-json key\service-account.json
python scripts\run_pipeline.py --service-account-json key\service-account.json
```

## Environment

Python normalizer требует один из вариантов подключения:

- `SUPABASE_DB_URL`
- или `SUPABASE_POOLER_URL` + `SUPABASE_DB_PASSWORD`

Либо Turso env:

- `TURSO_DATABASE_URL`
- `TURSO_AUTH_TOKEN`

При наличии Turso env normalizer и sync read-path автоматически переключаются на libSQL backend.

Python dependency:

```powershell
python -m pip install -r requirements.txt
```

### Turso bootstrap

Уже реализовано:

- [turso/bootstrap_schema.sql](/C:/visual%20projects/ym/turso/bootstrap_schema.sql)
- [turso_runtime.py](/C:/visual%20projects/ym/scripts/turso_runtime.py)
- [bootstrap_turso.py](/C:/visual%20projects/ym/scripts/bootstrap_turso.py)

Назначение:

- `bootstrap_schema.sql`:
  - Turso/libSQL-compatible DDL bootstrap;
- `turso_runtime.py`:
  - читает `TURSO_DATABASE_URL` и `TURSO_AUTH_TOKEN`;
  - открывает local replica;
  - сразу делает `sync()`, чтобы Python работал не против пустого локального snapshot, а против актуального remote state;
- `bootstrap_turso.py`:
  - применяет bootstrap schema;
  - делает `commit + sync` после DDL.

Нужные env:

- `TURSO_DATABASE_URL`
- `TURSO_AUTH_TOKEN`
- optional `TURSO_LOCAL_REPLICA_PATH`

Пример:

```powershell
$env:TURSO_DATABASE_URL='libsql://<db-name>-<org>.turso.io'
$env:TURSO_AUTH_TOKEN='<db-token>'
python scripts\bootstrap_turso.py
```

### Python ingest service scaffold

Уже реализовано:

- [ingest_service/app.py](/C:/visual%20projects/ym/ingest_service/app.py)
- [ingest_service/auth.py](/C:/visual%20projects/ym/ingest_service/auth.py)
- [ingest_service/models.py](/C:/visual%20projects/ym/ingest_service/models.py)

Что уже есть:

- `GET /health`
- `POST /reset`
- `POST /ingest`
- `GET /pipeline-runs/{run_date}`
- auth по `x-ingest-token`
- route-level contract, совместимый по форме с текущим Apps Script transport
- `csv/xlsx` parsing port в Python
- route handlers, которые уже умеют писать raw/state в SQLite-compatible bootstrap schema

Что ещё не перенесено в этот слой:

- production startup/deploy wiring на Turso env
- production startup/deploy

### Turso raw storage adapter

Уже реализовано:

- [ingest_service/storage.py](/C:/visual%20projects/ym/ingest_service/storage.py)

Что покрывает:

- `mark_pipeline_run_after_reset`
- `insert_file_record`
- `insert_row_records`
- `insert_file_payload_record`
- `refresh_pipeline_run_after_ingest`

Текущее назначение:

- это Turso/libSQL-compatible write-path для raw ingest;
- тестируется на SQLite-compatible bootstrap schema;
- уже используется runtime handlers внутри нового Python ingest service в integration tests.

### Python ingest parsing and handlers

Уже реализовано:

- [ingest_service/parse.py](/C:/visual%20projects/ym/ingest_service/parse.py)
- [ingest_service/types.py](/C:/visual%20projects/ym/ingest_service/types.py)
- [ingest_service/handlers.py](/C:/visual%20projects/ym/ingest_service/handlers.py)

Что покрыто:

- CSV delimiter detection;
- XLSX table detection через zip/xml parsing;
- поиск table block по UTM header;
- skip summary rows `Итого/Total`;
- reset handler;
- ingest handler;
- запись raw rows, payload и `pipeline_runs` в bootstrap schema.

Что пока ещё не доделано:

- cutover Apps Script на новый endpoint;
- переключение production env на Turso по умолчанию;
- full operational cutover так, чтобы `run_pipeline.py` и sheet sync всегда работали на Turso env по умолчанию.

### Python ingest runtime wiring

Уже реализовано:

- [ingest_service/runtime.py](/C:/visual%20projects/ym/ingest_service/runtime.py)
- [ingest_service/main.py](/C:/visual%20projects/ym/ingest_service/main.py)

Что покрыто:

- env-driven `INGEST_TOKEN` loading;
- создание app из реального Turso connection;
- lifecycle shutdown с `connection.close()`;
- единая ASGI entrypoint:
  - `uvicorn ingest_service.main:app --host 0.0.0.0 --port 8000`
- status endpoint для Apps Script backfill:
  - `GET /pipeline-runs/{run_date}`
  - ответ:
    - `run_date`
    - `exists`
    - `raw_files`
    - `raw_rows`
    - `normalize_status`

Что ещё не закрыто:

- production deployment target;
- Apps Script production cutover как operational default;
- живой ingest smoke уже не на in-memory bootstrap, а на реальную Turso migration DB.

### Live Turso smoke status

На `2026-04-14` новый Python ingest service уже прогнан против реальной migration DB:

- DB: `ym-migration-20260414`
- transport: `uvicorn ingest_service.main:app`
- requests:
  - `POST /reset`
  - `POST /ingest`

Подтверждённый результат после свежего `reset + ingest`:

- `ingest_files where run_date = '2026-04-14'` -> `1`
- `ingest_rows where run_date = '2026-04-14'` -> `2`
- `pipeline_runs` -> `raw_files=1`, `raw_rows=2`, `normalize_status='pending_normalize'`

`raw_revision` при этом должен увеличиваться на каждый `reset`, поэтому его абсолютное значение не считается фиксированным smoke-contract.

Это важно:

- новый runtime уже не только тестовый;
- raw write-path в реальный Turso/libSQL подтверждён;
- следующий cutover риск сидит уже не в ingest transport, а в полном переключении runtime endpoint и orchestrator env.

Дополнительно на той же migration DB уже подтверждено:

- `python -m scripts.normalize_supabase --run-date 2026-04-14`
- чтение:
  - `pipeline_status`
  - `operator_export_rows`
  - `goal_mapping_wide`

Подтверждённый результат:

- `normalize_finished` на Turso/libSQL без Postgres backend;
- `pipeline_runs.normalize_status = 'ready'`;
- `operator_export_rows` содержит агрегированные строки;
- `goal_mapping_wide` читается через Python sync read-path без `public.*` и Postgres cursor assumptions.
- при наличии Turso env normalizer сам уходит в libSQL runtime без обязательного `NORMALIZE_DB_BACKEND=turso`.

## Verification

Минимальные проверки:

```sql
select count(*) from public.fact_rows;
select count(*) from public.fact_dimensions;
select count(*) from public.fact_metrics;
select * from public.topic_goal_slots order by topic, goal_slot;
select * from public.goal_mapping_wide order by topic;
select * from public.export_rows_wide limit 20;
```

### Сквозная сверка raw -> wide -> union

На `2026-04-13` после свежего cold rebuild выполнена полная сквозная валидация:

- всех `ingested` тем;
- всех дней `2026-04-01 .. 2026-04-12`;
- сумм `visits` по дням;
- сумм `goal_*` по дням.

Сверялись три слоя:

- raw строки из `public.ingest_rows`, извлечённые из исходных файлов;
- агрегаты по `public.export_rows_wide`;
- агрегаты по листу `union` в Google Sheets.

Результат:

- `raw_visit_keys = 223`
- `wide_visit_keys = 223`
- `sheet_visit_keys = 223`
- `raw_goal_keys = 403`
- `wide_goal_keys = 403`
- `sheet_goal_keys = 403`
- `visit_mismatches = 0`
- `goal_mismatches = 0`
- текущий переход `raw -> wide -> union` не теряет visits и не искажает goal-метрики при операторской агрегации.

## Git Status

Рабочая папка является git-репозиторием и синхронизируется с:

- `https://github.com/Gogi213/ym`
