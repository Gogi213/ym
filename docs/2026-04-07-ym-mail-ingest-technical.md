# YM Mail Ingest Technical Design

## Scope

Текущий production-контур больше не использует Google Drive staging.

Рабочая цепочка сейчас такая:

- `Gmail`
- `Apps Script`
- `Supabase Edge Function`
- `Supabase raw tables`
- `Python normalizer`
- `Supabase export view`
- `Python operator sheet sync`

Что уже реализовано:

- чтение тем из proxy spreadsheet;
- поиск писем в Gmail по неполному вхождению темы;
- отправка `xlsx/csv` вложений в Supabase;
- сохранение raw-слоя файлов и строк;
- нормализация в канонический sparse-слой;
- построение wide-export view в Supabase.

## Runtime Components

### Apps Script

Файл: [Code.js](/C:/visual%20projects/ym/Code.js)

Ответственность:

- читать темы из spreadsheet `17izchH29LyxuTCNWJ0SThSXmuubMnNFCjtPJiWtcxFA`;
- искать письма в ящике `ya-stats@solta.io`;
- матчить письма по теме внутри тела заголовка;
- брать `xlsx/csv` вложения;
- слать вложения и метаданные в Supabase Function;
- не делать бизнес-нормализацию.

Важно:

- текущий код работает с `runDayOffset = -1`, то есть по умолчанию грузит вчерашнюю дату по часовому поясу скрипта;
- dedup и классификация файла происходят уже после Apps Script.

### Supabase Edge Function

Файл: [index.ts](/C:/visual%20projects/ym/supabase/functions/mail-ingest/index.ts)

Ответственность:

- принимать `reset` и `multipart/form-data`;
- классифицировать файл как `ingested`, `skipped`, `error`;
- сохранять файл в `public.ingest_file_payloads`;
- сохранять file-level метаданные в `public.ingest_files`;
- сохранять распарсенные строки в `public.ingest_rows`, если файл распознан как валидная UTM-таблица.

### Python normalizer

Файл: [normalize_supabase.py](/C:/visual%20projects/ym/scripts/normalize_supabase.py)

Ответственность:

- читать raw-слой из Supabase;
- нормализовать заголовки;
- раскладывать строку на dimensions, metrics и `goal_N`;
- стабильно назначать `goal_N` по теме;
- строить normalized слой и wide export-view.

### Python sheet sync

Файлы:

- [sync_goal_mapping_sheet.py](/C:/visual%20projects/ym/scripts/sync_goal_mapping_sheet.py)
- [sync_export_rows_wide_sheet.py](/C:/visual%20projects/ym/scripts/sync_export_rows_wide_sheet.py)

Ответственность:

- писать goal mapping обратно в лист `отчеты`;
- писать operator-facing `union` в отдельный лист;
- не трогать raw или normalized данные в БД.

## Raw Layer

Текущие raw-таблицы:

- `public.ingest_files`
- `public.ingest_rows`
- `public.ingest_file_payloads`

Назначение:

- `ingest_files` хранит метаданные файла, статус ingest и `header_json`;
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

## Operator Union Export

Лист: `union`

Экспорт строится Python-скриптом и отличается от DB-wide слоя:

- `utm_term` всегда схлопывается в `aggregated`;
- grouping идёт по всем остальным экспортируемым dimensions;
- `bounce_rate` превращается в `bounce_visits`;
- `page_depth` превращается в `pageviews`;
- `time_on_site_seconds` превращается в `time_on_site_total`;
- `robot_rate` превращается в `robot_visits`;
- `goal_1 ... goal_25` суммируются как additive metrics;
- даты и числа пишутся в Google Sheets типизированно, а не текстом.

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
2. Python normalizer запускается за ту же `run_date`.
3. `fact_rows_current_flags()` обновляет `is_current`.
4. Spreadsheet export строится уже из `public.export_rows_wide`.

### Goal Mapping Spreadsheet Sync

Goal mapping синхронизируется Python-скриптом, не Apps Script.

Текущие CLI-запуски:

```powershell
python scripts\normalize_supabase.py --run-date 2026-04-06
python scripts\sync_goal_mapping_sheet.py --spreadsheet-id 17izchH29LyxuTCNWJ0SThSXmuubMnNFCjtPJiWtcxFA --service-account-json key\service-account.json
python scripts\sync_export_rows_wide_sheet.py --spreadsheet-id 17izchH29LyxuTCNWJ0SThSXmuubMnNFCjtPJiWtcxFA --service-account-json key\service-account.json
```

## Environment

Python normalizer требует один из вариантов подключения:

- `SUPABASE_DB_URL`
- или `SUPABASE_POOLER_URL` + `SUPABASE_DB_PASSWORD`

Python dependency:

```powershell
python -m pip install -r requirements.txt
```

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

## Git Status

Рабочая папка является git-репозиторием и синхронизируется с:

- `https://github.com/Gogi213/ym`
