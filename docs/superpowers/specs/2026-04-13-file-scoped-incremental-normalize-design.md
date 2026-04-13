# File-Scoped Incremental Normalize Design

## Goal

Убрать `run_date`-scoped full rewrite как основную единицу пересчета и заменить его на `file`-scoped incremental pipeline, где Python пересчитывает только затронутые raw-файлы и только затронутые operator aggregate keys.

## Problem

Текущая архитектура уже ушла от полного monthly rebuild, но все еще остается слишком широкой по единице работы:

- dirty-marking происходит по `run_date`
- normalizer пересобирает весь день, даже если реально изменился только один файл
- operator cache пересчитывается на уровне всего дня

Это дает приемлемое поведение для маленьких прогонов, но плохо скейлится при:

- большом числе файлов в одном дне
- повторном ingest одного и того же дня
- backfill по длинному диапазону

Главная проблема не в Gmail и не в Google Sheets. Главная проблема в том, что единица пересчета слишком крупная.

## Recommended Approach

Сделать `file` основной единицей normalize-state.

### Core Rules

- новый или повторно ingested файл становится `dirty`
- normalizer пересчитывает только dirty files
- `fact_*` пересобираются только для конкретного `source_file_id`
- operator cache обновляется только для затронутых aggregate keys
- `pipeline_runs` сохраняется как summary-layer по `run_date`, а не как основной execution unit

## Data Model

Добавить новый state-layer: `public.normalized_files`

Suggested fields:

- `file_id bigint primary key references public.ingest_files(id) on delete cascade`
- `run_date date not null`
- `primary_topic text not null`
- `matched_topic text not null`
- `topic_role text not null`
- `raw_revision bigint not null default 1`
- `normalize_status text not null`
- `normalized_rows bigint not null default 0`
- `normalized_at timestamptz`
- `last_error text`
- `created_at timestamptz not null default now()`
- `updated_at timestamptz not null default now()`

Recommended statuses:

- `pending_normalize`
- `ready`
- `normalize_error`

### Why `pipeline_runs` stays

`pipeline_runs` остается нужен как operator/ops summary:

- что было ingested за день
- готов ли день целиком
- есть ли ошибки на уровне дня

Но execution orchestration перестает считать `run_date` первичной единицей работы.

## Runtime Shape

### Ingest

Edge function после успешного ingest:

- upsert в `normalized_files` для каждого `ingest_files.id`
- ставит `normalize_status = 'pending_normalize'`
- очищает старый normalized-state этого `file_id` только при повторном ingest того же файла
- обновляет `pipeline_runs` как day-level summary

### Normalizer

Python normalizer:

- читает dirty files из `normalized_files`
- по каждому dirty `file_id`:
  - забирает raw rows только этого файла
  - удаляет старые `fact_rows/fact_dimensions/fact_metrics` только для этого `source_file_id`
  - строит новые facts только для этого файла
  - определяет affected `(topic, row_hash)` только из нового/удаленного набора
  - refresh’ит `is_current` только для этих affected keys
  - отмечает `normalized_files.normalize_status = 'ready'`

### Operator Cache

После нормализации файла:

- определяется набор affected operator keys:
  - `topic`
  - `report_date`
  - `utm_source`
  - `utm_medium`
  - `utm_campaign`
- `public.operator_export_rows` пересчитывается не по `run_date`, а только по этим keys

### Sheets

Google Sheets по-прежнему можно переписывать целиком:

- `отчеты`
- `union`
- `pipeline_status`

Это остается допустимым, потому что текущие листы не являются главным bottleneck.

## Secondary Merge

Secondary merge остается exact-grain:

- `report_date`
- `report_date_from`
- `report_date_to`
- `utm_source`
- `utm_medium`
- `utm_campaign`
- `utm_content`
- `utm_term`

Этот redesign не меняет merge semantics. Он меняет только execution scope.

## Operator Cache Semantics

`union` сохраняет текущий grain:

- day-aware через `report_date`
- разные `utm_source / utm_medium / utm_campaign` остаются отдельными строками
- `utm_content = aggregated`
- `utm_term = aggregated`

Redesign не должен менять business semantics `union`.

## Migration Strategy

### Step 1

Добавить `normalized_files` и backfill из текущего `ingest_files`.

### Step 2

Научить edge function создавать/dirty-mark эти записи.

### Step 3

Переключить normalizer с `run_date` на `file_id`.

### Step 4

Переключить operator cache refresh с `run_date` на affected aggregate keys.

### Step 5

Переключить orchestrator:

- primary work unit = dirty files
- `pipeline_runs` использовать только для summary/status

## Why This Scales Better

Сейчас стоимость пересчета примерно равна:

- стоимости всего дня

После redesign стоимость пересчета должна быть ближе к:

- стоимости затронутого файла
- плюс стоимости affected aggregate keys

Это радикально уменьшает blast radius повторного ingest и делает прогон более предсказуемым.

## Risks

- state model становится сложнее: `pipeline_runs` + `normalized_files`
- нужен аккуратный контроль повторного ingest одного и того же файла
- при ошибке в affected-key расчете operator cache может стать stale локально
- migration потребует осторожной backfill-проверки на текущем corpus

## Non-Goals

- не вводим очередь, брокер или внешний worker fleet
- не меняем Apps Script intake model
- не меняем Sheets contract
- не меняем raw storage model
- не перепридумываем secondary merge

## Success Criteria

- повторный ingest одного файла не триггерит full rebuild дня
- normalizer работает по dirty files, а не по dirty days
- operator cache обновляется только по affected keys
- `pipeline_runs` остается корректным day-level summary
- `union` сохраняет текущую бизнес-семантику
- end-to-end прогон заметно быстрее на частичных повторных загрузках
