# Local Python + Cloud DB Pipeline Design

**Goal:** Keep Apps Script as the thin ingest collector, keep the cloud database as the raw and normalized source of truth, and treat local Python as the post-processing runner without overengineering the execution model.

## Scope

В scope этого дизайна входит:

- Apps Script остаётся в pipeline;
- raw ingest остаётся в облачной БД;
- `pipeline_runs` становится явным state layer по `run_date`;
- local Python остаётся post-processing runner для normalize + sheet sync;
- `run_date` остаётся единицей работы.

Вне scope:

- удаление Apps Script из контура;
- поиск и внедрение нового hosted runtime;
- queue / event bus / distributed workers;
- object storage или другой отдельный storage layer для raw files.

## Problem Statement

Проект уже имел рабочую бизнес-логику до попыток уехать в hosted runtime.
Проблемой оказался не сам data model, а operational runtime around it.

Нужно вернуться к рабочему контуру без отката в хаос:

- не ломать Apps Script как тонкий collector;
- не тащить лишние runtime experiments в repo architecture;
- не делать full rebuild всего диапазона по умолчанию;
- сохранить облачную БД как source of truth;
- оставить local Python runner до будущего server move.

## Recommended Architecture

Целевой контур:

- `Apps Script`
- `configured ingest endpoint`
- `cloud raw tables`
- `pipeline_runs`
- `local Python normalizer`
- `operator cache`
- `local Python sheet sync`

### Why this design

Ключевые решения:

- Apps Script остаётся thin ingress boundary;
- raw ingest и post-processing разделены;
- cloud DB остаётся единым source of truth для raw, normalized и state layers;
- local Python не делает ingest discovery, а только post-processing;
- единица работы — `run_date`, не вся база.

Это production-like design без лишней orchestration machinery.

## Component Boundaries

### Apps Script

Apps Script отвечает только за ingest-side работу:

- читать topics из spreadsheet;
- искать письма и вложения;
- слать `reset` и attachments в ingest endpoint;
- использовать status endpoint только для backfill skip logic.

Apps Script не отвечает за:

- normalize;
- operator export;
- Google Sheets sync;
- orchestration всего pipeline.

### Cloud database raw layer

Raw source of truth:

- `ingest_files`
- `ingest_rows`
- `ingest_file_payloads`

Cloud DB хранит:

- file-level audit trail;
- row-level extracted raw rows;
- временный raw payload body;
- topic metadata (`primary_topic`, `matched_topic`, `topic_role`).

### Pipeline state layer

`pipeline_runs` — обязательный control plane по `run_date`.

Минимальные поля:

- `run_date`
- `raw_revision`
- `normalize_status`
- `raw_files`
- `raw_rows`
- `normalized_files`
- `normalized_rows`
- `last_ingest_at`
- `normalized_at`
- `last_error`

Минимальные статусы:

- `raw_only`
- `pending_normalize`
- `ready`
- `normalize_error`

### Local Python runner

Local Python отвечает только за post-processing:

- читает `pipeline_runs`;
- выбирает dirty dates;
- normalizes raw data;
- refreshes operator cache;
- syncs Google Sheets.

Local Python не должен быть частью ingest transport.

### Normalized layer

Сохраняется как сейчас:

- `fact_rows`
- `fact_dimensions`
- `fact_metrics`
- `topic_goal_slots`

### Operator layer

Сохраняется как сейчас:

- `operator_export_rows`
- `goal_mapping_wide`
- `union` / `отчеты` / `pipeline_status` sheet outputs

## Execution Model

### Unit of work

Единица работы — `run_date`.

Правила:

- новый raw ingest за день делает только этот день dirty;
- повторный ingest того же дня bump'ает revision и делает только этот день dirty заново;
- остальные дни не трогаются.

### Apps Script lifecycle

Apps Script заканчивает работу на raw ingest.

Это означает:

- `run()` не orchestrates normalize/sheets;
- `runMonthBackfill()` не orchestrates normalize/sheets;
- Apps Script не должен быть global pipeline runner.

### Python runner lifecycle

`run_pipeline.py` — один post-ingest entrypoint.

Он делает:

1. находит `pending_normalize` run dates;
2. прогоняет normalizer по ним;
3. при успехе пишет `ready`;
4. при ошибке пишет `normalize_error`;
5. затем sync sheet outputs.

### Sheet sync policy

Sheet outputs разрешено переписывать целиком.

Следствие:

- scale bottleneck не в Google Sheets;
- оптимизация фокусируется на normalize/runtime path, а не на partial sheet writes.

## Failure Model

### Raw ingest failure

Если ingest за день неполный:

- day status не должен считаться `ready`;
- следующий ingest/retry должен безопасно работать только в пределах этой даты.

### Normalize failure

Если normalizer падает на конкретном `run_date`:

- ставится `normalize_error`;
- остальные даты не считаются implicitly fixed;
- следующий прогон подбирает только dirty/error dates.

### Rebuild policy

Full rebuild всего диапазона не должен быть дефолтным поведением.

Полный rebuild — только как явная техническая операция.

## Data Semantics

### Topics

- `primary_topic` остаётся business topic;
- `secondary` остаётся optional conversion topic;
- secondary merge работает только на exact grain match.

### Raw retention

Raw payload может временно жить в DB.

Допустимое operational правило на текущем этапе:

- raw files лежат до следующего цикла / месяца;
- отдельную lifecycle automation сейчас не проектируем.

### Operator export

`union` остаётся operator-facing aggregated export, а не raw dump.

## Success Criteria

Этот дизайн считается реализованным, если:

- Apps Script остаётся thin collector;
- cloud DB остаётся source of truth для raw + state + normalized layers;
- local Python runner обрабатывает только dirty `run_date`;
- `ready` даты не пересчитываются без причины;
- business semantics `raw -> wide -> union` остаются корректными;
- repo больше не содержит misleading architecture around abandoned hosted-runtime experiments.
