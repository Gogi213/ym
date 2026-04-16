# Cloudflare Worker R2 Ingest Design

**Goal:** Replace the current `Apps Script -> Render -> Turso` ingest path with `Apps Script -> Cloudflare Worker -> R2 + Turso manifest`, while keeping Python as the parsing, normalization, and Google Sheets layer.

## Scope

В scope этого этапа входит:

- замена `Render` ingest runtime на `Cloudflare Worker`;
- добавление `Cloudflare R2` как raw file storage;
- сохранение `Turso` как SQL storage для manifest, pipeline state и normalized layers;
- перевод Apps Script на новый ingest endpoint без смены его роли;
- перевод Python ingest-read path с SQL payload на `R2` object fetch.

Вне scope этого этапа:

- удаление Apps Script из контура;
- перенос parsing в Worker runtime;
- lifecycle automation для raw object cleanup;
- redesign Python execution model или job system.

## Problem Statement

Текущий `Render Free` runtime оказался плохим fit для ingest path.

Подтверждённые проблемы:

- cold start / sleeping runtime возвращает `502/503/504` и `Address unavailable`;
- `runMonthBackfill()` делает длинную серию `reset + ingest` HTTP вызовов;
- часть файлов успевает записаться, потом следующий запрос падает;
- день остаётся partial, а transport semantics становятся хрупкими;
- runtime не подходит как надёжный ingress boundary.

Отдельная проблема — raw storage fit:

- month backfill даёт `~700 MB - 1.5 GB` файлового raw payload;
- `Turso/libSQL` не должен быть permanent storage для тяжёлых бинарников;
- raw payload нужен временно, до конца месяца, а не навсегда.

## Recommended Architecture

Целевой контур:

- `Apps Script`
- `Cloudflare Worker`
- `Cloudflare R2`
- `Turso manifest/state tables`
- `Python parse + normalize + sheet sync`

### Why this design

Ключевые решения:

- `Worker` остаётся лёгким HTTP ingress runtime, а не местом для тяжёлого parsing;
- `R2` хранит raw files как object storage, а не SQL payload blobs;
- `Turso` хранит manifest, pipeline state, normalized tables и operator cache;
- Python остаётся местом, где уже живёт parsing и downstream data logic.

Это минимальный перенос, который убирает root cause `Render`-хрупкости и не тащит новый тяжёлый backend stack.

## Component Boundaries

### Apps Script

Apps Script остаётся тупым транспортом:

- читает тему/правила из Sheets;
- ищет письма и вложения;
- вызывает `reset` и `ingest` endpoint;
- использует status endpoint для backfill skip logic.

Apps Script не делает:

- SQL;
- file parsing;
- normalization;
- object storage orchestration beyond upload request assembly.

### Cloudflare Worker

Worker становится ingress boundary.

Ответственность:

- auth по ingest token;
- `POST /reset`;
- `POST /ingest`;
- `GET /pipeline-runs/{run_date}`;
- запись raw file в `R2`;
- запись manifest/state в `Turso`.

Worker не делает:

- `xlsx/csv` parsing;
- row extraction;
- normalization;
- Google Sheets sync.

### Cloudflare R2

`R2` — raw object storage.

Хранит:

- `xlsx/csv` payload files;
- objects per ingest file;
- raw retention window до конца месяца.

`R2` не хранит:

- normalized rows;
- pipeline state;
- operator cache.

### Turso

`Turso` остаётся SQL source of truth для:

- ingest manifest;
- pipeline state;
- normalized tables;
- operator cache;
- Sheets-facing views.

`Turso` больше не должен быть primary blob store для raw file bodies.

### Python pipeline

Python сохраняет текущую роль:

- читает pending raw manifest из `Turso`;
- качает file body из `R2` по `r2_key`;
- парсит `xlsx/csv`;
- пишет `ingest_rows` и downstream normalized layers;
- обновляет manifest/status.

## Data Model Changes

### Ingest manifest

Существующая идея `ingest_files` сохраняется, но становится настоящим source of truth.

Обязательные поля:

- `file_id`
- `run_date`
- `message_id`
- `thread_id`
- `matched_topic`
- `primary_topic`
- `topic_role`
- `file_name`
- `content_type`
- `status`
- `r2_key`
- `file_size_bytes`
- `parse_error`
- `raw_revision`
- `created_at`
- `updated_at`

### Statuses

Минимальный status model:

- `uploaded`
- `parsed`
- `skipped`
- `failed`

Если нужен transient state внутри Worker, допустим `uploading`, но downstream должен опираться на stable statuses above.

### Legacy payload storage

`ingest_file_payloads.file_base64` переводится в legacy path.

Новый runtime не пишет туда raw body как primary storage. Допустим один из двух вариантов:

- таблица остаётся, но больше не используется на новом path;
- таблица со временем удаляется после полного cutover.

## Endpoint Contract

### POST /reset

Input:

- `run_date`

Semantics:

- bump `raw_revision` for the day;
- invalidates previous raw manifest for current day scope;
- marks previous parsed/raw state for this revision as obsolete;
- prepares the day for fresh ingest.

Важно: `reset` должен быть idempotent.

### POST /ingest

Input:

- one attachment payload;
- same metadata contract as current Apps Script path:
  - `run_date`
  - `message_*`
  - `thread_*`
  - `matched_topic`
  - `primary_topic`
  - `topic_role`
  - attachment metadata.

Processing order:

1. auth and validate request
2. write object to `R2`
3. write/update manifest row in `Turso`
4. return success JSON

### GET /pipeline-runs/{run_date}

Должен возвращать truth-based status summary, а не просто `exists`.

Минимум:

- `run_date`
- `exists`
- `raw_files`
- `uploaded_files`
- `parsed_files`
- `failed_files`
- `normalize_status`

Apps Script skip logic должна считать день готовым только если summary реально показывает `ready`.

## Idempotency and Failure Model

### File identity

Для повторных ingest нужен стабильный natural key. Рекомендованный identity key:

- `run_date`
- `message_id`
- `file_name`
- `topic_role`
- `primary_topic`

Повторная загрузка того же файла не должна плодить дубли. Она должна обновлять existing manifest row/current revision.

### Object vs manifest ordering

Порядок записи критичен:

1. object write to `R2`
2. manifest write to `Turso`

Это делает `object without manifest` допустимым orphan case, а `manifest without object` — практически невозможным.

### Partial failure semantics

Если Worker упал до manifest write:

- возможен orphan object in `R2`;
- день не считается successfully ingested.

Если Python parsing упал:

- manifest row становится `failed`;
- пишется `parse_error`;
- другие файлы дня не блокируются.

### Retry safety

Повторный ingest после transient failure должен быть безопасным:

- duplicate file request updates same manifest identity;
- `reset` начинает новую revision for the day;
- status endpoint отражает current revision only.

## Python Pipeline Changes

Новый Python path:

1. select `ingest_files` with `status = uploaded`
2. download object body from `R2` using `r2_key`
3. parse `xlsx/csv`
4. write `ingest_rows`
5. update manifest to `parsed`
6. run existing downstream normalize/export flow

Новая ответственность Python:

- чтение raw body из object storage вместо SQL blob;
- обработка parse failures на уровне manifest status.

Что не меняется концептуально:

- normalize logic;
- secondary merge semantics;
- operator export semantics;
- Sheets sync semantics.

## Migration Strategy

### Phase 1: Worker + R2 bootstrap

Сделать:

- Cloudflare Worker project;
- R2 bucket;
- Turso connectivity from Worker;
- smoke flow for `reset`, `ingest`, `pipeline-runs/{run_date}`.

### Phase 2: Python raw-read preparation

Сделать:

- Python читает `R2` objects via `r2_key`;
- перестаёт зависеть от SQL payload body;
- подтверждается end-to-end parse/normalize correctness на Worker-written raw sample.

### Phase 3: Apps Script cutover

Сделать:

- перевести Apps Script endpoint с `Render` на `Worker`;
- сохранить existing transport contract максимально близким;
- прогнать fresh daily ingest и month backfill smoke.

### Phase 4: Legacy cleanup

Сделать:

- убрать `Render` из runtime path;
- пометить SQL payload blob path как legacy;
- обновить docs/runbooks под новый production contour.

## Risks

### 1. Worker CPU/runtime misuse

Риск:

- попытка перенести file parsing в Worker вернёт runtime fragility в новом месте.

Ответ:

- Worker keeps ingress-only responsibility;
- parsing stays in Python.

### 2. Object/manifest drift

Риск:

- orphan objects in `R2`;
- stale manifest rows after retries.

Ответ:

- strict write ordering;
- file identity key;
- `raw_revision` per run date.

### 3. Status truthfulness

Риск:

- Apps Script снова начнёт skip'ать partial days.

Ответ:

- status endpoint summarises current revision;
- skip only `ready`.

### 4. Legacy path coexistence

Риск:

- временный hybrid path породит путаницу.

Ответ:

- cutover phases should be short;
- once Worker path is proven, Render path is removed, not kept in parallel indefinitely.

## Success Criteria

Cutover считается успешным, если:

- `Apps Script -> Cloudflare Worker` работает для daily ingest и month backfill;
- raw files сохраняются в `R2`, не в SQL blob storage;
- `Turso` status/manifest честно отражает состояние дня и файлов;
- Python корректно читает raw file body из `R2` и даёт те же суммы `visits` и `goal_N`, что raw;
- `Render` полностью убран из ingest runtime path.
