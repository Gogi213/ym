# Two-Worker Parallel Run-Date Design

> Status: investigated on `2026-04-13`, measured on live data, not shipped. Real runs regressed versus sequential execution because DB contention on `fact_*` outweighed worker overlap. Kept as design record only.

## Goal

Кратно ускорить полный cold rebuild с пустой базы, не меняя бизнес-семантику пайплайна и не дробя execution model до `file_id`.

## Problem

Текущий bottleneck уже не в Apps Script и не в Google Sheets. Он в Python post-processing:

- dirty `run_date` обрабатываются последовательно
- каждый день нормализуется целиком
- operator cache для дня пересчитывается синхронно
- после этого синкаются листы

Для partial rerun это терпимо. Для пустой базы или month-backfill это дорого:

- много дней ждут друг друга
- wall-clock time почти равен сумме времен всех дней

Это и есть главная реальная боль текущего контура.

## Recommended Approach

Оставить `run_date` единицей работы, но выполнять dirty дни параллельно в двух воркерах.

### Core Rules

- нормализация по-прежнему остается `run_date`-scoped
- каждый dirty `run_date` обрабатывается независимо
- максимум два concurrent worker одновременно
- `union`, `отчеты`, `pipeline_status` синкаются один раз после завершения всех dirty дней

## Why This Is The Right Optimization

### Why not file-scoped first

`file`-scoped incremental больше помогает при частичных повторных загрузках и точечных corrections.

Но в сценарии:

- база пустая
- нужно пересчитать весь месяц

все файлы все равно должны быть обработаны. В этом сценарии `file`-level orchestration усложняет систему, но не дает максимального ускорения.

### Why run-date parallelism first

Сейчас дни уже естественным образом изолированы:

- raw scope ограничен `run_date`
- current-state refresh уже сузили до affected row keys
- operator cache пересчитывается по `run_date`

Это значит, что `run_date` — уже готовая единица безопасного распараллеливания.

## Runtime Shape

### Orchestrator

`run_pipeline.py`:

- получает список dirty `run_date`
- запускает максимум два Python worker task одновременно
- ждет завершения всех дней
- после этого один раз делает sheet sync

### Worker Unit

Один worker:

- вызывает текущий `normalize_run(run_date)`
- пишет phase logs
- возвращает structured result по дню

### Failure Model

Если один день падает:

- этот `run_date` получает `normalize_error`
- другие воркеры продолжают работу
- итоговый pipeline result показывает:
  - successful days
  - failed days
- full sheet sync не должен запускаться, если есть хотя бы один failed day, иначе оператор увидит частично обновленную витрину как будто все ок

## Concurrency Model

Рекомендуемый старт:

- `max_workers = 2`

Почему не больше:

- меньше риск DB contention
- меньше риск long lock chains в `fact_*`
- ниже шанс упереться в pooler/connection pressure
- проще rollback, наблюдение и отладка

Если после стабилизации окажется, что запас есть, параметр можно поднимать.

## Safety Constraints

### 1. No duplicate run_date processing

Один `run_date` не должен попадать одновременно в два worker.

### 2. No cross-day shared mutation during normalize

`normalize_run(run_date)` должен мутировать только:

- facts, связанные с этим днем
- operator cache этого дня
- state этого дня

Любые cross-day side effects должны быть исключены.

### 3. Sheet sync only after worker barrier

`отчеты`, `union`, `pipeline_status` обновляются только после завершения всех worker.

## Required Code Changes

### `scripts/run_pipeline.py`

- добавить worker pool с `max_workers=2`
- выполнять normalize tasks параллельно
- собирать structured results
- при любой ошибке пропускать heavy sheet sync

### `scripts/normalize_supabase.py`

- не менять business semantics
- убедиться, что `normalize_run(run_date)` не имеет cross-day побочных эффектов
- сохранить phase logging

### `scripts/sync_pipeline_status_sheet.py`

- pipeline status должен явно показывать mix:
  - `ready`
  - `normalize_error`
  - `pending_normalize`

### Tests

Нужны тесты на:

- выбор dirty `run_date`
- parallel scheduling без дублей
- behavior при частичном failure
- отсутствие full sync при ошибке одного worker

## Business Semantics That Must Not Change

- secondary merge остается exact-grain
- `union` сохраняет текущий grain:
  - `report_date`
  - `utm_source`
  - `utm_medium`
  - `utm_campaign`
  - `utm_content = aggregated`
  - `utm_term = aggregated`
- validation semantics не меняются:
  - `visits` по primary raw
  - `goal_*` по effective topic raw

## Expected Impact

Для cold rebuild:

- wall-clock time должен стать ближе к времени двух самых длинных последовательностей дней, а не сумме всех дней

Практически это должно дать заметный буст без смены архитектуры:

- лучше всего именно на full empty-start rebuild
- умеренно полезно на multi-day backfill
- почти нейтрально на one-day run

## Risks

- DB contention между двумя днями
- более сложный error-handling в orchestrator
- частичный успех batch-а требует явной семантики sheet sync

## Non-Goals

- не вводим queue/broker/worker fleet
- не меняем Apps Script
- не меняем raw schema
- не переходим на file-scoped execution
- не оптимизируем пока сами SQL-пути normalized layer beyond current state

## Success Criteria

- два dirty дня реально нормализуются параллельно
- при ошибке одного дня другие не откатываются
- sheet sync не происходит при частичном batch failure
- `visits` и `goal_*` остаются консистентными end-to-end
- cold rebuild с пустой базы заметно быстрее текущего последовательного режима
