# YM Deep Review

Дата: `2026-04-13`

Скоуп ревью:

- все коммиты текущей ветки/истории проекта от `a5c064a` до рабочего состояния `2026-04-13`;
- баги и ошибки;
- архитектура, дизайн, логика и математика;
- дублирование, избыточность, переусложнение;
- когнитивная нагрузка, god objects, dead code и smell;
- превентивная архитектура под скейлинг.

---

## Краткий вердикт

Проект уже вышел из состояния MVP-хаоса и умеет работать end-to-end, но текущая форма всё ещё слишком batch-heavy.

Главное:

- ingest-контур уже рабочий;
- модель данных стала заметно взрослее после `pipeline_runs` и `operator_export_rows`;
- но post-processing слой остаётся тяжёлым и хрупким из-за слишком широких пересчётов current-state;
- самые тяжёлые и cognitively expensive места проекта сосредоточены в трёх god objects:
  - [Code.js](/C:/visual%20projects/ym/Code.js)
  - [normalize_supabase.py](/C:/visual%20projects/ym/scripts/normalize_supabase.py)
  - [index.ts](/C:/visual%20projects/ym/supabase/functions/mail-ingest/index.ts)

---

## Review By Commit

### `a5c064a` Initial ym ingest pipeline

- Сильная сторона: быстро собран сквозной контур.
- Риск: слишком много ответственности сразу в одном слое, из-за чего проект с самого старта вырос в несколько крупных файлов без чётких границ.

### `029f0af` Polish repository for production handoff

- Плюс: repo hygiene, базовая упаковка, дока.
- Риск: структура репозитория стала чище, но архитектурная сложность кода не уменьшилась.

### `131b86f`, `8003cf2`, `8d3aa12`

- Плюс: Apps Script backfill стал реальнее и безопаснее.
- Риск: Gmail intake всё ещё остаётся толстым orchestration-слоем, а не тонким collector-слоем.

### `2a9f3b3`, `9e85359`, `69872dc`, `0853dc5`

- Плюс: operator export был отделён от source-of-truth.
- Плюс: term/content aggregation и переименование метрик были правильным продуктовым шагом.
- Риск: логика operator view разъехалась между SQL, Python sync и докой, что увеличивает стоимость будущих изменений.

### `013c918`, `33b46d5`, `b311d29`

- Плюс: появился orchestration layer, status sync и видимость фаз.
- Плюс: таймауты и phase logging убрали часть “немых зависаний”.
- Риск: оркестратор всё ещё поверх batch-процесса, а не поверх нормального job model.

### `112dd7e`

- Плюс: secondary-topic merge был добавлен аккуратно и с явной моделью `primary_topic/topic_role`.
- Плюс: exact-grain merge — правильная защитная логика.
- Риск: этот слой добавил ещё одну ось сложности в уже крупный normalizer.

### `4fe5442`, `6ee9fd1`

- Это ключевой архитектурный поворот в правильную сторону.
- Плюс: `pipeline_runs` и `operator_export_rows` сделали пайплайн инкрементальным на уровне run-date.
- Главный remaining gap: current-state refresh остался слишком широким и дорогим.

### `66f9ced`, `885efdb`

- Плюс: появилась реальная, а не декларативная сквозная валидация.
- Плюс: был найден и исправлен реальный баг в row identity / dedup.
- Вывод: проект уже способен ловить математические расхождения, а не только “выглядит ок”.

### `2026-04-13` post-review performance pass

- Плюс: shipped bootstrap fast path для empty normalized layer.
- Плюс: cold rebuild теперь не тратит время на per-day delete/finalize passes.
- Плюс: подтверждена полная сквозная валидация `raw -> wide -> union` после cold rebuild.
- Вывод: правильный perf-pass оказался не в two-worker parallelism, а в снятии самых дорогих per-day finalize шагов.

---

## Findings

### 1. Критично: `refresh_current_flags_for_topics()` пересчитывает слишком широкий объём строк

Файл:
- [normalize_supabase.py](/C:/visual%20projects/ym/scripts/normalize_supabase.py)

Проблема:

- текущий refresh идёт по `topic`, а не по реально затронутым `row_hash`;
- при новом dirty-day пересчитываются не только новые ключи, а весь исторический topic;
- это делает тяжёлые дни непропорционально дорогими;
- именно это место стало главным performance hotspot.

Почему это важно:

- это прямой источник “процесс долгий и выглядит хрупким”;
- это нарушает сам смысл incremental architecture: dirty-day пересчёт не должен тянуть весь historical topic scope.

Статус:

- исправить в первую очередь.

### 2. Высокий риск: `normalize_supabase.py` — god object

Файл:
- [normalize_supabase.py](/C:/visual%20projects/ym/scripts/normalize_supabase.py)

Симптомы:

- `1209` строк;
- смешаны:
  - parsing/normalization rules,
  - row identity,
  - goal-slot assignment,
  - secondary merge,
  - DB write-path,
  - operator cache refresh,
  - pipeline status updates.

Последствия:

- высокий cognitive load;
- локальные правки легко создают неожиданные side effects;
- сложно профилировать и отдельно оптимизировать стадии.

Статус:

- не критично ломает работу сейчас, но это архитектурный долг.

### 3. Высокий риск: ingest Edge Function тоже стал god object

Файл:
- [index.ts](/C:/visual%20projects/ym/supabase/functions/mail-ingest/index.ts)

Симптомы:

- `861` строк;
- вручную реализованный parser + auth + pipeline state updates + ingest persistence;
- CSV/XLSX parsing, table detection, payload validation, run-state mutation и DB writes живут в одном файле.

Последствия:

- высокий риск регрессий в ingest;
- сложно локально тестировать только parsing или только persistence;
- трудно отделить performance issues parsing от DB issues.

### 4. Средне: Apps Script остаётся толстым транспортным слоем

Файл:
- [Code.js](/C:/visual%20projects/ym/Code.js)

Симптомы:

- `855` строк;
- в одном файле живут:
  - reading topic bindings,
  - matching,
  - Gmail scan,
  - reset/upload orchestration,
  - month backfill orchestration.

Это уже не блокер, но дальнейшее наращивание логики в Apps Script почти гарантированно ухудшит поддерживаемость.

### 5. Средне: duplicated goal-slot shape в нескольких слоях

Файлы:
- [normalize_supabase.py](/C:/visual%20projects/ym/scripts/normalize_supabase.py)
- [sync_export_rows_wide_sheet.py](/C:/visual%20projects/ym/scripts/sync_export_rows_wide_sheet.py)
- SQL migrations / views

Проблема:

- `goal_1 ... goal_25` повторяются вручную в Python и SQL;
- это не ошибка сейчас, но это повышает стоимость любой будущей смены формы.

### 6. Средне: pipeline hot path всё ещё заканчивается на Sheets

Проблема:

- даже после успешной нормализации пайплайн считается operationally complete только после sync в Google Sheets;
- это делает внешний сервис частью hot path.

Для pre-prod это допустимо, но для production-grade надёжности это по-прежнему слабое место.

### 7. Низкий, но системный smell: docs растут быстрее, чем уменьшается сложность кода

Плюс:

- документация уже хорошая.

Риск:

- часть сложности сейчас “компенсируется” докой, а не уменьшением самих responsibility boundaries.

---

## Dead Code / Smells

### Проверено и не подтверждено как блокирующая проблема

- явного крупного мёртвого кода в Python post-processing сейчас не видно;
- последние cleanup-правки действительно убрали часть старого Apps Script хвоста.

### Но smell остаётся

- крупные многоответственные файлы;
- ручной SQL pivot на `goal_1 ... goal_25`;
- повторяющиеся схемы export columns;
- mixture of business logic and transport/orchestration logic.

---

## Логика и математика

Сильные стороны:

- current canonical checks по `visits` и `goal_N` уже доказуемо проходят;
- operator export отделён от source-of-truth;
- aggregation semantics теперь хотя бы явно задокументированы.

Ключевой риск:

- любое дальнейшее изменение row identity, exact merge keys или operator aggregation rules без централизованного контракта снова может породить silent math drift.

---

## Превентивная архитектура

Что уже хорошо:

- `pipeline_runs` — правильный state layer;
- `operator_export_rows` — правильный cache layer;
- exact secondary merge — правильная defensive логика.

Чего не хватает:

- меньшего объёма per-day rewrite даже после bootstrap;
- лучшего разбиения normalizer на фазы с явными boundaries;
- более явного execution model, чем один длинный orchestration process.

---

## Что исправлять первым

### Priority 1

- сузить current-state refresh с `topic` до реально затронутых `row_hash`

Ожидаемый эффект:

- заметное ускорение тяжёлых dirty-day прогонов;
- снижение хрупкости длинных batch runs.

Статус:

- исправлено в рабочем коде `2026-04-13`;
- refresh current flags больше не пересчитывает весь historical topic scope;
- используется affected key set из:
  - уже существовавших `(topic, row_hash)` удаляемого `run_date`
  - новых `(topic, row_hash)` этого же normalize run

Замер на тяжёлом дне `2026-04-10`:

- до сужения scope:
  - `normalize_finished ~88s`
  - `refresh_flags ~43s`
- после сужения scope и объединения `delete + affected keys`:
  - `normalize_finished ~60s`
  - `refresh_flags ~6s`

### Priority 2

- не распараллеливать dirty `run_date`, а удешевить empty-state rebuild через bootstrap fast path

Почему:

- live measurement показал, что two-worker parallelism даёт regression из-за DB contention на `fact_*`;
- реальный shipped выигрыш даёт перенос delete/finalize из per-day hot path в один общий finalize-pass.

Статус:

- исправлено в рабочем коде `2026-04-13`;
- при пустом normalized-слое `run_pipeline.py` автоматически включает bootstrap mode;
- full cold rebuild `2026-04-01 .. 2026-04-12` после этого завершён примерно за `606882ms`;
- после rebuild выполнена полная сквозная сверка:
  - `visit_mismatches = 0`
  - `goal_mismatches = 0`

Новый главный hotspot после этого фикса:

- `insert_fact_rows + insert_fact_dimensions + insert_fact_metrics`
- `refresh_operator_export_rows_for_run`

То есть следующий выигрыш уже лежит не в current-state logic, а в сокращении объёма day rewrite и более дешёвом operator cache refresh.

### Priority 2

- декомпозировать `normalize_supabase.py` по ответственности хотя бы на:
  - extraction / canonicalization
  - row identity / merge
  - persistence / refresh

### Priority 3

- декомпозировать ingest function на parser + persistence + run-state updates

---

## Review Outcome

Вердикт:

- архитектура уже не аварийная и не случайная;
- но post-processing слой всё ещё не дотягивает до “спокойно скейлится”;
- главный remaining bottleneck и основной structural flaw — слишком широкий current-state refresh;
- это и есть первый правильный объект для исправления до следующих улучшений пайплайна.
