# YM Turso + Northflank Deploy

## Target Runtime

Целевой production-контур:

- `Apps Script`
- `Python ingest service`
- `Turso/libSQL`
- `Python normalizer + sheet sync`

Northflank здесь используется только как runtime для Python ingest service.
Source of truth по данным остаётся в Turso/libSQL.

## What Gets Deployed

Деплоится только ingest service.

Файлы:

- [Dockerfile.ingest-service](/C:/visual%20projects/ym/Dockerfile.ingest-service)
- [ingest_service/main.py](/C:/visual%20projects/ym/ingest_service/main.py)
- [ingest_service/runtime.py](/C:/visual%20projects/ym/ingest_service/runtime.py)
- [ingest_service/app.py](/C:/visual%20projects/ym/ingest_service/app.py)
- [ingest_service/handlers.py](/C:/visual%20projects/ym/ingest_service/handlers.py)
- [ingest_service/parse.py](/C:/visual%20projects/ym/ingest_service/parse.py)
- [ingest_service/storage.py](/C:/visual%20projects/ym/ingest_service/storage.py)
- [scripts/turso_runtime.py](/C:/visual%20projects/ym/scripts/turso_runtime.py)

Сервис поднимает:

- `GET /health`
- `POST /reset`
- `POST /ingest`
- `GET /pipeline-runs/{run_date}`

## Required Environment

Для сервиса нужны env:

- `INGEST_TOKEN`
- `TURSO_DATABASE_URL`
- `TURSO_AUTH_TOKEN`
- optional `PORT`
- optional `TURSO_LOCAL_REPLICA_PATH`

Локальный шаблон:

- [.env.ingest-service.example](/C:/visual%20projects/ym/.env.ingest-service.example)

## Northflank Service Setup

Минимальный путь:

1. Создать новый service из репозитория `Gogi213/ym`.
2. В качестве Dockerfile указать:
   - `Dockerfile.ingest-service`
3. Build context:
   - repo root
4. Exposed port:
   - `8000`
5. Health check path:
   - `/health`
6. Добавить env:
   - `INGEST_TOKEN`
   - `TURSO_DATABASE_URL`
   - `TURSO_AUTH_TOKEN`

Ожидаемый runtime command уже задан в Dockerfile:

- `uvicorn ingest_service.main:app --host 0.0.0.0 --port ${PORT:-8000}`

## Post-Deploy Verification

Проверки после деплоя:

1. Открыть:
   - `https://<northflank-service-url>/health`
2. Ожидать:
   - `{"ok":true}`
3. Выполнить auth check на protected route:
   - `GET /pipeline-runs/2026-04-14`
   - header `x-ingest-token: <INGEST_TOKEN>`
4. Ожидать `200` и JSON-ответ с полями:
   - `run_date`
   - `exists`
   - `raw_files`
   - `raw_rows`
   - `normalize_status`

## Apps Script Cutover

После получения live URL в Apps Script project properties задать:

- `INGEST_BASE_URL = https://<northflank-service-url>`
- `INGEST_TOKEN = <тот же token>`

Опционально:

- `INGEST_STATUS_URL = https://<northflank-service-url>/pipeline-runs`

Если `INGEST_STATUS_URL` не задан, Apps Script сам строит его из `INGEST_BASE_URL`.

Legacy `SUPABASE_*` properties можно оставить до полного cutover, но после успешного переключения они больше не нужны для нового transport path.

## Operational Boundary

Northflank в этом контуре держит только ingest HTTP layer.

Не делает:

- long-running normalizer
- Google Sheets sync
- full pipeline orchestration

То есть после Apps Script ingest остаётся Python post-processing job:

- `scripts/run_pipeline.py`

Этот кусок ещё не перенесён на Northflank runtime и остаётся отдельным operational шагом.
