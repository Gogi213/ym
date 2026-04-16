# YM Turso + Render Deploy

## Target Runtime

Текущий production ingress runtime:

- `Apps Script`
- `Render web service`
- `Python ingest service`
- `Turso/libSQL`
- `Python normalizer + sheet sync`

Source of truth по данным остаётся в Turso/libSQL.

## Live Service

Текущий live endpoint:

- `https://ym-ingest-service.onrender.com`

Проверенный health check:

- `GET /health`
- ответ: `{"ok":true}`

Render service metadata:

- service name: `ym-ingest-service`
- service id: `srv-d7frp9naqgkc73a24gug`

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

## Render Service Setup

Использованные настройки:

1. Service type:
   - `Web Service`
2. Runtime:
   - `Docker`
3. Dockerfile path:
   - `Dockerfile.ingest-service`
4. Docker context:
   - repo root
5. Plan:
   - `Free`
6. Port:
   - `8000`
7. Health check path:
   - `/health`

## Required Environment

Для сервиса нужны env:

- `INGEST_TOKEN`
- `TURSO_DATABASE_URL`
- `TURSO_AUTH_TOKEN`
- optional `PORT`
- optional `TURSO_LOCAL_REPLICA_PATH`

Текущий боевой transport token:

- `render-ingest-token-ym`

Локальный шаблон:

- [.env.ingest-service.example](/C:/visual%20projects/ym/.env.ingest-service.example)

## Post-Deploy Verification

Проверки после деплоя:

1. Открыть:
   - `https://ym-ingest-service.onrender.com/health`
2. Ожидать:
   - `{"ok":true}`
3. Проверить deploy status в Render:
   - текущий первый deploy завершился статусом `live`

## Apps Script Cutover

В Apps Script project properties задать:

- `INGEST_BASE_URL = https://ym-ingest-service.onrender.com`
- `INGEST_TOKEN = render-ingest-token-ym`

Опционально:

- `INGEST_STATUS_URL = https://ym-ingest-service.onrender.com`

Важно:

- Apps Script сам добавляет `/pipeline-runs/{run_date}` к `INGEST_STATUS_URL` или `INGEST_BASE_URL`
- отдельный suffix `/pipeline-runs` в property не нужен

Legacy `SUPABASE_*` properties можно оставить до полного cutover, но после успешного переключения они больше не нужны для нового transport path.

## Operational Boundary

Render в этом контуре держит только ingest HTTP layer.

Не делает:

- long-running normalizer
- Google Sheets sync
- full pipeline orchestration

То есть после Apps Script ingest остаётся Python post-processing job:

- `scripts/run_pipeline.py`
