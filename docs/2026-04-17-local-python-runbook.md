# Local Python Runbook

## Supported Workflow

Рабочий контур сейчас такой:

1. Apps Script отправляет raw ingest в настроенный endpoint.
2. Raw слой сохраняется в облачной БД.
3. Локальный Python запускает normalize и sheet sync.

## Step 1: Start local ingest service

```powershell
$env:INGEST_TOKEN='<ingest-token>'
$env:TURSO_DATABASE_URL='libsql://<db-name>-<org>.turso.io'
$env:TURSO_AUTH_TOKEN='<db-token>'
uvicorn ingest_service.main:app --host 0.0.0.0 --port 8000
```

## Step 1b: Expose local ingest for Apps Script

Если Apps Script должен ходить в локальный ingest service, нужен публичный URL. Самый короткий рабочий путь:

```powershell
cloudflared tunnel --url http://127.0.0.1:8000 --no-autoupdate
```

Команда отдаст временный URL вида:

- `https://<random>.trycloudflare.com`

Именно этот URL нужно использовать для:

- `INGEST_BASE_URL`
- optional `INGEST_STATUS_URL`

## Step 2: Run Apps Script ingest

Apps Script должен знать:

- `INGEST_BASE_URL`
- `INGEST_TOKEN`
- optional `INGEST_STATUS_URL`

Обычные entrypoints:

- `run()`
- `runMonthBackfill()`

## Step 3: Run local Python pipeline

После того как raw уже долетел в облачную БД:

```powershell
python scripts\run_pipeline.py --service-account-json key\service-account.json
```

Это делает:

- normalize dirty `run_date`
- sync `отчеты`
- sync `union`
- sync `pipeline_status`

## One-day fallback

Если нужен только один день:

```powershell
python scripts\normalize_supabase.py --run-date YYYY-MM-DD
```

## Notes

- Apps Script заканчивает работу на raw ingest.
- `run_pipeline.py` — supported local post-processing entrypoint.
- `pipeline_runs` — operational truth for day status.
- Локальный `127.0.0.1` недоступен из Google напрямую. Без туннеля или другого публичного URL Apps Script в локальный ingest не попадёт.
- `cloudflared` quick tunnel — временный operational bridge, а не новый supported hosted runtime.
