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
