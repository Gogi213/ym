# Local Python Runbook

## Supported Workflow

Рабочий контур сейчас такой:

1. Apps Script пишет raw ingest прямо в Turso.
2. Raw слой сохраняется в облачной БД.
3. Локальный Python запускает normalize и sheet sync.

## Step 1: Configure Apps Script direct Turso access

Apps Script primary properties now are:

- `TURSO_DATABASE_URL`
- `TURSO_AUTH_TOKEN`

Это единственный поддерживаемый Apps Script transport.

## Step 2: Run Apps Script ingest

Обычные entrypoints:

- `run()`
- `runMonthBackfill()`

После этого в Turso появляются raw files/payloads со статусом `raw_only`.

## Step 3: Run local Python post-processing

```powershell
python scripts\run_pipeline.py --service-account-json key\service-account.json
```

Теперь это основной локальный шаг после Apps Script.
`run_pipeline.py` сам дочитает `raw_only` payload из Turso, распарсит `csv/xlsx`, обновит file metadata, построит compact export rows и затем прогонит sheet sync.

## Step 2.5: Optional doctor / smoke check

Если нужно быстро понять, что именно Apps Script уже положил в Turso, не трогая данные:

```powershell
npm run doctor:turso -- --run-date YYYY-MM-DD --validate-payloads
```

Это read-only проверка. Она:

- проверит реальное подключение к Turso;
- покажет свежие `pipeline_runs`;
- покажет summary по выбранному `run_date`;
- проверит, что `raw_only` payload вообще парсится как ожидаемый `csv/xlsx`.

## One-day normalize

Если нужен только один день:

```powershell
python scripts\normalize_one_run.py --run-date YYYY-MM-DD
```

## Notes

- Apps Script заканчивает работу на raw ingest.
- `run_pipeline.py` — supported local post-processing entrypoint.
- `pipeline_runs` — operational truth for day status.
- Apps Script больше не использует локальный HTTP contour.
- `ingest_rows` не является частью поддерживаемого hot path.
