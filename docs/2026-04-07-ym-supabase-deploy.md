# YM Supabase Deploy Notes

## Current Shape

Сейчас backend состоит из двух частей:

- Supabase Edge Function для ingest
- Python normalizer для raw -> normalized

Файлы проекта:

- ingest function: [index.ts](/C:/visual%20projects/ym/supabase/functions/mail-ingest/index.ts)
- SQL migrations: [20260407053000_create_mail_ingest_tables.sql](/C:/visual%20projects/ym/supabase/migrations/20260407053000_create_mail_ingest_tables.sql), [20260407061000_create_ingest_file_payloads.sql](/C:/visual%20projects/ym/supabase/migrations/20260407061000_create_ingest_file_payloads.sql), [20260412160000_create_normalized_layer.sql](/C:/visual%20projects/ym/supabase/migrations/20260412160000_create_normalized_layer.sql)
- Python normalizer: [normalize_supabase.py](/C:/visual%20projects/ym/scripts/normalize_supabase.py)

## 1. Link the project

```powershell
npx supabase login
npx supabase link --project-ref jchvqvuudclgodsrhctb
```

## 2. Push database schema

```powershell
npx supabase db push
```

Это применяет raw и normalized слой:

- `ingest_files`
- `ingest_rows`
- `ingest_file_payloads`
- `topic_goal_slots`
- `fact_rows`
- `fact_dimensions`
- `fact_metrics`
- `export_rows_wide`

## 3. Set ingest secret

```powershell
npx supabase secrets set INGEST_TOKEN=replace_with_long_random_token
```

## 4. Deploy the ingest function

```powershell
npx supabase functions deploy mail-ingest --no-verify-jwt
```

JWT verification выключен намеренно. Функция использует собственный `x-ingest-token`.

## 5. Configure Apps Script

В Apps Script project нужны script properties:

- `SUPABASE_FUNCTION_URL`
- `SUPABASE_INGEST_TOKEN`

Значения:

- `SUPABASE_FUNCTION_URL = https://jchvqvuudclgodsrhctb.supabase.co/functions/v1/mail-ingest`
- `SUPABASE_INGEST_TOKEN = тот же токен, что в INGEST_TOKEN`

## 6. Install Python dependency

```powershell
python -m pip install -r requirements.txt
```

## 7. Configure Python DB access

Нормализатор поддерживает:

- `SUPABASE_DB_URL`
- или `SUPABASE_POOLER_URL` + `SUPABASE_DB_PASSWORD`

Пример через pooler:

```powershell
$env:SUPABASE_POOLER_URL='postgresql://postgres.<project-ref>@aws-1-ap-northeast-1.pooler.supabase.com:5432/postgres'
$env:SUPABASE_DB_PASSWORD='replace_with_db_password'
python scripts\normalize_supabase.py --run-date 2026-04-06
```

## 8. Verify data

Raw layer:

```sql
select run_date, status, matched_topic, attachment_name, row_count
from public.ingest_files
order by created_at desc;
```

Normalized layer:

```sql
select count(*) from public.fact_rows;
select count(*) from public.fact_dimensions;
select count(*) from public.fact_metrics;
select * from public.topic_goal_slots order by topic, goal_slot;
select * from public.export_rows_wide limit 20;
```

## 9. Git blocker

Текущая папка `C:\visual projects\ym` не содержит `.git`, поэтому коммит и push отсюда невозможны до тех пор, пока пользователь не даст настоящий git-репозиторий или не инициализирует git в этой директории.
