create table if not exists public.pipeline_runs (
  run_date date primary key,
  raw_revision bigint not null default 0 check (raw_revision >= 0),
  normalize_status text not null check (normalize_status in ('raw_only', 'pending_normalize', 'ready', 'normalize_error')),
  raw_files integer not null default 0,
  raw_rows bigint not null default 0,
  normalized_files integer not null default 0,
  normalized_rows bigint not null default 0,
  last_ingest_at timestamptz,
  normalized_at timestamptz,
  last_error text,
  created_at timestamptz not null default timezone('utc', now()),
  updated_at timestamptz not null default timezone('utc', now())
);

create index if not exists pipeline_runs_normalize_status_idx
  on public.pipeline_runs (normalize_status);

create index if not exists pipeline_runs_normalized_at_idx
  on public.pipeline_runs (normalized_at);

with ingest_summary as (
  select
    f.run_date,
    count(*) as total_files,
    count(*) filter (where f.status = 'ingested') as ingested_files,
    coalesce(sum(f.row_count) filter (where f.status = 'ingested'), 0) as raw_rows,
    max(f.created_at) as last_ingest_at
  from public.ingest_files f
  group by f.run_date
),
normalized_summary as (
  select
    f.run_date,
    count(distinct fr.source_file_id) as normalized_files,
    count(*) as normalized_rows,
    max(fr.created_at) as normalized_at
  from public.fact_rows fr
  join public.ingest_files f on f.id = fr.source_file_id
  group by f.run_date
)
insert into public.pipeline_runs (
  run_date,
  raw_revision,
  normalize_status,
  raw_files,
  raw_rows,
  normalized_files,
  normalized_rows,
  last_ingest_at,
  normalized_at,
  last_error
)
select
  i.run_date,
  1 as raw_revision,
  case
    when coalesce(i.ingested_files, 0) <= 0 then 'raw_only'
    when coalesce(n.normalized_rows, 0) >= coalesce(i.raw_rows, 0) then 'ready'
    else 'pending_normalize'
  end as normalize_status,
  i.total_files,
  i.raw_rows,
  coalesce(n.normalized_files, 0) as normalized_files,
  coalesce(n.normalized_rows, 0) as normalized_rows,
  i.last_ingest_at,
  n.normalized_at,
  null::text as last_error
from ingest_summary i
left join normalized_summary n on n.run_date = i.run_date
on conflict (run_date) do nothing;
