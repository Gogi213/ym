create table if not exists public.topic_goal_slots (
  topic text not null,
  goal_slot integer not null check (goal_slot > 0),
  source_header text not null,
  goal_label text,
  first_seen_file_id uuid references public.ingest_files(id) on delete set null,
  created_at timestamptz not null default timezone('utc', now()),
  updated_at timestamptz not null default timezone('utc', now()),
  primary key (topic, goal_slot),
  unique (topic, source_header)
);

create table if not exists public.fact_rows (
  fact_row_id uuid primary key default gen_random_uuid(),
  topic text not null,
  source_file_id uuid not null references public.ingest_files(id) on delete cascade,
  source_row_index integer not null,
  report_date date,
  report_date_from date,
  report_date_to date,
  message_date timestamptz,
  layout_signature text not null,
  row_hash text not null,
  is_current boolean not null default true,
  source_row_json jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default timezone('utc', now()),
  unique (source_file_id, source_row_index)
);

create table if not exists public.fact_dimensions (
  fact_row_id uuid not null references public.fact_rows(fact_row_id) on delete cascade,
  dimension_key text not null,
  dimension_value text,
  created_at timestamptz not null default timezone('utc', now()),
  primary key (fact_row_id, dimension_key)
);

create table if not exists public.fact_metrics (
  fact_row_id uuid not null references public.fact_rows(fact_row_id) on delete cascade,
  metric_key text not null,
  metric_value double precision,
  created_at timestamptz not null default timezone('utc', now()),
  primary key (fact_row_id, metric_key)
);

create index if not exists topic_goal_slots_topic_idx
  on public.topic_goal_slots (topic);

create index if not exists fact_rows_topic_idx
  on public.fact_rows (topic);

create index if not exists fact_rows_report_date_idx
  on public.fact_rows (report_date);

create index if not exists fact_rows_row_hash_idx
  on public.fact_rows (row_hash);

create index if not exists fact_rows_is_current_idx
  on public.fact_rows (is_current);

create index if not exists fact_dimensions_dimension_key_idx
  on public.fact_dimensions (dimension_key);

create index if not exists fact_metrics_metric_key_idx
  on public.fact_metrics (metric_key);

create or replace function public.refresh_fact_rows_current_flags()
returns void
language sql
as $$
  with ranked as (
    select
      fact_row_id,
      row_number() over (
        partition by topic, row_hash
        order by message_date desc nulls last, created_at desc, source_file_id desc
      ) as rn
    from public.fact_rows
  )
  update public.fact_rows fr
  set is_current = (ranked.rn = 1)
  from ranked
  where ranked.fact_row_id = fr.fact_row_id;
$$;

create or replace view public.export_rows_wide as
with current_rows as (
  select *
  from public.fact_rows
  where is_current = true
),
dimension_pivot as (
  select
    fd.fact_row_id,
    max(case when fd.dimension_key = 'utm_source' then fd.dimension_value end) as utm_source,
    max(case when fd.dimension_key = 'utm_medium' then fd.dimension_value end) as utm_medium,
    max(case when fd.dimension_key = 'utm_campaign' then fd.dimension_value end) as utm_campaign,
    max(case when fd.dimension_key = 'utm_content' then fd.dimension_value end) as utm_content,
    max(case when fd.dimension_key = 'utm_term' then fd.dimension_value end) as utm_term,
    max(case when fd.dimension_key = 'visit_date' then fd.dimension_value end) as visit_date
  from public.fact_dimensions fd
  group by fd.fact_row_id
),
metric_pivot as (
  select
    fm.fact_row_id,
    max(case when fm.metric_key = 'visits' then fm.metric_value end) as visits,
    max(case when fm.metric_key = 'users' then fm.metric_value end) as users,
    max(case when fm.metric_key = 'bounce_rate' then fm.metric_value end) as bounce_rate,
    max(case when fm.metric_key = 'page_depth' then fm.metric_value end) as page_depth,
    max(case when fm.metric_key = 'time_on_site_seconds' then fm.metric_value end) as time_on_site_seconds,
    max(case when fm.metric_key = 'robot_rate' then fm.metric_value end) as robot_rate,
    max(case when fm.metric_key = 'goal_1' then fm.metric_value end) as goal_1,
    max(case when fm.metric_key = 'goal_2' then fm.metric_value end) as goal_2,
    max(case when fm.metric_key = 'goal_3' then fm.metric_value end) as goal_3,
    max(case when fm.metric_key = 'goal_4' then fm.metric_value end) as goal_4,
    max(case when fm.metric_key = 'goal_5' then fm.metric_value end) as goal_5,
    max(case when fm.metric_key = 'goal_6' then fm.metric_value end) as goal_6,
    max(case when fm.metric_key = 'goal_7' then fm.metric_value end) as goal_7,
    max(case when fm.metric_key = 'goal_8' then fm.metric_value end) as goal_8,
    max(case when fm.metric_key = 'goal_9' then fm.metric_value end) as goal_9,
    max(case when fm.metric_key = 'goal_10' then fm.metric_value end) as goal_10,
    max(case when fm.metric_key = 'goal_11' then fm.metric_value end) as goal_11,
    max(case when fm.metric_key = 'goal_12' then fm.metric_value end) as goal_12,
    max(case when fm.metric_key = 'goal_13' then fm.metric_value end) as goal_13,
    max(case when fm.metric_key = 'goal_14' then fm.metric_value end) as goal_14,
    max(case when fm.metric_key = 'goal_15' then fm.metric_value end) as goal_15,
    max(case when fm.metric_key = 'goal_16' then fm.metric_value end) as goal_16,
    max(case when fm.metric_key = 'goal_17' then fm.metric_value end) as goal_17,
    max(case when fm.metric_key = 'goal_18' then fm.metric_value end) as goal_18,
    max(case when fm.metric_key = 'goal_19' then fm.metric_value end) as goal_19,
    max(case when fm.metric_key = 'goal_20' then fm.metric_value end) as goal_20,
    max(case when fm.metric_key = 'goal_21' then fm.metric_value end) as goal_21,
    max(case when fm.metric_key = 'goal_22' then fm.metric_value end) as goal_22,
    max(case when fm.metric_key = 'goal_23' then fm.metric_value end) as goal_23,
    max(case when fm.metric_key = 'goal_24' then fm.metric_value end) as goal_24,
    max(case when fm.metric_key = 'goal_25' then fm.metric_value end) as goal_25
  from public.fact_metrics fm
  group by fm.fact_row_id
)
select
  fr.fact_row_id,
  fr.topic,
  fr.source_file_id,
  fr.source_row_index,
  fr.report_date,
  fr.report_date_from,
  fr.report_date_to,
  fr.message_date,
  fr.layout_signature,
  fr.row_hash,
  dp.utm_source,
  dp.utm_medium,
  dp.utm_campaign,
  dp.utm_content,
  dp.utm_term,
  dp.visit_date,
  mp.visits,
  mp.users,
  mp.bounce_rate,
  mp.page_depth,
  mp.time_on_site_seconds,
  mp.robot_rate,
  mp.goal_1,
  mp.goal_2,
  mp.goal_3,
  mp.goal_4,
  mp.goal_5,
  mp.goal_6,
  mp.goal_7,
  mp.goal_8,
  mp.goal_9,
  mp.goal_10,
  mp.goal_11,
  mp.goal_12,
  mp.goal_13,
  mp.goal_14,
  mp.goal_15,
  mp.goal_16,
  mp.goal_17,
  mp.goal_18,
  mp.goal_19,
  mp.goal_20,
  mp.goal_21,
  mp.goal_22,
  mp.goal_23,
  mp.goal_24,
  mp.goal_25,
  fr.source_row_json,
  fr.created_at
from current_rows fr
left join dimension_pivot dp on dp.fact_row_id = fr.fact_row_id
left join metric_pivot mp on mp.fact_row_id = fr.fact_row_id;
