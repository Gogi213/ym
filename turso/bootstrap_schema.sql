create table if not exists ingest_files (
  id text primary key,
  run_date text not null,
  message_id text not null,
  thread_id text,
  message_date text,
  message_subject text not null,
  primary_topic text not null,
  matched_topic text not null,
  topic_role text not null check (topic_role in ('primary', 'secondary')),
  attachment_name text not null,
  attachment_type text not null check (attachment_type in ('xlsx', 'csv')),
  status text not null check (status in ('ingested', 'skipped', 'error', 'uploaded', 'parsed', 'failed')),
  header_json text not null default '[]',
  row_count integer not null default 0,
  r2_key text,
  file_size_bytes integer,
  parse_error text,
  raw_revision integer not null default 0 check (raw_revision >= 0),
  error_text text,
  created_at text not null default current_timestamp,
  updated_at text not null default current_timestamp
);

create table if not exists ingest_rows (
  id integer primary key,
  file_id text not null references ingest_files(id) on delete cascade,
  run_date text not null,
  row_index integer not null,
  row_json text not null,
  created_at text not null default current_timestamp
);

create table if not exists ingest_file_payloads (
  file_id text primary key references ingest_files(id) on delete cascade,
  content_type text,
  file_size_bytes integer not null,
  file_base64 text not null,
  created_at text not null default current_timestamp
);

create table if not exists topic_goal_slots (
  topic text not null,
  goal_slot integer not null check (goal_slot > 0),
  source_header text not null,
  goal_label text,
  first_seen_file_id text references ingest_files(id) on delete set null,
  created_at text not null default current_timestamp,
  updated_at text not null default current_timestamp,
  primary key (topic, goal_slot),
  unique (topic, source_header)
);

create table if not exists fact_rows (
  fact_row_id text primary key,
  topic text not null,
  source_file_id text not null references ingest_files(id) on delete cascade,
  source_row_index integer not null,
  report_date text,
  report_date_from text,
  report_date_to text,
  message_date text,
  layout_signature text not null,
  row_hash text not null,
  is_current integer not null default 1,
  source_row_json text not null default '{}',
  created_at text not null default current_timestamp,
  unique (source_file_id, source_row_index)
);

create table if not exists fact_dimensions (
  fact_row_id text not null references fact_rows(fact_row_id) on delete cascade,
  dimension_key text not null,
  dimension_value text,
  created_at text not null default current_timestamp,
  primary key (fact_row_id, dimension_key)
);

create table if not exists fact_metrics (
  fact_row_id text not null references fact_rows(fact_row_id) on delete cascade,
  metric_key text not null,
  metric_value numeric,
  created_at text not null default current_timestamp,
  primary key (fact_row_id, metric_key)
);

create table if not exists pipeline_runs (
  run_date text primary key,
  raw_revision integer not null default 0 check (raw_revision >= 0),
  normalize_status text not null check (normalize_status in ('raw_only', 'pending_normalize', 'ready', 'normalize_error')),
  raw_files integer not null default 0,
  raw_rows integer not null default 0,
  normalized_files integer not null default 0,
  normalized_rows integer not null default 0,
  last_ingest_at text,
  normalized_at text,
  last_error text,
  created_at text not null default current_timestamp,
  updated_at text not null default current_timestamp
);

create table if not exists operator_export_rows (
  row_id integer primary key,
  run_date text not null,
  topic text not null,
  report_date text,
  report_date_from text,
  report_date_to text,
  utm_source text,
  utm_medium text,
  utm_campaign text,
  utm_content text not null default 'aggregated',
  utm_term text not null default 'aggregated',
  visits numeric,
  users numeric,
  bounce_rate numeric,
  page_depth numeric,
  time_on_site_seconds numeric,
  robot_rate numeric,
  goal_1 numeric,
  goal_2 numeric,
  goal_3 numeric,
  goal_4 numeric,
  goal_5 numeric,
  goal_6 numeric,
  goal_7 numeric,
  goal_8 numeric,
  goal_9 numeric,
  goal_10 numeric,
  goal_11 numeric,
  goal_12 numeric,
  goal_13 numeric,
  goal_14 numeric,
  goal_15 numeric,
  goal_16 numeric,
  goal_17 numeric,
  goal_18 numeric,
  goal_19 numeric,
  goal_20 numeric,
  goal_21 numeric,
  goal_22 numeric,
  goal_23 numeric,
  goal_24 numeric,
  goal_25 numeric,
  created_at text not null default current_timestamp,
  updated_at text not null default current_timestamp
);

create index if not exists ingest_files_run_date_idx on ingest_files (run_date);
create index if not exists ingest_files_message_id_idx on ingest_files (message_id);
create index if not exists ingest_files_matched_topic_idx on ingest_files (matched_topic);
create index if not exists ingest_files_primary_topic_idx on ingest_files (primary_topic);
create index if not exists ingest_files_topic_role_idx on ingest_files (topic_role);
create index if not exists ingest_rows_file_id_idx on ingest_rows (file_id);
create index if not exists ingest_rows_run_date_idx on ingest_rows (run_date);
create index if not exists ingest_file_payloads_created_at_idx on ingest_file_payloads (created_at);
create index if not exists topic_goal_slots_topic_idx on topic_goal_slots (topic);
create index if not exists fact_rows_topic_idx on fact_rows (topic);
create index if not exists fact_rows_report_date_idx on fact_rows (report_date);
create index if not exists fact_rows_row_hash_idx on fact_rows (row_hash);
create index if not exists fact_rows_is_current_idx on fact_rows (is_current);
create index if not exists fact_dimensions_dimension_key_idx on fact_dimensions (dimension_key);
create index if not exists fact_metrics_metric_key_idx on fact_metrics (metric_key);
create index if not exists pipeline_runs_normalize_status_idx on pipeline_runs (normalize_status);
create index if not exists pipeline_runs_normalized_at_idx on pipeline_runs (normalized_at);
create index if not exists operator_export_rows_run_date_idx on operator_export_rows (run_date);
create index if not exists operator_export_rows_report_date_idx on operator_export_rows (report_date);

create view if not exists export_rows_wide as
with current_rows as (
  select *
  from fact_rows
  where is_current = 1
),
dimension_pivot as (
  select
    fd.fact_row_id,
    max(case when fd.dimension_key = 'utm_source' then fd.dimension_value end) as utm_source,
    max(case when fd.dimension_key = 'utm_medium' then fd.dimension_value end) as utm_medium,
    max(case when fd.dimension_key = 'utm_campaign' then fd.dimension_value end) as utm_campaign,
    max(case when fd.dimension_key = 'utm_content' then fd.dimension_value end) as utm_content,
    max(case when fd.dimension_key = 'utm_term' then fd.dimension_value end) as utm_term
  from fact_dimensions fd
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
  from fact_metrics fm
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

create view if not exists goal_mapping_wide as
with topics as (
  select distinct matched_topic as topic
  from ingest_files
  where status = 'ingested'
)
select
  topics.topic,
  max(case when tgs.goal_slot = 1 then coalesce(tgs.goal_label, tgs.source_header) end) as goal_1,
  max(case when tgs.goal_slot = 2 then coalesce(tgs.goal_label, tgs.source_header) end) as goal_2,
  max(case when tgs.goal_slot = 3 then coalesce(tgs.goal_label, tgs.source_header) end) as goal_3,
  max(case when tgs.goal_slot = 4 then coalesce(tgs.goal_label, tgs.source_header) end) as goal_4,
  max(case when tgs.goal_slot = 5 then coalesce(tgs.goal_label, tgs.source_header) end) as goal_5,
  max(case when tgs.goal_slot = 6 then coalesce(tgs.goal_label, tgs.source_header) end) as goal_6,
  max(case when tgs.goal_slot = 7 then coalesce(tgs.goal_label, tgs.source_header) end) as goal_7,
  max(case when tgs.goal_slot = 8 then coalesce(tgs.goal_label, tgs.source_header) end) as goal_8,
  max(case when tgs.goal_slot = 9 then coalesce(tgs.goal_label, tgs.source_header) end) as goal_9,
  max(case when tgs.goal_slot = 10 then coalesce(tgs.goal_label, tgs.source_header) end) as goal_10,
  max(case when tgs.goal_slot = 11 then coalesce(tgs.goal_label, tgs.source_header) end) as goal_11,
  max(case when tgs.goal_slot = 12 then coalesce(tgs.goal_label, tgs.source_header) end) as goal_12,
  max(case when tgs.goal_slot = 13 then coalesce(tgs.goal_label, tgs.source_header) end) as goal_13,
  max(case when tgs.goal_slot = 14 then coalesce(tgs.goal_label, tgs.source_header) end) as goal_14,
  max(case when tgs.goal_slot = 15 then coalesce(tgs.goal_label, tgs.source_header) end) as goal_15,
  max(case when tgs.goal_slot = 16 then coalesce(tgs.goal_label, tgs.source_header) end) as goal_16,
  max(case when tgs.goal_slot = 17 then coalesce(tgs.goal_label, tgs.source_header) end) as goal_17,
  max(case when tgs.goal_slot = 18 then coalesce(tgs.goal_label, tgs.source_header) end) as goal_18,
  max(case when tgs.goal_slot = 19 then coalesce(tgs.goal_label, tgs.source_header) end) as goal_19,
  max(case when tgs.goal_slot = 20 then coalesce(tgs.goal_label, tgs.source_header) end) as goal_20,
  max(case when tgs.goal_slot = 21 then coalesce(tgs.goal_label, tgs.source_header) end) as goal_21,
  max(case when tgs.goal_slot = 22 then coalesce(tgs.goal_label, tgs.source_header) end) as goal_22,
  max(case when tgs.goal_slot = 23 then coalesce(tgs.goal_label, tgs.source_header) end) as goal_23,
  max(case when tgs.goal_slot = 24 then coalesce(tgs.goal_label, tgs.source_header) end) as goal_24,
  max(case when tgs.goal_slot = 25 then coalesce(tgs.goal_label, tgs.source_header) end) as goal_25
from topics
left join topic_goal_slots tgs
  on tgs.topic = topics.topic
group by topics.topic;
