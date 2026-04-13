alter table public.ingest_files
  add column if not exists primary_topic text,
  add column if not exists topic_role text;

update public.ingest_files
set
  primary_topic = coalesce(primary_topic, matched_topic),
  topic_role = coalesce(topic_role, 'primary');

alter table public.ingest_files
  alter column primary_topic set not null,
  alter column topic_role set not null;

alter table public.ingest_files
  drop constraint if exists ingest_files_topic_role_check;

alter table public.ingest_files
  add constraint ingest_files_topic_role_check
  check (topic_role in ('primary', 'secondary'));

create index if not exists ingest_files_primary_topic_idx
  on public.ingest_files (primary_topic);

create index if not exists ingest_files_topic_role_idx
  on public.ingest_files (topic_role);
