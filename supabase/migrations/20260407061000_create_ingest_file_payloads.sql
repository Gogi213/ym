create table if not exists public.ingest_file_payloads (
  file_id uuid primary key references public.ingest_files(id) on delete cascade,
  content_type text,
  file_size_bytes integer not null,
  file_base64 text not null,
  created_at timestamptz not null default timezone('utc', now())
);

create index if not exists ingest_file_payloads_created_at_idx
  on public.ingest_file_payloads (created_at);
