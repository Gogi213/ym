# Apps Script Direct Turso Ingest Design

**Goal:** Replace the current `Apps Script -> local HTTP ingest service` ingress with `Apps Script -> Turso raw tables`, while keeping local Python as the manual post-ingest processor that reads from Turso and updates Google Sheets.

## Scope

In scope:

- direct raw writes from Apps Script into Turso/libSQL;
- removing local HTTP ingest from the primary path;
- preserving the existing raw schema:
  - `ingest_files`
  - `ingest_file_payloads`
  - `pipeline_runs`
- moving CSV/XLSX parsing from ingest time to local Python post-processing time;
- keeping `run_pipeline.py` as the manual command the user runs after Apps Script.

Out of scope for this cut:

- scheduler / watcher / Task Scheduler automation;
- background workers;
- public HTTP bridge or tunnel;
- changing Sheets sync semantics.

## Target Architecture

The new contour is:

- `Apps Script`
- `Turso raw tables`
- `local Python normalize pipeline`
- `Google Sheets sync`

`Apps Script` becomes responsible only for:

- finding candidate Gmail attachments;
- resetting the target `run_date` in Turso;
- writing one raw file record plus raw payload per attachment.

Local Python becomes responsible for:

- reading raw file metadata and raw payloads from Turso;
- parsing CSV/XLSX payloads into header + rows;
- materializing `ingest_rows`;
- refreshing `pipeline_runs`;
- running the existing normalize + Sheets sync flow.

## Data Contract

Apps Script must keep writing enough metadata for local Python to behave exactly like the current ingest service.

### `pipeline_runs`

For `reset`:

- delete prior raw rows for `run_date`;
- increment `raw_revision`;
- set `normalize_status = 'pending_normalize'`;
- zero out `raw_files`, `raw_rows`, `normalized_files`, `normalized_rows`;
- clear `last_error`;
- update timestamps.

### `ingest_files`

Apps Script writes one row per accepted attachment with:

- generated `id`;
- `run_date`;
- `message_id`;
- `thread_id`;
- `message_date`;
- `message_subject`;
- `primary_topic`;
- `matched_topic`;
- `topic_role`;
- `attachment_name`;
- `attachment_type`;
- `status = 'raw_only'`;
- `header_json = '[]'`;
- `row_count = 0`;
- current `raw_revision`;
- `error_text = null`.

### `ingest_file_payloads`

Apps Script writes one row per file payload with:

- `file_id`;
- `content_type`;
- `file_size_bytes`;
- `file_base64`.

## Schema Change

Current schema only allows `ingest_files.status in ('ingested', 'skipped', 'error')`.

This cut introduces a new raw-only lifecycle state:

- `raw_only`

Reason:

- after direct write from Apps Script the file exists in Turso, but header/rows are not parsed yet;
- `pending_normalize` on `pipeline_runs` must no longer imply `ingest_files.status = 'ingested'`.

## Processing Flow

### Apps Script `run()`

For each `run_date`:

1. reset raw state in Turso;
2. collect matching latest messages;
3. for each CSV/XLSX attachment:
   - build metadata;
   - base64-encode file bytes;
   - insert `ingest_files`;
   - insert `ingest_file_payloads`;
4. refresh `pipeline_runs.raw_files`;
5. finish without touching local Python.

### Local Python `run_pipeline.py`

Before normalization for each selected `run_date`:

1. load files with statuses `raw_only` and `ingested`;
2. for any `raw_only` file:
   - decode `file_base64`;
   - parse attachment using the existing parser;
   - update `ingest_files.status`, `header_json`, `row_count`, `error_text`;
   - replace `ingest_rows`;
3. recompute `pipeline_runs.raw_files` / `raw_rows` and normalize status;
4. continue through existing normalize/finalize/sync steps.

This keeps the existing normalized model intact and only moves the parsing boundary.

## Access Model

Apps Script will talk to Turso through HTTP with a dedicated auth token stored in Script Properties.

New script properties:

- `TURSO_DATABASE_URL`
- `TURSO_AUTH_TOKEN`

Existing ingest-service properties become non-primary for the new path:

- `INGEST_BASE_URL`
- `INGEST_STATUS_URL`
- `INGEST_TOKEN`

## Risks

### Payload size

Apps Script must base64-encode full attachments. This increases payload size in memory and over HTTP. The first cut should optimize for correctness and explicit errors, not for large-file throughput.

### SQL over HTTP from Apps Script

Apps Script has no Turso client library. The direct path needs carefully shaped SQL requests and predictable error handling.

### Status transition drift

Any code that assumes only `ingested/skipped/error` on `ingest_files` must be updated for `raw_only`.

## Success Criteria

This cut is successful when:

- Apps Script can populate Turso raw tables without local HTTP ingest;
- `run_pipeline.py` can parse `raw_only` payloads out of Turso and continue normalization;
- the user only needs to run Apps Script and then manually run local Python when they want post-processing;
- `cloudflared` and local ingest service are no longer required for the main path.
