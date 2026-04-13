# Incremental Run-State Pipeline Design

## Goal

Replace the current rebuild-oriented Python post-processing with a stateful incremental pipeline that only re-normalizes `dirty` `run_date` values while preserving the existing Gmail intake, Supabase raw layer, and operator-facing Google Sheets exports.

## Problem

The current bottleneck is not Apps Script. It is the Python post-processing model:

- pending days are normalized one by one
- normalized tables are rebuilt for every selected day
- operator sheets are then refreshed

This is acceptable for small batches, but it scales poorly when a month-sized raw backlog is present or when exploratory work repeatedly forces rebuilds.

The core issue is architectural:

- the system infers pipeline state indirectly
- it does not maintain an explicit run-state registry
- reprocessing decisions are driven by table contents instead of a first-class state model

## Recommended Approach

Introduce an explicit `pipeline_runs` state table and make incremental processing the default behavior.

### Core Rule

- A `run_date` is processed only when it is marked `dirty`
- A repeated ingest for the same `run_date` marks only that day dirty
- All other already-ready days are left untouched
- Full rebuild becomes an explicit maintenance operation, not the default pipeline behavior

## Data Model

Add a new table: `public.pipeline_runs`

Suggested fields:

- `run_date date primary key`
- `raw_revision bigint not null default 0`
- `normalize_status text not null`
- `raw_files integer not null default 0`
- `raw_rows bigint not null default 0`
- `normalized_files integer not null default 0`
- `normalized_rows bigint not null default 0`
- `last_ingest_at timestamptz`
- `normalized_at timestamptz`
- `last_error text`

Recommended statuses:

- `raw_only`
- `pending_normalize`
- `ready`
- `normalize_error`

## State Transitions

### Ingest / Reset

When Apps Script triggers ingest for a `run_date`:

- existing raw rows for that date are cleared as they are now
- new raw files and rows are inserted
- `pipeline_runs` for that date is upserted with:
  - incremented `raw_revision`
  - refreshed `raw_files`
  - refreshed `raw_rows`
  - `normalize_status = 'pending_normalize'`
  - `last_ingest_at = now()`

### Normalize

When Python normalizes a `run_date`:

- only rows sourced from that `run_date` are replaced in `fact_rows`, `fact_dimensions`, `fact_metrics`
- after successful replacement:
  - `normalized_files`
  - `normalized_rows`
  - `normalized_at`
  - `normalize_status = 'ready'`
  are updated in `pipeline_runs`

### Failure

If normalization fails:

- only that `run_date` gets `normalize_status = 'normalize_error'`
- `last_error` is stored
- other ready dates are unaffected

## Runtime Shape

### Apps Script

No major conceptual change.

Apps Script continues to:

- read topics from spreadsheet
- search Gmail
- send attachments to Supabase

The only important system-level responsibility is:

- repeated ingest for one date must dirty only that date

### Edge Function

The ingest function becomes responsible for updating `pipeline_runs` after reset/ingest for a date.

### Python Normalizer

The normalizer remains day-scoped, but the orchestrator changes:

- it selects `run_date` from `pipeline_runs where normalize_status in ('pending_normalize', 'normalize_error')`
- it normalizes only those dates
- it does not try to infer state indirectly from `fact_*`

### Sheets Sync

Sheets may still be fully rewritten after normalization.

This is acceptable because the current operator sheets are not the main scaling limit.

## Why This Is the Recommended Production-Like Variant

This design fixes the actual scaling problem without replacing the whole system:

- no queues
- no broker
- no worker fleet
- no event bus
- no materialized incremental DAG framework

It adds just enough explicit state to make incremental processing correct and fast enough for the current workload.

## Non-Goals

- No migration to a fully asynchronous event-driven architecture in this change
- No replacement of Google Sheets as operator output in this change
- No change to the raw ingest model beyond adding run-state tracking
- No attempt to make full rebuilds fast by default

## Success Criteria

- A new ingest for one `run_date` dirties only that date
- Re-running the Python pipeline normalizes only dirty dates
- Already-ready dates are not rebuilt
- Operator sheets can still be refreshed as before
- Full rebuild remains possible, but only as an explicit maintenance path

## Risks

- State table correctness becomes critical; buggy state transitions will cause stale or skipped dates
- Existing operator scripts must stop inferring readiness indirectly from `fact_*`
- A one-time migration will be needed to seed `pipeline_runs` from current raw data

## Rollout

1. Add `pipeline_runs` schema
2. Update ingest function to maintain state rows
3. Update normalizer/orchestrator to read and write `pipeline_runs`
4. Keep current operator sheet model unchanged
5. Add a dedicated full-rebuild maintenance command only after incremental path is stable
