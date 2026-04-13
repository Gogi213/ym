# Incremental Run-State Pipeline Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace rebuild-by-inference with an explicit run-state model so Python only normalizes dirty `run_date` values while keeping current Apps Script intake and operator sheet exports.

**Architecture:** Add a `pipeline_runs` table that becomes the source of truth for per-day processing state. Make ingest mark only the touched day dirty, make the normalizer update readiness for only that day, and make the orchestrator drive normalization from `pipeline_runs` instead of inferring state from the current normalized tables.

**Tech Stack:** Apps Script, Supabase Edge Function, Postgres, Python, Google Sheets

---

## Chunk 1: Run-State Schema

### Task 1: Add pipeline run-state table

**Files:**
- Create: `supabase/migrations/20260413xxxxxx_add_pipeline_runs.sql`
- Modify: `docs/2026-04-07-ym-mail-ingest-technical.md`

- [ ] Add `public.pipeline_runs` with `run_date`, `raw_revision`, `normalize_status`, counters, timestamps, and `last_error`
- [ ] Add check constraint for allowed statuses
- [ ] Add indexes for `normalize_status` and `normalized_at`
- [ ] Document the new state model in the technical doc

### Task 2: Seed run-state rows from existing raw data

**Files:**
- Modify: `supabase/migrations/20260413xxxxxx_add_pipeline_runs.sql`

- [ ] Backfill one `pipeline_runs` row per existing `run_date` in `public.ingest_files`
- [ ] Initialize `raw_files`, `raw_rows`, and `normalize_status`
- [ ] Ensure empty projects still migrate cleanly

## Chunk 2: Ingest Dirty-Marking

### Task 3: Make ingest function update run-state

**Files:**
- Modify: `supabase/functions/mail-ingest/index.ts`
- Test: `tests/ym_mail_ingest.test.js` if any contract shape changes in Apps Script helpers

- [ ] After `reset`, upsert `pipeline_runs` for that `run_date`
- [ ] After successful ingest inserts, update `raw_files`, `raw_rows`, `last_ingest_at`
- [ ] Increment `raw_revision` when a day is re-ingested
- [ ] Set `normalize_status = 'pending_normalize'`

### Task 4: Keep ingest idempotent per day

**Files:**
- Modify: `supabase/functions/mail-ingest/index.ts`

- [ ] Make sure repeated ingest of the same `run_date` only dirties that date
- [ ] Do not affect unrelated `run_date` rows in `pipeline_runs`

## Chunk 3: Incremental Normalizer

### Task 5: Replace inferred readiness with explicit run-state writes

**Files:**
- Modify: `scripts/normalize_supabase.py`
- Test: `tests/test_normalize_supabase.py`

- [ ] Add helpers to read and update `pipeline_runs`
- [ ] On successful normalize, update `normalized_files`, `normalized_rows`, `normalized_at`, `normalize_status = 'ready'`
- [ ] On failure, set `normalize_status = 'normalize_error'` and store `last_error`

### Task 6: Keep normalization day-scoped and selective

**Files:**
- Modify: `scripts/normalize_supabase.py`
- Test: `tests/test_normalize_supabase.py`

- [ ] Ensure only rows sourced from the target `run_date` are replaced in `fact_*`
- [ ] Keep the current exact-grain secondary merge behavior intact
- [ ] Add tests for state updates around success and failure

## Chunk 4: Orchestrator Rewrite

### Task 7: Make orchestrator select dirty days from pipeline state

**Files:**
- Modify: `scripts/run_pipeline.py`
- Modify: `scripts/sync_pipeline_status_sheet.py`
- Test: `tests/test_run_pipeline.py`
- Test: `tests/test_sync_pipeline_status_sheet.py`

- [ ] Replace current pending-date inference with reads from `public.pipeline_runs`
- [ ] Select only `pending_normalize` and optionally `normalize_error`
- [ ] Keep phase logs and explicit timings
- [ ] Update `pipeline_status` sheet to use `pipeline_runs` as the canonical source

### Task 8: Preserve current sheet behavior while removing unnecessary rebuild logic

**Files:**
- Modify: `scripts/run_pipeline.py`

- [ ] Keep full rewrite of operator sheets after a successful incremental batch
- [ ] Skip heavy sheet sync only when there were no normalized dirty dates
- [ ] Remove leftover readiness heuristics that depend on comparing `raw_rows` vs `normalized_rows` indirectly

## Chunk 5: Verification and Handoff

### Task 9: Verify incremental behavior end-to-end

**Files:**
- Modify: `README.md`
- Add or modify: `docs/2026-04-13-review-notes.md` if rollout notes need updating

- [ ] Verify fresh ingest marks only one `run_date` dirty
- [ ] Verify `run_pipeline.py` normalizes only that `run_date`
- [ ] Verify repeat ingest of the same day only reprocesses that day
- [ ] Verify operator sheets still refresh correctly
- [ ] Commit and push
