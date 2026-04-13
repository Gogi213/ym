# File-Scoped Incremental Normalize Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace day-scoped normalization and operator-cache rebuilds with file-scoped incremental processing while preserving current raw ingest, secondary merge semantics, and Sheets contracts.

**Architecture:** Add a `normalized_files` state table, make the edge function dirty-mark individual files, make the normalizer rebuild facts only for `source_file_id`, and make operator cache refresh only affected aggregate keys. Keep `pipeline_runs` as day-level summary and keep full sheet rewrites.

**Tech Stack:** Supabase Edge Functions, Postgres, Python, Apps Script, Google Sheets

---

## Chunk 1: File-State Schema

### Task 1: Add file-level normalize state table

**Files:**
- Create: `supabase/migrations/20260413xxxxxx_add_normalized_files.sql`
- Modify: `docs/2026-04-07-ym-mail-ingest-technical.md`

- [ ] **Step 1: Write migration for `public.normalized_files`**

Include:
- `file_id`
- `run_date`
- `primary_topic`
- `matched_topic`
- `topic_role`
- `raw_revision`
- `normalize_status`
- `normalized_rows`
- `normalized_at`
- `last_error`
- timestamps

- [ ] **Step 2: Add constraints and indexes**

Add:
- status check constraint
- indexes for `normalize_status`, `run_date`, `primary_topic`

- [ ] **Step 3: Backfill from existing `ingest_files`**

Migration should seed `normalized_files` rows for existing raw files.

- [ ] **Step 4: Document the new state layer**

Update the technical doc with the new execution model.

- [ ] **Step 5: Commit**

```bash
git add supabase/migrations docs/2026-04-07-ym-mail-ingest-technical.md
git commit -m "feat: add normalized file state table"
```

## Chunk 2: Edge Function Dirty-Marking

### Task 2: Mark individual files dirty on ingest

**Files:**
- Modify: `supabase/functions/mail-ingest/index.ts`
- Test: `tests/ym_mail_ingest.test.js`

- [ ] **Step 1: Write failing tests for file-state writes**

Cover:
- fresh ingest creates `normalized_files` row
- repeated ingest re-dirties same file/day
- unrelated files remain untouched

- [ ] **Step 2: Run targeted JS tests to verify failure**

Run: `node --test tests\\ym_mail_ingest.test.js`

- [ ] **Step 3: Implement file-state upsert in edge function**

After successful file ingest:
- upsert `normalized_files`
- set `normalize_status = 'pending_normalize'`
- increment `raw_revision` on repeated ingest

- [ ] **Step 4: Keep `pipeline_runs` as day summary**

Do not remove existing `pipeline_runs` updates; keep them as run-date status.

- [ ] **Step 5: Re-run tests**

Run: `node --test tests\\ym_mail_ingest.test.js`

- [ ] **Step 6: Commit**

```bash
git add supabase/functions/mail-ingest/index.ts tests/ym_mail_ingest.test.js
git commit -m "feat: dirty mark normalized files on ingest"
```

## Chunk 3: File-Scoped Normalizer

### Task 3: Normalize by `file_id` instead of `run_date`

**Files:**
- Modify: `scripts/normalize_supabase.py`
- Test: `tests/test_normalize_supabase.py`

- [ ] **Step 1: Write failing Python tests for file-scoped rebuild**

Cover:
- only one `source_file_id` is deleted/rebuilt
- unrelated files in same day survive untouched
- `normalized_files` status flips to `ready`

- [ ] **Step 2: Run targeted Python tests to verify failure**

Run: `python -m unittest discover -s tests -p "test_normalize_supabase.py" -v`

- [ ] **Step 3: Add file-state read/write helpers**

Add helpers to:
- fetch dirty files
- mark file `ready`
- mark file `normalize_error`

- [ ] **Step 4: Replace day-scoped delete/rebuild with file-scoped delete/rebuild**

Delete and rebuild facts only for one `source_file_id`.

- [ ] **Step 5: Keep current row identity and exact-grain secondary merge intact**

Do not change merge keys or current validation semantics.

- [ ] **Step 6: Re-run tests**

Run: `python -m unittest discover -s tests -p "test_normalize_supabase.py" -v`

- [ ] **Step 7: Commit**

```bash
git add scripts/normalize_supabase.py tests/test_normalize_supabase.py
git commit -m "feat: normalize incrementally by file"
```

## Chunk 4: Affected Operator Keys

### Task 4: Refresh operator cache only for affected keys

**Files:**
- Modify: `scripts/normalize_supabase.py`
- Test: `tests/test_normalize_supabase.py`

- [ ] **Step 1: Write failing tests for affected operator key refresh**

Cover:
- only keys touched by changed file are rebuilt
- untouched keys remain unchanged
- `union` semantics stay the same

- [ ] **Step 2: Implement affected key collection**

Use:
- `topic`
- `report_date`
- `utm_source`
- `utm_medium`
- `utm_campaign`

- [ ] **Step 3: Replace run-date operator refresh with affected-key refresh**

Do not rebuild full day cache if a narrower set is sufficient.

- [ ] **Step 4: Re-run tests**

Run: `python -m unittest discover -s tests -p "test_normalize_supabase.py" -v`

- [ ] **Step 5: Commit**

```bash
git add scripts/normalize_supabase.py tests/test_normalize_supabase.py
git commit -m "feat: refresh operator cache by affected keys"
```

## Chunk 5: Orchestrator Rewrite

### Task 5: Drive pipeline from dirty files

**Files:**
- Modify: `scripts/run_pipeline.py`
- Modify: `scripts/sync_pipeline_status_sheet.py`
- Test: `tests/test_run_pipeline.py`
- Test: `tests/test_sync_pipeline_status_sheet.py`

- [ ] **Step 1: Write failing tests for file-driven orchestration**

Cover:
- no-op run when no dirty files
- process only dirty files
- preserve day-level summary output in `pipeline_status`

- [ ] **Step 2: Update orchestrator selection logic**

Read dirty work from `normalized_files`, not from day-level pending inference.

- [ ] **Step 3: Keep `pipeline_runs` as day summary**

`pipeline_status` sheet should still show day-level operational state.

- [ ] **Step 4: Re-run orchestrator tests**

Run:
- `python -m unittest discover -s tests -p "test_run_pipeline.py" -v`
- `python -m unittest discover -s tests -p "test_sync_pipeline_status_sheet.py" -v`

- [ ] **Step 5: Commit**

```bash
git add scripts/run_pipeline.py scripts/sync_pipeline_status_sheet.py tests
git commit -m "feat: drive pipeline from dirty files"
```

## Chunk 6: Verification and Rollout

### Task 6: Verify end-to-end behavior and document rollout

**Files:**
- Modify: `README.md`
- Modify: `docs/2026-04-13-deep-review.md`

- [ ] **Step 1: Run full automated test suite**

Run:
- `python -m unittest discover -s tests -p "test_*.py" -v`
- `node --test tests\\ym_mail_ingest.test.js`
- `node --check Code.js`

- [ ] **Step 2: Run end-to-end on a small controlled ingest**

Verify:
- one dirty file only rebuilds itself
- operator sheets still refresh correctly
- `visits` and `goal_*` remain consistent end-to-end

- [ ] **Step 3: Update docs with measured performance deltas**

Capture before/after timings for:
- repeated file ingest
- repeated day ingest
- no-op orchestrator run

- [ ] **Step 4: Commit**

```bash
git add README.md docs/2026-04-13-deep-review.md
git commit -m "docs: record file-scoped incremental pipeline rollout"
```
