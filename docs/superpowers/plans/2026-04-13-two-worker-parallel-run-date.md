# Two-Worker Parallel Run-Date Implementation Plan

> Status: measured on live data on `2026-04-13` and intentionally not shipped. Sequential bootstrap fast path outperformed two-worker execution because Postgres contention on `fact_*` dominated the run.

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Speed up cold rebuilds by processing dirty `run_date` values in parallel with two workers while keeping current day-scoped normalization semantics, current validation semantics, and current sheet contracts.

**Architecture:** Keep `run_date` as the execution unit, run up to two `normalize_run(run_date)` tasks concurrently, barrier on worker completion, and perform sheet sync only once after all successful workers complete. Preserve current `pipeline_runs` and operator cache semantics.

**Tech Stack:** Python, Postgres/Supabase, Google Sheets, existing Apps Script intake

---

## Chunk 1: Parallel Orchestrator Semantics

### Task 1: Add tests for two-worker scheduling

**Files:**
- Modify: `tests/test_run_pipeline.py`
- Modify: `scripts/run_pipeline.py`

- [ ] **Step 1: Write failing tests for scheduling behavior**

Cover:
- no duplicate processing of same `run_date`
- selected run dates remain deterministic
- worker count is capped at 2

- [ ] **Step 2: Run the targeted test file and verify failure**

Run:
`python -m unittest discover -s tests -p "test_run_pipeline.py" -v`

- [ ] **Step 3: Add scheduling helpers in `scripts/run_pipeline.py`**

Implement pure helpers for:
- worker batch sizing
- result aggregation
- failure detection

- [ ] **Step 4: Re-run tests and verify pass**

Run:
`python -m unittest discover -s tests -p "test_run_pipeline.py" -v`

- [ ] **Step 5: Commit**

```bash
git add tests/test_run_pipeline.py scripts/run_pipeline.py
git commit -m "test: cover parallel run-date scheduling"
```

## Chunk 2: Parallel Normalize Execution

### Task 2: Execute dirty days concurrently with two workers

**Files:**
- Modify: `scripts/run_pipeline.py`
- Test: `tests/test_run_pipeline.py`

- [ ] **Step 1: Write failing tests for concurrent execution result handling**

Cover:
- multiple normalize results are collected
- failures are surfaced without hiding successful results
- output shape remains machine-readable

- [ ] **Step 2: Implement two-worker execution**

Use a simple bounded executor:
- max workers = 2
- one future per dirty `run_date`
- collect structured per-day results

- [ ] **Step 3: Preserve progress logs**

Ensure logs still include:
- `pipeline_started`
- `normalize_started`
- `normalize_finished`
- `pipeline_finished`

- [ ] **Step 4: Re-run targeted tests**

Run:
`python -m unittest discover -s tests -p "test_run_pipeline.py" -v`

- [ ] **Step 5: Commit**

```bash
git add scripts/run_pipeline.py tests/test_run_pipeline.py
git commit -m "feat: run dirty days with two workers"
```

## Chunk 3: Failure Barrier and Sheet Sync Safety

### Task 3: Prevent full sheet sync on partial worker failure

**Files:**
- Modify: `scripts/run_pipeline.py`
- Modify: `tests/test_run_pipeline.py`
- Modify: `scripts/sync_pipeline_status_sheet.py`
- Modify: `tests/test_sync_pipeline_status_sheet.py`

- [ ] **Step 1: Write failing tests for partial failure behavior**

Cover:
- if one worker fails, `union` and `отчеты` are not synced
- `pipeline_status` still syncs
- failed `run_date` remains visible as error

- [ ] **Step 2: Implement failure barrier**

Behavior:
- full operator sync only if all workers succeeded
- status sheet sync always allowed

- [ ] **Step 3: Re-run targeted tests**

Run:
- `python -m unittest discover -s tests -p "test_run_pipeline.py" -v`
- `python -m unittest discover -s tests -p "test_sync_pipeline_status_sheet.py" -v`

- [ ] **Step 4: Commit**

```bash
git add scripts/run_pipeline.py scripts/sync_pipeline_status_sheet.py tests
git commit -m "feat: guard sheet sync behind full worker success"
```

## Chunk 4: Verify Normalize Isolation Assumptions

### Task 4: Confirm day-scoped normalize remains safe under two-worker execution

**Files:**
- Modify: `tests/test_normalize_supabase.py`
- Modify: `scripts/normalize_supabase.py` only if a real cross-day mutation issue is found

- [ ] **Step 1: Write failing tests only if isolation gaps are discovered**

Focus on:
- delete scope remains within one day
- affected row-key refresh remains local to changed day
- operator export refresh remains local to changed day

- [ ] **Step 2: Fix any discovered cross-day mutation**

Do not change business semantics. Only fix execution isolation if needed.

- [ ] **Step 3: Re-run normalizer tests**

Run:
`python -m unittest discover -s tests -p "test_normalize_supabase.py" -v`

- [ ] **Step 4: Commit**

```bash
git add scripts/normalize_supabase.py tests/test_normalize_supabase.py
git commit -m "fix: preserve day-scoped isolation for parallel normalize"
```

## Chunk 5: End-to-End Verification

### Task 5: Measure real cold-start performance and validate data consistency

**Files:**
- Modify: `README.md`
- Modify: `docs/2026-04-13-deep-review.md`
- Modify: `docs/2026-04-07-ym-mail-ingest-technical.md`

- [ ] **Step 1: Run the full automated test suite**

Run:
- `python -m unittest discover -s tests -p "test_*.py" -v`
- `node --test tests\\ym_mail_ingest.test.js`
- `node --check Code.js`

- [ ] **Step 2: Run a real cold rebuild after raw ingest**

Measure:
- full pipeline wall-clock
- per-day normalize durations
- no-op pipeline duration

- [ ] **Step 3: Re-validate data consistency**

Check again:
- `visits` end-to-end
- `goal_*` end-to-end
- `union` semantics unchanged

- [ ] **Step 4: Update docs with the new execution model and measured gains**

- [ ] **Step 5: Commit**

```bash
git add README.md docs/2026-04-07-ym-mail-ingest-technical.md docs/2026-04-13-deep-review.md
git commit -m "docs: record two-worker parallel rebuild design and rollout"
```
