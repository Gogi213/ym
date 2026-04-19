# Turso Runtime V2 Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the current row-explosion Turso normalize runtime with a direct raw-to-`operator_export_rows` path that preserves business semantics and cuts remote writes dramatically.

**Architecture:** Keep raw ingest, pipeline state, and goal-slot mapping in Turso. Move normalization, merge logic, current-row resolution, and aggregation fully into local Python memory, then persist only compact publish-ready rows back into `operator_export_rows`.

**Tech Stack:** Python 3.11, Turso SQL-over-HTTP runtime, Google Sheets sync scripts, unittest

---

## Chunk 1: New Direct Publish Builder

### Task 1: Add failing tests for direct raw-to-operator-export build

**Files:**
- Create: `tests/test_normalize_operator_export_runtime.py`
- Reference: `scripts/normalize/transform.py`
- Reference: `scripts/normalize/turso_operator_export.py`

- [ ] **Step 1: Write the failing tests**

Cover:
- raw rows are parsed and aggregated directly into publish grain;
- `goal_1 ... goal_N` mapping is preserved;
- secondary-topic merge keeps current `union` semantics;
- current-version resolution still prefers latest message/file version.

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest discover -s tests -p "test_normalize_operator_export_runtime.py" -v`
Expected: FAIL because runtime module does not exist yet.

- [ ] **Step 3: Write minimal implementation**

Create a new focused module, for example:
- `scripts/normalize/operator_export_runtime.py`

Responsibilities:
- build publish-ready rows directly from raw files/rows/payloads;
- reuse existing parsing and business transform helpers where possible;
- no DB writes in this module.

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m unittest discover -s tests -p "test_normalize_operator_export_runtime.py" -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add tests/test_normalize_operator_export_runtime.py scripts/normalize/operator_export_runtime.py
git commit -m "feat: add direct operator export runtime builder"
```

## Chunk 2: Direct Turso Write Path

### Task 2: Add write helpers for replacing `operator_export_rows` per run date

**Files:**
- Modify: `scripts/normalize/turso_operator_export.py`
- Test: `tests/test_normalize_turso_write_path.py`

- [ ] **Step 1: Write the failing test**

Add tests for:
- delete existing `operator_export_rows` for one `run_date`;
- insert direct publish rows in chunks;
- preserve current sheet-facing schema.

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest discover -s tests -p "test_normalize_turso_write_path.py" -v`
Expected: FAIL on missing direct replace helper.

- [ ] **Step 3: Write minimal implementation**

Add focused helper(s):
- `replace_operator_export_rows_for_run(...)`
- optional chunked insert progress callback

Do not touch Sheets sync yet.

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m unittest discover -s tests -p "test_normalize_turso_write_path.py" -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add scripts/normalize/turso_operator_export.py tests/test_normalize_turso_write_path.py
git commit -m "feat: add direct operator export replace path"
```

## Chunk 3: Switch Pipeline Runtime

### Task 3: Make `normalize_run()` use direct publish runtime instead of `fact_*`

**Files:**
- Modify: `scripts/normalize/pipeline.py`
- Modify: `scripts/normalize/db.py`
- Test: `tests/test_run_pipeline.py`
- Test: `tests/test_normalize_turso_layout.py`

- [ ] **Step 1: Write the failing test**

Add tests proving:
- `normalize_run()` no longer writes `fact_rows/fact_dimensions/fact_metrics`;
- it writes direct publish rows to `operator_export_rows`;
- `pipeline_runs` still reaches `ready` on success;
- status and progress logs still work.

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest discover -s tests -p "test_run_pipeline.py" -v`
Expected: FAIL because pipeline still uses legacy `fact_*` path.

- [ ] **Step 3: Write minimal implementation**

In `normalize_run()`:
- keep raw preparation and goal slot updates;
- replace `build_normalized_payloads -> insert_fact_* -> refresh flags -> refresh operator export`
  with:
  - direct build of publish rows
  - replace `operator_export_rows` for `run_date`
  - mark pipeline ready

Keep progress logs, but re-target them to the new phases.

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m unittest discover -s tests -p "test_run_pipeline.py" -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add scripts/normalize/pipeline.py scripts/normalize/db.py tests/test_run_pipeline.py tests/test_normalize_turso_layout.py
git commit -m "feat: switch pipeline to direct operator export runtime"
```

## Chunk 4: Live-Oriented Runtime Cleanup

### Task 4: Remove legacy `fact_*` hot path from operator runtime

**Files:**
- Modify: `scripts/normalize_one_run.py`
- Modify: `README.md`
- Modify: `docs/2026-04-07-ym-mail-ingest-technical.md`
- Modify: `docs/2026-04-17-chat-transition.md`

- [ ] **Step 1: Write the failing doc/runtime assertions**

Add/update tests so code layout docs no longer describe `fact_*` as the active hot path.

- [ ] **Step 2: Run test to verify it fails**

Run relevant layout/doc tests.

- [ ] **Step 3: Write minimal implementation**

Update docs and helper exports:
- `operator_export_rows` is the durable normalized output;
- `fact_*` is legacy/compat only until removal.

- [ ] **Step 4: Run tests to verify they pass**

Run:
`python -m unittest discover -s tests -p "test_*.py" -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add scripts/normalize_one_run.py README.md docs/2026-04-07-ym-mail-ingest-technical.md docs/2026-04-17-chat-transition.md
git commit -m "docs: mark direct operator export runtime as primary path"
```

## Chunk 5: Controlled Live Validation

### Task 5: Validate one real run date, then the remaining dirty days

**Files:**
- No new files required
- Use: `scripts/doctor_direct_turso.py`
- Use: `scripts/run_pipeline.py`

- [ ] **Step 1: Verify no stale `run_pipeline.py` processes exist**

Run a process check before any live validation.

- [ ] **Step 2: Run one controlled day**

Run:
`python scripts/run_pipeline.py --service-account-json key/stalwart-bounty-355816-994d612c3122.json --run-date 2026-04-17`

Expected:
- no parallel-run lock violation;
- no `UNIQUE(source_file_id, source_row_index)` path because legacy `fact_*` write path is gone;
- `pipeline_runs.normalize_status = ready`.

- [ ] **Step 3: Inspect live Turso**

Run doctor and confirm:
- `2026-04-17` is `ready`;
- `operator_export_rows` populated;
- no unexpected `normalize_error`.

- [ ] **Step 4: Run remaining dirty days**

Run:
`python scripts/run_pipeline.py --service-account-json key/stalwart-bounty-355816-994d612c3122.json`

- [ ] **Step 5: Record before/after timing**

Capture:
- old baseline;
- new one-day timing;
- month timing estimate.

- [ ] **Step 6: Commit**

```bash
git add .
git commit -m "chore: validate direct operator export runtime live"
```
