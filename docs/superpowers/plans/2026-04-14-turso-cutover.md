# Turso Cutover Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the current Supabase/Postgres runtime with a Python ingest service and Turso/libSQL backend while preserving the external Apps Script and Google Sheets operating flow.

**Architecture:** Keep `Apps Script -> HTTP ingest -> Python normalize -> Sheets` as the external contract, but replace the backend under it. First establish Turso schema and connectivity, then move ingest, then move normalization and operator sync, then cut Apps Script over to the new endpoint.

**Tech Stack:** Python, FastAPI, Turso/libSQL, Google Sheets API, Apps Script

---

## Chunk 1: Turso Foundation

### Task 1: Add Turso runtime configuration module

**Files:**
- Create: `scripts/turso_runtime.py`
- Test: `tests/test_turso_runtime.py`

- [ ] **Step 1: Write failing tests for Turso env loading**

- [ ] **Step 2: Implement minimal config loader**

Support:
- `TURSO_DATABASE_URL`
- `TURSO_AUTH_TOKEN`

- [ ] **Step 3: Add helper for constructing libSQL client/connection**

- [ ] **Step 4: Run tests**

- [ ] **Step 5: Commit**

### Task 2: Add Turso schema bootstrap helpers

**Files:**
- Modify: `turso/bootstrap_schema.sql`
- Create: `scripts/bootstrap_turso.py`
- Test: `tests/test_bootstrap_turso.py`

- [ ] **Step 1: Write failing tests for bootstrap file presence and loader**

- [ ] **Step 2: Implement Python bootstrap runner for Turso**

- [ ] **Step 3: Verify bootstrap against `ym-migration-20260414`**

- [ ] **Step 4: Commit**

## Chunk 2: Python Ingest Service

### Task 3: Scaffold ASGI ingest service

**Files:**
- Create: `ingest_service/app.py`
- Create: `ingest_service/auth.py`
- Create: `ingest_service/models.py`
- Test: `tests/test_ingest_service_app.py`

- [ ] **Step 1: Write failing tests for `/health`, `/reset`, `/ingest` routes**

- [ ] **Step 2: Implement thin FastAPI app skeleton**

- [ ] **Step 3: Implement ingest-token auth**

- [ ] **Step 4: Run tests**

- [ ] **Step 5: Commit**

### Task 4: Port file parsing into Python ingest service

**Files:**
- Create: `ingest_service/parse.py`
- Create: `ingest_service/types.py`
- Test: `tests/test_ingest_service_parse.py`

- [ ] **Step 1: Write failing tests for CSV/XLSX table detection**

- [ ] **Step 2: Port minimal parsing logic from current ingest boundary**

- [ ] **Step 3: Run tests**

- [ ] **Step 4: Commit**

### Task 5: Write Turso raw storage adapter for ingest

**Files:**
- Create: `ingest_service/storage.py`
- Test: `tests/test_ingest_service_storage.py`

- [ ] **Step 1: Write failing tests for raw writes**

- [ ] **Step 2: Implement writes for**
  - `ingest_files`
  - `ingest_rows`
  - `ingest_file_payloads`
  - `pipeline_runs`

- [ ] **Step 3: Run tests**

- [ ] **Step 4: Commit**

## Chunk 3: Normalizer Cutover

### Task 6: Introduce Turso DB access layer for normalizer

**Files:**
- Create: `scripts/normalize/turso_connection.py`
- Create: `scripts/normalize/turso_reads.py`
- Create: `scripts/normalize/turso_writes.py`
- Create: `scripts/normalize/turso_operator.py`
- Test: `tests/test_normalize_turso_layout.py`

- [ ] **Step 1: Write failing tests for Turso backend module layout**

- [ ] **Step 2: Implement connection and read/write stubs**

- [ ] **Step 3: Commit**

### Task 7: Switch normalizer pipeline to storage abstraction

**Files:**
- Modify: `scripts/normalize/pipeline.py`
- Modify: `scripts/normalize/db.py`
- Modify: `scripts/normalize_supabase.py`
- Test: `tests/test_normalize_supabase.py`

- [ ] **Step 1: Add failing tests for backend-selectable pipeline**

- [ ] **Step 2: Introduce backend selection without changing math**

- [ ] **Step 3: Run normalization tests**

- [ ] **Step 4: Commit**

### Task 8: Implement Turso-side current/export refresh

**Files:**
- Modify: `scripts/normalize/turso_operator.py`
- Test: `tests/test_normalize_turso_operator.py`

- [ ] **Step 1: Write failing tests for current-row refresh and operator export refresh**

- [ ] **Step 2: Implement libSQL-compatible refresh path**

- [ ] **Step 3: Validate against migration DB**

- [ ] **Step 4: Commit**

## Chunk 4: Sheets and Orchestrator Cutover

### Task 9: Move sync scripts to backend-selectable storage

**Files:**
- Modify: `scripts/sync_goal_mapping_sheet.py`
- Modify: `scripts/sync_export_rows_wide_sheet.py`
- Modify: `scripts/sync_pipeline_status_sheet.py`
- Test: `tests/test_sync_goal_mapping_sheet.py`
- Test: `tests/test_sync_export_rows_wide_sheet.py`
- Test: `tests/test_sync_pipeline_status_sheet.py`

- [ ] **Step 1: Write failing tests for Turso-backed sync reads**

- [ ] **Step 2: Implement storage backend selection**

- [ ] **Step 3: Run tests**

- [ ] **Step 4: Commit**

### Task 10: Move orchestrator to Turso

**Files:**
- Modify: `scripts/run_pipeline.py`
- Test: `tests/test_run_pipeline.py`

- [ ] **Step 1: Write failing tests for Turso-backed pending-run selection**

- [ ] **Step 2: Switch orchestrator to Turso backend**

- [ ] **Step 3: Run tests**

- [ ] **Step 4: Commit**

## Chunk 5: Cutover and Validation

### Task 11: Add deployment/runbook docs for Turso path

**Files:**
- Modify: `README.md`
- Create: `docs/2026-04-14-ym-turso-deploy.md`

- [ ] **Step 1: Document Turso runtime config**

- [ ] **Step 2: Document ingest service startup**

- [ ] **Step 3: Document Apps Script cutover values**

- [ ] **Step 4: Commit**

### Task 12: End-to-end validation on Turso

**Files:**
- Modify: `README.md`
- Modify: `docs/2026-04-07-ym-mail-ingest-technical.md`

- [ ] **Step 1: Run fresh ingest against new service**

- [ ] **Step 2: Run `run_pipeline.py` on Turso**

- [ ] **Step 3: Validate `raw -> wide -> union` for visits and goals**

- [ ] **Step 4: Document results**

- [ ] **Step 5: Commit**

Plan complete and saved to `docs/superpowers/plans/2026-04-14-turso-cutover.md`. Ready to execute?
