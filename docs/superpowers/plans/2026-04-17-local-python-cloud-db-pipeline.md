# Local Python + Cloud DB Pipeline Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Lock the repo to the supported operating model where Apps Script handles ingest, the cloud DB remains the source of truth, and local Python handles normalize + sheet sync without hidden hosted-runtime assumptions.

**Architecture:** Keep the existing raw/normalized/state tables and local Python tooling, but finish the cleanup from abandoned runtime experiments, harden `pipeline_runs` as the execution truth for `run_date`, and make the documentation and entrypoints reflect the supported workflow only.

**Tech Stack:** Apps Script, Python, Turso/libSQL, Node test runner, Python `unittest`, Google Sheets sync scripts.

---

## File Structure

### Documentation and repository boundaries
- Modify: `README.md`
- Modify: `docs/2026-04-07-ym-mail-ingest-technical.md`
- Modify: `docs/2026-04-13-deep-review.md`
- Modify: `docs/2026-04-13-review-notes.md`
- Create: `docs/2026-04-17-local-python-runbook.md`

### Pipeline state and local runner semantics
- Modify: `scripts/run_pipeline.py`
- Modify: `scripts/sync_pipeline_status_sheet.py`
- Modify: `ingest_service/storage.py`
- Modify: `tests/test_run_pipeline.py`
- Modify: `tests/test_sync_pipeline_status_sheet.py`
- Modify: `tests/test_ingest_service_storage.py`

### Apps Script expectations
- Modify: `appsscript-src/20_transport_and_runtime.js`
- Modify: `appsscript-src/30_entrypoints.js`
- Modify: `tests/ym_mail_ingest.test.js`
- Modify: `Code.js`

## Chunk 1: Lock Documentation to the Supported Workflow

### Task 1: Add explicit local runbook

**Files:**
- Create: `docs/2026-04-17-local-python-runbook.md`

- [ ] **Step 1: Write the runbook**

Include only the supported flow:
- start local ingest service
- Apps Script sends raw ingest
- run local Python pipeline
- verify `pipeline_status`

- [ ] **Step 2: Review for forbidden hosted-runtime references**

Run: `rg -n "Render|Northflank|Cloudflare|Docker|Workers|onrender|workers.dev" docs/2026-04-17-local-python-runbook.md`
Expected: no matches.

- [ ] **Step 3: Commit**

```bash
git add docs/2026-04-17-local-python-runbook.md
git commit -m "docs: add local python runbook"
```

### Task 2: Align top-level docs with the supported contour

**Files:**
- Modify: `README.md`
- Modify: `docs/2026-04-07-ym-mail-ingest-technical.md`
- Modify: `docs/2026-04-13-deep-review.md`
- Modify: `docs/2026-04-13-review-notes.md`

- [ ] **Step 1: Remove or rewrite stale runtime assumptions**

Update docs so they only describe:
- Apps Script as thin collector
- cloud DB as source of truth
- local Python as post-processing runner
- `pipeline_runs` as execution truth

- [ ] **Step 2: Verify no stale hosted-runtime instructions remain in supported docs**

Run: `rg -n "Render|Northflank|Cloudflare|Dockerfile.ingest-service|docker-compose.ingest-service|workers.dev|onrender" README.md docs`
Expected: only legacy/archive references if intentionally preserved, otherwise no matches.

- [ ] **Step 3: Commit**

```bash
git add README.md docs/2026-04-07-ym-mail-ingest-technical.md docs/2026-04-13-deep-review.md docs/2026-04-13-review-notes.md
git commit -m "docs: align repo with local python cloud db workflow"
```

## Chunk 2: Harden `pipeline_runs` as the Execution Truth

### Task 3: Make `pipeline_runs` the only readiness source for runner selection

**Files:**
- Modify: `scripts/run_pipeline.py`
- Test: `tests/test_run_pipeline.py`

- [ ] **Step 1: Write or tighten failing tests for pending selection**

Add/adjust tests to assert:
- only `pending_normalize` and `normalize_error` days are candidates
- `ready` days are not touched
- no implicit inference from raw tables is needed when `pipeline_runs` is present

- [ ] **Step 2: Run targeted tests to confirm the gap**

Run: `python -m unittest tests.test_run_pipeline -v`
Expected: fail if runner still infers readiness too loosely.

- [ ] **Step 3: Implement the minimal selection cleanup**

Ensure `run_pipeline.py`:
- treats `pipeline_runs` as source of truth
- keeps `run_date` as the unit of work
- does not widen scope beyond dirty dates

- [ ] **Step 4: Re-run targeted tests**

Run: `python -m unittest tests.test_run_pipeline -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add scripts/run_pipeline.py tests/test_run_pipeline.py
git commit -m "refactor: use pipeline runs as execution truth"
```

### Task 4: Tighten pipeline status sheet semantics

**Files:**
- Modify: `scripts/sync_pipeline_status_sheet.py`
- Test: `tests/test_sync_pipeline_status_sheet.py`

- [ ] **Step 1: Write or tighten failing tests for status classification**

Cover:
- `pending_normalize`
- `raw_only`
- `ready`
- `normalize_error`

- [ ] **Step 2: Run targeted tests to confirm the gap**

Run: `python -m unittest tests.test_sync_pipeline_status_sheet -v`
Expected: fail if any status classification is still ambiguous or under-specified.

- [ ] **Step 3: Implement minimal cleanup**

Make sure operator-facing pipeline status is derived directly from `pipeline_runs`, not inferred from secondary signals.

- [ ] **Step 4: Re-run targeted tests**

Run: `python -m unittest tests.test_sync_pipeline_status_sheet -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add scripts/sync_pipeline_status_sheet.py tests/test_sync_pipeline_status_sheet.py
git commit -m "refactor: make pipeline status sheet follow pipeline runs"
```

## Chunk 3: Preserve Raw Ingest Semantics Without Runtime Drift

### Task 5: Document and test raw retention/status assumptions in storage helpers

**Files:**
- Modify: `ingest_service/storage.py`
- Modify: `tests/test_ingest_service_storage.py`

- [ ] **Step 1: Add failing or clarifying tests around raw file lifecycle**

Cover:
- `reset` clears a target `run_date`
- raw payload remains DB-backed
- `fetch_pipeline_run_status()` reflects the supported local workflow only

- [ ] **Step 2: Run targeted storage tests**

Run: `python -m unittest tests.test_ingest_service_storage -v`
Expected: fail if status or reset semantics are inconsistent.

- [ ] **Step 3: Make minimal storage cleanup changes**

Keep storage semantics aligned with the supported contour:
- raw payload remains in DB
- no hidden references to removed runtime experiments
- status payload remains stable for Apps Script and runner consumers

- [ ] **Step 4: Re-run targeted tests**

Run: `python -m unittest tests.test_ingest_service_storage -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add ingest_service/storage.py tests/test_ingest_service_storage.py
git commit -m "refactor: align storage helpers with local workflow"
```

## Chunk 4: Freeze Apps Script to Generic Ingest Expectations

### Task 6: Remove stale runtime assumptions from Apps Script transport docs/tests

**Files:**
- Modify: `appsscript-src/20_transport_and_runtime.js`
- Modify: `appsscript-src/30_entrypoints.js`
- Modify: `tests/ym_mail_ingest.test.js`
- Modify: `Code.js`

- [ ] **Step 1: Write or adjust failing tests for generic endpoint behavior**

Cover:
- `INGEST_BASE_URL`
- optional `INGEST_STATUS_URL`
- no references to a specific hosting provider in test fixtures or messages

- [ ] **Step 2: Run JS tests to confirm stale assumptions**

Run: `node --test tests/ym_mail_ingest.test.js`
Expected: fail if provider-specific assumptions remain.

- [ ] **Step 3: Make the minimal cleanup**

Ensure transport remains generic and only encodes the contract:
- `POST /reset`
- `POST /ingest`
- `GET /pipeline-runs/{run_date}`

Rebuild bundle:
- `python scripts/build_appsscript_bundle.py`

- [ ] **Step 4: Re-run JS verification**

Run:
- `node --test tests/ym_mail_ingest.test.js`
- `node --check Code.js`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add appsscript-src/20_transport_and_runtime.js appsscript-src/30_entrypoints.js tests/ym_mail_ingest.test.js Code.js
git commit -m "cleanup: freeze apps script to generic ingest contract"
```

## Chunk 5: Full Verification and Handoff

### Task 7: Run full verification on the supported contour

**Files:**
- No code changes unless verification exposes a real defect

- [ ] **Step 1: Run full Python suite**

Run: `python -m unittest discover -s tests -p "test_*.py" -v`
Expected: PASS.

- [ ] **Step 2: Run JS suite and bundle syntax check**

Run:
- `node --test tests/ym_mail_ingest.test.js`
- `node --check Code.js`
Expected: PASS.

- [ ] **Step 3: Check repo for dead hosting references one final time**

Run: `rg -n "Render|Northflank|Cloudflare|Dockerfile.ingest-service|docker-compose.ingest-service|workers.dev|onrender" README.md docs tests package.json`
Expected: no supported-path references remain.

- [ ] **Step 4: Commit any final fixes if needed**

```bash
git add .
git commit -m "chore: finalize local python cloud db pipeline cleanup"
```
