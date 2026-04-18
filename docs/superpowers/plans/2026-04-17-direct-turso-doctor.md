# Direct Turso Doctor Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a read-only doctor/smoke script for the direct `Apps Script -> Turso -> local Python` contour and clean up CLI/docs so that this contour is clearly primary.

**Architecture:** Introduce a separate `scripts/doctor_direct_turso.py` that uses the shared Turso runtime and normalize query helpers to inspect connection health, `pipeline_runs`, raw payload presence, and raw-only payload parseability. Keep it read-only and leave `run_pipeline.py` focused on actual processing. Update CLI help/docs to mark old local HTTP ingest as fallback and `normalize_supabase.py` as a legacy facade.

**Tech Stack:** Python, libsql/Turso, unittest, existing normalize/query helpers

---

## Chunk 1: Doctor Script

### Task 1: Add failing tests for direct Turso doctor helpers

**Files:**
- Create: `tests/test_doctor_direct_turso.py`
- Reference: `scripts/normalize/query_utils.py`

- [ ] **Step 1: Write the failing tests**

Cover:
- connection smoke summary from a DB handle;
- latest `pipeline_runs` listing;
- run-date raw summary;
- raw-only payload parse validation.

- [ ] **Step 2: Run the focused test to verify it fails**

Run: `python -m unittest discover -s tests -p "test_doctor_direct_turso.py" -v`
Expected: FAIL because the script module does not exist.

- [ ] **Step 3: Commit**

```bash
git add tests/test_doctor_direct_turso.py
git commit -m "test: add direct Turso doctor coverage"
```

### Task 2: Implement read-only doctor script

**Files:**
- Create: `scripts/doctor_direct_turso.py`
- Modify: `package.json`
- Test: `tests/test_doctor_direct_turso.py`

- [ ] **Step 1: Implement minimal doctor helpers**

Add:
- connection smoke query (`select 1`);
- recent `pipeline_runs` snapshot;
- per-run-date raw summary;
- optional raw payload validation using `ingest_service.parse.parse_attachment` without DB writes.

- [ ] **Step 2: Add CLI entrypoint**

Support:
- `--run-date`
- `--limit`
- `--validate-payloads`

- [ ] **Step 3: Run the focused test to verify it passes**

Run: `python -m unittest discover -s tests -p "test_doctor_direct_turso.py" -v`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add scripts/doctor_direct_turso.py package.json tests/test_doctor_direct_turso.py
git commit -m "feat: add direct Turso doctor script"
```

## Chunk 2: Cleanup

### Task 3: Clarify CLI/help text around primary vs fallback contour

**Files:**
- Modify: `scripts/normalize_supabase.py`
- Modify: `scripts/run_pipeline.py`
- Modify: `README.md`
- Modify: `docs/2026-04-17-local-python-runbook.md`

- [ ] **Step 1: Write/update minimal assertions where needed**

If useful, extend existing tests to assert the new help/description wording.

- [ ] **Step 2: Update descriptions**

Make clear:
- `run_pipeline.py` is the primary local post-ingest step;
- `normalize_supabase.py` is a legacy compatibility facade;
- local ingest service / tunnel are fallback-only.

- [ ] **Step 3: Run verification**

Run:
- `python -m unittest discover -s tests -p "test_*.py" -v`
- `node --test tests\\ym_mail_ingest.test.js`
- `node --check Code.js`

Expected: all pass.

- [ ] **Step 4: Commit**

```bash
git add scripts/normalize_supabase.py scripts/run_pipeline.py README.md docs/2026-04-17-local-python-runbook.md
git commit -m "docs: clarify direct Turso contour and doctor workflow"
```

Plan complete and saved to `docs/superpowers/plans/2026-04-17-direct-turso-doctor.md`. Ready to execute?
