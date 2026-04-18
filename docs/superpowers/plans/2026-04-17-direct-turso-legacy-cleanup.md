# Direct Turso Legacy Cleanup Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove legacy HTTP/Supabase and fallback runtime paths so the repo only reflects the current `Apps Script -> Turso raw -> local Python pipeline -> Sheets` contour.

**Architecture:** Keep only direct Turso write/read paths in Apps Script and Python normalization. Delete fallback HTTP ingest transport, local tunnel launcher, postgres backend branching, and stale Supabase edge-function artifacts. Preserve only parser code that is still used by the local pipeline.

**Tech Stack:** Apps Script JavaScript, Python unittest, Node test runner, Turso/libSQL, local Python pipeline

---

## Chunk 1: Guardrails

### Task 1: Add failing direct-only boundary tests

**Files:**
- Create: `tests/test_direct_turso_only_layout.py`

- [ ] **Step 1: Write the failing test**

Assert that:
- `appsscript-src/21_transport_http_ingest.js` does not exist;
- Apps Script source does not reference `http_ingest` or `INGEST_BASE_URL`;
- `scripts/run_local_stack.py` does not exist;
- `supabase/functions/mail-ingest` does not exist;
- `scripts/normalize/db.py` does not reference `postgres`.

- [ ] **Step 2: Run the focused test to verify it fails**

Run: `python -m unittest discover -s tests -p "test_direct_turso_only_layout.py" -v`
Expected: FAIL on the current legacy files/references.

## Chunk 2: Code Cleanup

### Task 2: Remove non-current runtime paths

**Files:**
- Modify: `appsscript-src/00_config_and_topics.js`
- Modify: `appsscript-src/20_transport_and_runtime.js`
- Delete: `appsscript-src/21_transport_http_ingest.js`
- Modify: `appsscript-src/22_transport_turso.js`
- Modify: `appsscript-src/23_runtime_settings.js`
- Modify: `appsscript-src/30_entrypoints.js`
- Modify: `Code.js`
- Modify/Delete legacy Python runtime files under `ingest_service/`
- Modify: `scripts/normalize/db.py`
- Delete: `scripts/normalize/db_connection.py`
- Delete: legacy postgres modules under `scripts/normalize/`
- Move: `scripts/normalize_supabase.py` to a current-name CLI file
- Delete: `scripts/run_local_stack.py`
- Delete: `supabase/functions/mail-ingest/**`

- [ ] **Step 1: Remove Apps Script HTTP transport and direct-only config branching**
- [ ] **Step 2: Remove local HTTP ingest runtime files that are no longer used by the pipeline**
- [ ] **Step 3: Simplify normalization DB access to Turso-only modules**
- [ ] **Step 4: Rename/remove legacy CLI and Supabase artifacts**
- [ ] **Step 5: Rebuild `Code.js`**

## Chunk 3: Cleanup Verification

### Task 3: Update tests/docs/package metadata and verify the repo

**Files:**
- Modify/Delete legacy tests that assert fallback paths
- Modify: `README.md`
- Modify: `docs/2026-04-07-ym-mail-ingest-technical.md`
- Modify: `docs/2026-04-17-local-python-runbook.md`
- Modify: `docs/2026-04-17-chat-transition.md`
- Modify: `package.json`

- [ ] **Step 1: Update tests to current direct-only contract**
- [ ] **Step 2: Remove fallback launcher/docs/package scripts**
- [ ] **Step 3: Run full verification**

Run:
- `python -m unittest discover -s tests -p "test_*.py" -v`
- `node --test tests\\ym_mail_ingest.test.js`
- `node --check Code.js`

Expected: all pass with only current contour references remaining.
