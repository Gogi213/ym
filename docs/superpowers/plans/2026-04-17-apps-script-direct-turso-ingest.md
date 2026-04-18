# Apps Script Direct Turso Ingest Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move raw ingest from the local HTTP service to direct Apps Script writes into Turso, while keeping local Python as the manual post-ingest parser and normalizer.

**Architecture:** Apps Script writes raw file metadata and base64 payloads directly into Turso and performs the per-run reset. Local Python reads `raw_only` files from Turso, parses CSV/XLSX payloads into `ingest_rows`, updates raw status, and then runs the existing normalize and Sheets sync flow. No scheduler or watcher is added in this cut.

**Tech Stack:** Apps Script, Turso/libSQL HTTP API, Python, libsql, Google Sheets API

---

## Chunk 1: Raw Model and Readiness

### Task 1: Add raw-only status to the Turso schema and storage tests

**Files:**
- Modify: `turso/bootstrap_schema.sql`
- Modify: `tests/test_ingest_service_storage.py`

- [ ] **Step 1: Write the failing storage test**

Add a test that inserts an `ingest_files` row with `status = 'raw_only'` and asserts the schema accepts it.

- [ ] **Step 2: Run the focused test to verify it fails**

Run: `python -m unittest tests.test_ingest_service_storage -v`
Expected: FAIL on the status check constraint.

- [ ] **Step 3: Update the schema**

Extend the `ingest_files.status` check to allow `raw_only`.

- [ ] **Step 4: Run the focused test to verify it passes**

Run: `python -m unittest tests.test_ingest_service_storage -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add turso/bootstrap_schema.sql tests/test_ingest_service_storage.py
git commit -m "feat: allow raw-only ingest files"
```

### Task 2: Teach local Python to parse raw-only payloads from Turso

**Files:**
- Modify: `scripts/normalize/pipeline.py`
- Modify: `scripts/normalize/turso_reads.py`
- Modify: `scripts/normalize/db.py`
- Test: `tests/test_normalize_turso_layout.py`
- Test: `tests/test_run_pipeline.py`

- [ ] **Step 1: Write the failing tests**

Add tests covering:
- `fetch_ingested_files()` includes `raw_only` files needed for preprocessing;
- `run_pipeline()` can take a run date with only raw payloads and still reach parsed `ingest_rows`.

- [ ] **Step 2: Run the focused tests to verify they fail**

Run: `python -m unittest tests.test_normalize_turso_layout tests.test_run_pipeline -v`
Expected: FAIL because raw-only files are ignored.

- [ ] **Step 3: Add raw payload preprocessing**

Implement a preprocessing step that:
- loads `raw_only` payloads;
- parses them with `ingest_service.parse.parse_attachment`;
- updates `ingest_files.status`, `header_json`, `row_count`, `error_text`;
- rewrites `ingest_rows`;
- refreshes `pipeline_runs` before normalization continues.

- [ ] **Step 4: Run the focused tests to verify they pass**

Run: `python -m unittest tests.test_normalize_turso_layout tests.test_run_pipeline -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add scripts/normalize/pipeline.py scripts/normalize/turso_reads.py scripts/normalize/db.py tests/test_normalize_turso_layout.py tests/test_run_pipeline.py
git commit -m "feat: preprocess raw-only Turso payloads before normalize"
```

## Chunk 2: Apps Script Direct Turso Write Path

### Task 3: Add direct Turso settings and SQL request builders in Apps Script

**Files:**
- Modify: `appsscript-src/20_transport_and_runtime.js`
- Modify: `Code.js`
- Test: `tests/ym_mail_ingest.test.js`

- [ ] **Step 1: Write the failing tests**

Add tests for:
- reading `TURSO_DATABASE_URL` and `TURSO_AUTH_TOKEN` from script properties;
- building Turso HTTP requests for reset and file insert operations;
- rejecting missing Turso settings in direct mode.

- [ ] **Step 2: Run the JS test file to verify it fails**

Run: `node --test tests\\ym_mail_ingest.test.js`
Expected: FAIL because the direct Turso helpers do not exist.

- [ ] **Step 3: Implement minimal direct-write helpers**

Add helpers for:
- Turso script settings;
- SQL-over-HTTP request shaping;
- UUID generation;
- base64 encoding of attachment bytes;
- reset statement batch;
- insert statement batch for `ingest_files` and `ingest_file_payloads`.

- [ ] **Step 4: Run the JS test file to verify it passes**

Run: `node --test tests\\ym_mail_ingest.test.js`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add appsscript-src/20_transport_and_runtime.js Code.js tests/ym_mail_ingest.test.js
git commit -m "feat: add Turso direct-write transport helpers"
```

### Task 4: Switch Apps Script run flow from HTTP ingest to Turso raw writes

**Files:**
- Modify: `appsscript-src/30_entrypoints.js`
- Modify: `Code.js`
- Test: `tests/ym_mail_ingest.test.js`

- [ ] **Step 1: Write the failing tests**

Add tests proving:
- `runForDate_()` issues a Turso reset instead of HTTP `/reset`;
- each attachment becomes direct raw writes instead of multipart upload;
- month backfill existence checks still work against Turso state.

- [ ] **Step 2: Run the JS test file to verify it fails**

Run: `node --test tests\\ym_mail_ingest.test.js`
Expected: FAIL because `runForDate_()` still targets local HTTP ingest.

- [ ] **Step 3: Implement the flow switch**

Replace:
- `postReset_()` HTTP reset
- `buildAttachmentRequest_()` multipart upload path

With:
- Turso reset statements;
- direct raw inserts into `ingest_files` and `ingest_file_payloads`;
- raw file counters in `pipeline_runs`.

- [ ] **Step 4: Run the JS test file to verify it passes**

Run: `node --test tests\\ym_mail_ingest.test.js`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add appsscript-src/30_entrypoints.js Code.js tests/ym_mail_ingest.test.js
git commit -m "feat: write raw ingest directly to Turso from Apps Script"
```

## Chunk 3: Cleanup and Docs

### Task 5: Demote local ingest service from the main path in docs

**Files:**
- Modify: `README.md`
- Modify: `docs/2026-04-07-ym-mail-ingest-technical.md`
- Modify: `docs/2026-04-17-chat-transition.md`
- Modify: `docs/2026-04-17-local-python-runbook.md`

- [ ] **Step 1: Update the operator docs**

Document:
- new script properties for direct Turso write;
- that local Python is now the manual post-ingest step;
- that `cloudflared` is no longer needed for the primary path.

- [ ] **Step 2: Run quick consistency checks**

Run:
- `node --check Code.js`
- `python -m unittest discover -s tests -p "test_*.py" -v`
- `node --test tests\\ym_mail_ingest.test.js`

Expected: all pass.

- [ ] **Step 3: Commit**

```bash
git add README.md docs/2026-04-07-ym-mail-ingest-technical.md docs/2026-04-17-chat-transition.md docs/2026-04-17-local-python-runbook.md
git commit -m "docs: document direct Apps Script to Turso ingest flow"
```

Plan complete and saved to `docs/superpowers/plans/2026-04-17-apps-script-direct-turso-ingest.md`. Ready to execute?
