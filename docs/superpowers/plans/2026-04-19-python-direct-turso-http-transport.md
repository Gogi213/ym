# Python Direct Turso HTTP Transport Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the local Python Turso `libsql` runtime with the same SQL-over-HTTP transport already used by Apps Script so the live normalize pipeline can write to Turso reliably.

**Architecture:** Keep `scripts/turso_runtime.py` as the single runtime boundary, but change it from returning a `libsql` replica-backed connection to returning a small HTTP-backed connection wrapper with `execute()`, `executemany()`, `commit()`, `rollback()`, `close()`, and context-manager support. Reuse the existing SQL-over-HTTP `/v2/pipeline` protocol and keep `scripts/normalize/*` on the same DB-facing API so the normalize pipeline does not need a larger rewrite.

**Tech Stack:** Python, urllib/json, Turso SQL-over-HTTP `/v2/pipeline`, unittest

---

## Chunk 1: Runtime Boundary And Tests

### Task 1: Add failing runtime tests for HTTP transport

**Files:**
- Modify: `tests/test_turso_runtime.py`
- Modify: `scripts/turso_runtime.py`

- [ ] **Step 1: Write the failing tests**

Add tests that assert:
- `load_turso_config()` still resolves `TURSO_DATABASE_URL` and `TURSO_AUTH_TOKEN`, but no longer depends on a usable local replica path.
- `connect_turso()` builds an HTTP-backed connection without importing `libsql`.
- `execute()` sends a SQL-over-HTTP request and exposes cursor-style `fetchall()` / `description`.
- auth failures from Turso HTTP responses become actionable runtime errors.

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest tests.test_turso_runtime -v`

Expected: FAIL because runtime still imports `libsql` and does not expose the HTTP-backed cursor/connection contract.

- [ ] **Step 3: Write minimal implementation**

Implement a small Turso HTTP connection wrapper in `scripts/turso_runtime.py`:
- normalize `libsql://...` into `https://.../v2/pipeline`
- map Python values into Turso JSON arg values
- send `execute` / `close` requests over HTTP
- convert `results[0].response.result.cols/rows` into a cursor-like object
- keep `commit()` / `rollback()` as no-op compatibility methods

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m unittest tests.test_turso_runtime -v`

Expected: PASS

## Chunk 2: Pipeline Compatibility

### Task 2: Prove the normalize DB boundary still works on the new runtime

**Files:**
- Modify: `tests/test_normalize_turso_layout.py`
- Modify: `tests/test_run_pipeline.py`
- Modify: `scripts/turso_runtime.py`

- [ ] **Step 1: Write the failing tests**

Add tests that assert:
- `prepare_raw_ingest_files()` still works when the runtime returns HTTP-backed cursor rows.
- `run_pipeline`-level DB helpers can operate through the runtime wrapper without relying on `sync()`.

- [ ] **Step 2: Run tests to verify they fail**

Run:
- `python -m unittest tests.test_normalize_turso_layout -v`
- `python -m unittest tests.test_run_pipeline -v`

Expected: FAIL if any DB helper still depends on `libsql`-specific behavior.

- [ ] **Step 3: Write minimal implementation**

Adjust the runtime wrapper only as needed so the existing normalize/read/write helpers continue to work unchanged.

- [ ] **Step 4: Run tests to verify they pass**

Run:
- `python -m unittest tests.test_normalize_turso_layout -v`
- `python -m unittest tests.test_run_pipeline -v`

Expected: PASS

## Chunk 3: Verification And Integration

### Task 3: Verify the live path and finish the branch

**Files:**
- Modify: `README.md`
- Modify: `docs/2026-04-17-local-python-runbook.md`
- Modify: `docs/2026-04-17-chat-transition.md`

- [ ] **Step 1: Update docs**

Document that local Python now uses direct Turso SQL-over-HTTP and no longer relies on the `libsql` local replica write path.

- [ ] **Step 2: Run verification**

Run:
- `python -m unittest discover -s tests -p "test_*.py" -v`
- `node --test tests\\ym_mail_ingest.test.js`
- `node --check Code.js`
- `python scripts\\doctor_direct_turso.py --run-date 2026-04-16 --validate-payloads`
- `python scripts\\run_pipeline.py --service-account-json key\\stalwart-bounty-355816-994d612c3122.json --run-date 2026-04-16`

Expected:
- all automated tests pass;
- doctor succeeds;
- one live `run_pipeline.py` day completes without the previous Turso write hang.

- [ ] **Step 3: Commit**

```bash
git add docs/superpowers/plans/2026-04-19-python-direct-turso-http-transport.md scripts/turso_runtime.py tests/test_turso_runtime.py tests/test_normalize_turso_layout.py tests/test_run_pipeline.py README.md docs/2026-04-17-local-python-runbook.md docs/2026-04-17-chat-transition.md
git commit -m "feat: switch python turso runtime to http transport"
git push
```
