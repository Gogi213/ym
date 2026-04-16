# Cloudflare Worker R2 Ingest Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the fragile Render ingest runtime with `Cloudflare Worker + R2`, keep `Turso` for manifest/state/normalized data, and switch Python raw reads from SQL blobs to `R2` objects.

**Architecture:** Add a small JavaScript Cloudflare Worker that handles `reset`, `ingest`, and `pipeline-runs/{run_date}`, writes attachment bodies to `R2`, and writes manifest/state rows to `Turso`. Keep parsing and downstream normalization in Python; only change the raw-read path so Python downloads file bodies from `R2` by `r2_key`.

**Tech Stack:** Cloudflare Workers, Cloudflare R2, Turso/libSQL, Python `libsql`, Python `boto3`-style S3 client for R2, Apps Script, Node test runner, Python `unittest`.

---

## File Structure

### New worker runtime
- Create: `cloudflare_worker/wrangler.toml`
- Create: `cloudflare_worker/src/index.js`
- Create: `cloudflare_worker/src/config.js`
- Create: `cloudflare_worker/src/response.js`
- Create: `cloudflare_worker/src/routes.js`
- Create: `cloudflare_worker/src/r2.js`
- Create: `cloudflare_worker/src/turso.js`
- Create: `cloudflare_worker/src/ingest.js`
- Create: `cloudflare_worker/src/reset.js`
- Create: `cloudflare_worker/src/status.js`
- Create: `tests/worker/test_config.test.js`
- Create: `tests/worker/test_routes.test.js`
- Create: `tests/worker/test_ingest_logic.test.js`

### Turso schema and ingest manifest
- Modify: `turso/bootstrap_schema.sql`
- Modify: `scripts/bootstrap_turso.py`
- Modify: `ingest_service/storage.py`
- Modify: `tests/test_ingest_service_storage.py`
- Create: `tests/test_turso_r2_manifest_schema.py`

### Python R2 raw-read path
- Modify: `requirements.txt`
- Create: `scripts/r2_runtime.py`
- Modify: `scripts/normalize/turso_reads.py`
- Modify: `scripts/normalize/db_reads.py`
- Modify: `scripts/normalize/db.py`
- Modify: `tests/test_normalize_turso_write_path.py`
- Create: `tests/test_r2_runtime.py`
- Create: `tests/test_normalize_turso_r2_reads.py`

### Apps Script cutover and docs
- Modify: `appsscript-src/20_transport_and_runtime.js`
- Modify: `appsscript-src/30_entrypoints.js`
- Modify: `tests/ym_mail_ingest.test.js`
- Modify: `README.md`
- Modify: `docs/2026-04-07-ym-mail-ingest-technical.md`
- Create: `docs/2026-04-17-cloudflare-worker-r2-deploy.md`

## Chunk 1: Worker Runtime and Contract

### Task 1: Scaffold Cloudflare Worker config

**Files:**
- Create: `cloudflare_worker/wrangler.toml`
- Create: `cloudflare_worker/src/config.js`
- Test: `tests/worker/test_config.test.js`

- [ ] **Step 1: Write the failing config test**

```javascript
import test from 'node:test';
import assert from 'node:assert/strict';
import { buildConfig } from '../../cloudflare_worker/src/config.js';

test('buildConfig validates required bindings', () => {
  assert.throws(() => buildConfig({}), /INGEST_TOKEN/);
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `node --test tests/worker/test_config.test.js`
Expected: FAIL with module not found.

- [ ] **Step 3: Write minimal implementation**

Create `cloudflare_worker/src/config.js` with a `buildConfig(env)` helper that reads:
- `INGEST_TOKEN`
- `TURSO_DATABASE_URL`
- `TURSO_AUTH_TOKEN`
- `R2_BUCKET`

Create `cloudflare_worker/wrangler.toml` with:
- worker name placeholder
- `main = "src/index.js"`
- `compatibility_date`
- `workers_dev = true`
- `r2_buckets` binding placeholder

- [ ] **Step 4: Run test to verify it passes**

Run: `node --test tests/worker/test_config.test.js`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add cloudflare_worker/wrangler.toml cloudflare_worker/src/config.js tests/worker/test_config.test.js
git commit -m "feat: scaffold cloudflare worker config"
```

### Task 2: Add route dispatch and health endpoint

**Files:**
- Create: `cloudflare_worker/src/index.js`
- Create: `cloudflare_worker/src/routes.js`
- Create: `cloudflare_worker/src/response.js`
- Test: `tests/worker/test_routes.test.js`

- [ ] **Step 1: Write the failing route test**

```javascript
test('router returns ok for GET /health', async () => {
  const response = await handleRequest(new Request('https://example.com/health'), stubEnv());
  assert.equal(response.status, 200);
  assert.equal(await response.text(), 'ok');
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `node --test tests/worker/test_routes.test.js`
Expected: FAIL with `handleRequest is not defined`.

- [ ] **Step 3: Write minimal implementation**

Implement:
- `handleRequest(request, env)` router
- `GET /health`
- `POST /reset`
- `POST /ingest`
- `GET /pipeline-runs/:run_date`
- small JSON helpers in `response.js`

- [ ] **Step 4: Run test to verify it passes**

Run: `node --test tests/worker/test_routes.test.js`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add cloudflare_worker/src/index.js cloudflare_worker/src/routes.js cloudflare_worker/src/response.js tests/worker/test_routes.test.js
git commit -m "feat: add worker router skeleton"
```

### Task 3: Implement reset/status contract against Turso

**Files:**
- Create: `cloudflare_worker/src/turso.js`
- Create: `cloudflare_worker/src/reset.js`
- Create: `cloudflare_worker/src/status.js`
- Test: `tests/worker/test_ingest_logic.test.js`
- Modify: `turso/bootstrap_schema.sql`
- Modify: `scripts/bootstrap_turso.py`
- Modify: `tests/test_turso_r2_manifest_schema.py`

- [ ] **Step 1: Write failing schema and status tests**

Add tests that assert:
- `ingest_files` has `r2_key`, `file_size_bytes`, `parse_error`, `raw_revision`
- `pipeline-runs/{run_date}` summary includes `uploaded_files`, `parsed_files`, `failed_files`
- `reset` bumps `raw_revision` and clears current-day counters for the new revision

- [ ] **Step 2: Run tests to verify they fail**

Run:
- `python -m unittest tests.test_turso_r2_manifest_schema -v`
- `node --test tests/worker/test_ingest_logic.test.js`
Expected: FAIL on missing columns/handlers.

- [ ] **Step 3: Write minimal implementation**

Update `turso/bootstrap_schema.sql` to:
- add/alter `ingest_files` columns
- ensure `pipeline_runs` can summarize current revision truthfully

Implement in worker modules:
- `resetRunDate(connection, runDate)`
- `fetchRunDateSummary(connection, runDate)`

- [ ] **Step 4: Run tests to verify they pass**

Run:
- `python -m unittest tests.test_turso_r2_manifest_schema -v`
- `node --test tests/worker/test_ingest_logic.test.js`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add turso/bootstrap_schema.sql scripts/bootstrap_turso.py cloudflare_worker/src/turso.js cloudflare_worker/src/reset.js cloudflare_worker/src/status.js tests/test_turso_r2_manifest_schema.py tests/worker/test_ingest_logic.test.js
git commit -m "feat: add turso manifest revision contract"
```

## Chunk 2: Worker Ingest to R2 + Turso Manifest

### Task 4: Implement R2 object write helper

**Files:**
- Create: `cloudflare_worker/src/r2.js`
- Test: `tests/worker/test_ingest_logic.test.js`

- [ ] **Step 1: Write the failing R2 helper test**

Add a test asserting `storeAttachment(env, payload)`:
- writes to `env.RAW_FILES_BUCKET.put(key, body, options)`
- returns `{ r2Key, fileSizeBytes }`

- [ ] **Step 2: Run test to verify it fails**

Run: `node --test tests/worker/test_ingest_logic.test.js`
Expected: FAIL because `storeAttachment` does not exist.

- [ ] **Step 3: Write minimal implementation**

Implement `storeAttachment(env, metadata, body)` with deterministic key layout such as:

```text
raw/<run_date>/<message_id>/<file_name>
```

Include metadata needed for debugging only; do not overdesign lifecycle logic.

- [ ] **Step 4: Run test to verify it passes**

Run: `node --test tests/worker/test_ingest_logic.test.js`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add cloudflare_worker/src/r2.js tests/worker/test_ingest_logic.test.js
git commit -m "feat: add worker r2 storage helper"
```

### Task 5: Implement `/ingest` manifest write flow

**Files:**
- Create: `cloudflare_worker/src/ingest.js`
- Modify: `cloudflare_worker/src/routes.js`
- Modify: `cloudflare_worker/src/turso.js`
- Test: `tests/worker/test_ingest_logic.test.js`

- [ ] **Step 1: Write the failing ingest flow test**

Add test that posts one attachment and asserts ordered side effects:
1. auth checked
2. object stored in R2
3. manifest row written in Turso
4. response is `200` with `ok: true`

Also add duplicate-ingest test asserting same natural key updates existing row instead of inserting a duplicate.

- [ ] **Step 2: Run test to verify it fails**

Run: `node --test tests/worker/test_ingest_logic.test.js`
Expected: FAIL on missing `/ingest` logic.

- [ ] **Step 3: Write minimal implementation**

Implement:
- multipart/form-data parsing in Worker
- auth token check
- `storeAttachment(...)`
- `upsertIngestManifest(...)`

Natural key should use:
- `run_date`
- `message_id`
- `file_name`
- `topic_role`
- `primary_topic`

- [ ] **Step 4: Run test to verify it passes**

Run: `node --test tests/worker/test_ingest_logic.test.js`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add cloudflare_worker/src/ingest.js cloudflare_worker/src/routes.js cloudflare_worker/src/turso.js tests/worker/test_ingest_logic.test.js
git commit -m "feat: add worker ingest manifest upsert"
```

### Task 6: Deploy smoke worker and record runtime config

**Files:**
- Modify: `cloudflare_worker/wrangler.toml`
- Modify: `README.md`
- Create: `docs/2026-04-17-cloudflare-worker-r2-deploy.md`

- [ ] **Step 1: Write down required bindings in docs before deploy**

Document exact secrets/bindings:
- `INGEST_TOKEN`
- `TURSO_DATABASE_URL`
- `TURSO_AUTH_TOKEN`
- `RAW_FILES_BUCKET`

- [ ] **Step 2: Deploy the worker**

Run from `cloudflare_worker`:
- `npx wrangler r2 bucket create <bucket-name>` if bucket does not exist
- `npx wrangler deploy`
- `npx wrangler secret put INGEST_TOKEN`
- `npx wrangler secret put TURSO_DATABASE_URL`
- `npx wrangler secret put TURSO_AUTH_TOKEN`

Expected: deploy succeeds and `/health` returns `ok`.

- [ ] **Step 3: Verify the live endpoints**

Run:
- `curl https://<worker>.workers.dev/health`
- `curl https://<worker>.workers.dev/pipeline-runs/2026-04-17`
Expected: `200` responses.

- [ ] **Step 4: Commit docs/config changes**

```bash
git add cloudflare_worker/wrangler.toml README.md docs/2026-04-17-cloudflare-worker-r2-deploy.md
git commit -m "docs: add cloudflare worker deploy runbook"
```

## Chunk 3: Python Raw-Read Cutover from SQL Blob to R2

### Task 7: Add Python R2 runtime helper

**Files:**
- Modify: `requirements.txt`
- Create: `scripts/r2_runtime.py`
- Test: `tests/test_r2_runtime.py`

- [ ] **Step 1: Write the failing Python R2 runtime test**

```python
def test_build_r2_client_requires_endpoint_and_keys():
    with self.assertRaisesRegex(RuntimeError, 'R2'):
        build_r2_client({})
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest tests.test_r2_runtime -v`
Expected: FAIL because module does not exist.

- [ ] **Step 3: Write minimal implementation**

Add dependency for S3-compatible access in `requirements.txt`.
Create `scripts/r2_runtime.py` with:
- env reader for endpoint/access key/secret/bucket
- helper to fetch object bytes by `r2_key`

Keep it narrow; no cleanup logic.

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m unittest tests.test_r2_runtime -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add requirements.txt scripts/r2_runtime.py tests/test_r2_runtime.py
git commit -m "feat: add python r2 runtime helper"
```

### Task 8: Switch Turso raw reads to manifest + R2 download

**Files:**
- Modify: `scripts/normalize/turso_reads.py`
- Modify: `scripts/normalize/db_reads.py`
- Modify: `scripts/normalize/db.py`
- Create: `tests/test_normalize_turso_r2_reads.py`

- [ ] **Step 1: Write the failing R2 raw-read test**

Add test that seeds:
- `ingest_files` with `status='uploaded'` and `r2_key`
- no usable `ingest_file_payloads.file_base64`

Test should assert the read path now requests body through `fetch_r2_object_bytes(r2_key)`.

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest tests.test_normalize_turso_r2_reads -v`
Expected: FAIL because normalize still expects SQL payload blob.

- [ ] **Step 3: Write minimal implementation**

Change Turso read path to:
- select manifest rows from `ingest_files`
- for each file use `r2_key` to fetch raw body
- keep SQL blob path only as explicit legacy fallback during migration

- [ ] **Step 4: Run targeted tests**

Run:
- `python -m unittest tests.test_normalize_turso_r2_reads -v`
- `python -m unittest tests.test_normalize_turso_write_path -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add scripts/normalize/turso_reads.py scripts/normalize/db_reads.py scripts/normalize/db.py tests/test_normalize_turso_r2_reads.py
git commit -m "feat: read turso raw payloads from r2"
```

### Task 9: End-to-end Python smoke on Worker-written raw sample

**Files:**
- Modify: `tests/test_normalize_supabase.py`
- Modify: `tests/test_run_pipeline.py`
- Modify: `README.md`

- [ ] **Step 1: Add a smoke fixture path for Worker-written manifest rows**

Add a test fixture that represents:
- `uploaded` manifest row
- `r2_key`
- parsed output equality expectations

- [ ] **Step 2: Run the smoke tests and confirm failure**

Run:
- `python -m unittest tests.test_normalize_supabase tests.test_run_pipeline -v`
Expected: FAIL on missing Worker-written raw support.

- [ ] **Step 3: Make the minimum compatibility fixes**

Update normalization/pipeline code only where needed so the existing pipeline can process Worker-written manifest rows without changing math or downstream semantics.

- [ ] **Step 4: Re-run the smoke tests**

Run:
- `python -m unittest tests.test_normalize_supabase tests.test_run_pipeline -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add tests/test_normalize_supabase.py tests/test_run_pipeline.py README.md
git commit -m "test: cover worker manifest normalize path"
```

## Chunk 4: Apps Script Cutover and Legacy Cleanup

### Task 10: Cut Apps Script from Render to Worker endpoint

**Files:**
- Modify: `appsscript-src/20_transport_and_runtime.js`
- Modify: `appsscript-src/30_entrypoints.js`
- Modify: `tests/ym_mail_ingest.test.js`

- [ ] **Step 1: Write the failing Apps Script transport test**

Add tests that assert:
- `INGEST_BASE_URL` can point to `workers.dev`
- `INGEST_STATUS_URL` base normalization still works
- month backfill still retries transient status errors
- no Render-specific assumptions remain in transport helpers

- [ ] **Step 2: Run test to verify it fails**

Run: `node --test tests/ym_mail_ingest.test.js`
Expected: FAIL on Render-specific assumptions or missing worker contract cases.

- [ ] **Step 3: Write minimal implementation**

Remove any remaining Render-specific wording/assumptions from Apps Script transport and keep the contract generic:
- `POST /reset`
- `POST /ingest`
- `GET /pipeline-runs/{run_date}`

Rebuild bundle:
- `python scripts/build_appsscript_bundle.py`

- [ ] **Step 4: Run JS validation**

Run:
- `node --test tests/ym_mail_ingest.test.js`
- `node --check Code.js`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add appsscript-src/20_transport_and_runtime.js appsscript-src/30_entrypoints.js Code.js tests/ym_mail_ingest.test.js
git commit -m "feat: point apps script transport to cloudflare worker"
```

### Task 11: Live cutover smoke and status verification

**Files:**
- Modify: `README.md`
- Modify: `docs/2026-04-07-ym-mail-ingest-technical.md`
- Modify: `docs/2026-04-17-cloudflare-worker-r2-deploy.md`

- [ ] **Step 1: Configure live runtime values**

Set:
- Worker URL in Apps Script `Script Properties`
- ingest token secret in Worker
- R2 bucket binding
- Turso env in Worker
- R2 credentials/env for Python runtime

- [ ] **Step 2: Run daily smoke**

Run Apps Script `run()` and verify:
- file appears in `R2`
- manifest row appears in `Turso`
- `GET /pipeline-runs/{run_date}` reports uploaded files truthfully

- [ ] **Step 3: Run Python pipeline smoke**

Run:
- `python scripts/run_pipeline.py --service-account-json <path>`
Expected:
- Python downloads object from `R2`
- `ingest_rows` populate
- downstream normalize/sheet sync complete

- [ ] **Step 4: Re-run full test suite**

Run:
- `python -m unittest discover -s tests -p "test_*.py" -v`
- `node --test tests/ym_mail_ingest.test.js`
- `node --check Code.js`
Expected: PASS.

- [ ] **Step 5: Commit docs/runtime updates**

```bash
git add README.md docs/2026-04-07-ym-mail-ingest-technical.md docs/2026-04-17-cloudflare-worker-r2-deploy.md
git commit -m "docs: finalize cloudflare worker r2 ingest cutover"
```

### Task 12: Remove Render from the active path

**Files:**
- Modify: `README.md`
- Modify: `docs/2026-04-16-turso-render-deploy.md`
- Modify: `ingest_service/` docs references only if still used

- [ ] **Step 1: Mark Render runtime as legacy**

Update docs to make clear that:
- Render is no longer the active ingest runtime
- Worker + R2 is the active path

- [ ] **Step 2: Verify no active deploy/runbook points new users at Render**

Run: `rg -n "Render|onrender|ym-ingest-service" README.md docs appsscript-src`
Expected: only legacy references remain.

- [ ] **Step 3: Commit legacy cleanup**

```bash
git add README.md docs/2026-04-16-turso-render-deploy.md docs/2026-04-07-ym-mail-ingest-technical.md
git commit -m "docs: mark render ingest runtime as legacy"
```
