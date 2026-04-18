# Apps Script Transport Split Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove the main structural smells in the Apps Script source by splitting transport/settings code into smaller files without changing runtime behavior.

**Architecture:** Keep `Code.js` as the generated deploy bundle, but refactor `appsscript-src` so that orchestration stays in `30_entrypoints.js`, shared fetch/retry helpers stay in a common transport file, direct Turso transport lives in its own file, HTTP fallback transport lives in its own file, and runtime/settings resolution lives in its own file. Existing tests remain the behavior guardrail; add a small structural test for the split itself.

**Tech Stack:** Apps Script JavaScript, Python unittest, existing bundle builder

---

## Chunk 1: Structural Guardrail

### Task 1: Add failing structural test for the split source layout

**Files:**
- Create: `tests/test_appsscript_source_layout.py`

- [ ] **Step 1: Write the failing test**

Assert:
- new split files exist in `appsscript-src`;
- `20_transport_and_runtime.js` is no longer the giant mixed-responsibility file;
- `30_entrypoints.js` stays orchestration-focused.

- [ ] **Step 2: Run the focused test to verify it fails**

Run: `python -m unittest discover -s tests -p "test_appsscript_source_layout.py" -v`
Expected: FAIL because the new source files do not exist yet.

- [ ] **Step 3: Commit**

```bash
git add tests/test_appsscript_source_layout.py
git commit -m "test: add apps script source split guardrail"
```

## Chunk 2: Source Split

### Task 2: Split transport and settings code into focused source files

**Files:**
- Modify: `appsscript-src/20_transport_and_runtime.js`
- Create: `appsscript-src/21_transport_http_ingest.js`
- Create: `appsscript-src/22_transport_turso.js`
- Create: `appsscript-src/23_runtime_settings.js`
- Modify: `scripts/build_appsscript_bundle.py` only if ordering needs adjustment
- Modify: `Code.js`
- Test: `tests/test_appsscript_source_layout.py`
- Test: `tests/ym_mail_ingest.test.js`

- [ ] **Step 1: Move shared fetch/retry/logging helpers into the common transport file**

- [ ] **Step 2: Move HTTP fallback request builders and status/reset logic into `21_transport_http_ingest.js`**

- [ ] **Step 3: Move direct Turso SQL-over-HTTP request builders and response parsing into `22_transport_turso.js`**

- [ ] **Step 4: Move script-property resolution and runtime context helpers into `23_runtime_settings.js`**

- [ ] **Step 5: Rebuild `Code.js`**

Run: `python scripts\\build_appsscript_bundle.py`

- [ ] **Step 6: Run focused tests**

Run:
- `python -m unittest discover -s tests -p "test_appsscript_source_layout.py" -v`
- `node --test tests\\ym_mail_ingest.test.js`

Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add appsscript-src scripts/build_appsscript_bundle.py Code.js tests/test_appsscript_source_layout.py tests/ym_mail_ingest.test.js
git commit -m "refactor: split apps script transport and runtime modules"
```

## Chunk 3: Full Verification

### Task 3: Verify full repo behavior after the split

**Files:**
- Modify only if verification exposes breakage

- [ ] **Step 1: Run full verification**

Run:
- `python -m unittest discover -s tests -p "test_*.py" -v`
- `node --test tests\\ym_mail_ingest.test.js`
- `node --check Code.js`

Expected: all pass.

- [ ] **Step 2: Report the structural outcome**

State:
- new source boundaries;
- reduced file sizes;
- whether any behavior changes were required.

Plan complete and saved to `docs/superpowers/plans/2026-04-17-appsscript-transport-split.md`. Ready to execute?
