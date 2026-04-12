# YM Supabase Ingest Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace Drive-based staging with a direct `Apps Script -> Supabase Edge Function -> Postgres` ingest flow for Gmail attachments.

**Architecture:** Keep Apps Script as a thin Gmail transport layer that finds matching messages for the target day and posts `xlsx/csv` attachments to a single Supabase Edge Function. Put all table detection, UTM-header validation, and row insertion in Supabase, and store parsed rows as `jsonb` so the MVP does not need a fixed union schema yet.

**Tech Stack:** Google Apps Script (V8), Supabase Edge Functions (Deno/TypeScript), Postgres, Node.js built-in test runner, plain JavaScript

---

## Chunk 1: Contracts And Local Test Coverage

### Task 1: Freeze the new ingest contract in tests

**Files:**
- Modify: `tests/ym_mail_ingest.test.js`
- Modify: `tests/load_code.js`

- [ ] **Step 1: Write the failing tests**

Add tests for:
- building the reset payload for one target run date
- shaping multipart metadata for one attachment POST
- keeping only `xlsx/csv` attachments for transport
- preserving existing CSV/XLSX table-detection behavior used by the server

- [ ] **Step 2: Run test to verify it fails**

Run: `npm test`
Expected: FAIL on missing transport helpers and changed manifest/storage assumptions.

- [ ] **Step 3: Write minimal implementation**

Implement only the helper surface needed by the new flow:
- reset request payload helper
- ingest metadata helper
- HTTP request helper boundaries for Apps Script

- [ ] **Step 4: Run test to verify it passes**

Run: `npm test`
Expected: PASS for the new contract tests.

## Chunk 2: Supabase Database Schema

### Task 2: Add the ingest tables migration

**Files:**
- Create: `supabase/migrations/20260407053000_create_mail_ingest_tables.sql`

- [ ] **Step 1: Write the schema shape in the migration**

Create:
- `public.ingest_files`
- `public.ingest_rows`

Include:
- primary keys
- `run_date`
- message/file metadata
- `jsonb` storage for parsed headers and rows
- foreign key from `ingest_rows.file_id` to `ingest_files.id`
- indexes on `run_date`, `message_id`, and `matched_topic`

- [ ] **Step 2: Verify migration SQL is internally coherent**

Review:
- table/column names
- index targets
- cascade behavior on delete

Expected: schema is ready for `supabase db push` or `apply_migration`.

## Chunk 3: Supabase Edge Function

### Task 3: Create the edge ingest function

**Files:**
- Create: `supabase/functions/mail-ingest/index.ts`
- Create: `supabase/functions/mail-ingest/deno.json`

- [ ] **Step 1: Write the failing tests first in local helpers**

Extend `tests/ym_mail_ingest.test.js` with pure-function tests for:
- `reset` request validation
- row-object shaping from detected headers/data rows
- skipping files without a valid UTM header row

- [ ] **Step 2: Run test to verify it fails**

Run: `npm test`
Expected: FAIL on missing row-shaping and request-mode helpers.

- [ ] **Step 3: Write minimal implementation**

In `mail-ingest`:
- accept `POST` only
- accept JSON `{ action: 'reset', run_date }` and delete existing rows/files for that date
- accept `multipart/form-data` with one file plus message metadata
- detect `csv/xlsx`
- parse rows
- find the first valid table block whose header row contains at least one canonical `utm_*`
- insert one `ingest_files` row
- insert parsed data rows into `ingest_rows`
- return JSON status

- [ ] **Step 4: Run test to verify it passes**

Run: `npm test`
Expected: PASS for all local pure-function coverage.

## Chunk 4: Apps Script Transport Rewrite

### Task 4: Replace Drive/manifest flow with Supabase transport

**Files:**
- Modify: `Code.js`
- Modify: `appsscript.json`

- [ ] **Step 1: Write the failing tests**

Add tests for:
- choosing the target run date (`2026-04-06` when running on `2026-04-07` with offset `-1`)
- building the reset request body
- building ingest metadata for a candidate attachment

- [ ] **Step 2: Run test to verify it fails**

Run: `npm test`
Expected: FAIL because `Code.js` still targets Drive/manifest behavior.

- [ ] **Step 3: Write minimal implementation**

Update `Code.js` so `run()`:
- loads topic rules from the source spreadsheet
- computes the target run date
- finds Gmail messages in a recent attachment window
- filters messages to the target date and matched topics
- sends one reset request for the target date
- sends each `xlsx/csv` attachment from matched messages to the edge function
- logs a compact run summary

Remove Drive-specific writes and manifest writes from the runtime path.

- [ ] **Step 4: Run test to verify it passes**

Run: `npm test`
Expected: PASS

## Chunk 5: Verification

### Task 5: Run end-to-end local verification

**Files:**
- Test: `tests/ym_mail_ingest.test.js`
- Review: `Code.js`
- Review: `supabase/functions/mail-ingest/index.ts`
- Review: `supabase/migrations/20260407053000_create_mail_ingest_tables.sql`

- [ ] **Step 1: Run the full test suite**

Run: `npm test`
Expected: PASS

- [ ] **Step 2: Review local project layout**

Run: `Get-ChildItem -Recurse supabase`
Expected: shows `functions/mail-ingest` and `migrations`

- [ ] **Step 3: Review exported Apps Script helper surface**

Run: `node -e "const ingest=require('./tests/load_code.js'); console.log(Object.keys(ingest).sort())"`
Expected: includes the transport helpers used by tests.

