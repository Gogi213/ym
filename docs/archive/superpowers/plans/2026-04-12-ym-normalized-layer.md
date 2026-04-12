# YM Normalized Layer Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a normalized Supabase layer and Python backend that converts raw ingest files/rows into canonical UTM/metric rows plus per-topic goal slots and a wide export view.

**Architecture:** Keep the current raw ingest pipeline unchanged. Add a normalized database layer in Supabase, then implement a Python job that reads raw tables, applies canonical header mappings, assigns stable `goal_N` slots per topic, rebuilds normalized rows, and exposes a wide export view for spreadsheet export.

**Tech Stack:** Supabase Postgres, SQL migrations, Python 3 standard library, Supabase REST API, node test runner for existing JS tests, Python unittest

---

## Chunk 1: Database Shape

### Task 1: Add normalized schema migration

**Files:**
- Create: `supabase/migrations/20260412160000_create_normalized_layer.sql`
- Reference: `supabase/migrations/20260407053000_create_mail_ingest_tables.sql`
- Reference: `supabase/migrations/20260407061000_create_ingest_file_payloads.sql`

- [ ] **Step 1: Define the normalized tables and view in the migration**

Create:
- `public.topic_goal_slots`
- `public.normalized_rows`
- `public.normalized_goals`
- `public.export_rows_wide` view

Include indexes for `run_date`, `topic`, `source_file_id`, and `is_current`.

- [ ] **Step 2: Add a minimal verification query**

Plan for:
```sql
select table_name
from information_schema.tables
where table_schema = 'public'
  and table_name in ('topic_goal_slots', 'normalized_rows', 'normalized_goals');
```

- [ ] **Step 3: Commit**

Blocked unless git repo exists.

## Chunk 2: Python Backend

### Task 2: Add failing tests for normalization helpers

**Files:**
- Create: `tests/test_normalize_supabase.py`
- Create: `scripts/normalize_supabase.py`

- [ ] **Step 1: Write failing tests**

Cover:
- header alias normalization
- metric value parsing
- duration parsing
- report date extraction
- stable per-topic goal slot assignment
- sparse-to-wide row shaping

- [ ] **Step 2: Run test to verify it fails**

Run:
```bash
python -m unittest tests.test_normalize_supabase -v
```

- [ ] **Step 3: Implement minimal helpers in `scripts/normalize_supabase.py`**

Implement helper functions first, no API calls yet.

- [ ] **Step 4: Re-run test to verify it passes**

Run:
```bash
python -m unittest tests.test_normalize_supabase -v
```

### Task 3: Implement normalization job

**Files:**
- Modify: `scripts/normalize_supabase.py`

- [ ] **Step 1: Add raw-table readers**

Use Supabase REST with env vars:
- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`

Read:
- `ingest_files`
- `ingest_rows`

- [ ] **Step 2: Add canonical normalization logic**

Map:
- UTM columns
- standard metrics
- selected-goal summary metrics
- custom goal columns

Store raw leftovers in `source_row_json`.

- [ ] **Step 3: Add writers for normalized tables**

For a target `run_date`:
- delete existing normalized rows/goals for that date
- rebuild `topic_goal_slots`
- insert `normalized_rows`
- insert `normalized_goals`

- [ ] **Step 4: Add CLI entrypoint**

Support:
```bash
python scripts/normalize_supabase.py --run-date 2026-04-06
```

## Chunk 3: Docs

### Task 4: Update docs for the normalized layer

**Files:**
- Modify: `docs/2026-04-07-ym-mail-ingest-technical.md`
- Modify: `тз.md`

- [ ] **Step 1: Document the new normalized tables, goal-slot logic, and export view**

- [ ] **Step 2: Remove outdated wording that implies Drive staging or daily-only ingestion**

- [ ] **Step 3: Document the current git blocker**

State plainly that commit/push requires the user to put this workspace under git or point to the correct repo.

## Verification

- [ ] Run:
```bash
node --test
```

- [ ] Run:
```bash
node --check Code.js
```

- [ ] Run:
```bash
python -m unittest tests.test_normalize_supabase -v
```

- [ ] Run:
```bash
npx supabase db push
```

- [ ] Run:
```bash
python scripts/normalize_supabase.py --run-date 2026-04-06
```

- [ ] Validate with SQL:
```sql
select count(*) from public.normalized_rows;
select count(*) from public.normalized_goals;
select * from public.export_rows_wide limit 10;
```
