# Secondary Topic Merge Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add primary/secondary topic mapping from spreadsheet column `B`, ingest secondary files with explicit metadata, and merge secondary rows into primary rows only on exact grain match.

**Architecture:** Extend Apps Script topic loading to read columns `A:B` and emit topic-role metadata into raw ingest. Extend normalization to carry `primary_topic` and `topic_role`, then merge secondary rows into primary rows on an exact canonical grain before operator exports.

**Tech Stack:** Apps Script, Supabase Edge Function, Postgres, Python, Google Sheets

---

## Chunk 1: Ingest Metadata

### Task 1: Add primary/secondary topic rule loading in Apps Script

**Files:**
- Modify: `Code.js`
- Test: `tests/ym_mail_ingest.test.js`

- [ ] Read `A:B` from sheet `отчеты`
- [ ] Produce rule objects with `primaryTopic`, `matchedTopic`, `topicRole`
- [ ] Update topic matching helpers to return the full rule instead of plain topic string
- [ ] Add failing and passing tests for column `B` secondary topics

### Task 2: Carry topic-role metadata into ingest payload

**Files:**
- Modify: `Code.js`
- Modify: `supabase/functions/mail-ingest/index.ts`

- [ ] Add `primary_topic` and `topic_role` into upload metadata
- [ ] Persist both fields in raw ingest file records
- [ ] Preserve existing behavior for primary-only rows

## Chunk 2: Normalization Merge

### Task 3: Persist topic role in normalized rows

**Files:**
- Modify: `scripts/normalize_supabase.py`
- Test: `tests/test_normalize_supabase.py`

- [ ] Fetch `primary_topic` and `topic_role` from raw files
- [ ] Carry them through normalized payload building
- [ ] Use `primary_topic` as the business topic key for merged output

### Task 4: Merge secondary rows into primary rows by exact grain

**Files:**
- Modify: `scripts/normalize_supabase.py`
- Test: `tests/test_normalize_supabase.py`

- [ ] Define exact merge grain from canonical dimensions + `report_date`
- [ ] Build primary row index by grain
- [ ] Merge secondary metrics/goals into primary rows only on exact match
- [ ] Count unmatched secondary rows for diagnostics

## Chunk 3: Docs and Verification

### Task 5: Update docs and verify pipeline

**Files:**
- Modify: `README.md`
- Modify: `docs/2026-04-07-ym-mail-ingest-technical.md`

- [ ] Document spreadsheet `A/B` contract
- [ ] Document exact-grain merge rule
- [ ] Run JS + Python tests
- [ ] Commit and push
