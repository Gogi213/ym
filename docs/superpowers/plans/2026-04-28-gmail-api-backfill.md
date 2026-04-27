# Gmail API Backfill Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace `GmailApp`-based month backfill reads with Gmail API reads that only fetch missing report days from Turso.

**Architecture:** Keep the existing Turso ingest path and topic matching, but replace Gmail discovery and attachment download with Gmail API helpers. Month backfill first checks which `run_date` values already exist, then queries Gmail only for missing days using the “message day is report day + 1” rule with subject-date override.

**Tech Stack:** Google Apps Script, Advanced Gmail Service, Turso HTTP pipeline, Node test runner

---

### Task 1: Lock the new Gmail API behavior in tests

**Files:**
- Modify: `tests/load_code.js`
- Modify: `tests/ym_mail_ingest.test.js`

- [ ] Add failing tests for Gmail API query construction and report-date inference.
- [ ] Add failing tests that `runMonthBackfill()` no longer performs month-wide Gmail pre-scan.
- [ ] Run `node --test tests/ym_mail_ingest.test.js` and verify the new tests fail for the expected missing behavior.

### Task 2: Add Gmail API discovery helpers

**Files:**
- Modify: `Code.js`
- Modify: `appsscript-src/30_entrypoints.js`

- [ ] Add helpers for per-day Gmail API query windows, report date inference, metadata parsing, and message candidate selection.
- [ ] Keep subject `за DD.MM.YYYY` as the primary report-date source.
- [ ] Use message date minus one day in script timezone as fallback when subject date is absent.

### Task 3: Switch ingest execution from GmailApp to Gmail API

**Files:**
- Modify: `Code.js`
- Modify: `appsscript-src/30_entrypoints.js`

- [ ] Replace `GmailApp.search`, `thread.getMessages()`, and `message.getAttachments()` usage with `Gmail.Users.Messages.list/get/attachments.get`.
- [ ] Fetch lightweight metadata first; fetch attachment payloads only for matched candidates.
- [ ] Preserve existing Turso dedupe and adaptive upload behavior.

### Task 4: Update runtime checks and verification

**Files:**
- Modify: `Code.js`
- Modify: `appsscript-src/30_entrypoints.js`
- Modify: `tests/load_code.js`
- Modify: `tests/ym_mail_ingest.test.js`

- [ ] Update runtime guards to require the Advanced Gmail Service global.
- [ ] Run `node --test tests/ym_mail_ingest.test.js` until green.
- [ ] Review `git diff` to ensure only intended files are included.

### Task 5: Commit and push safe changes

**Files:**
- Modify: git index/history only

- [ ] Stage only the files changed for this feature.
- [ ] Commit with a focused message.
- [ ] Push the branch.
- [ ] Report whether unrelated pre-existing worktree changes prevent a fully clean tree after push.
