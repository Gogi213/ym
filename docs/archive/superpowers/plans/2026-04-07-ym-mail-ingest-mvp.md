# YM Mail Ingest MVP Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build an Apps Script intake flow that scans today's Gmail messages in `ya-stats@solta.io`, validates `xlsx/csv` attachments for a real UTM header table, saves valid files into a dated staging folder, and writes manifest rows into the shared spreadsheet.

**Architecture:** Keep the Apps Script integration layer thin and push matching, parsing, table-detection, filename generation, and manifest-row shaping into plain JavaScript functions that can be tested locally with Node's built-in test runner. The runtime entrypoint will depend on Apps Script globals only for Gmail, Drive, Sheets, Utilities, and Session access.

**Tech Stack:** Google Apps Script (V8), plain JavaScript, Node.js built-in test runner, `assert`

---

## Chunk 1: Project Skeleton And Test Harness

### Task 1: Create repo skeleton

**Files:**
- Create: `appsscript.json`
- Create: `src/ym_mail_ingest.js`
- Create: `tests/ym_mail_ingest.test.js`
- Create: `package.json`

- [ ] **Step 1: Write the failing smoke test**

```javascript
const test = require('node:test');
const assert = require('node:assert/strict');
const ingest = require('../src/ym_mail_ingest.js');

test('exports core ingest helpers', () => {
  assert.equal(typeof ingest.normalizeText_, 'function');
  assert.equal(typeof ingest.subjectMatchesTopics_, 'function');
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `node --test tests/ym_mail_ingest.test.js`
Expected: FAIL because `src/ym_mail_ingest.js` does not exist yet.

- [ ] **Step 3: Write minimal implementation**

Create:
- `package.json` with a `test` script using `node --test`
- `src/ym_mail_ingest.js` exporting placeholder functions
- `appsscript.json` with V8 runtime and timezone placeholder

- [ ] **Step 4: Run test to verify it passes**

Run: `node --test tests/ym_mail_ingest.test.js`
Expected: PASS

## Chunk 2: Topic Matching And Header Normalization

### Task 2: Implement subject normalization and topic matching

**Files:**
- Modify: `src/ym_mail_ingest.js`
- Modify: `tests/ym_mail_ingest.test.js`

- [ ] **Step 1: Write the failing tests**

Add tests for:
- lowercasing and `ё -> е`
- punctuation collapsing
- tokenization
- topic match requiring all topic words
- no sender filter behavior implied by message selection logic

- [ ] **Step 2: Run test to verify it fails**

Run: `node --test tests/ym_mail_ingest.test.js`
Expected: FAIL on missing normalization and matching behavior.

- [ ] **Step 3: Write minimal implementation**

Implement:
- `normalizeText_`
- `tokenizeTopic_`
- `loadTopicRulesFromValues_`
- `subjectMatchesTopics_`
- `findMatchedTopic_`

- [ ] **Step 4: Run test to verify it passes**

Run: `node --test tests/ym_mail_ingest.test.js`
Expected: PASS

### Task 3: Implement header normalization and table-block heuristics

**Files:**
- Modify: `src/ym_mail_ingest.js`
- Modify: `tests/ym_mail_ingest.test.js`

- [ ] **Step 1: Write the failing tests**

Add tests for:
- `utm source` -> `utm_source`
- `UTM-Campaign` -> `utm_campaign`
- header row requiring at least one UTM column
- table block requiring 2+ non-empty header cells and 2+ non-empty data cells
- rejecting stray narrative rows containing `utm`

- [ ] **Step 2: Run test to verify it fails**

Run: `node --test tests/ym_mail_ingest.test.js`
Expected: FAIL on missing header detection helpers.

- [ ] **Step 3: Write minimal implementation**

Implement:
- `normalizeHeaderCell_`
- `rowHasUtmHeader_`
- `countNonEmptyCells_`
- `findTableBlockInRows_`

- [ ] **Step 4: Run test to verify it passes**

Run: `node --test tests/ym_mail_ingest.test.js`
Expected: PASS

## Chunk 3: CSV And XLSX Content Parsing

### Task 4: Implement CSV parsing helpers

**Files:**
- Modify: `src/ym_mail_ingest.js`
- Modify: `tests/ym_mail_ingest.test.js`

- [ ] **Step 1: Write the failing tests**

Add tests for:
- delimiter detection across `,`, `;`, and tab
- BOM stripping
- finding a valid header/data block after narrative lines
- rejecting CSV without a valid UTM header block

- [ ] **Step 2: Run test to verify it fails**

Run: `node --test tests/ym_mail_ingest.test.js`
Expected: FAIL on missing CSV behavior.

- [ ] **Step 3: Write minimal implementation**

Implement:
- `stripBom_`
- `detectDelimiter_`
- `parseCsvText_`
- `csvTextHasValidTableBlock_`

- [ ] **Step 4: Run test to verify it passes**

Run: `node --test tests/ym_mail_ingest.test.js`
Expected: PASS

### Task 5: Implement XLSX XML parsing helpers

**Files:**
- Modify: `src/ym_mail_ingest.js`
- Modify: `tests/ym_mail_ingest.test.js`

- [ ] **Step 1: Write the failing tests**

Add tests for:
- workbook rel parsing
- sheet path resolution
- shared string parsing
- extracting row cells from sheet XML
- finding a valid UTM header table block in sheet rows

- [ ] **Step 2: Run test to verify it fails**

Run: `node --test tests/ym_mail_ingest.test.js`
Expected: FAIL on missing XLSX helpers.

- [ ] **Step 3: Write minimal implementation**

Implement:
- `parseWorkbookRels_`
- `parseSheetPaths_`
- `resolveXlsxPath_`
- `parseSharedStrings_`
- `extractXlsxRowCells_`
- `sheetXmlHasValidTableBlock_`

- [ ] **Step 4: Run test to verify it passes**

Run: `node --test tests/ym_mail_ingest.test.js`
Expected: PASS

## Chunk 4: Manifest And Storage Shaping

### Task 6: Implement storage naming and manifest shaping

**Files:**
- Modify: `src/ym_mail_ingest.js`
- Modify: `tests/ym_mail_ingest.test.js`

- [ ] **Step 1: Write the failing tests**

Add tests for:
- safe file names
- safe subject names
- storage name shape including timestamp
- selecting latest message per topic
- manifest row shaping for `VALID` and `SKIP`

- [ ] **Step 2: Run test to verify it fails**

Run: `node --test tests/ym_mail_ingest.test.js`
Expected: FAIL on missing storage/manifest helpers.

- [ ] **Step 3: Write minimal implementation**

Implement:
- `safeFolderName_`
- `safeFileName_`
- `buildStoredFilename_`
- `markLatestMessagesByTopic_`
- `buildManifestRow_`
- `getManifestHeaders_`

- [ ] **Step 4: Run test to verify it passes**

Run: `node --test tests/ym_mail_ingest.test.js`
Expected: PASS

## Chunk 5: Apps Script Runtime Integration

### Task 7: Add Apps Script orchestration

**Files:**
- Modify: `src/ym_mail_ingest.js`
- Modify: `tests/ym_mail_ingest.test.js`

- [ ] **Step 1: Write the failing tests**

Add tests around thin runtime helpers using fake adapters for:
- daily folder name formatting
- manifest cleanup filtering current run date
- choosing only `xlsx/csv` candidate attachments by metadata

- [ ] **Step 2: Run test to verify it fails**

Run: `node --test tests/ym_mail_ingest.test.js`
Expected: FAIL on missing runtime helper behavior.

- [ ] **Step 3: Write minimal implementation**

Implement Apps Script-oriented helpers:
- `getTodayRangeQuery_`
- `formatRunDate_`
- `detectAttachmentType_`
- `ensureDailyFolder_`
- `ensureManifestSheet_`
- `clearManifestRowsForDate_`
- `run()`

`run()` should:
- load topics from `отчеты`
- read today's candidate Gmail messages
- match topics at message level
- mark latest message per topic
- validate each `xlsx/csv`
- save `VALID` files into the dated staging folder
- write manifest rows
- log a summary

- [ ] **Step 4: Run test to verify it passes**

Run: `node --test tests/ym_mail_ingest.test.js`
Expected: PASS

## Chunk 6: Verification

### Task 8: Run the local test suite

**Files:**
- Test: `tests/ym_mail_ingest.test.js`

- [ ] **Step 1: Run test suite**

Run: `npm test`
Expected: PASS

- [ ] **Step 2: Review exports and Apps Script entrypoint**

Run: `node -e "const ingest=require('./src/ym_mail_ingest.js'); console.log(Object.keys(ingest).sort())"`
Expected: prints stable helper exports including `run`

- [ ] **Step 3: Review git status**

Run: `git status --short`
Expected: only intended new project files
