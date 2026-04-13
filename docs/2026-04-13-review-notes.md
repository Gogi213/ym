# 2026-04-13 Review Notes

## Scope

Review cycle over the current YM pipeline with focus on:

- smell
- design
- performance

The goal of this memo is not to restate the architecture, but to capture the main risks and known limits in a form that survives chat history.

## Smell Review

### 1. Apps Script remains transport-heavy

`Code.js` is intentionally thinner than before, but it still owns:

- spreadsheet topic loading
- Gmail search
- topic matching
- attachment batching
- backfill orchestration
- Supabase REST checks

This is acceptable for current pre-prod scope, but it means Apps Script is still operational glue, not a minimal edge collector.

### 2. Python operator sync is tightly coupled to Google Sheets

The sync scripts are correct for the current workflow, but `union` and `отчеты` are still rebuilt as sheets, not exposed as a reusable API/reporting layer.

That is fine for operators, but it keeps Sheets as a hard runtime dependency.

## Design Review

### 1. Exact-grain merge for secondary topics is correct, but narrow

The new `primary_topic / topic_role` contract is the right shape.

However, the merge logic is intentionally strict:

- `report_date`
- `report_date_from`
- `report_date_to`
- `utm_source`
- `utm_medium`
- `utm_campaign`
- `utm_content`
- `utm_term`

This is defensible because it avoids silent bad joins.

It also means some business pairs will never merge if their underlying reports are emitted at different grain.

That is not a bug. It is the correct failure mode for an exact-join design.

### 2. Operator `union` is presentation, not source of truth

This is the right decision.

`union` already:

- collapses `utm_term`
- collapses `utm_content`
- converts rate/average metrics into additive values

This makes it usable for operators, but it also means it must never be treated as raw audit data.

That boundary should stay hard.

## Performance Review

### 1. Full rebuild cost is real

The slow path is not one SQL query. It is the sum of:

- per-day normalization
- fact table replacement
- goal mapping rebuild
- `union` sync to Google Sheets
- `pipeline_status` sync

On a month-sized rebuild this naturally becomes multi-minute work.

### 2. The expensive path is operationally valid, but bad for ad-hoc validation

Using `run_pipeline.py` to validate one topic or one pair of topics is the wrong tool.

For narrow validation the right tool is:

- direct SQL on raw layer
- or `normalize_supabase.py --run-date ...`

### 3. A 5x speedup is unlikely without changing the execution model

Current non-architectural optimizations already include:

- COPY-style inserts in normalizer
- skip full sheet sync on no-op pipeline runs
- phase logs
- DB timeouts

Further major speedup would likely require one of:

- incremental normalized updates instead of full day rebuilds
- more selective sheet sync
- asynchronous job execution / queueing
- reducing Google Sheets as part of the hot path

Those are architecture-level changes, not simple cleanup.

## Current Recommendation

Treat the current pipeline as:

- correct enough for pre-prod
- explicit about its failure modes
- not optimized for repeated broad rebuilds during exploratory debugging

Use full pipeline runs for real refreshes.
Use targeted normalization and SQL for narrow validation work.
