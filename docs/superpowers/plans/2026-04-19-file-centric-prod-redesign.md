# Plan: File-Centric Prod Redesign

**Architecture:** Keep Turso as the backend, but restore the Supabase-era business model: idempotent file-centric raw registry, local-memory normalization, canonical sparse facts, and publish as a separate phase. Stop using destructive day reset as the default ingest path.

## Phase 1: Freeze the New Contract

### Task 1: Fix the target model in docs

- [ ] Add a new prod redesign spec tied to the original business requirements.
- [ ] State explicitly that backend migration must not change:
  - content-based dedupe;
  - file-centric identity;
  - `report_date` from file content;
  - sparse canonical facts;
  - stable topic goal slots;
  - `union` as publish layer.

### Task 2: Mark current migration debt as non-target

- [ ] Document that:
  - default day reset is legacy;
  - `pipeline_runs` is summary, not primary identity;
  - `ingest_rows` is not part of the target prod path.

## Phase 2: Make Raw Ingest Idempotent

### Task 3: Introduce stable raw file identity

Files:

- `turso/bootstrap_schema.sql`
- `appsscript-src/22_transport_turso.js`
- `tests/ym_mail_ingest.test.js`

- [ ] Add `raw_file_key` and `file_hash` fields to raw registry schema.
- [ ] Add uniqueness that prevents duplicate raw payload insertion for the same content-bearing file identity.
- [ ] Keep metadata fields needed for audit/debug.

### Task 4: Stop default destructive day reset

Files:

- `appsscript-src/22_transport_turso.js`
- `appsscript-src/23_runtime_settings.js`
- `appsscript-src/30_entrypoints.js`
- `tests/ym_mail_ingest.test.js`

- [ ] Remove day-reset as the default step in `run()` and `runMonthBackfill()`.
- [ ] Replace it with per-file idempotent upsert.
- [ ] Keep explicit operator repair/reset capability only if needed, off the default path.

### Task 5: Make `pipeline_runs` a touched-day summary only

Files:

- `appsscript-src/22_transport_turso.js`
- `turso/bootstrap_schema.sql`
- tests as needed

- [ ] Update `pipeline_runs` only as a summary of touched raw files.
- [ ] Do not treat `pipeline_runs` existence as proof that the day is fully rebuilt.
- [ ] Preserve backfill skip logic only for genuinely `ready` days.

## Phase 3: Remove Cloud Scratch Parse State

### Task 6: Stop using `ingest_rows` as the primary parse handoff

Files:

- `scripts/normalize/pipeline.py`
- `scripts/normalize/db.py`
- `scripts/normalize/turso_reads.py`
- `scripts/normalize/turso_writes.py`
- `tests/test_normalize_turso_layout.py`
- `tests/test_run_pipeline.py`

- [ ] Parse raw payloads in local memory.
- [ ] Feed canonical normalization directly from parsed rows in memory.
- [ ] Keep `ingest_rows` only as temporary compatibility/debug support until it can be removed.

### Task 7: Introduce honest file statuses

Files:

- schema + raw normalize code + tests

- [ ] Replace transitional ambiguous statuses with:
  - `raw_loaded`
  - `parse_ok`
  - `parse_skipped`
  - `parse_error`
- [ ] Make `pipeline_runs` statuses honest and phase-based.

## Phase 4: Split Operator Stages

### Task 8: Separate normalize and publish

Files:

- `scripts/run_pipeline.py`
- sync scripts
- related tests/docs

- [ ] Split the operator path into explicit phases:
  - prepare / normalize
  - publish DB layer
  - sync sheets
- [ ] Make sheet sync failure non-destructive to normalized DB state.

### Task 9: Improve observability

Files:

- `scripts/doctor_direct_turso.py`
- `scripts/run_pipeline.py`
- docs/tests

- [ ] Report file-level and day-level progress separately.
- [ ] Show dirty files, parse errors, and publish errors clearly.
- [ ] Make it obvious which phase is slow or failed.

## Verification

- [ ] JS tests cover:
  - content-based raw identity;
  - no default day reset;
  - idempotent raw ingest upsert behavior.
- [ ] Python tests cover:
  - local-memory parse path;
  - canonical fact rebuild for touched files;
  - no dependency on cloud `ingest_rows` for the main path.
- [ ] Docs describe one coherent prod contour.

## First Cut

This session should complete the first critical cut:

- add new spec + plan;
- add tests proving the new raw-ingest contract;
- implement stable raw file identity in Apps Script path;
- remove default day reset from Apps Script path;
- keep the rest of normalize flow working while deeper pipeline changes follow.
