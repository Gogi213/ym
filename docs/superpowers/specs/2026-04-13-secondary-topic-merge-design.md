# Secondary Topic Merge Design

## Goal

Support optional secondary report topics from column `B` of sheet `отчеты`, ingest them as explicitly linked to a primary topic from column `A`, and merge secondary rows into the primary dataset only when the row grain matches exactly.

## Design

- Column `A` remains the primary topic.
- Column `B` is an optional secondary topic for the same business report.
- Apps Script reads both columns and builds topic rules with:
  - `primaryTopic`
  - `matchedTopic`
  - `topicRole = primary | secondary`
- Ingest metadata sent to Supabase includes:
  - `primary_topic`
  - `matched_topic`
  - `topic_role`
- Raw ingest stores both the actual matched topic and the linked primary topic.
- Python normalization treats:
  - `primary` rows as the main dataset
  - `secondary` rows as supplemental rows
- Supplemental rows are merged into primary rows only on exact grain:
  - `report_date`
  - `utm_source`
  - `utm_medium`
  - `utm_campaign`
  - `utm_content`
  - `utm_term`
- If a secondary row does not find an exact primary match, it is excluded from operator output and counted as unmatched for diagnostics.
- Operator `union` continues to expose only primary-topic rows after merge.

## Non-Goals

- No fuzzy matching between primary and secondary rows.
- No automatic fallback if grain differs.
- No separate operator sheet for unmatched secondary rows in this change.

## Success Criteria

- Apps Script ingests both primary and secondary topics from the same spreadsheet.
- Raw layer records enough metadata to distinguish primary vs secondary files.
- Secondary goal-only rows augment primary rows when grain matches exactly.
- If grain does not match, no silent data fabrication occurs.
