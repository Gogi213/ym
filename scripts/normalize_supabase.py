from __future__ import annotations

import argparse
import json
from typing import Any, Dict

from scripts.normalize.common import emit_log, public_payload
from scripts.normalize.db import (
    connect_db,
    delete_existing_rows_for_run,
    fetch_existing_goal_slots,
    fetch_ingest_payloads,
    fetch_ingest_rows,
    fetch_ingested_files,
    insert_fact_dimensions,
    insert_fact_metrics,
    insert_fact_rows,
    mark_pipeline_run_error,
    mark_pipeline_run_ready,
    refresh_current_flags_for_row_keys,
    refresh_operator_export_rows_for_run,
    upsert_topic_goal_slots,
)
from scripts.normalize.fields import (
    assign_goal_slots,
    build_fact_payload,
    build_layout_signature,
    build_topic_goal_slot_records,
    canonical_field_for_header,
    extract_report_date,
    extract_report_period_from_payload,
    extract_report_period_from_text,
    header_affects_row_identity,
    normalize_header,
    parse_duration_to_seconds,
    parse_metric_value,
)
from scripts.normalize.pipeline import finalize_normalized_runs, normalize_run
from scripts.normalize.transform import (
    build_affected_row_keys,
    build_merge_key,
    build_normalized_payloads,
    build_pipeline_run_error_update,
    build_pipeline_run_ready_update,
    collect_goal_slots,
    merge_secondary_payloads_into_primary,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Normalize raw Supabase ingest rows into canonical fact tables.")
    parser.add_argument("--run-date", required=True, help="Target run date in YYYY-MM-DD format.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    def cli_logger(phase: str, payload: Dict[str, Any]) -> None:
        print(json.dumps({"phase": phase, **payload}, ensure_ascii=False), flush=True)

    result = normalize_run(args.run_date, logger=cli_logger)
    print(
        json.dumps({"ok": True, "run_date": args.run_date, **public_payload(result)}, ensure_ascii=False),
        flush=True,
    )


__all__ = [
    "assign_goal_slots",
    "build_affected_row_keys",
    "build_fact_payload",
    "build_layout_signature",
    "build_merge_key",
    "build_normalized_payloads",
    "build_pipeline_run_error_update",
    "build_pipeline_run_ready_update",
    "build_topic_goal_slot_records",
    "canonical_field_for_header",
    "collect_goal_slots",
    "connect_db",
    "delete_existing_rows_for_run",
    "emit_log",
    "extract_report_date",
    "extract_report_period_from_payload",
    "extract_report_period_from_text",
    "fetch_existing_goal_slots",
    "fetch_ingest_payloads",
    "fetch_ingest_rows",
    "fetch_ingested_files",
    "finalize_normalized_runs",
    "header_affects_row_identity",
    "insert_fact_dimensions",
    "insert_fact_metrics",
    "insert_fact_rows",
    "mark_pipeline_run_error",
    "mark_pipeline_run_ready",
    "merge_secondary_payloads_into_primary",
    "normalize_header",
    "normalize_run",
    "parse_duration_to_seconds",
    "parse_metric_value",
    "public_payload",
    "refresh_current_flags_for_row_keys",
    "refresh_operator_export_rows_for_run",
    "upsert_topic_goal_slots",
]


if __name__ == "__main__":
    main()
