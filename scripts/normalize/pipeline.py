from __future__ import annotations

import time
from typing import Any, Callable, Dict, Optional, Sequence

from .common import emit_log, public_payload
from .db import (
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
from .fields import build_topic_goal_slot_records
from .transform import build_affected_row_keys, build_normalized_payloads, collect_goal_slots


def _sync_if_supported(conn) -> None:
    sync = getattr(conn, "sync", None)
    if callable(sync):
        sync()


def finalize_normalized_runs(
    normalized_results: Sequence[Dict[str, Any]],
    logger: Optional[Callable[[str, Dict[str, Any]], None]] = None,
) -> None:
    started_at = time.perf_counter()

    def phase(name: str, **payload: Any) -> None:
        emit_log(
            logger,
            name,
            {
                "elapsed_ms": round((time.perf_counter() - started_at) * 1000, 2),
                **payload,
            },
        )

    successful_results = [result for result in normalized_results if not str(result.get("error") or "").strip()]
    if not successful_results:
        return

    affected_row_keys = sorted(
        {
            (str(topic), str(row_hash))
            for result in successful_results
            for topic, row_hash in result.get("_affected_row_keys", [])
            if str(topic or "").strip() and str(row_hash or "").strip()
        }
    )
    run_dates = []
    seen_run_dates = set()
    for result in successful_results:
        run_date = str(result.get("run_date") or "").strip()
        if run_date and run_date not in seen_run_dates:
            run_dates.append(run_date)
            seen_run_dates.add(run_date)

    phase(
        "finalize_normalized_runs_started",
        run_dates=run_dates,
        run_count=len(run_dates),
        row_keys=len(affected_row_keys),
    )

    with connect_db() as conn:
        phase("finalize_refresh_flags_started", row_keys=len(affected_row_keys))
        refresh_current_flags_for_row_keys(conn, affected_row_keys)
        phase("finalize_refresh_flags_finished")

        for run_date in run_dates:
            phase("finalize_refresh_operator_export_started", run_date=run_date)
            refresh_operator_export_rows_for_run(conn, run_date)
            phase("finalize_refresh_operator_export_finished", run_date=run_date)

        for result in successful_results:
            run_date = str(result["run_date"])
            phase("finalize_mark_ready_started", run_date=run_date)
            mark_pipeline_run_ready(
                conn,
                run_date,
                files_count=int(result.get("files") or 0),
                fact_rows_count=int(result.get("fact_rows") or 0),
            )
            phase("finalize_mark_ready_finished", run_date=run_date)

        phase("finalize_commit_started")
        conn.commit()
        _sync_if_supported(conn)
        phase("finalize_commit_finished")


def normalize_run(
    run_date: str,
    logger: Optional[Callable[[str, Dict[str, Any]], None]] = None,
    *,
    defer_finalize: bool = False,
    skip_delete_existing: bool = False,
) -> Dict[str, int]:
    started_at = time.perf_counter()

    def phase(name: str, **payload: Any) -> None:
        emit_log(
            logger,
            name,
            {
                "run_date": run_date,
                "elapsed_ms": round((time.perf_counter() - started_at) * 1000, 2),
                **payload,
            },
        )

    phase("normalize_connecting")
    with connect_db() as conn:
        try:
            phase("normalize_fetch_ingested_files_started")
            files = fetch_ingested_files(conn, run_date)
            phase("normalize_fetch_ingested_files_finished", files=len(files))
            topics = sorted({file_row["matched_topic"] for file_row in files})
            file_ids = [str(file_row["id"]) for file_row in files]

            phase("normalize_fetch_rows_started", file_ids=len(file_ids))
            rows_by_file_id = fetch_ingest_rows(conn, file_ids)
            phase(
                "normalize_fetch_rows_finished",
                files_with_rows=len(rows_by_file_id),
                raw_rows=sum(len(rows) for rows in rows_by_file_id.values()),
            )
            phase("normalize_fetch_payloads_started", file_ids=len(file_ids))
            payloads_by_file_id = fetch_ingest_payloads(conn, file_ids)
            phase("normalize_fetch_payloads_finished", payloads=len(payloads_by_file_id))
            phase("normalize_fetch_goal_slots_started", topics=len(topics))
            existing_goal_slots = fetch_existing_goal_slots(conn, topics)
            phase("normalize_fetch_goal_slots_finished", topics_with_slots=len(existing_goal_slots))

            phase("normalize_collect_goal_slots_started")
            goal_slots_by_topic, first_seen_file_ids = collect_goal_slots(files, existing_goal_slots)
            phase(
                "normalize_collect_goal_slots_finished",
                topics=len(goal_slots_by_topic),
                slots=sum(len(topic_slots) for topic_slots in goal_slots_by_topic.values()),
            )
            phase("normalize_build_goal_slot_records_started")
            topic_goal_slot_records = build_topic_goal_slot_records(
                goal_slots_by_topic=goal_slots_by_topic,
                first_seen_file_ids=first_seen_file_ids,
            )
            phase("normalize_build_goal_slot_records_finished", goal_slot_records=len(topic_goal_slot_records))
            phase("normalize_build_payloads_started")
            fact_rows, fact_dimensions, fact_metrics, secondary_merge_stats = build_normalized_payloads(
                files,
                rows_by_file_id,
                payloads_by_file_id,
                goal_slots_by_topic,
            )
            phase(
                "normalize_build_payloads_finished",
                fact_rows=len(fact_rows),
                fact_dimensions=len(fact_dimensions),
                fact_metrics=len(fact_metrics),
                **secondary_merge_stats,
            )
            if skip_delete_existing:
                deleted_row_keys = []
                phase("normalize_delete_existing_skipped")
            else:
                phase("normalize_delete_existing_rows_started")
                deleted_row_keys = delete_existing_rows_for_run(conn, run_date)
                phase("normalize_delete_existing_rows_finished", row_keys=len(deleted_row_keys))
            affected_row_keys = build_affected_row_keys(
                existing_keys=deleted_row_keys,
                fact_rows=fact_rows,
            )
            phase("normalize_build_affected_row_keys_finished", row_keys=len(affected_row_keys))
            phase("normalize_upsert_goal_slots_started", goal_slot_records=len(topic_goal_slot_records))
            upsert_topic_goal_slots(conn, topic_goal_slot_records)
            phase("normalize_upsert_goal_slots_finished")
            phase("normalize_insert_fact_rows_started", fact_rows=len(fact_rows))
            insert_fact_rows(conn, fact_rows)
            phase("normalize_insert_fact_rows_finished")
            phase("normalize_insert_fact_dimensions_started", fact_dimensions=len(fact_dimensions))
            insert_fact_dimensions(conn, fact_dimensions)
            phase("normalize_insert_fact_dimensions_finished")
            phase("normalize_insert_fact_metrics_started", fact_metrics=len(fact_metrics))
            insert_fact_metrics(conn, fact_metrics)
            phase("normalize_insert_fact_metrics_finished")
            if not defer_finalize:
                phase("normalize_refresh_flags_started", row_keys=len(affected_row_keys))
                refresh_current_flags_for_row_keys(conn, affected_row_keys)
                phase("normalize_refresh_flags_finished")
                phase("normalize_refresh_operator_export_started")
                refresh_operator_export_rows_for_run(conn, run_date)
                phase("normalize_refresh_operator_export_finished")
                phase("normalize_mark_ready_started")
                mark_pipeline_run_ready(
                    conn,
                    run_date,
                    files_count=len(files),
                    fact_rows_count=len(fact_rows),
                )
                phase("normalize_mark_ready_finished")
            phase("normalize_commit_started")
            conn.commit()
            _sync_if_supported(conn)
            phase("normalize_commit_finished")
        except Exception as error:
            conn.rollback()
            mark_pipeline_run_error(conn, run_date, str(error))
            conn.commit()
            raise

    result = {
        "files": len(files),
        "topics": len(topics),
        "fact_rows": len(fact_rows),
        "fact_dimensions": len(fact_dimensions),
        "fact_metrics": len(fact_metrics),
        "goal_slots": len(topic_goal_slot_records),
        **secondary_merge_stats,
    }
    if defer_finalize:
        result["_affected_row_keys"] = affected_row_keys
    phase("normalize_finished", **public_payload(result))
    return result
