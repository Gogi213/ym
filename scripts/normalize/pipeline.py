from __future__ import annotations

import json
import time
from typing import Any, Callable, Dict, Optional, Sequence

from .common import emit_log, public_payload
from .db import (
    connect_db,
    fetch_existing_goal_slots,
    fetch_ingest_payloads,
    fetch_run_files,
    mark_pipeline_run_error,
    mark_pipeline_run_ready,
    replace_operator_export_rows_for_run,
    upsert_topic_goal_slots,
)
from .fields import build_topic_goal_slot_records
from .operator_export_runtime import build_operator_export_rows, prepare_files_for_operator_export
from .transform import collect_goal_slots


def _update_ingest_file_metadata(conn, metadata_updates: Sequence[Dict[str, Any]]) -> None:
    if not metadata_updates:
        return
    conn.executemany(
        """
        update ingest_files
        set status = ?,
            header_json = ?,
            row_count = ?,
            error_text = ?
        where id = ?
        """,
        [
            (
                str(update.get("status") or ""),
                json.dumps(update.get("header_json") or [], ensure_ascii=False),
                int(update.get("row_count") or 0),
                update.get("error_text"),
                str(update.get("file_id") or ""),
            )
            for update in metadata_updates
        ],
    )


def _refresh_pipeline_run_after_prepare(conn, run_date: str, *, total_files: int, raw_rows: int, ingested_files: int) -> None:
    conn.execute(
        """
        update pipeline_runs
        set raw_files = ?,
            raw_rows = ?,
            normalized_files = 0,
            normalized_rows = 0,
            normalize_status = ?,
            last_ingest_at = current_timestamp,
            normalized_at = null,
            last_error = null,
            updated_at = current_timestamp
        where run_date = ?
        """,
        (
            total_files,
            raw_rows,
            "pending_normalize" if ingested_files > 0 else "raw_only",
            run_date,
        ),
    )


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
    )

    with connect_db() as conn:
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
            phase("normalize_fetch_run_files_started")
            run_files = fetch_run_files(conn, run_date)
            phase("normalize_fetch_run_files_finished", files=len(run_files))
            file_ids = [str(file_row["id"]) for file_row in run_files]

            phase("normalize_fetch_payloads_started", file_ids=len(file_ids))
            payloads_by_file_id = fetch_ingest_payloads(conn, file_ids)
            phase("normalize_fetch_payloads_finished", payloads=len(payloads_by_file_id))
            phase("normalize_prepare_raw_files_started", files=len(run_files))
            files, rows_by_file_id, metadata_updates, prepared_stats = prepare_files_for_operator_export(
                run_files,
                payloads_by_file_id,
            )
            phase("normalize_prepare_raw_files_finished", **prepared_stats)
            phase("normalize_update_ingest_file_metadata_started", files=len(metadata_updates))
            _update_ingest_file_metadata(conn, metadata_updates)
            phase("normalize_update_ingest_file_metadata_finished")
            phase("normalize_refresh_pipeline_run_started")
            _refresh_pipeline_run_after_prepare(
                conn,
                run_date,
                total_files=len(run_files),
                raw_rows=int(prepared_stats.get("raw_rows") or 0),
                ingested_files=int(prepared_stats.get("ingested_files") or 0),
            )
            phase("normalize_refresh_pipeline_run_finished")
            topics = sorted({file_row["matched_topic"] for file_row in files})
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
            phase("normalize_build_operator_export_rows_started")
            operator_export_rows, operator_export_stats = build_operator_export_rows(
                files,
                rows_by_file_id,
                payloads_by_file_id,
                goal_slots_by_topic,
            )
            phase(
                "normalize_build_operator_export_rows_finished",
                operator_export_rows=len(operator_export_rows),
                **operator_export_stats,
            )
            phase("normalize_upsert_goal_slots_started", goal_slot_records=len(topic_goal_slot_records))
            upsert_topic_goal_slots(conn, topic_goal_slot_records)
            phase("normalize_upsert_goal_slots_finished")
            phase("normalize_replace_operator_export_rows_started", operator_export_rows=len(operator_export_rows))
            replace_operator_export_rows_for_run(
                conn,
                run_date,
                operator_export_rows,
                progress=lambda payload: phase("normalize_replace_operator_export_rows_progress", **payload),
            )
            phase("normalize_replace_operator_export_rows_finished")
            if not defer_finalize:
                phase("normalize_mark_ready_started")
                mark_pipeline_run_ready(
                    conn,
                    run_date,
                    files_count=len(files),
                    fact_rows_count=int(operator_export_stats.get("current_rows") or 0),
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
        "fact_rows": int(operator_export_stats.get("current_rows") or 0),
        "operator_export_rows": len(operator_export_rows),
        "goal_slots": len(topic_goal_slot_records),
        **operator_export_stats,
    }
    phase("normalize_finished", **public_payload(result))
    return result
