from __future__ import annotations

from typing import Any, Dict, List, Sequence, Tuple

from .transform import build_pipeline_run_error_update, build_pipeline_run_ready_update


def delete_existing_rows_for_run(conn, run_date: str) -> List[Tuple[str, str]]:
    rows = conn.execute(
        """
        select distinct fr.topic, fr.row_hash
        from fact_rows fr
        join ingest_files f
          on f.id = fr.source_file_id
        where f.run_date = ?
          and fr.topic is not null
          and fr.row_hash is not null
        order by fr.topic, fr.row_hash
        """,
        (run_date,),
    ).fetchall()
    conn.execute(
        """
        delete from fact_rows
        where source_file_id in (
          select id
          from ingest_files
          where run_date = ?
        )
        """,
        (run_date,),
    )
    return [(str(row[0]), str(row[1])) for row in rows]


def mark_pipeline_run_ready(conn, run_date: str, *, files_count: int, fact_rows_count: int) -> None:
    payload = build_pipeline_run_ready_update(
        files_count=files_count,
        fact_rows_count=fact_rows_count,
    )
    conn.execute(
        """
        update pipeline_runs
        set
          normalized_files = ?,
          normalized_rows = ?,
          normalize_status = ?,
          normalized_at = current_timestamp,
          last_error = ?,
          updated_at = current_timestamp
        where run_date = ?
        """,
        (
            payload["normalized_files"],
            payload["normalized_rows"],
            payload["normalize_status"],
            payload["last_error"],
            run_date,
        ),
    )


def mark_pipeline_run_error(conn, run_date: str, error_message: str) -> None:
    payload = build_pipeline_run_error_update(error_message)
    conn.execute(
        """
        update pipeline_runs
        set
          normalize_status = ?,
          last_error = ?,
          updated_at = current_timestamp
        where run_date = ?
        """,
        (
            payload["normalize_status"],
            payload["last_error"],
            run_date,
        ),
    )


def upsert_topic_goal_slots(conn, records: Sequence[Dict[str, Any]]) -> None:
    if not records:
        return

    conn.executemany(
        """
        insert into topic_goal_slots (
          topic,
          goal_slot,
          source_header,
          goal_label,
          first_seen_file_id
        ) values (?, ?, ?, ?, ?)
        on conflict (topic, goal_slot) do update
        set
          source_header = excluded.source_header,
          goal_label = coalesce(topic_goal_slots.goal_label, excluded.goal_label),
          updated_at = current_timestamp
        """,
        [
            (
                row["topic"],
                row["goal_slot"],
                row["source_header"],
                row["goal_label"],
                row["first_seen_file_id"],
            )
            for row in records
        ],
    )


def insert_fact_rows(conn, rows: Sequence[Dict[str, Any]]) -> None:
    if not rows:
        return

    conn.executemany(
        """
        insert into fact_rows (
          fact_row_id,
          topic,
          source_file_id,
          source_row_index,
          report_date,
          report_date_from,
          report_date_to,
          message_date,
          layout_signature,
          row_hash,
          source_row_json
        ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                row["fact_row_id"],
                row["topic"],
                row["source_file_id"],
                row["source_row_index"],
                row.get("report_date"),
                row.get("report_date_from"),
                row.get("report_date_to"),
                row.get("message_date"),
                row["layout_signature"],
                row["row_hash"],
                row["source_row_json"],
            )
            for row in rows
        ],
    )


def insert_fact_dimensions(conn, rows: Sequence[Dict[str, Any]]) -> None:
    if not rows:
        return

    conn.executemany(
        """
        insert into fact_dimensions (
          fact_row_id,
          dimension_key,
          dimension_value
        ) values (?, ?, ?)
        """,
        [
            (
                row["fact_row_id"],
                row["dimension_key"],
                row.get("dimension_value"),
            )
            for row in rows
        ],
    )


def insert_fact_metrics(conn, rows: Sequence[Dict[str, Any]]) -> None:
    if not rows:
        return

    conn.executemany(
        """
        insert into fact_metrics (
          fact_row_id,
          metric_key,
          metric_value
        ) values (?, ?, ?)
        """,
        [
            (
                row["fact_row_id"],
                row["metric_key"],
                row.get("metric_value"),
            )
            for row in rows
        ],
    )


__all__ = [
    "delete_existing_rows_for_run",
    "insert_fact_dimensions",
    "insert_fact_metrics",
    "insert_fact_rows",
    "mark_pipeline_run_error",
    "mark_pipeline_run_ready",
    "upsert_topic_goal_slots",
]
