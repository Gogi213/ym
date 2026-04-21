from __future__ import annotations

from math import ceil
from typing import Any, Callable, Dict, Iterable, Sequence

from .transform import build_pipeline_run_error_update, build_pipeline_run_ready_update


ProgressCallback = Callable[[Dict[str, Any]], None]

MAX_MULTI_VALUES_PARAMS = 5000


def _iter_chunks(rows: Sequence[Any], chunk_size: int) -> Iterable[Sequence[Any]]:
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")
    for start in range(0, len(rows), chunk_size):
        yield rows[start : start + chunk_size]


def _emit_progress(
    progress: ProgressCallback | None,
    *,
    processed: int,
    total: int,
    chunk_index: int,
    chunk_count: int,
    ) -> None:
    if progress is None or total <= 0:
        return
    progress(
        {
            "processed": processed,
            "total": total,
            "chunk_index": chunk_index,
            "chunk_count": chunk_count,
            "percent": round((processed / total) * 100, 2),
        }
    )


def _multi_values_chunk_size(column_count: int, preferred_chunk_size: int) -> int:
    if column_count <= 0:
        raise ValueError("column_count must be positive")
    return max(1, min(preferred_chunk_size, MAX_MULTI_VALUES_PARAMS // column_count))


def _build_multi_values_sql(*, table_name: str, columns: Sequence[str], row_count: int, suffix_sql: str = "") -> str:
    placeholders = "(" + ", ".join(["?"] * len(columns)) + ")"
    values_sql = ", ".join([placeholders] * row_count)
    suffix = f" {suffix_sql.strip()}" if suffix_sql.strip() else ""
    return f"insert into {table_name} ({', '.join(columns)}) values {values_sql}{suffix}"


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
    payload_rows = [
        (
            row["topic"],
            row["goal_slot"],
            row["source_header"],
            row["goal_label"],
            row["first_seen_file_id"],
        )
        for row in records
    ]
    columns = ("topic", "goal_slot", "source_header", "goal_label", "first_seen_file_id")
    chunk_size = _multi_values_chunk_size(len(columns), len(payload_rows))
    suffix_sql = """
    on conflict (topic, goal_slot) do update
    set
      source_header = excluded.source_header,
      goal_label = coalesce(topic_goal_slots.goal_label, excluded.goal_label),
      updated_at = current_timestamp
    """
    for chunk in _iter_chunks(payload_rows, chunk_size):
        conn.execute(
            _build_multi_values_sql(
                table_name="topic_goal_slots",
                columns=columns,
                row_count=len(chunk),
                suffix_sql=suffix_sql,
            ),
            tuple(value for row in chunk for value in row),
        )


__all__ = [
    "mark_pipeline_run_error",
    "mark_pipeline_run_ready",
    "upsert_topic_goal_slots",
]
