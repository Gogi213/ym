from __future__ import annotations

from math import ceil
from typing import Any, Callable, Dict, Iterable, List, Sequence, Tuple

from .transform import build_pipeline_run_error_update, build_pipeline_run_ready_update


ProgressCallback = Callable[[Dict[str, Any]], None]

DEFAULT_DELETE_CHUNK_SIZE = 25
DEFAULT_FACT_ROWS_CHUNK_SIZE = 500
DEFAULT_FACT_DIMENSIONS_CHUNK_SIZE = 2000
DEFAULT_FACT_METRICS_CHUNK_SIZE = 2000
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


def delete_existing_rows_for_run(
    conn,
    run_date: str,
    *,
    progress: ProgressCallback | None = None,
    chunk_size: int = DEFAULT_DELETE_CHUNK_SIZE,
) -> List[Tuple[str, str]]:
    file_ids = [
        str(row[0])
        for row in conn.execute(
            """
            select id
            from ingest_files
            where run_date = ?
            order by id
            """,
            (run_date,),
        ).fetchall()
    ]
    if not file_ids:
        return []

    placeholders = ", ".join(["?"] * len(file_ids))
    rows = conn.execute(
        f"""
        select distinct fr.topic, fr.row_hash
        from fact_rows fr
        where fr.source_file_id in ({placeholders})
          and fr.topic is not null
          and fr.row_hash is not null
        order by fr.topic, fr.row_hash
        """,
        tuple(file_ids),
    ).fetchall()
    # Turso's HTTP pipeline path can leave rows behind when deleting through a run_date subquery.
    # Delete by explicit file ids so reruns clean partial state deterministically.
    chunk_count = ceil(len(file_ids) / chunk_size)
    processed = 0
    for chunk_index, chunk in enumerate(_iter_chunks(file_ids, chunk_size), start=1):
        conn.executemany(
            """
            delete from fact_rows
            where source_file_id = ?
            """,
            [(file_id,) for file_id in chunk],
        )
        processed += len(chunk)
        _emit_progress(
            progress,
            processed=processed,
            total=len(file_ids),
            chunk_index=chunk_index,
            chunk_count=chunk_count,
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


def insert_fact_rows(
    conn,
    rows: Sequence[Dict[str, Any]],
    *,
    progress: ProgressCallback | None = None,
    chunk_size: int = DEFAULT_FACT_ROWS_CHUNK_SIZE,
) -> None:
    if not rows:
        return

    payload_rows = [
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
    ]
    chunk_count = ceil(len(payload_rows) / chunk_size)
    processed = 0
    for chunk_index, chunk in enumerate(_iter_chunks(payload_rows, chunk_size), start=1):
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
            chunk,
        )
        processed += len(chunk)
        _emit_progress(
            progress,
            processed=processed,
            total=len(payload_rows),
            chunk_index=chunk_index,
            chunk_count=chunk_count,
        )


def insert_fact_dimensions(
    conn,
    rows: Sequence[Dict[str, Any]],
    *,
    progress: ProgressCallback | None = None,
    chunk_size: int = DEFAULT_FACT_DIMENSIONS_CHUNK_SIZE,
) -> None:
    if not rows:
        return

    payload_rows = [
        (
            row["fact_row_id"],
            row["dimension_key"],
            row.get("dimension_value"),
        )
        for row in rows
    ]
    chunk_count = ceil(len(payload_rows) / chunk_size)
    processed = 0
    for chunk_index, chunk in enumerate(_iter_chunks(payload_rows, chunk_size), start=1):
        conn.executemany(
            """
            insert into fact_dimensions (
              fact_row_id,
              dimension_key,
              dimension_value
            ) values (?, ?, ?)
            """,
            chunk,
        )
        processed += len(chunk)
        _emit_progress(
            progress,
            processed=processed,
            total=len(payload_rows),
            chunk_index=chunk_index,
            chunk_count=chunk_count,
        )


def insert_fact_metrics(
    conn,
    rows: Sequence[Dict[str, Any]],
    *,
    progress: ProgressCallback | None = None,
    chunk_size: int = DEFAULT_FACT_METRICS_CHUNK_SIZE,
) -> None:
    if not rows:
        return

    payload_rows = [
        (
            row["fact_row_id"],
            row["metric_key"],
            row.get("metric_value"),
        )
        for row in rows
    ]
    chunk_count = ceil(len(payload_rows) / chunk_size)
    processed = 0
    for chunk_index, chunk in enumerate(_iter_chunks(payload_rows, chunk_size), start=1):
        conn.executemany(
            """
            insert into fact_metrics (
              fact_row_id,
              metric_key,
              metric_value
            ) values (?, ?, ?)
            """,
            chunk,
        )
        processed += len(chunk)
        _emit_progress(
            progress,
            processed=processed,
            total=len(payload_rows),
            chunk_index=chunk_index,
            chunk_count=chunk_count,
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
