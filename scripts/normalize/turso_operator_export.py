from __future__ import annotations

from decimal import Decimal
from math import ceil
from typing import Any, Callable, Dict, Sequence

GOAL_COLUMNS = tuple(f"goal_{index}" for index in range(1, 26))
INSERT_COLUMNS = (
    "run_date",
    "topic",
    "report_date",
    "report_date_from",
    "report_date_to",
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_content",
    "utm_term",
    "visits",
    "users",
    "bounce_rate",
    "page_depth",
    "time_on_site_seconds",
    "robot_rate",
    *GOAL_COLUMNS,
)
DEFAULT_EXPORT_INSERT_CHUNK_SIZE = 500
MAX_MULTI_VALUES_PARAMS = 5000
def _build_insert_columns_sql() -> str:
    return ", ".join(INSERT_COLUMNS)


def _multi_values_chunk_size(column_count: int, preferred_chunk_size: int) -> int:
    if column_count <= 0:
        raise ValueError("column_count must be positive")
    return max(1, min(preferred_chunk_size, MAX_MULTI_VALUES_PARAMS // column_count))


def _build_multi_values_insert_sql(row_count: int) -> str:
    placeholders = "(" + ", ".join(["?"] * len(INSERT_COLUMNS)) + ")"
    values_sql = ", ".join([placeholders] * row_count)
    return f"""
        insert into operator_export_rows (
          {_build_insert_columns_sql()}
        ) values {values_sql}
        """


def replace_operator_export_rows_for_run(
    conn,
    run_date: str,
    rows: Sequence[Dict[str, Any]],
    *,
    progress: Callable[[Dict[str, Any]], None] | None = None,
    chunk_size: int = DEFAULT_EXPORT_INSERT_CHUNK_SIZE,
) -> None:
    conn.execute(
        """
        delete from operator_export_rows
        where run_date = ?
        """,
        (run_date,),
    )
    if not rows:
        return

    def normalize_value(value: Any) -> Any:
        if isinstance(value, Decimal):
            return str(value)
        return value

    payload_rows = [tuple(normalize_value(row.get(column)) for column in INSERT_COLUMNS) for row in rows]
    chunk_size = _multi_values_chunk_size(len(INSERT_COLUMNS), chunk_size)
    chunk_count = ceil(len(payload_rows) / chunk_size)
    for chunk_index in range(chunk_count):
        chunk = payload_rows[chunk_index * chunk_size : (chunk_index + 1) * chunk_size]
        conn.execute(
            _build_multi_values_insert_sql(len(chunk)),
            tuple(value for row in chunk for value in row),
        )
        if progress is not None:
            processed = min((chunk_index + 1) * chunk_size, len(payload_rows))
            progress(
                {
                    "processed": processed,
                    "total": len(payload_rows),
                    "chunk_index": chunk_index + 1,
                    "chunk_count": chunk_count,
                    "percent": round((processed / len(payload_rows)) * 100, 2),
                }
            )

__all__ = ["GOAL_COLUMNS", "replace_operator_export_rows_for_run"]
