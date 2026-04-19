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


def _build_metric_pivot_sql() -> str:
    metric_cases = [
        "max(case when fm.metric_key = 'visits' then fm.metric_value end) as visits",
        "max(case when fm.metric_key = 'users' then fm.metric_value end) as users",
        "max(case when fm.metric_key = 'bounce_rate' then fm.metric_value end) as bounce_rate",
        "max(case when fm.metric_key = 'page_depth' then fm.metric_value end) as page_depth",
        "max(case when fm.metric_key = 'time_on_site_seconds' then fm.metric_value end) as time_on_site_seconds",
        "max(case when fm.metric_key = 'robot_rate' then fm.metric_value end) as robot_rate",
    ]
    metric_cases.extend(
        f"max(case when fm.metric_key = '{goal_column}' then fm.metric_value end) as {goal_column}"
        for goal_column in GOAL_COLUMNS
    )
    return ",\n            ".join(metric_cases)


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


def _build_select_goal_sums_sql() -> str:
    return ",\n          ".join(f"sum(mp.{goal_column}) as {goal_column}" for goal_column in GOAL_COLUMNS)


OPERATOR_EXPORT_REFRESH_SQL = f"""
    with target_rows as (
      select
        fr.fact_row_id,
        f.run_date,
        fr.topic,
        fr.report_date,
        fr.report_date_from,
        fr.report_date_to
      from fact_rows fr
      join ingest_files f
        on f.id = fr.source_file_id
      where fr.is_current = 1
        and f.run_date = ?
    ),
    dimension_pivot as (
      select
        fd.fact_row_id,
        max(case when fd.dimension_key = 'utm_source' then fd.dimension_value end) as utm_source,
        max(case when fd.dimension_key = 'utm_medium' then fd.dimension_value end) as utm_medium,
        max(case when fd.dimension_key = 'utm_campaign' then fd.dimension_value end) as utm_campaign
      from fact_dimensions fd
      join target_rows tr
        on tr.fact_row_id = fd.fact_row_id
      group by fd.fact_row_id
    ),
    metric_pivot as (
      select
        fm.fact_row_id,
        {_build_metric_pivot_sql()}
      from fact_metrics fm
      join target_rows tr
        on tr.fact_row_id = fm.fact_row_id
      group by fm.fact_row_id
    )
    insert into operator_export_rows (
      {_build_insert_columns_sql()}
    )
    select
      tr.run_date,
      tr.topic,
      tr.report_date,
      tr.report_date_from,
      tr.report_date_to,
      dp.utm_source,
      dp.utm_medium,
      dp.utm_campaign,
      'aggregated' as utm_content,
      'aggregated' as utm_term,
      sum(mp.visits) as visits,
      sum(mp.users) as users,
      sum(case when mp.bounce_rate is not null then mp.bounce_rate * mp.visits end) as bounce_rate,
      sum(case when mp.page_depth is not null then mp.page_depth * mp.visits end) as page_depth,
      sum(case when mp.time_on_site_seconds is not null then mp.time_on_site_seconds * mp.visits end) as time_on_site_seconds,
      sum(case when mp.robot_rate is not null then mp.robot_rate * mp.visits end) as robot_rate,
      {_build_select_goal_sums_sql()}
    from target_rows tr
    left join dimension_pivot dp
      on dp.fact_row_id = tr.fact_row_id
    left join metric_pivot mp
      on mp.fact_row_id = tr.fact_row_id
    group by
      tr.run_date, tr.topic, tr.report_date, tr.report_date_from, tr.report_date_to,
      dp.utm_source, dp.utm_medium, dp.utm_campaign
"""


def refresh_operator_export_rows_for_run(conn, run_date: str) -> None:
    conn.execute(
        """
        delete from operator_export_rows
        where run_date = ?
        """,
        (run_date,),
    )
    conn.execute(OPERATOR_EXPORT_REFRESH_SQL, (run_date,))


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


__all__ = ["GOAL_COLUMNS", "refresh_operator_export_rows_for_run", "replace_operator_export_rows_for_run"]
