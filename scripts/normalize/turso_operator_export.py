from __future__ import annotations


GOAL_COLUMNS = tuple(f"goal_{index}" for index in range(1, 26))


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
    return ", ".join(
        [
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
        ]
    )


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


__all__ = ["GOAL_COLUMNS", "refresh_operator_export_rows_for_run"]
