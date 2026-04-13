from __future__ import annotations

from typing import Any, Dict, List, Sequence, Tuple

from .transform import build_pipeline_run_error_update, build_pipeline_run_ready_update


def delete_existing_rows_for_run(conn, run_date: str) -> List[Tuple[str, str]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            with deleted as (
              delete from public.fact_rows fr
              using public.ingest_files f
              where fr.source_file_id = f.id
                and f.run_date = %s
              returning fr.topic, fr.row_hash
            )
            select distinct topic, row_hash
            from deleted
            """,
            (run_date,),
        )
        return [
            (str(row["topic"]), str(row["row_hash"]))
            for row in cur.fetchall()
            if row["topic"] and row["row_hash"]
        ]


def mark_pipeline_run_ready(conn, run_date: str, *, files_count: int, fact_rows_count: int) -> None:
    payload = build_pipeline_run_ready_update(
        files_count=files_count,
        fact_rows_count=fact_rows_count,
    )
    with conn.cursor() as cur:
        cur.execute(
            """
            update public.pipeline_runs
            set
              normalized_files = %(normalized_files)s,
              normalized_rows = %(normalized_rows)s,
              normalize_status = %(normalize_status)s,
              normalized_at = timezone('utc', now()),
              last_error = %(last_error)s,
              updated_at = timezone('utc', now())
            where run_date = %(run_date)s
            """,
            {
                "run_date": run_date,
                **payload,
            },
        )


def mark_pipeline_run_error(conn, run_date: str, error_message: str) -> None:
    payload = build_pipeline_run_error_update(error_message)
    with conn.cursor() as cur:
        cur.execute(
            """
            update public.pipeline_runs
            set
              normalize_status = %(normalize_status)s,
              last_error = %(last_error)s,
              updated_at = timezone('utc', now())
            where run_date = %(run_date)s
            """,
            {
                "run_date": run_date,
                **payload,
            },
        )


def upsert_topic_goal_slots(conn, records: Sequence[Dict[str, Any]]) -> None:
    if not records:
        return

    with conn.cursor() as cur:
        cur.executemany(
            """
            insert into public.topic_goal_slots (
              topic,
              goal_slot,
              source_header,
              goal_label,
              first_seen_file_id
            )
            values (
              %(topic)s,
              %(goal_slot)s,
              %(source_header)s,
              %(goal_label)s,
              %(first_seen_file_id)s
            )
            on conflict (topic, goal_slot) do update
            set
              source_header = excluded.source_header,
              goal_label = coalesce(public.topic_goal_slots.goal_label, excluded.goal_label),
              updated_at = timezone('utc', now())
            """,
            records,
        )


def copy_records(
    conn,
    *,
    table_name: str,
    columns: Sequence[str],
    rows: Sequence[Dict[str, Any]],
) -> None:
    if not rows:
        return

    column_list = ", ".join(columns)
    copy_sql = f"copy {table_name} ({column_list}) from stdin"

    with conn.cursor() as cur:
        with cur.copy(copy_sql) as copy:
            for row in rows:
                copy.write_row(tuple(row.get(column) for column in columns))


def insert_fact_rows(conn, rows: Sequence[Dict[str, Any]]) -> None:
    copy_records(
        conn,
        table_name="public.fact_rows",
        columns=[
            "fact_row_id",
            "topic",
            "source_file_id",
            "source_row_index",
            "report_date",
            "report_date_from",
            "report_date_to",
            "message_date",
            "layout_signature",
            "row_hash",
            "source_row_json",
        ],
        rows=rows,
    )


def insert_fact_dimensions(conn, rows: Sequence[Dict[str, Any]]) -> None:
    copy_records(
        conn,
        table_name="public.fact_dimensions",
        columns=[
            "fact_row_id",
            "dimension_key",
            "dimension_value",
        ],
        rows=rows,
    )


def insert_fact_metrics(conn, rows: Sequence[Dict[str, Any]]) -> None:
    copy_records(
        conn,
        table_name="public.fact_metrics",
        columns=[
            "fact_row_id",
            "metric_key",
            "metric_value",
        ],
        rows=rows,
    )
