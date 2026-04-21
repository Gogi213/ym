from __future__ import annotations

from . import (
    turso_connection,
    turso_operator_export,
    turso_reads,
    turso_writes,
)


def load_connection_string():
    return turso_connection.load_turso_config().database_url


def backend_name() -> str:
    return "turso"


def connect_db():
    return turso_connection.connect_db()


def fetch_ingested_files(conn, run_date: str):
    return turso_reads.fetch_ingested_files(conn, run_date)


def fetch_run_files(conn, run_date: str):
    return turso_reads.fetch_run_files(conn, run_date)


def fetch_existing_goal_slots(conn, topics):
    return turso_reads.fetch_existing_goal_slots(conn, topics)


def mark_pipeline_run_ready(conn, run_date: str, *, files_count: int, fact_rows_count: int):
    return turso_writes.mark_pipeline_run_ready(
        conn,
        run_date,
        files_count=files_count,
        fact_rows_count=fact_rows_count,
    )


def mark_pipeline_run_error(conn, run_date: str, error_message: str):
    return turso_writes.mark_pipeline_run_error(conn, run_date, error_message)


def upsert_topic_goal_slots(conn, records):
    return turso_writes.upsert_topic_goal_slots(conn, records)


def replace_operator_export_rows_for_run(conn, run_date: str, rows, **kwargs):
    return turso_operator_export.replace_operator_export_rows_for_run(conn, run_date, rows, **kwargs)


def copy_records(conn, *, table_name: str, columns, rows):
    raise NotImplementedError("copy_records is not supported for the Turso backend.")


__all__ = [
    "backend_name",
    "connect_db",
    "copy_records",
    "fetch_existing_goal_slots",
    "fetch_ingested_files",
    "fetch_run_files",
    "load_connection_string",
    "mark_pipeline_run_error",
    "mark_pipeline_run_ready",
    "replace_operator_export_rows_for_run",
    "upsert_topic_goal_slots",
]
