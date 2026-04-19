from __future__ import annotations

from . import (
    turso_connection,
    turso_operator_export,
    turso_operator_flags,
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


def fetch_ingest_rows(conn, file_ids):
    return turso_reads.fetch_ingest_rows(conn, file_ids)


def fetch_ingest_payloads(conn, file_ids):
    return turso_reads.fetch_ingest_payloads(conn, file_ids)


def fetch_existing_goal_slots(conn, topics):
    return turso_reads.fetch_existing_goal_slots(conn, topics)


def delete_existing_rows_for_run(conn, run_date: str, **kwargs):
    return turso_writes.delete_existing_rows_for_run(conn, run_date, **kwargs)


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


def insert_fact_rows(conn, rows, **kwargs):
    return turso_writes.insert_fact_rows(conn, rows, **kwargs)


def insert_fact_dimensions(conn, rows, **kwargs):
    return turso_writes.insert_fact_dimensions(conn, rows, **kwargs)


def insert_fact_metrics(conn, rows, **kwargs):
    return turso_writes.insert_fact_metrics(conn, rows, **kwargs)


def refresh_current_flags_for_row_keys(conn, row_keys):
    return turso_operator_flags.refresh_current_flags_for_row_keys(conn, row_keys)


def refresh_operator_export_rows_for_run(conn, run_date: str):
    return turso_operator_export.refresh_operator_export_rows_for_run(conn, run_date)


def replace_operator_export_rows_for_run(conn, run_date: str, rows, **kwargs):
    return turso_operator_export.replace_operator_export_rows_for_run(conn, run_date, rows, **kwargs)


def copy_records(conn, *, table_name: str, columns, rows):
    raise NotImplementedError("copy_records is not supported for the Turso backend.")


__all__ = [
    "backend_name",
    "connect_db",
    "copy_records",
    "delete_existing_rows_for_run",
    "fetch_existing_goal_slots",
    "fetch_ingest_payloads",
    "fetch_ingest_rows",
    "fetch_ingested_files",
    "fetch_run_files",
    "insert_fact_dimensions",
    "insert_fact_metrics",
    "insert_fact_rows",
    "load_connection_string",
    "mark_pipeline_run_error",
    "mark_pipeline_run_ready",
    "replace_operator_export_rows_for_run",
    "refresh_current_flags_for_row_keys",
    "refresh_operator_export_rows_for_run",
    "upsert_topic_goal_slots",
]
