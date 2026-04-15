from __future__ import annotations

import os

from . import (
    db_connection,
    db_operator,
    db_reads,
    db_writes,
    turso_connection,
    turso_operator_export,
    turso_operator_flags,
    turso_reads,
    turso_writes,
)


def load_connection_string():
    return db_connection.load_connection_string()


def _has_turso_env() -> bool:
    return bool(
        str(os.getenv("TURSO_DATABASE_URL") or "").strip()
        and str(os.getenv("TURSO_AUTH_TOKEN") or "").strip()
    )


def _backend_name() -> str:
    value = str(os.getenv("NORMALIZE_DB_BACKEND") or "").strip().lower()
    if not value:
        return "turso" if _has_turso_env() else "postgres"
    return "turso" if value == "turso" else "postgres"


def backend_name() -> str:
    return _backend_name()


def connect_db():
    return turso_connection.connect_db() if _backend_name() == "turso" else db_connection.connect_db()


def fetch_ingested_files(conn, run_date: str):
    return turso_reads.fetch_ingested_files(conn, run_date) if _backend_name() == "turso" else db_reads.fetch_ingested_files(conn, run_date)


def fetch_ingest_rows(conn, file_ids):
    return turso_reads.fetch_ingest_rows(conn, file_ids) if _backend_name() == "turso" else db_reads.fetch_ingest_rows(conn, file_ids)


def fetch_ingest_payloads(conn, file_ids):
    return turso_reads.fetch_ingest_payloads(conn, file_ids) if _backend_name() == "turso" else db_reads.fetch_ingest_payloads(conn, file_ids)


def fetch_existing_goal_slots(conn, topics):
    return turso_reads.fetch_existing_goal_slots(conn, topics) if _backend_name() == "turso" else db_reads.fetch_existing_goal_slots(conn, topics)


def delete_existing_rows_for_run(conn, run_date: str):
    return turso_writes.delete_existing_rows_for_run(conn, run_date) if _backend_name() == "turso" else db_writes.delete_existing_rows_for_run(conn, run_date)


def mark_pipeline_run_ready(conn, run_date: str, *, files_count: int, fact_rows_count: int):
    if _backend_name() == "turso":
        return turso_writes.mark_pipeline_run_ready(conn, run_date, files_count=files_count, fact_rows_count=fact_rows_count)
    return db_writes.mark_pipeline_run_ready(conn, run_date, files_count=files_count, fact_rows_count=fact_rows_count)


def mark_pipeline_run_error(conn, run_date: str, error_message: str):
    return turso_writes.mark_pipeline_run_error(conn, run_date, error_message) if _backend_name() == "turso" else db_writes.mark_pipeline_run_error(conn, run_date, error_message)


def upsert_topic_goal_slots(conn, records):
    return turso_writes.upsert_topic_goal_slots(conn, records) if _backend_name() == "turso" else db_writes.upsert_topic_goal_slots(conn, records)


def insert_fact_rows(conn, rows):
    return turso_writes.insert_fact_rows(conn, rows) if _backend_name() == "turso" else db_writes.insert_fact_rows(conn, rows)


def insert_fact_dimensions(conn, rows):
    return turso_writes.insert_fact_dimensions(conn, rows) if _backend_name() == "turso" else db_writes.insert_fact_dimensions(conn, rows)


def insert_fact_metrics(conn, rows):
    return turso_writes.insert_fact_metrics(conn, rows) if _backend_name() == "turso" else db_writes.insert_fact_metrics(conn, rows)


def refresh_current_flags_for_row_keys(conn, row_keys):
    return turso_operator_flags.refresh_current_flags_for_row_keys(conn, row_keys) if _backend_name() == "turso" else db_operator.refresh_current_flags_for_row_keys(conn, row_keys)


def refresh_operator_export_rows_for_run(conn, run_date: str):
    return turso_operator_export.refresh_operator_export_rows_for_run(conn, run_date) if _backend_name() == "turso" else db_operator.refresh_operator_export_rows_for_run(conn, run_date)


def copy_records(conn, *, table_name: str, columns, rows):
    if _backend_name() == "turso":
        raise NotImplementedError("copy_records is not supported for the Turso backend.")
    return db_writes.copy_records(conn, table_name=table_name, columns=columns, rows=rows)

__all__ = [
    "connect_db",
    "backend_name",
    "copy_records",
    "delete_existing_rows_for_run",
    "fetch_existing_goal_slots",
    "fetch_ingest_payloads",
    "fetch_ingest_rows",
    "fetch_ingested_files",
    "insert_fact_dimensions",
    "insert_fact_metrics",
    "insert_fact_rows",
    "load_connection_string",
    "mark_pipeline_run_error",
    "mark_pipeline_run_ready",
    "refresh_current_flags_for_row_keys",
    "refresh_operator_export_rows_for_run",
    "upsert_topic_goal_slots",
]
