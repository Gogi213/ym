from __future__ import annotations

import base64
import json
from datetime import datetime, timezone
from uuid import uuid4


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _row_value(row, columns: list[str], key: str, default=None):
    if row is None:
        return default
    if isinstance(row, dict):
        return row.get(key, default)
    try:
        return row[key]
    except (TypeError, KeyError, IndexError):
        pass
    if hasattr(row, "keys"):
        try:
            return row[key]
        except Exception:
            pass
    try:
        index = columns.index(key)
    except ValueError:
        return default
    return row[index] if index < len(row) else default


def _fetchone_with_columns(cursor):
    row = cursor.fetchone()
    columns = [description[0] for description in (cursor.description or [])]
    return row, columns


def _fetchall_with_columns(cursor):
    rows = cursor.fetchall()
    columns = [description[0] for description in (cursor.description or [])]
    return rows, columns


def _commit_and_sync(connection) -> None:
    connection.commit()
    sync = getattr(connection, "sync", None)
    if callable(sync):
        sync()


def mark_pipeline_run_after_reset(connection, run_date: str) -> None:
    file_id_cursor = connection.execute(
        "select id from ingest_files where run_date = ?",
        (run_date,),
    )
    file_ids, columns = _fetchall_with_columns(file_id_cursor)
    resolved_file_ids = [_row_value(row, columns, "id") for row in file_ids]
    if resolved_file_ids:
        placeholders = ", ".join(["?"] * len(resolved_file_ids))
        connection.execute(
            f"delete from ingest_file_payloads where file_id in ({placeholders})",
            tuple(resolved_file_ids),
        )
        connection.execute(
            f"delete from ingest_rows where file_id in ({placeholders})",
            tuple(resolved_file_ids),
        )
    connection.execute(
        "delete from ingest_files where run_date = ?",
        (run_date,),
    )
    existing_cursor = connection.execute(
        "select raw_revision from pipeline_runs where run_date = ?",
        (run_date,),
    )
    existing, columns = _fetchone_with_columns(existing_cursor)
    next_revision = int(_row_value(existing, columns, "raw_revision", 0) or 0) + 1
    now = _utc_now_iso()
    connection.execute(
        """
        insert into pipeline_runs (
          run_date, raw_revision, normalize_status, raw_files, raw_rows,
          normalized_files, normalized_rows, last_ingest_at, normalized_at,
          last_error, updated_at
        ) values (?, ?, 'pending_normalize', 0, 0, 0, 0, ?, null, null, ?)
        on conflict(run_date) do update set
          raw_revision = excluded.raw_revision,
          normalize_status = excluded.normalize_status,
          raw_files = excluded.raw_files,
          raw_rows = excluded.raw_rows,
          normalized_files = excluded.normalized_files,
          normalized_rows = excluded.normalized_rows,
          last_ingest_at = excluded.last_ingest_at,
          normalized_at = excluded.normalized_at,
          last_error = excluded.last_error,
          updated_at = excluded.updated_at
        """,
        (run_date, next_revision, now, now),
    )
    _commit_and_sync(connection)


def insert_file_record(
    connection,
    meta: dict[str, str],
    attachment_type: str,
    status: str,
    header: list[str],
    row_count: int,
    error_text: str | None,
) -> str:
    file_id = str(uuid4())
    raw_revision_cursor = connection.execute(
        "select raw_revision from pipeline_runs where run_date = ?",
        (meta["run_date"],),
    )
    raw_revision_row, raw_revision_columns = _fetchone_with_columns(raw_revision_cursor)
    raw_revision = int(_row_value(raw_revision_row, raw_revision_columns, "raw_revision", 0) or 0)
    now = _utc_now_iso()
    connection.execute(
        """
        insert into ingest_files (
          id, run_date, message_id, thread_id, message_date, message_subject,
          primary_topic, matched_topic, topic_role, attachment_name, attachment_type,
          status, header_json, row_count, raw_revision, error_text, updated_at
        ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            file_id,
            meta["run_date"],
            meta["message_id"],
            meta.get("thread_id"),
            meta.get("message_date"),
            meta["message_subject"],
            meta["primary_topic"],
            meta["matched_topic"],
            meta["topic_role"],
            meta["attachment_name"],
            attachment_type,
            status,
            json.dumps(header, ensure_ascii=False),
            int(row_count),
            raw_revision,
            error_text,
            now,
        ),
    )
    _commit_and_sync(connection)
    return file_id


def insert_file_payload_record(
    connection,
    file_id: str,
    file_content_type: str | None,
    bytes_payload: bytes,
) -> None:
    connection.execute(
        """
        insert into ingest_file_payloads (
          file_id, content_type, file_size_bytes, file_base64
        ) values (?, ?, ?, ?)
        """,
        (
            file_id,
            file_content_type,
            len(bytes_payload),
            base64.b64encode(bytes_payload).decode("ascii"),
        ),
    )
    _commit_and_sync(connection)


def insert_row_records(connection, file_id: str, run_date: str, rows: list[dict[str, str]]) -> None:
    payload = [
        (
            file_id,
            run_date,
            index + 1,
            json.dumps(row, ensure_ascii=False),
        )
        for index, row in enumerate(rows)
    ]
    connection.executemany(
        """
        insert into ingest_rows (file_id, run_date, row_index, row_json)
        values (?, ?, ?, ?)
        """,
        payload,
    )
    _commit_and_sync(connection)


def refresh_pipeline_run_after_ingest(connection, run_date: str) -> None:
    files_cursor = connection.execute(
        "select status, row_count from ingest_files where run_date = ?",
        (run_date,),
    )
    files, columns = _fetchall_with_columns(files_cursor)
    total_files = len(files)
    ingested_files = 0
    raw_rows = 0
    for row in files:
        status = _row_value(row, columns, "status")
        if status == "ingested":
            ingested_files += 1
            raw_rows += int(_row_value(row, columns, "row_count", 0) or 0)

    now = _utc_now_iso()
    connection.execute(
        """
        update pipeline_runs
        set raw_files = ?,
            raw_rows = ?,
            normalized_files = 0,
            normalized_rows = 0,
            normalize_status = ?,
            last_ingest_at = ?,
            normalized_at = null,
            last_error = null,
            updated_at = ?
        where run_date = ?
        """,
        (
            total_files,
            raw_rows,
            "pending_normalize" if ingested_files > 0 else "raw_only",
            now,
            now,
            run_date,
        ),
    )
    _commit_and_sync(connection)


def fetch_pipeline_run_status(connection, run_date: str) -> dict[str, object]:
    cursor = connection.execute(
        """
        select
          run_date,
          raw_revision,
          normalize_status,
          raw_files,
          raw_rows,
          normalized_files,
          normalized_rows,
          last_error
        from pipeline_runs
        where run_date = ?
        """,
        (run_date,),
    )
    row, columns = _fetchone_with_columns(cursor)
    if row is None:
        return {"ok": True, "run_date": run_date, "exists": False}

    return {
        "ok": True,
        "run_date": str(_row_value(row, columns, "run_date") or run_date),
        "exists": True,
        "normalize_status": _row_value(row, columns, "normalize_status"),
        "raw_files": int(_row_value(row, columns, "raw_files", 0) or 0),
        "raw_rows": int(_row_value(row, columns, "raw_rows", 0) or 0),
        "normalized_files": int(_row_value(row, columns, "normalized_files", 0) or 0),
        "normalized_rows": int(_row_value(row, columns, "normalized_rows", 0) or 0),
        "raw_revision": int(_row_value(row, columns, "raw_revision", 0) or 0),
        "last_error": _row_value(row, columns, "last_error"),
    }
