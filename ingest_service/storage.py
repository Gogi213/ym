from __future__ import annotations

import base64
import json
from datetime import datetime, timezone
from uuid import uuid4


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def mark_pipeline_run_after_reset(connection, run_date: str) -> None:
    existing = connection.execute(
        "select raw_revision from pipeline_runs where run_date = ?",
        (run_date,),
    ).fetchone()
    next_revision = int(existing["raw_revision"] if existing else 0) + 1
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
    connection.commit()


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
    connection.execute(
        """
        insert into ingest_files (
          id, run_date, message_id, thread_id, message_date, message_subject,
          primary_topic, matched_topic, topic_role, attachment_name, attachment_type,
          status, header_json, row_count, error_text
        ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            error_text,
        ),
    )
    connection.commit()
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
    connection.commit()


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
    connection.commit()


def refresh_pipeline_run_after_ingest(connection, run_date: str) -> None:
    files = connection.execute(
        "select status, row_count from ingest_files where run_date = ?",
        (run_date,),
    ).fetchall()
    total_files = len(files)
    ingested_files = 0
    raw_rows = 0
    for row in files:
        if row["status"] == "ingested":
            ingested_files += 1
            raw_rows += int(row["row_count"] or 0)

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
    connection.commit()
