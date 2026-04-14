from __future__ import annotations

from dataclasses import dataclass

from fastapi import UploadFile

from ingest_service.models import ResetPayload
from ingest_service.parse import parse_attachment
from ingest_service.storage import (
    insert_file_payload_record,
    insert_file_record,
    insert_row_records,
    mark_pipeline_run_after_reset,
    refresh_pipeline_run_after_ingest,
)


@dataclass(frozen=True)
class RuntimeHandlers:
    reset_handler: object
    ingest_handler: object


def normalize_attachment_type(meta_type: object, filename: str, content_type: str) -> str | None:
    lowered_type = str(meta_type or "").lower()
    lowered_name = str(filename or "").lower()
    lowered_content_type = str(content_type or "").lower()

    if (
        lowered_type == "xlsx"
        or lowered_name.endswith(".xlsx")
        or "openxmlformats-officedocument.spreadsheetml.sheet" in lowered_content_type
    ):
        return "xlsx"
    if lowered_type == "csv" or lowered_name.endswith(".csv") or "csv" in lowered_content_type:
        return "csv"
    return None


def create_runtime_handlers(connection) -> RuntimeHandlers:
    def reset_handler(payload: ResetPayload):
        mark_pipeline_run_after_reset(connection, payload.run_date)
        return {"ok": True, "action": payload.action, "run_date": payload.run_date}

    def ingest_handler(meta: dict[str, object], upload: UploadFile):
        attachment_type = normalize_attachment_type(
            meta.get("attachment_type"),
            upload.filename or str(meta.get("attachment_name") or ""),
            upload.content_type or "",
        )
        if not attachment_type:
            raise ValueError("Unsupported attachment type")

        payload = upload.file.read()
        parsed = parse_attachment(attachment_type, payload)
        status = "ingested" if parsed.table else "skipped"
        header = parsed.table.header if parsed.table else []
        rows = parsed.table.rows if parsed.table else []
        file_id = insert_file_record(
            connection,
            meta={
                "run_date": str(meta.get("run_date") or ""),
                "message_id": str(meta.get("message_id") or ""),
                "thread_id": str(meta.get("thread_id") or ""),
                "message_date": str(meta.get("message_date") or ""),
                "message_subject": str(meta.get("message_subject") or ""),
                "primary_topic": str(meta.get("primary_topic") or ""),
                "matched_topic": str(meta.get("matched_topic") or ""),
                "topic_role": str(meta.get("topic_role") or ""),
                "attachment_name": str(meta.get("attachment_name") or upload.filename or ""),
            },
            attachment_type=attachment_type,
            status=status,
            header=header,
            row_count=len(rows),
            error_text=None,
        )
        insert_file_payload_record(
            connection,
            file_id=file_id,
            file_content_type=upload.content_type,
            bytes_payload=payload,
        )
        if rows:
            insert_row_records(
                connection,
                file_id=file_id,
                run_date=str(meta.get("run_date") or ""),
                rows=[
                    {
                        str(header[index]): str(row[index] if index < len(row) else "")
                        for index in range(len(header))
                        if str(header[index]).strip()
                    }
                    for row in rows
                ],
            )
        refresh_pipeline_run_after_ingest(connection, str(meta.get("run_date") or ""))
        return {
            "ok": True,
            "status": status,
            "file_id": file_id,
            "rows": len(rows),
            "debug": {
                "type": parsed.debug.type,
                "summary": parsed.debug.summary,
            },
        }

    return RuntimeHandlers(reset_handler=reset_handler, ingest_handler=ingest_handler)
