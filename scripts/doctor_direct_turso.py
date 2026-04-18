from __future__ import annotations

import argparse
import base64
import json
from pathlib import Path
import sys
from typing import Any, Dict, List

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from scripts.normalize.raw_parse import parse_attachment
from scripts.normalize.query_utils import execute_select
from scripts.sync_pipeline_status_sheet import classify_pipeline_status
from scripts.turso_runtime import connect_turso, load_turso_config


def run_connection_smoke(conn) -> Dict[str, Any]:
    rows = execute_select(conn, "select 1 as scalar")
    scalar = int(rows[0]["scalar"]) if rows else 0
    return {"ok": scalar == 1, "scalar": scalar}


def fetch_recent_pipeline_runs(conn, *, limit: int = 10) -> List[Dict[str, Any]]:
    rows = execute_select(
        conn,
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
        order by run_date desc
        limit ?
        """,
        (max(1, int(limit or 10)),),
    )
    return [
        {
            "run_date": str(row.get("run_date") or ""),
            "raw_revision": int(row.get("raw_revision") or 0),
            "normalize_status": str(row.get("normalize_status") or ""),
            "raw_files": int(row.get("raw_files") or 0),
            "raw_rows": int(row.get("raw_rows") or 0),
            "normalized_files": int(row.get("normalized_files") or 0),
            "normalized_rows": int(row.get("normalized_rows") or 0),
            "last_error": row.get("last_error"),
        }
        for row in rows
    ]


def inspect_run_date(conn, run_date: str) -> Dict[str, Any]:
    run_rows = execute_select(
        conn,
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
        limit 1
        """,
        (run_date,),
    )
    file_rows = execute_select(
        conn,
        """
        select
          count(*) as files_total,
          sum(
            case
              when status = 'raw_only' then 1
              when status = 'ingested'
                and coalesce(row_count, 0) = 0
                and coalesce(header_json, '[]') = '[]'
                and exists (select 1 from ingest_file_payloads p where p.file_id = ingest_files.id)
                and not exists (select 1 from ingest_rows r where r.file_id = ingest_files.id)
              then 1
              else 0
            end
          ) as raw_only_files,
          sum(case when status = 'ingested' then 1 else 0 end) as ingested_files,
          sum(case when status = 'skipped' then 1 else 0 end) as skipped_files,
          sum(case when status = 'error' then 1 else 0 end) as error_files
        from ingest_files
        where run_date = ?
        """,
        (run_date,),
    )
    payload_rows = execute_select(
        conn,
        """
        select count(*) as payload_files
        from ingest_file_payloads
        where file_id in (
          select id
          from ingest_files
          where run_date = ?
        )
        """,
        (run_date,),
    )

    run_row = run_rows[0] if run_rows else {}
    file_row = file_rows[0] if file_rows else {}
    payload_row = payload_rows[0] if payload_rows else {}

    return {
        "run_date": run_date,
        "pipeline_status": classify_pipeline_status(run_row) if run_row else "missing",
        "raw_revision": int(run_row.get("raw_revision") or 0),
        "files_total": int(file_row.get("files_total") or 0),
        "raw_only_files": int(file_row.get("raw_only_files") or 0),
        "ingested_files": int(file_row.get("ingested_files") or 0),
        "skipped_files": int(file_row.get("skipped_files") or 0),
        "error_files": int(file_row.get("error_files") or 0),
        "payload_files": int(payload_row.get("payload_files") or 0),
        "raw_rows": int(run_row.get("raw_rows") or 0),
        "normalized_files": int(run_row.get("normalized_files") or 0),
        "normalized_rows": int(run_row.get("normalized_rows") or 0),
        "last_error": run_row.get("last_error"),
    }


def validate_raw_payloads(conn, run_date: str) -> Dict[str, Any]:
    rows = execute_select(
        conn,
        """
        select
          f.id as file_id,
          f.attachment_name,
          f.attachment_type,
          p.file_base64
        from ingest_files f
        left join ingest_file_payloads p on p.file_id = f.id
        where f.run_date = ?
          and (
            f.status = 'raw_only'
            or (
              f.status = 'ingested'
              and coalesce(f.row_count, 0) = 0
              and coalesce(f.header_json, '[]') = '[]'
              and exists (select 1 from ingest_file_payloads p2 where p2.file_id = f.id)
              and not exists (select 1 from ingest_rows r where r.file_id = f.id)
            )
          )
        order by f.message_date, f.created_at, f.id
        """,
        (run_date,),
    )
    details: List[Dict[str, Any]] = []
    parseable_files = 0
    skipped_files = 0
    error_files = 0

    for row in rows:
        file_id = str(row.get("file_id") or "")
        attachment_name = str(row.get("attachment_name") or "")
        file_base64 = str(row.get("file_base64") or "")
        if not file_base64:
            error_files += 1
            details.append(
                {
                    "file_id": file_id,
                    "attachment_name": attachment_name,
                    "status": "error",
                    "error": "Missing payload",
                }
            )
            continue

        try:
            parsed = parse_attachment(
                str(row.get("attachment_type") or ""),
                base64.b64decode(file_base64),
            )
        except Exception as error:
            error_files += 1
            details.append(
                {
                    "file_id": file_id,
                    "attachment_name": attachment_name,
                    "status": "error",
                    "error": str(error),
                }
            )
            continue

        if parsed.table:
            parseable_files += 1
            details.append(
                {
                    "file_id": file_id,
                    "attachment_name": attachment_name,
                    "status": "parseable",
                    "rows": len(parsed.table.rows),
                }
            )
        else:
            skipped_files += 1
            details.append(
                {
                    "file_id": file_id,
                    "attachment_name": attachment_name,
                    "status": "skipped",
                    "rows": 0,
                }
            )

    return {
        "run_date": run_date,
        "validated_files": len(rows),
        "parseable_files": parseable_files,
        "skipped_files": skipped_files,
        "error_files": error_files,
        "details": details,
    }


def build_doctor_report(conn, *, run_date: str | None = None, limit: int = 10, validate_payloads: bool = False) -> Dict[str, Any]:
    report: Dict[str, Any] = {
        "connection": run_connection_smoke(conn),
        "recent_runs": fetch_recent_pipeline_runs(conn, limit=limit),
    }
    if run_date:
        report["run_date"] = inspect_run_date(conn, run_date)
    if validate_payloads:
        if not run_date:
            raise ValueError("--validate-payloads requires --run-date")
        report["payload_validation"] = validate_raw_payloads(conn, run_date)
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read-only doctor for the direct Apps Script -> Turso -> local Python contour."
    )
    parser.add_argument("--run-date")
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument("--validate-payloads", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    conn = connect_turso(load_turso_config())
    try:
        report = build_doctor_report(
            conn,
            run_date=args.run_date,
            limit=args.limit,
            validate_payloads=args.validate_payloads,
        )
    finally:
        close = getattr(conn, "close", None)
        if callable(close):
            close()
    print(json.dumps(report, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
