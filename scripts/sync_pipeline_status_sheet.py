from __future__ import annotations

import argparse
import json
from datetime import date, datetime
from pathlib import Path
import sys
from typing import Any, Dict, List

import gspread
from google.oauth2.service_account import Credentials

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from scripts.normalize.db import connect_db
from scripts.normalize.query_utils import execute_select


SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
]


def format_date_cell(value: Any) -> str:
    if value is None or value == "":
        return ""
    if isinstance(value, datetime):
        return value.strftime("%d.%m.%Y %H:%M:%S")
    if isinstance(value, date):
        return value.strftime("%d.%m.%Y")
    return str(value)


def classify_pipeline_status(record: Dict[str, Any]) -> str:
    explicit_status = str(record.get("normalize_status") or "").strip()
    if explicit_status:
        return explicit_status

    ingested_files = int(record.get("ingested_files") or 0)
    if ingested_files <= 0:
        return "raw_only"
    return "pending_normalize"


def fetch_pipeline_status_records() -> List[Dict[str, Any]]:
    with connect_db() as conn:
        records = execute_select(
            conn,
            """
            with run_state as (
              select
                pr.run_date,
                pr.raw_revision,
                pr.normalize_status,
                pr.raw_files,
                pr.raw_rows,
                pr.normalized_files,
                pr.normalized_rows,
                pr.last_ingest_at,
                pr.normalized_at,
                pr.last_error
              from pipeline_runs pr
            ),
            ingest_summary as (
              select
                f.run_date,
                count(*) as total_files,
                sum(case when f.status = 'ingested' then 1 else 0 end) as ingested_files,
                sum(case when f.status = 'skipped' then 1 else 0 end) as skipped_files,
                sum(case when f.status = 'error' then 1 else 0 end) as error_files,
                coalesce(sum(case when f.status = 'ingested' then f.row_count else 0 end), 0) as raw_rows,
                min(f.message_date) as first_message_at,
                max(f.message_date) as last_message_at
              from ingest_files f
              group by f.run_date
            ),
            normalized_summary as (
              select
                f.run_date,
                count(distinct fr.source_file_id) as normalized_files,
                count(*) as normalized_rows,
                max(fr.created_at) as normalized_at
              from fact_rows fr
              join ingest_files f on f.id = fr.source_file_id
              group by f.run_date
            )
            select
              rs.run_date,
              rs.raw_revision,
              rs.normalize_status,
              coalesce(i.total_files, rs.raw_files, 0) as total_files,
              coalesce(i.ingested_files, 0) as ingested_files,
              coalesce(i.skipped_files, 0) as skipped_files,
              coalesce(i.error_files, 0) as error_files,
              coalesce(rs.raw_rows, i.raw_rows, 0) as raw_rows,
              coalesce(rs.normalized_files, n.normalized_files, 0) as normalized_files,
              coalesce(rs.normalized_rows, n.normalized_rows, 0) as normalized_rows,
              coalesce(i.first_message_at, rs.last_ingest_at) as first_message_at,
              coalesce(i.last_message_at, rs.last_ingest_at) as last_message_at,
              coalesce(rs.normalized_at, n.normalized_at) as normalized_at,
              rs.last_error
            from run_state rs
            left join ingest_summary i on i.run_date = rs.run_date
            left join normalized_summary n on n.run_date = rs.run_date
            order by rs.run_date desc
            """,
        )
        for record in records:
            record["pipeline_status"] = classify_pipeline_status(record)
        return records


def build_pipeline_status_grid(records: List[Dict[str, Any]]) -> List[List[Any]]:
    header = [
        "run_date",
        "pipeline_status",
        "total_files",
        "ingested_files",
        "skipped_files",
        "error_files",
        "raw_rows",
        "normalized_files",
        "normalized_rows",
        "first_message_at",
        "last_message_at",
        "normalized_at",
    ]
    grid: List[List[Any]] = [header]

    for record in records:
        grid.append(
            [
                format_date_cell(record.get("run_date")),
                str(record.get("pipeline_status") or ""),
                int(record.get("total_files") or 0),
                int(record.get("ingested_files") or 0),
                int(record.get("skipped_files") or 0),
                int(record.get("error_files") or 0),
                int(record.get("raw_rows") or 0),
                int(record.get("normalized_files") or 0),
                int(record.get("normalized_rows") or 0),
                format_date_cell(record.get("first_message_at")),
                format_date_cell(record.get("last_message_at")),
                format_date_cell(record.get("normalized_at")),
            ]
        )

    return grid


def open_sheet(spreadsheet_id: str, sheet_name: str, service_account_path: Path):
    credentials = Credentials.from_service_account_file(
        str(service_account_path),
        scopes=SCOPES,
    )
    client = gspread.authorize(credentials)
    spreadsheet = client.open_by_key(spreadsheet_id)
    try:
        return spreadsheet.worksheet(sheet_name)
    except gspread.WorksheetNotFound:
        return spreadsheet.add_worksheet(title=sheet_name, rows=100, cols=20)


def sync_pipeline_status_sheet(
    *,
    spreadsheet_id: str,
    sheet_name: str,
    service_account_path: Path,
) -> Dict[str, Any]:
    records = fetch_pipeline_status_records()
    grid = build_pipeline_status_grid(records)
    worksheet = open_sheet(spreadsheet_id, sheet_name, service_account_path)
    worksheet.clear()
    worksheet.update(range_name="A1", values=grid, raw=False)
    worksheet.freeze(rows=1)
    return {
        "ok": True,
        "sheet_name": sheet_name,
        "rows_written": max(len(grid) - 1, 0),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync pipeline status summary to Google Sheets.")
    parser.add_argument("--spreadsheet-id", required=True)
    parser.add_argument("--sheet-name", default="pipeline_status")
    parser.add_argument("--service-account-json", required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = sync_pipeline_status_sheet(
        spreadsheet_id=args.spreadsheet_id,
        sheet_name=args.sheet_name,
        service_account_path=Path(args.service_account_json),
    )
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
