from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any, Dict, List

import gspread
from google.oauth2.service_account import Credentials

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from scripts.normalize_supabase import connect_db


SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
]

EXCLUDED_COLUMNS = {
    "fact_row_id",
    "source_file_id",
    "source_row_index",
    "message_date",
    "layout_signature",
    "row_hash",
    "source_row_json",
    "created_at",
}


def stringify_cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return str(value)


def filter_export_columns(columns: List[str]) -> List[str]:
    return [column for column in columns if column not in EXCLUDED_COLUMNS]


def build_export_rows_grid(columns: List[str], records: List[Dict[str, Any]]) -> List[List[str]]:
    grid: List[List[str]] = [list(columns)]
    for record in records:
        grid.append([stringify_cell(record.get(column)) for column in columns])
    return grid


def fetch_export_rows() -> tuple[List[str], List[Dict[str, Any]]]:
    with connect_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select *
                from public.export_rows_wide
                order by report_date, topic, source_file_id, source_row_index
                """
            )
            rows = list(cur.fetchall())
            columns = [desc.name for desc in cur.description]
            return columns, rows


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
        return spreadsheet.add_worksheet(title=sheet_name, rows=1000, cols=80)


def sync_export_rows_wide_sheet(
    *,
    spreadsheet_id: str,
    sheet_name: str,
    service_account_path: Path,
) -> Dict[str, Any]:
    columns, records = fetch_export_rows()
    columns = filter_export_columns(columns)
    grid = build_export_rows_grid(columns, records)
    worksheet = open_sheet(spreadsheet_id, sheet_name, service_account_path)
    worksheet.clear()
    worksheet.update(range_name="A1", values=grid)
    worksheet.freeze(rows=1)
    return {
        "ok": True,
        "sheet_name": sheet_name,
        "rows_written": max(len(grid) - 1, 0),
        "columns_written": len(columns),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync export_rows_wide from Supabase to Google Sheets.")
    parser.add_argument("--spreadsheet-id", required=True)
    parser.add_argument("--sheet-name", default="union")
    parser.add_argument("--service-account-json", required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = sync_export_rows_wide_sheet(
        spreadsheet_id=args.spreadsheet_id,
        sheet_name=args.sheet_name,
        service_account_path=Path(args.service_account_json),
    )
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
