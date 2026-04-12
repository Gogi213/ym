from __future__ import annotations

import argparse
import json
from datetime import date, datetime
from decimal import Decimal
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

DATE_COLUMNS = {
    "report_date",
    "report_date_from",
    "report_date_to",
}

NUMBER_COLUMNS = {
    "visits",
    "users",
    "bounce_rate",
    "page_depth",
    "robot_rate",
}

DURATION_COLUMNS = {
    "time_on_site_seconds",
}

WRITE_BATCH_SIZE = 500


def decimal_to_sheet_number(value: Decimal) -> int | float:
    if value == value.to_integral_value():
        return int(value)
    return float(value)


def parse_decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def format_sheet_date(value: Any) -> str:
    if value is None or value == "":
        return ""
    if isinstance(value, datetime):
        return value.date().strftime("%d.%m.%Y")
    if isinstance(value, date):
        return value.strftime("%d.%m.%Y")
    return datetime.strptime(str(value), "%Y-%m-%d").strftime("%d.%m.%Y")


def transform_operator_record(record: Dict[str, Any]) -> Dict[str, Any]:
    transformed = dict(record)
    visits = parse_decimal(transformed.get("visits")) or Decimal("0")

    for column in DATE_COLUMNS:
        if transformed.get(column):
            transformed[column] = format_sheet_date(transformed[column])

    if transformed.get("bounce_rate") not in (None, ""):
        transformed["bounce_rate"] = decimal_to_sheet_number(
            visits * parse_decimal(transformed["bounce_rate"])
        )

    if transformed.get("page_depth") not in (None, ""):
        transformed["page_depth"] = decimal_to_sheet_number(
            visits * parse_decimal(transformed["page_depth"])
        )

    if transformed.get("robot_rate") not in (None, ""):
        transformed["robot_rate"] = decimal_to_sheet_number(
            visits * parse_decimal(transformed["robot_rate"])
        )

    if transformed.get("time_on_site_seconds") not in (None, ""):
        total_seconds = visits * parse_decimal(transformed["time_on_site_seconds"])
        transformed["time_on_site_seconds"] = float(total_seconds / Decimal("86400"))

    return transformed


def stringify_cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, Decimal):
        return decimal_to_sheet_number(value)
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return str(value)


def filter_export_columns(columns: List[str]) -> List[str]:
    return [column for column in columns if column not in EXCLUDED_COLUMNS]


def build_export_rows_grid(columns: List[str], records: List[Dict[str, Any]]) -> List[List[str]]:
    grid: List[List[str]] = [list(columns)]
    for record in records:
        transformed = transform_operator_record(record)
        grid.append([stringify_cell(transformed.get(column)) for column in columns])
    return grid


def chunk_grid_rows(grid: List[List[str]], batch_size: int) -> List[List[List[str]]]:
    if not grid:
        return []
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    return [grid[index:index + batch_size] for index in range(0, len(grid), batch_size)]


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


def apply_column_formats(worksheet, columns: List[str]) -> None:
    requests: List[Dict[str, Any]] = []

    for column_index, column_name in enumerate(columns):
        number_format = None

        if column_name in DATE_COLUMNS:
            number_format = {"type": "DATE", "pattern": "dd.mm.yyyy"}
        elif column_name in DURATION_COLUMNS:
            number_format = {"type": "NUMBER", "pattern": "[h]:mm:ss"}
        elif column_name in NUMBER_COLUMNS or column_name.startswith("goal_"):
            number_format = {"type": "NUMBER", "pattern": "0.############"}

        if not number_format:
            continue

        requests.append(
            {
                "repeatCell": {
                    "range": {
                        "sheetId": worksheet.id,
                        "startRowIndex": 1,
                        "startColumnIndex": column_index,
                        "endColumnIndex": column_index + 1,
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "numberFormat": number_format,
                        }
                    },
                    "fields": "userEnteredFormat.numberFormat",
                }
            }
        )

    if requests:
        worksheet.spreadsheet.batch_update({"requests": requests})


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
    worksheet.resize(rows=max(len(grid), 1), cols=max(len(columns), 1))
    for batch_index, batch_rows in enumerate(chunk_grid_rows(grid, WRITE_BATCH_SIZE), start=1):
        start_row = 1 + ((batch_index - 1) * WRITE_BATCH_SIZE)
        worksheet.update(range_name=f"A{start_row}", values=batch_rows, raw=False)
        print(
            json.dumps(
                {
                    "phase": "sheet_batch_written",
                    "sheet_name": sheet_name,
                    "batch_index": batch_index,
                    "batch_count": (len(grid) + WRITE_BATCH_SIZE - 1) // WRITE_BATCH_SIZE,
                    "rows_written": len(batch_rows),
                },
                ensure_ascii=False,
            )
        )
    worksheet.freeze(rows=1)
    apply_column_formats(worksheet, columns)
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
