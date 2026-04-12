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


def build_goal_mapping_grid(records: List[Dict[str, Any]]) -> List[List[str]]:
    header = ["Отчёт"] + [f"goal_{index}" for index in range(1, 26)]
    rows: List[List[str]] = []

    for record in records:
        row = [str(record.get("topic") or "")]
        for index in range(1, 26):
            row.append(str(record.get(f"goal_{index}") or ""))
        rows.append(row)

    rows.sort(key=lambda item: item[0])
    return [header, *rows]


def apply_goal_mapping_to_sheet_values(
    existing_values: List[List[str]],
    records: List[Dict[str, Any]],
) -> List[List[str]]:
    if not existing_values:
        return build_goal_mapping_grid(records)

    updated = [list(row) for row in existing_values]
    header = updated[0]
    goal_columns = {
        str(cell).strip(): index
        for index, cell in enumerate(header)
        if str(cell).strip().startswith("goal_")
    }
    record_by_topic = {
        str(record.get("topic") or "").strip(): record
        for record in records
        if str(record.get("topic") or "").strip()
    }

    for row_index in range(1, len(updated)):
        row = updated[row_index]
        topic = str(row[0] if row else "").strip()
        if not topic:
            continue

        record = record_by_topic.get(topic, {})
        max_column_index = max(goal_columns.values(), default=-1)
        if len(row) <= max_column_index:
            row.extend([""] * (max_column_index + 1 - len(row)))

        for goal_name, column_index in goal_columns.items():
            row[column_index] = str(record.get(goal_name) or "")

    return updated


def fetch_goal_mapping_records() -> List[Dict[str, Any]]:
    with connect_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select *
                from public.goal_mapping_wide
                order by topic
                """
            )
            return list(cur.fetchall())


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
        return spreadsheet.add_worksheet(title=sheet_name, rows=100, cols=26)


def sync_goal_mapping_sheet(
    *,
    spreadsheet_id: str,
    sheet_name: str,
    service_account_path: Path,
) -> Dict[str, Any]:
    records = fetch_goal_mapping_records()
    worksheet = open_sheet(spreadsheet_id, sheet_name, service_account_path)
    existing = worksheet.get_values()
    grid = apply_goal_mapping_to_sheet_values(existing, records)
    worksheet.update(range_name="A1", values=grid)
    worksheet.freeze(rows=1)
    return {
        "ok": True,
        "sheet_name": sheet_name,
        "rows_written": max(len(grid) - 1, 0),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync goal mapping view from Supabase to Google Sheets.")
    parser.add_argument("--spreadsheet-id", required=True)
    parser.add_argument("--sheet-name", default="отчеты")
    parser.add_argument("--service-account-json", required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = sync_goal_mapping_sheet(
        spreadsheet_id=args.spreadsheet_id,
        sheet_name=args.sheet_name,
        service_account_path=Path(args.service_account_json),
    )
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
