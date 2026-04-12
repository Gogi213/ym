from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any, Dict, List

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from scripts.normalize_supabase import connect_db, normalize_run
from scripts.sync_export_rows_wide_sheet import sync_export_rows_wide_sheet
from scripts.sync_goal_mapping_sheet import sync_goal_mapping_sheet
from scripts.sync_pipeline_status_sheet import fetch_pipeline_status_records, sync_pipeline_status_sheet


DEFAULT_SPREADSHEET_ID = "17izchH29LyxuTCNWJ0SThSXmuubMnNFCjtPJiWtcxFA"


def select_pending_run_dates(records: List[Dict[str, Any]]) -> List[str]:
    pending: List[str] = []
    for record in records:
        if str(record.get("pipeline_status") or "") != "pending_normalize":
            continue
        run_date = str(record.get("run_date") or "")
        if run_date:
            pending.append(run_date)
    return pending


def sync_operator_views(*, spreadsheet_id: str, service_account_path: Path) -> Dict[str, Any]:
    goals_result = sync_goal_mapping_sheet(
        spreadsheet_id=spreadsheet_id,
        sheet_name="отчеты",
        service_account_path=service_account_path,
    )
    union_result = sync_export_rows_wide_sheet(
        spreadsheet_id=spreadsheet_id,
        sheet_name="union",
        service_account_path=service_account_path,
    )
    status_result = sync_pipeline_status_sheet(
        spreadsheet_id=spreadsheet_id,
        sheet_name="pipeline_status",
        service_account_path=service_account_path,
    )
    return {
        "goal_mapping": goals_result,
        "union": union_result,
        "pipeline_status": status_result,
    }


def run_pipeline(
    *,
    spreadsheet_id: str,
    service_account_path: Path,
    run_dates: List[str] | None = None,
) -> Dict[str, Any]:
    status_before = fetch_pipeline_status_records()
    selected_run_dates = run_dates[:] if run_dates else select_pending_run_dates(status_before)
    normalized_results: List[Dict[str, Any]] = []

    for run_date in selected_run_dates:
        result = normalize_run(run_date)
        normalized_results.append({"run_date": run_date, **result})

    sync_results = sync_operator_views(
        spreadsheet_id=spreadsheet_id,
        service_account_path=service_account_path,
    )
    status_after = fetch_pipeline_status_records()

    return {
        "ok": True,
        "selected_run_dates": selected_run_dates,
        "normalized": normalized_results,
        "sync": sync_results,
        "status_before": status_before,
        "status_after": status_after,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run normalize + sheet sync pipeline for pending or explicit run dates.")
    parser.add_argument("--spreadsheet-id", default=DEFAULT_SPREADSHEET_ID)
    parser.add_argument("--service-account-json", required=True)
    parser.add_argument("--run-date", action="append", dest="run_dates", default=[])
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = run_pipeline(
        spreadsheet_id=args.spreadsheet_id,
        service_account_path=Path(args.service_account_json),
        run_dates=args.run_dates or None,
    )
    print(
        json.dumps(
            {
                "ok": result["ok"],
                "selected_run_dates": result["selected_run_dates"],
                "normalized_runs": result["normalized"],
                "sync": result["sync"],
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
