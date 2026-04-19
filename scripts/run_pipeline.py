from __future__ import annotations

import argparse
from contextlib import contextmanager
import json
import os
from pathlib import Path
import sys
from typing import Any, Dict, List

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from scripts.normalize.common import public_payload
from scripts.normalize.pipeline import finalize_normalized_runs, normalize_run
from scripts.sync_export_rows_wide_sheet import sync_export_rows_wide_sheet
from scripts.sync_goal_mapping_sheet import sync_goal_mapping_sheet
from scripts.sync_pipeline_status_sheet import fetch_pipeline_status_records, sync_pipeline_status_sheet


DEFAULT_SPREADSHEET_ID = "17izchH29LyxuTCNWJ0SThSXmuubMnNFCjtPJiWtcxFA"
PIPELINE_LOCK_PATH = ROOT_DIR / ".run_pipeline.lock"


class PipelineLockError(RuntimeError):
    pass


def _is_process_running(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except (OSError, SystemError, ValueError):
        return False
    return True


@contextmanager
def acquire_pipeline_lock(*, lock_path: Path = PIPELINE_LOCK_PATH):
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    while lock_path.exists():
        try:
            payload = json.loads(lock_path.read_text(encoding="utf-8") or "{}")
        except json.JSONDecodeError:
            payload = {}
        existing_pid = int(payload.get("pid") or 0)
        if existing_pid and _is_process_running(existing_pid):
            raise PipelineLockError(f"run_pipeline.py is already running under pid {existing_pid}")
        lock_path.unlink(missing_ok=True)

    lock_path.write_text(
        json.dumps(
            {
                "pid": os.getpid(),
            }
        ),
        encoding="utf-8",
    )
    try:
        yield
    finally:
        lock_path.unlink(missing_ok=True)


def log_progress(phase: str, payload: Dict[str, Any]) -> None:
    print(json.dumps({"phase": phase, **payload}, ensure_ascii=False), flush=True)


def select_pending_run_dates(records: List[Dict[str, Any]]) -> List[str]:
    pending: List[str] = []
    for record in records:
        normalize_status = str(record.get("normalize_status") or record.get("pipeline_status") or "")
        if normalize_status not in {"raw_only", "pending_normalize", "normalize_error"}:
            continue
        run_date = str(record.get("run_date") or "")
        if run_date:
            pending.append(run_date)
    return pending


def has_failed_runs(results: List[Dict[str, Any]]) -> bool:
    return any(str(result.get("error") or "").strip() for result in results)


def should_use_bootstrap_mode(records: List[Dict[str, Any]], selected_run_dates: List[str]) -> bool:
    # The fast path is unsafe with the current Turso HTTP write path: a failed run can
    # leave partially written fact rows even when pipeline status still reports zero
    # normalized rows. Always rebuild explicitly until writes are fully transactional.
    return False


def should_sync_full_operator_views(
    failed_results: List[Dict[str, Any]],
    normalized_results: List[Dict[str, Any]],
) -> bool:
    return bool(normalized_results) and not has_failed_runs(failed_results)


def _emit_sheet_sync_progress(
    logger,
    *,
    sheet: str,
    status: str,
    sheet_index: int,
    sheet_count: int,
    result: Dict[str, Any] | None = None,
) -> None:
    if logger is None:
        return
    payload = {
        "sheet": sheet,
        "status": status,
        "sheet_index": sheet_index,
        "sheet_count": sheet_count,
    }
    if result:
        payload.update(public_payload(result))
    logger("sheet_sync_progress", payload)


def sync_operator_views(
    *,
    spreadsheet_id: str,
    service_account_path: Path,
    logger=None,
) -> Dict[str, Any]:
    _emit_sheet_sync_progress(logger, sheet="отчеты", status="started", sheet_index=1, sheet_count=3)
    goals_result = sync_goal_mapping_sheet(
        spreadsheet_id=spreadsheet_id,
        sheet_name="отчеты",
        service_account_path=service_account_path,
    )
    _emit_sheet_sync_progress(
        logger,
        sheet="отчеты",
        status="finished",
        sheet_index=1,
        sheet_count=3,
        result=goals_result,
    )
    _emit_sheet_sync_progress(logger, sheet="union", status="started", sheet_index=2, sheet_count=3)
    union_result = sync_export_rows_wide_sheet(
        spreadsheet_id=spreadsheet_id,
        sheet_name="union",
        service_account_path=service_account_path,
    )
    _emit_sheet_sync_progress(
        logger,
        sheet="union",
        status="finished",
        sheet_index=2,
        sheet_count=3,
        result=union_result,
    )
    _emit_sheet_sync_progress(logger, sheet="pipeline_status", status="started", sheet_index=3, sheet_count=3)
    status_result = sync_pipeline_status_sheet(
        spreadsheet_id=spreadsheet_id,
        sheet_name="pipeline_status",
        service_account_path=service_account_path,
    )
    _emit_sheet_sync_progress(
        logger,
        sheet="pipeline_status",
        status="finished",
        sheet_index=3,
        sheet_count=3,
        result=status_result,
    )
    return {
        "goal_mapping": goals_result,
        "union": union_result,
        "pipeline_status": status_result,
    }


def sync_status_only(*, spreadsheet_id: str, service_account_path: Path, logger=None) -> Dict[str, Any]:
    _emit_sheet_sync_progress(logger, sheet="pipeline_status", status="started", sheet_index=1, sheet_count=1)
    status_result = sync_pipeline_status_sheet(
        spreadsheet_id=spreadsheet_id,
        sheet_name="pipeline_status",
        service_account_path=service_account_path,
    )
    _emit_sheet_sync_progress(
        logger,
        sheet="pipeline_status",
        status="finished",
        sheet_index=1,
        sheet_count=1,
        result=status_result,
    )
    return {
        "pipeline_status": status_result,
    }


def normalize_one_run_date(run_date: str, *, bootstrap_mode: bool = False) -> Dict[str, Any]:
    return {
        "run_date": run_date,
        **normalize_run(
            run_date,
            logger=log_progress,
            defer_finalize=True,
            skip_delete_existing=bootstrap_mode,
        ),
    }


def run_pipeline(
    *,
    spreadsheet_id: str,
    service_account_path: Path,
    run_dates: List[str] | None = None,
) -> Dict[str, Any]:
    with acquire_pipeline_lock():
        status_before = fetch_pipeline_status_records()
        selected_run_dates = run_dates[:] if run_dates else select_pending_run_dates(status_before)
        normalized_results: List[Dict[str, Any]] = []
        failed_results: List[Dict[str, Any]] = []
        bootstrap_mode = should_use_bootstrap_mode(status_before, selected_run_dates)

        log_progress(
            "pipeline_started",
            {
                "selected_run_dates": selected_run_dates,
                "pending_count": len(selected_run_dates),
                "bootstrap_mode": bootstrap_mode,
            },
        )

        for run_date in selected_run_dates:
            log_progress("normalize_started", {"run_date": run_date})
            try:
                result = normalize_one_run_date(run_date, bootstrap_mode=bootstrap_mode)
            except Exception as error:
                failure = {"run_date": run_date, "error": str(error)}
                failed_results.append(failure)
                log_progress("normalize_failed", failure)
                continue

            normalized_results.append(result)
            log_progress("normalize_finished", public_payload(result))

        if normalized_results:
            log_progress(
                "finalize_started",
                {
                    "run_dates": [result["run_date"] for result in normalized_results],
                    "run_count": len(normalized_results),
                },
            )
            finalize_normalized_runs(normalized_results, logger=None)
            log_progress(
                "finalize_finished",
                {
                    "run_dates": [result["run_date"] for result in normalized_results],
                    "run_count": len(normalized_results),
                },
            )

        full_sync = should_sync_full_operator_views(failed_results, normalized_results)
        sync_sheets = ["pipeline_status"] if not full_sync else ["отчеты", "union", "pipeline_status"]
        log_progress("sheet_sync_started", {"sheets": sync_sheets})
        sync_results = (
            sync_operator_views(
                spreadsheet_id=spreadsheet_id,
                service_account_path=service_account_path,
                logger=log_progress,
            )
            if full_sync
            else sync_status_only(
                spreadsheet_id=spreadsheet_id,
                service_account_path=service_account_path,
                logger=log_progress,
            )
        )
        log_progress("sheet_sync_finished", sync_results)
        status_after = fetch_pipeline_status_records()
        log_progress(
            "pipeline_finished",
            {
                "selected_run_dates": selected_run_dates,
                "normalized_count": len(normalized_results),
                "failed_count": len(failed_results),
            },
        )

        return {
            "ok": True,
            "selected_run_dates": selected_run_dates,
            "normalized": normalized_results,
            "failed": failed_results,
            "sync": sync_results,
            "status_before": status_before,
            "status_after": status_after,
        }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Primary local post-ingest pipeline for the direct Apps Script -> Turso contour."
    )
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
                "normalized_runs": [public_payload(item) for item in result["normalized"]],
                "failed_runs": [public_payload(item) for item in result["failed"]],
                "sync": result["sync"],
            },
            ensure_ascii=False,
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()
