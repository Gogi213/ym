from __future__ import annotations

import base64
from collections import defaultdict
from decimal import Decimal
from typing import Any, Dict, List, Sequence, Tuple

from .fields import BASE_METRIC_KEYS, build_fact_payload, extract_report_period_from_payload
from .raw_parse import parse_attachment
from .transform import merge_secondary_payloads_into_primary


GOAL_COLUMNS = tuple(f"goal_{index}" for index in range(1, 26))
PUBLISH_BASE_METRICS = (
    "visits",
    "users",
    "bounce_rate",
    "page_depth",
    "time_on_site_seconds",
    "robot_rate",
)


def _message_date_to_text(value: Any) -> str:
    if not value:
        return ""
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _build_row_dicts(header: list[str], rows: list[list[str]]) -> list[dict[str, str]]:
    return [
        {
            str(header[index]): str(row[index] if index < len(row) else "")
            for index in range(len(header))
            if str(header[index]).strip()
        }
        for row in rows
    ]


def prepare_files_for_operator_export(
    files: Sequence[Dict[str, Any]],
    payloads_by_file_id: Dict[str, Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], Dict[str, List[Dict[str, Any]]], List[Dict[str, Any]], Dict[str, int]]:
    prepared_files: List[Dict[str, Any]] = []
    rows_by_file_id: Dict[str, List[Dict[str, Any]]] = {}
    metadata_updates: List[Dict[str, Any]] = []
    stats = {
        "prepared_files": 0,
        "ingested_files": 0,
        "skipped_files": 0,
        "error_files": 0,
        "raw_rows": 0,
    }

    for file_row in files:
        file_id = str(file_row.get("id") or "")
        if not file_id:
            continue
        stats["prepared_files"] += 1
        payload = payloads_by_file_id.get(file_id) or {}
        file_base64 = str(payload.get("file_base64") or "")

        status = "error"
        file_report_period = None
        header: list[str] = []
        row_dicts: list[dict[str, str]] = []
        error_text: str | None = None

        try:
            if not file_base64:
                raise ValueError(f"Missing payload for raw file {file_id}")
            file_report_period = extract_report_period_from_payload(
                attachment_type=str(file_row.get("attachment_type") or ""),
                file_base64=file_base64,
            )
            parsed = parse_attachment(str(file_row.get("attachment_type") or ""), base64.b64decode(file_base64))
            if parsed.table is None:
                status = "skipped"
                stats["skipped_files"] += 1
            else:
                status = "ingested"
                header = list(parsed.table.header)
                row_dicts = _build_row_dicts(header, parsed.table.rows)
                rows_by_file_id[file_id] = [
                    {"row_index": index + 1, "row_json": row}
                    for index, row in enumerate(row_dicts)
                ]
                stats["ingested_files"] += 1
                stats["raw_rows"] += len(row_dicts)
        except Exception as error:
            status = "error"
            error_text = str(error)
            stats["error_files"] += 1

        metadata_update = {
            "file_id": file_id,
            "status": status,
            "header_json": header,
            "row_count": len(row_dicts),
            "error_text": error_text,
        }
        metadata_updates.append(metadata_update)

        prepared_file_row = dict(file_row)
        prepared_file_row.update(metadata_update)
        prepared_file_row["file_report_period"] = file_report_period if status == "ingested" else None
        if status == "ingested":
            prepared_files.append(prepared_file_row)

    return prepared_files, rows_by_file_id, metadata_updates, stats


def _build_primary_and_secondary_entries(
    files: Sequence[Dict[str, Any]],
    rows_by_file_id: Dict[str, List[Dict[str, Any]]],
    payloads_by_file_id: Dict[str, Dict[str, Any]],
    goal_slots_by_topic: Dict[str, Dict[str, int]],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    primary_entries: List[Dict[str, Any]] = []
    secondary_entries: List[Dict[str, Any]] = []

    for file_row in files:
        file_id = str(file_row["id"])
        matched_topic = file_row["matched_topic"]
        primary_topic = file_row.get("primary_topic") or matched_topic
        topic_role = str(file_row.get("topic_role") or "primary")
        message_date = _message_date_to_text(file_row.get("message_date"))
        goal_slots = goal_slots_by_topic.get(primary_topic, {})
        payload_row = payloads_by_file_id.get(file_id, {})
        file_report_period = file_row.get("file_report_period")
        if file_report_period is None:
            file_report_period = extract_report_period_from_payload(
                attachment_type=file_row.get("attachment_type") or "",
                file_base64=payload_row.get("file_base64"),
            )

        for raw_row in rows_by_file_id.get(file_id, []):
            payload = build_fact_payload(
                topic=primary_topic,
                file_id=file_id,
                row_index=raw_row["row_index"],
                row=raw_row["row_json"],
                message_date=message_date,
                goal_slots=goal_slots,
                file_report_period=file_report_period,
            )
            if not payload["dimensions"] and not payload["metrics"] and not payload["goals"]:
                continue
            entry = {
                "file_row": file_row,
                "raw_row": raw_row,
                "payload": payload,
                "matched_topic": matched_topic,
                "primary_topic": primary_topic,
                "topic_role": topic_role,
                "message_date": message_date,
            }
            if topic_role == "secondary":
                secondary_entries.append(entry)
            else:
                primary_entries.append(entry)

    return primary_entries, secondary_entries


def _select_current_entries(primary_entries: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    latest_by_key: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for entry in primary_entries:
        payload = entry["payload"]
        key = (str(payload["topic"]), str(payload["row_hash"]))
        current = latest_by_key.get(key)
        if current is None:
            latest_by_key[key] = entry
            continue
        candidate_order = (str(entry["message_date"]), str(entry["file_row"]["id"]))
        current_order = (str(current["message_date"]), str(current["file_row"]["id"]))
        if candidate_order >= current_order:
            latest_by_key[key] = entry
    return list(latest_by_key.values())


def _empty_publish_row(*, run_date: str, topic: str, report_date: str, report_date_from: str, report_date_to: str, utm_source: str | None, utm_medium: str | None, utm_campaign: str | None) -> Dict[str, Any]:
    row = {
        "run_date": run_date,
        "topic": topic,
        "report_date": report_date,
        "report_date_from": report_date_from,
        "report_date_to": report_date_to,
        "utm_source": utm_source,
        "utm_medium": utm_medium,
        "utm_campaign": utm_campaign,
        "utm_content": "aggregated",
        "utm_term": "aggregated",
        "visits": None,
        "users": None,
        "bounce_rate": None,
        "page_depth": None,
        "time_on_site_seconds": None,
        "robot_rate": None,
    }
    row.update({goal_column: None for goal_column in GOAL_COLUMNS})
    return row


def _sum_decimal(current: Decimal | None, value: Decimal | None) -> Decimal | None:
    if value is None:
        return current
    if current is None:
        return value
    return current + value


def build_operator_export_rows(
    files: Sequence[Dict[str, Any]],
    rows_by_file_id: Dict[str, List[Dict[str, Any]]],
    payloads_by_file_id: Dict[str, Dict[str, Any]],
    goal_slots_by_topic: Dict[str, Dict[str, int]],
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    primary_entries, secondary_entries = _build_primary_and_secondary_entries(
        files,
        rows_by_file_id,
        payloads_by_file_id,
        goal_slots_by_topic,
    )
    current_primary_entries = _select_current_entries(primary_entries)
    current_secondary_entries = _select_current_entries(secondary_entries)
    secondary_merge_stats = merge_secondary_payloads_into_primary(current_primary_entries, current_secondary_entries)
    current_entries = current_primary_entries

    grouped_rows: Dict[Tuple[Any, ...], Dict[str, Any]] = {}
    for entry in current_entries:
        file_row = entry["file_row"]
        payload = entry["payload"]
        dimensions = payload["dimensions"]
        aggregate_key = (
            str(file_row["run_date"]),
            str(payload["topic"]),
            str(payload["report_date"]),
            str(payload["report_date_from"]),
            str(payload["report_date_to"]),
            dimensions.get("utm_source"),
            dimensions.get("utm_medium"),
            dimensions.get("utm_campaign"),
        )
        row = grouped_rows.get(aggregate_key)
        if row is None:
            row = _empty_publish_row(
                run_date=aggregate_key[0],
                topic=aggregate_key[1],
                report_date=aggregate_key[2],
                report_date_from=aggregate_key[3],
                report_date_to=aggregate_key[4],
                utm_source=aggregate_key[5],
                utm_medium=aggregate_key[6],
                utm_campaign=aggregate_key[7],
            )
            grouped_rows[aggregate_key] = row

        metrics = payload["metrics"]
        visits = metrics.get("visits")
        row["visits"] = _sum_decimal(row["visits"], visits)
        row["users"] = _sum_decimal(row["users"], metrics.get("users"))

        for metric_key in ("bounce_rate", "page_depth", "time_on_site_seconds", "robot_rate"):
            metric_value = metrics.get(metric_key)
            if metric_value is not None and visits is not None:
                row[metric_key] = _sum_decimal(row[metric_key], metric_value * visits)

        for goal_column in GOAL_COLUMNS:
            row[goal_column] = _sum_decimal(row[goal_column], payload["goals"].get(goal_column))

    rows = [grouped_rows[key] for key in sorted(grouped_rows)]
    return rows, {
        "current_rows": len(current_entries),
        "publish_rows": len(rows),
        **secondary_merge_stats,
    }


__all__ = ["GOAL_COLUMNS", "build_operator_export_rows", "prepare_files_for_operator_export"]
