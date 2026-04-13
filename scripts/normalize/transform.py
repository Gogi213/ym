from __future__ import annotations

from collections import defaultdict
import json
from typing import Any, Dict, List, Sequence, Tuple
import uuid

from .fields import (
    BASE_METRIC_KEYS,
    assign_goal_slots,
    build_fact_payload,
    build_layout_signature,
    canonical_field_for_header,
    extract_report_period_from_payload,
)


def collect_goal_slots(
    files: Sequence[Dict[str, Any]],
    existing_goal_slots: Dict[str, Dict[str, int]],
) -> Tuple[Dict[str, Dict[str, int]], Dict[str, Dict[str, str]]]:
    goal_slots_by_topic: Dict[str, Dict[str, int]] = {
        topic: dict(topic_slots) for topic, topic_slots in existing_goal_slots.items()
    }
    first_seen_file_ids: Dict[str, Dict[str, str]] = defaultdict(dict)

    for file_row in files:
        topic = file_row.get("primary_topic") or file_row["matched_topic"]
        headers = file_row.get("header_json") or []
        goal_headers = [
            header
            for header in headers
            if canonical_field_for_header(str(header or "")) and canonical_field_for_header(str(header or ""))[0] == "goal"
        ]
        if not goal_headers:
            continue

        updated_slots = assign_goal_slots(
            topic=topic,
            goal_headers=goal_headers,
            existing_slots=goal_slots_by_topic,
        )
        for header in goal_headers:
            if header not in goal_slots_by_topic.get(topic, {}):
                first_seen_file_ids[topic][header] = str(file_row["id"])
        goal_slots_by_topic[topic] = updated_slots

    return goal_slots_by_topic, dict(first_seen_file_ids)


def build_merge_key(payload: Dict[str, Any]) -> Tuple[Any, ...]:
    dimensions = payload["dimensions"]
    return (
        payload["topic"],
        payload["report_date"],
        payload["report_date_from"],
        payload["report_date_to"],
        dimensions.get("utm_source", ""),
        dimensions.get("utm_medium", ""),
        dimensions.get("utm_campaign", ""),
        dimensions.get("utm_content", ""),
        dimensions.get("utm_term", ""),
    )


def merge_secondary_payloads_into_primary(
    primary_entries: List[Dict[str, Any]],
    secondary_entries: List[Dict[str, Any]],
) -> Dict[str, int]:
    primary_index: Dict[Tuple[Any, ...], List[Dict[str, Any]]] = defaultdict(list)
    for entry in primary_entries:
        primary_index[build_merge_key(entry["payload"])].append(entry)

    matched_secondary_rows = 0
    unmatched_secondary_rows = 0
    ambiguous_secondary_rows = 0

    for entry in secondary_entries:
        payload = entry["payload"]
        merge_candidates = primary_index.get(build_merge_key(payload), [])
        if not merge_candidates:
            unmatched_secondary_rows += 1
            continue
        if len(merge_candidates) != 1:
            ambiguous_secondary_rows += 1
            continue

        target_payload = merge_candidates[0]["payload"]
        for goal_key, goal_value in sorted(payload["goals"].items()):
            current_value = target_payload["goals"].get(goal_key)
            if current_value is None:
                target_payload["goals"][goal_key] = goal_value
            else:
                target_payload["goals"][goal_key] = current_value + goal_value

        for metric_key, metric_value in sorted(payload["metrics"].items()):
            if metric_key in BASE_METRIC_KEYS:
                continue
            current_value = target_payload["metrics"].get(metric_key)
            if current_value is None:
                target_payload["metrics"][metric_key] = metric_value
            else:
                target_payload["metrics"][metric_key] = current_value + metric_value

        matched_secondary_rows += 1

    return {
        "matched_secondary_rows": matched_secondary_rows,
        "unmatched_secondary_rows": unmatched_secondary_rows,
        "ambiguous_secondary_rows": ambiguous_secondary_rows,
    }


def build_pipeline_run_ready_update(*, files_count: int, fact_rows_count: int) -> Dict[str, Any]:
    return {
        "normalized_files": files_count,
        "normalized_rows": fact_rows_count,
        "normalize_status": "ready",
        "last_error": None,
    }


def build_pipeline_run_error_update(error_message: str) -> Dict[str, Any]:
    return {
        "normalize_status": "normalize_error",
        "last_error": error_message,
    }


def build_normalized_payloads(
    files: Sequence[Dict[str, Any]],
    rows_by_file_id: Dict[str, List[Dict[str, Any]]],
    payloads_by_file_id: Dict[str, Dict[str, Any]],
    goal_slots_by_topic: Dict[str, Dict[str, int]],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, int]]:
    primary_entries: List[Dict[str, Any]] = []
    secondary_entries: List[Dict[str, Any]] = []
    fact_rows: List[Dict[str, Any]] = []
    fact_dimensions: List[Dict[str, Any]] = []
    fact_metrics: List[Dict[str, Any]] = []

    for file_row in files:
        file_id = str(file_row["id"])
        matched_topic = file_row["matched_topic"]
        primary_topic = file_row.get("primary_topic") or matched_topic
        topic_role = str(file_row.get("topic_role") or "primary")
        headers = file_row.get("header_json") or []
        layout_signature = build_layout_signature(headers)
        message_date = file_row["message_date"].isoformat() if file_row.get("message_date") else ""
        goal_slots = goal_slots_by_topic.get(primary_topic, {})
        payload_row = payloads_by_file_id.get(file_id, {})
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
                "layout_signature": layout_signature,
                "payload": payload,
                "matched_topic": matched_topic,
                "primary_topic": primary_topic,
                "topic_role": topic_role,
            }

            if topic_role == "secondary":
                secondary_entries.append(entry)
            else:
                primary_entries.append(entry)

    secondary_merge_stats = merge_secondary_payloads_into_primary(primary_entries, secondary_entries)

    for entry in primary_entries:
        file_row = entry["file_row"]
        raw_row = entry["raw_row"]
        payload = entry["payload"]
        layout_signature = entry["layout_signature"]
        file_id = str(file_row["id"])
        fact_row_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{file_id}:{raw_row['row_index']}"))

        fact_rows.append(
            {
                "fact_row_id": fact_row_id,
                "topic": payload["topic"],
                "source_file_id": file_id,
                "source_row_index": raw_row["row_index"],
                "report_date": payload["report_date"],
                "report_date_from": payload["report_date_from"],
                "report_date_to": payload["report_date_to"],
                "message_date": file_row["message_date"],
                "layout_signature": layout_signature,
                "row_hash": payload["row_hash"],
                "source_row_json": json.dumps(payload["source_row_json"], ensure_ascii=False),
            }
        )

        for dimension_key, dimension_value in sorted(payload["dimensions"].items()):
            fact_dimensions.append(
                {
                    "fact_row_id": fact_row_id,
                    "dimension_key": dimension_key,
                    "dimension_value": dimension_value,
                }
            )

        metric_items = dict(payload["metrics"])
        metric_items.update(payload["goals"])
        for metric_key, metric_value in sorted(metric_items.items()):
            fact_metrics.append(
                {
                    "fact_row_id": fact_row_id,
                    "metric_key": metric_key,
                    "metric_value": str(metric_value),
                }
            )

    return fact_rows, fact_dimensions, fact_metrics, secondary_merge_stats


def build_affected_row_keys(
    *,
    existing_keys: Sequence[Tuple[str, str]],
    fact_rows: Sequence[Dict[str, Any]],
) -> List[Tuple[str, str]]:
    affected = {
        (str(topic), str(row_hash))
        for topic, row_hash in existing_keys
        if str(topic or "").strip() and str(row_hash or "").strip()
    }
    for row in fact_rows:
        topic = str(row.get("topic") or "").strip()
        row_hash = str(row.get("row_hash") or "").strip()
        if topic and row_hash:
            affected.add((topic, row_hash))
    return sorted(affected)
