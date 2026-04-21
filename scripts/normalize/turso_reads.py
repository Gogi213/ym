from __future__ import annotations

from collections import defaultdict
import json
from typing import Any, Dict, List, Sequence


def _row_value(row, columns: list[str], key: str, default=None):
    if row is None:
        return default
    if isinstance(row, dict):
        return row.get(key, default)
    try:
        return row[key]
    except (TypeError, KeyError, IndexError):
        pass
    if hasattr(row, "keys"):
        try:
            return row[key]
        except Exception:
            pass
    try:
        index = columns.index(key)
    except ValueError:
        return default
    return row[index] if index < len(row) else default


def _row_to_dict(row, columns: list[str]) -> Dict[str, Any]:
    return {column: _row_value(row, columns, column) for column in columns}


def _decode_json_column(value: Any, fallback):
    if value in (None, ""):
        return fallback
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value))
    except json.JSONDecodeError:
        return fallback


def _fetchall_with_columns(cursor):
    rows = cursor.fetchall()
    columns = [description[0] for description in (cursor.description or [])]
    return rows, columns


def fetch_run_files(conn, run_date: str) -> List[Dict[str, Any]]:
    cursor = conn.execute(
        """
        select
          f.id,
          f.run_date,
          f.message_id,
          f.thread_id,
          f.message_date,
          f.message_subject,
          f.primary_topic,
          f.matched_topic,
          f.topic_role,
          f.attachment_name,
          f.attachment_type,
          f.status,
          f.header_json,
          p.content_type,
          p.file_base64
        from ingest_files f
        join ingest_file_payloads p
          on p.file_id = f.id
        where f.run_date = ?
        order by coalesce(f.primary_topic, f.matched_topic),
                 case when coalesce(f.topic_role, 'primary') = 'primary' then 0 else 1 end,
                 f.message_date, f.created_at, f.id
        """,
        (run_date,),
    )
    rows, columns = _fetchall_with_columns(cursor)
    records = [_row_to_dict(row, columns) for row in rows]
    for record in records:
        record["header_json"] = _decode_json_column(record.get("header_json"), [])
    return records


def fetch_ingested_files(conn, run_date: str) -> List[Dict[str, Any]]:
    cursor = conn.execute(
        """
        select
          id,
          run_date,
          message_id,
          thread_id,
          message_date,
          message_subject,
          primary_topic,
          matched_topic,
          topic_role,
          attachment_name,
          attachment_type,
          header_json
        from ingest_files
        where run_date = ?
          and status = 'ingested'
        order by coalesce(primary_topic, matched_topic),
                 case when coalesce(topic_role, 'primary') = 'primary' then 0 else 1 end,
                 message_date, created_at, id
        """,
        (run_date,),
    )
    rows, columns = _fetchall_with_columns(cursor)
    records = [_row_to_dict(row, columns) for row in rows]
    for record in records:
        record["header_json"] = _decode_json_column(record.get("header_json"), [])
    return records


def fetch_existing_goal_slots(conn, topics: Sequence[str]) -> Dict[str, Dict[str, int]]:
    if not topics:
        return {}

    placeholders = ", ".join(["?"] * len(topics))
    cursor = conn.execute(
        f"""
        select topic, goal_slot, source_header
        from topic_goal_slots
        where topic in ({placeholders})
        order by topic, goal_slot
        """,
        tuple(topics),
    )
    rows, columns = _fetchall_with_columns(cursor)
    slots: Dict[str, Dict[str, int]] = defaultdict(dict)
    for row in rows:
        topic = str(_row_value(row, columns, "topic"))
        source_header = str(_row_value(row, columns, "source_header"))
        goal_slot = int(_row_value(row, columns, "goal_slot", 0) or 0)
        slots[topic][source_header] = goal_slot
    return dict(slots)


__all__ = [
    "fetch_existing_goal_slots",
    "fetch_ingested_files",
    "fetch_run_files",
]
