from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, List, Sequence


def fetch_ingested_files(conn, run_date: str) -> List[Dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
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
            from public.ingest_files
            where run_date = %s
              and status = 'ingested'
            order by coalesce(primary_topic, matched_topic),
                     case when coalesce(topic_role, 'primary') = 'primary' then 0 else 1 end,
                     message_date, created_at, id
            """,
            (run_date,),
        )
        return list(cur.fetchall())


def fetch_ingest_rows(conn, file_ids: Sequence[str]) -> Dict[str, List[Dict[str, Any]]]:
    if not file_ids:
        return {}

    with conn.cursor() as cur:
        cur.execute(
            """
            select file_id, row_index, row_json
            from public.ingest_rows
            where file_id = any(%s)
            order by file_id, row_index
            """,
            (list(file_ids),),
        )
        grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for row in cur.fetchall():
            grouped[str(row["file_id"])].append(row)
        return grouped


def fetch_ingest_payloads(conn, file_ids: Sequence[str]) -> Dict[str, Dict[str, Any]]:
    if not file_ids:
        return {}

    with conn.cursor() as cur:
        cur.execute(
            """
            select file_id, content_type, file_base64
            from public.ingest_file_payloads
            where file_id = any(%s)
            """,
            (list(file_ids),),
        )
        return {str(row["file_id"]): row for row in cur.fetchall()}


def fetch_existing_goal_slots(conn, topics: Sequence[str]) -> Dict[str, Dict[str, int]]:
    if not topics:
        return {}

    with conn.cursor() as cur:
        cur.execute(
            """
            select topic, goal_slot, source_header
            from public.topic_goal_slots
            where topic = any(%s)
            order by topic, goal_slot
            """,
            (list(topics),),
        )
        slots: Dict[str, Dict[str, int]] = defaultdict(dict)
        for row in cur.fetchall():
            slots[row["topic"]][row["source_header"]] = row["goal_slot"]
        return dict(slots)
