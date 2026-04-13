from __future__ import annotations

import argparse
import base64
from collections import defaultdict
from datetime import datetime
from decimal import Decimal
from decimal import InvalidOperation
import hashlib
import io
import json
import os
import re
import time
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import quote, urlsplit, urlunsplit
import uuid
import zipfile
import xml.etree.ElementTree as ET


HEADER_ALIASES: Dict[str, Tuple[str, str]] = {
    "utm_source": ("dimension", "utm_source"),
    "utm_medium": ("dimension", "utm_medium"),
    "utm_campaign": ("dimension", "utm_campaign"),
    "utm_content": ("dimension", "utm_content"),
    "utm_term": ("dimension", "utm_term"),
    "визиты": ("metric", "visits"),
    "посетители": ("metric", "users"),
    "отказы": ("metric", "bounce_rate"),
    "глубина_просмотра": ("metric", "page_depth"),
    "время_на_сайте": ("metric", "time_on_site_seconds"),
    "роботность": ("metric", "robot_rate"),
}

MAIN_NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
BASE_METRIC_KEYS = {
    "visits",
    "users",
    "bounce_rate",
    "page_depth",
    "time_on_site_seconds",
    "robot_rate",
}

IGNORED_IDENTITY_HEADER_PREFIXES = (
    "целевые_визиты",
    "конверсия",
)

IGNORED_IDENTITY_HEADERS = {
    "дата_визита",
    "просмотры_товаров",
    "посетители_посмотревшие_товар",
    "посетители_добавившие_товар_в_корзину",
}


def emit_log(
    logger: Optional[Callable[[str, Dict[str, Any]], None]],
    phase: str,
    payload: Dict[str, Any],
) -> None:
    if logger is None:
        return
    logger(phase, payload)


def public_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    return {
        key: value
        for key, value in payload.items()
        if not str(key).startswith("_")
    }


def normalize_header(value: str) -> str:
    normalized = str(value or "").strip().lower().replace("ё", "е")
    normalized = re.sub(r"[^\w]+", "_", normalized, flags=re.UNICODE)
    normalized = re.sub(r"_+", "_", normalized)
    return normalized.strip("_")


def canonical_field_for_header(header: str) -> Optional[Tuple[str, str]]:
    normalized = normalize_header(header)
    if normalized in HEADER_ALIASES:
        return HEADER_ALIASES[normalized]
    if "конверсия" in normalized:
        return None
    if "роботность" in normalized:
        return ("metric", "robot_rate")
    if normalized in {
        "товаров_куплено",
        "посетители_купившие_товар",
        "достижения_избранных_целей",
    }:
        return ("goal", normalized)
    if "доход" in normalized:
        return ("goal", normalized)
    if normalized.startswith("достижения_цели_"):
        return ("goal", normalized)
    return None


def extract_report_period_from_text(value: str) -> Optional[Tuple[str, str]]:
    text = str(value or "")
    iso_match = re.search(r"(\d{4}-\d{2}-\d{2})\s+по\s+(\d{4}-\d{2}-\d{2})", text)
    if iso_match:
        return iso_match.group(1), iso_match.group(2)

    ru_match = re.search(r"(\d{2}\.\d{2}\.\d{4})\s+по\s+(\d{2}\.\d{2}\.\d{4})", text)
    if ru_match:
        left = datetime.strptime(ru_match.group(1), "%d.%m.%Y").date().isoformat()
        right = datetime.strptime(ru_match.group(2), "%d.%m.%Y").date().isoformat()
        return left, right

    return None


def _extract_shared_strings(archive: zipfile.ZipFile) -> List[str]:
    if "xl/sharedStrings.xml" not in archive.namelist():
        return []

    root = ET.fromstring(archive.read("xl/sharedStrings.xml"))
    strings: List[str] = []
    for si in root:
        parts: List[str] = []
        for node in si.iter():
            if node.tag.endswith("}t") and node.text:
                parts.append(node.text)
        strings.append("".join(parts))
    return strings


def _extract_top_sheet_texts_from_xlsx_bytes(data: bytes, max_rows: int = 5) -> List[str]:
    archive = zipfile.ZipFile(io.BytesIO(data))
    shared_strings = _extract_shared_strings(archive)
    if "xl/worksheets/sheet1.xml" not in archive.namelist():
        return []

    sheet = ET.fromstring(archive.read("xl/worksheets/sheet1.xml"))
    rows: List[str] = []
    for row in sheet.findall(f".//{MAIN_NS}row"):
        values: List[str] = []
        for cell in row.findall(f"{MAIN_NS}c"):
            cell_type = cell.attrib.get("t")
            value = ""
            value_node = cell.find(f"{MAIN_NS}v")
            inline_node = cell.find(f"{MAIN_NS}is")
            if cell_type == "s" and value_node is not None and value_node.text is not None:
                idx = int(value_node.text)
                value = shared_strings[idx] if idx < len(shared_strings) else ""
            elif cell_type == "inlineStr" and inline_node is not None:
                value = "".join((node.text or "") for node in inline_node.iter() if node.tag.endswith("}t"))
            elif value_node is not None and value_node.text is not None:
                value = value_node.text
            if value:
                values.append(value)
        if values:
            rows.append(" | ".join(values))
        if len(rows) >= max_rows:
            break
    return rows


def extract_report_period_from_payload(*, attachment_type: str, file_base64: Optional[str]) -> Optional[Tuple[str, str]]:
    if not file_base64:
        return None

    decoded = base64.b64decode(file_base64)
    text_candidates: List[str] = []

    if attachment_type == "xlsx":
        text_candidates = _extract_top_sheet_texts_from_xlsx_bytes(decoded)
    elif attachment_type == "csv":
        text_candidates = decoded.decode("utf-8", errors="ignore").splitlines()[0:5]

    for candidate in text_candidates:
        period = extract_report_period_from_text(candidate)
        if period is not None:
            return period

    return None


def parse_metric_value(raw_value: str) -> Optional[Decimal]:
    value = str(raw_value or "").strip().replace(" ", "").replace(",", ".")
    if not value:
        return None
    try:
        return Decimal(value)
    except InvalidOperation:
        return None


def parse_duration_to_seconds(raw_value: str) -> Optional[int]:
    value = str(raw_value or "").strip()
    if not value:
        return None
    parts = value.split(":")
    if len(parts) != 3:
        return None
    hours, minutes, seconds = (int(part) for part in parts)
    return hours * 3600 + minutes * 60 + seconds


def header_affects_row_identity(header: str, raw_value: str) -> bool:
    value = str(raw_value or "").strip()
    if not value:
        return False

    normalized = normalize_header(header)
    if normalized in IGNORED_IDENTITY_HEADERS:
        return False
    if any(normalized.startswith(prefix) for prefix in IGNORED_IDENTITY_HEADER_PREFIXES):
        return False

    field = canonical_field_for_header(header)
    if field is not None:
        return False

    if parse_duration_to_seconds(value) is not None:
        return False
    if parse_metric_value(value) is not None:
        return False
    if re.match(r"^\d{4}-\d{2}-\d{2}$", value):
        return False
    if re.match(r"^\d{2}\.\d{2}\.\d{4}$", value):
        return False

    return True


def extract_report_date(*, row: Dict[str, str], message_date: str) -> str:
    row_date = str(row.get("Дата визита", "")).strip()
    if row_date:
        return row_date
    return datetime.fromisoformat(message_date.replace("Z", "+00:00")).date().isoformat()


def assign_goal_slots(
    *,
    topic: str,
    goal_headers: Iterable[str],
    existing_slots: Dict[str, Dict[str, int]],
) -> Dict[str, int]:
    topic_slots = dict(existing_slots.get(topic, {}))
    next_slot = max(topic_slots.values(), default=0) + 1

    for header in goal_headers:
        if header in topic_slots:
            continue
        topic_slots[header] = next_slot
        next_slot += 1

    return topic_slots


def build_layout_signature(headers: Iterable[str]) -> str:
    return "|".join(normalize_header(header) for header in headers if str(header or "").strip())


def build_fact_payload(
    *,
    topic: str,
    file_id: str,
    row_index: int,
    row: Dict[str, str],
    message_date: str,
    goal_slots: Dict[str, int],
    file_report_period: Optional[Tuple[str, str]] = None,
) -> Dict[str, object]:
    dimensions: Dict[str, str] = {}
    metrics: Dict[str, Decimal] = {}
    goals: Dict[str, Decimal] = {}
    identity_dimensions: Dict[str, str] = {}

    for header, raw_value in row.items():
        if header_affects_row_identity(header, str(raw_value or "")):
            identity_dimensions[normalize_header(header)] = str(raw_value or "").strip()

        field = canonical_field_for_header(header)
        if field is None:
            continue

        field_kind, canonical_key = field
        if field_kind == "dimension":
            value = str(raw_value or "").strip()
            if value:
                dimensions[canonical_key] = value
            continue

        if field_kind == "metric":
            if canonical_key == "time_on_site_seconds":
                parsed_duration = parse_duration_to_seconds(str(raw_value or ""))
                if parsed_duration is not None:
                    metrics[canonical_key] = Decimal(parsed_duration)
            else:
                parsed_metric = parse_metric_value(str(raw_value or ""))
                if parsed_metric is not None:
                    metrics[canonical_key] = parsed_metric
            continue

        if field_kind == "goal":
            slot_number = goal_slots.get(header)
            parsed_goal = parse_metric_value(str(raw_value or ""))
            if slot_number is not None and parsed_goal is not None:
                goals[f"goal_{slot_number}"] = parsed_goal

    report_date = extract_report_date(row=row, message_date=message_date)
    report_date_from = report_date
    report_date_to = report_date
    if file_report_period is not None and not str(row.get("Дата визита", "")).strip():
        report_date_from, report_date_to = file_report_period
        report_date = report_date_from
    row_hash_payload = {
        "topic": topic,
        "dimensions": dimensions,
        "identity_dimensions": identity_dimensions,
        "report_date": report_date,
    }
    row_hash = hashlib.sha256(
        json.dumps(row_hash_payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    ).hexdigest()

    return {
        "topic": topic,
        "source_file_id": file_id,
        "source_row_index": row_index,
        "report_date": report_date,
        "report_date_from": report_date_from,
        "report_date_to": report_date_to,
        "dimensions": dimensions,
        "metrics": metrics,
        "goals": goals,
        "row_hash": row_hash,
        "source_row_json": row,
    }


def build_topic_goal_slot_records(
    *,
    goal_slots_by_topic: Dict[str, Dict[str, int]],
    first_seen_file_ids: Dict[str, Dict[str, str]],
) -> List[Dict[str, object]]:
    records: List[Dict[str, object]] = []
    for topic in sorted(goal_slots_by_topic):
        topic_slots = goal_slots_by_topic[topic]
        topic_first_seen = first_seen_file_ids.get(topic, {})
        for source_header, goal_slot in sorted(topic_slots.items(), key=lambda item: item[1]):
            records.append(
                {
                    "topic": topic,
                    "goal_slot": goal_slot,
                    "source_header": source_header,
                    "goal_label": source_header,
                    "first_seen_file_id": topic_first_seen.get(source_header),
                }
            )
    return records


def load_connection_string() -> str:
    dsn = os.getenv("SUPABASE_DB_URL") or os.getenv("DATABASE_URL")
    if dsn:
        return dsn

    pooler_url = os.getenv("SUPABASE_POOLER_URL")
    password = os.getenv("SUPABASE_DB_PASSWORD")
    if pooler_url and password:
        parsed = urlsplit(pooler_url)
        if "@" not in parsed.netloc:
            raise RuntimeError("SUPABASE_POOLER_URL must include the database username.")
        username, host = parsed.netloc.rsplit("@", 1)
        return urlunsplit(
            (
                parsed.scheme,
                f"{username}:{quote(password, safe='')}@{host}",
                parsed.path,
                parsed.query,
                parsed.fragment,
            )
        )

    raise RuntimeError(
        "Database connection is not configured. Set SUPABASE_DB_URL or both SUPABASE_POOLER_URL and SUPABASE_DB_PASSWORD."
    )


def connect_db():
    try:
        import psycopg
        from psycopg.rows import dict_row
    except ImportError as exc:
        raise RuntimeError("Missing psycopg dependency. Install requirements before running the normalizer.") from exc

    connect_timeout = int(os.getenv("SUPABASE_CONNECT_TIMEOUT_SECONDS", "15"))
    statement_timeout_ms = int(os.getenv("SUPABASE_STATEMENT_TIMEOUT_MS", "300000"))
    idle_tx_timeout_ms = int(os.getenv("SUPABASE_IDLE_IN_TX_TIMEOUT_MS", "60000"))
    application_name = os.getenv("SUPABASE_APPLICATION_NAME", "ym_pipeline")

    options = (
        f"-c statement_timeout={statement_timeout_ms} "
        f"-c idle_in_transaction_session_timeout={idle_tx_timeout_ms}"
    )

    return psycopg.connect(
        load_connection_string(),
        row_factory=dict_row,
        connect_timeout=connect_timeout,
        application_name=application_name,
        options=options,
    )


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


def delete_existing_rows_for_run(conn, run_date: str) -> List[Tuple[str, str]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            with deleted as (
              delete from public.fact_rows fr
              using public.ingest_files f
              where fr.source_file_id = f.id
                and f.run_date = %s
              returning fr.topic, fr.row_hash
            )
            select distinct topic, row_hash
            from deleted
            """,
            (run_date,),
        )
        return [
            (str(row["topic"]), str(row["row_hash"]))
            for row in cur.fetchall()
            if row["topic"] and row["row_hash"]
        ]


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


def mark_pipeline_run_ready(conn, run_date: str, *, files_count: int, fact_rows_count: int) -> None:
    payload = build_pipeline_run_ready_update(
        files_count=files_count,
        fact_rows_count=fact_rows_count,
    )
    with conn.cursor() as cur:
        cur.execute(
            """
            update public.pipeline_runs
            set
              normalized_files = %(normalized_files)s,
              normalized_rows = %(normalized_rows)s,
              normalize_status = %(normalize_status)s,
              normalized_at = timezone('utc', now()),
              last_error = %(last_error)s,
              updated_at = timezone('utc', now())
            where run_date = %(run_date)s
            """,
            {
                "run_date": run_date,
                **payload,
            },
        )


def mark_pipeline_run_error(conn, run_date: str, error_message: str) -> None:
    payload = build_pipeline_run_error_update(error_message)
    with conn.cursor() as cur:
        cur.execute(
            """
            update public.pipeline_runs
            set
              normalize_status = %(normalize_status)s,
              last_error = %(last_error)s,
              updated_at = timezone('utc', now())
            where run_date = %(run_date)s
            """,
            {
                "run_date": run_date,
                **payload,
            },
        )


def upsert_topic_goal_slots(conn, records: Sequence[Dict[str, Any]]) -> None:
    if not records:
        return

    with conn.cursor() as cur:
        cur.executemany(
            """
            insert into public.topic_goal_slots (
              topic,
              goal_slot,
              source_header,
              goal_label,
              first_seen_file_id
            )
            values (
              %(topic)s,
              %(goal_slot)s,
              %(source_header)s,
              %(goal_label)s,
              %(first_seen_file_id)s
            )
            on conflict (topic, goal_slot) do update
            set
              source_header = excluded.source_header,
              goal_label = coalesce(public.topic_goal_slots.goal_label, excluded.goal_label),
              updated_at = timezone('utc', now())
            """,
            records,
        )


def copy_records(
    conn,
    *,
    table_name: str,
    columns: Sequence[str],
    rows: Sequence[Dict[str, Any]],
) -> None:
    if not rows:
        return

    column_list = ", ".join(columns)
    copy_sql = f"copy {table_name} ({column_list}) from stdin"

    with conn.cursor() as cur:
        with cur.copy(copy_sql) as copy:
            for row in rows:
                copy.write_row(tuple(row.get(column) for column in columns))


def insert_fact_rows(conn, rows: Sequence[Dict[str, Any]]) -> None:
    copy_records(
        conn,
        table_name="public.fact_rows",
        columns=[
            "fact_row_id",
            "topic",
            "source_file_id",
            "source_row_index",
            "report_date",
            "report_date_from",
            "report_date_to",
            "message_date",
            "layout_signature",
            "row_hash",
            "source_row_json",
        ],
        rows=rows,
    )


def insert_fact_dimensions(conn, rows: Sequence[Dict[str, Any]]) -> None:
    copy_records(
        conn,
        table_name="public.fact_dimensions",
        columns=[
            "fact_row_id",
            "dimension_key",
            "dimension_value",
        ],
        rows=rows,
    )


def insert_fact_metrics(conn, rows: Sequence[Dict[str, Any]]) -> None:
    copy_records(
        conn,
        table_name="public.fact_metrics",
        columns=[
            "fact_row_id",
            "metric_key",
            "metric_value",
        ],
        rows=rows,
    )


def refresh_current_flags_for_row_keys(conn, row_keys: Sequence[Tuple[str, str]]) -> None:
    filtered_keys = [
        (str(topic or "").strip(), str(row_hash or "").strip())
        for topic, row_hash in row_keys
        if str(topic or "").strip() and str(row_hash or "").strip()
    ]
    if not filtered_keys:
        return

    with conn.cursor() as cur:
        cur.execute(
            """
            create temporary table if not exists tmp_affected_row_keys (
              topic text not null,
              row_hash text not null,
              primary key (topic, row_hash)
            ) on commit drop
            """
        )
        cur.execute("truncate tmp_affected_row_keys")
        cur.executemany(
            """
            insert into tmp_affected_row_keys (topic, row_hash)
            values (%s, %s)
            on conflict (topic, row_hash) do nothing
            """,
            filtered_keys,
        )
        cur.execute(
            """
            with ranked as (
              select
                fr.fact_row_id,
                row_number() over (
                  partition by fr.topic, fr.row_hash
                  order by fr.message_date desc nulls last, fr.created_at desc, fr.source_file_id desc
                ) as rn
              from public.fact_rows fr
              join tmp_affected_row_keys ark
                on ark.topic = fr.topic
               and ark.row_hash = fr.row_hash
            )
            update public.fact_rows fr
            set is_current = (ranked.rn = 1)
            from ranked
            where ranked.fact_row_id = fr.fact_row_id
            """
        )


def refresh_operator_export_rows_for_run(conn, run_date: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            delete from public.operator_export_rows
            where run_date = %s
            """,
            (run_date,),
        )
        cur.execute(
            """
            with target_rows as (
              select
                fr.fact_row_id,
                f.run_date,
                fr.topic,
                fr.report_date,
                fr.report_date_from,
                fr.report_date_to
              from public.fact_rows fr
              join public.ingest_files f
                on f.id = fr.source_file_id
              where fr.is_current = true
                and f.run_date = %s
            ),
            dimension_pivot as (
              select
                fd.fact_row_id,
                max(case when fd.dimension_key = 'utm_source' then fd.dimension_value end) as utm_source,
                max(case when fd.dimension_key = 'utm_medium' then fd.dimension_value end) as utm_medium,
                max(case when fd.dimension_key = 'utm_campaign' then fd.dimension_value end) as utm_campaign
              from public.fact_dimensions fd
              join target_rows tr
                on tr.fact_row_id = fd.fact_row_id
              group by fd.fact_row_id
            ),
            metric_pivot as (
              select
                fm.fact_row_id,
                max(case when fm.metric_key = 'visits' then fm.metric_value end) as visits,
                max(case when fm.metric_key = 'users' then fm.metric_value end) as users,
                max(case when fm.metric_key = 'bounce_rate' then fm.metric_value end) as bounce_rate,
                max(case when fm.metric_key = 'page_depth' then fm.metric_value end) as page_depth,
                max(case when fm.metric_key = 'time_on_site_seconds' then fm.metric_value end) as time_on_site_seconds,
                max(case when fm.metric_key = 'robot_rate' then fm.metric_value end) as robot_rate,
                max(case when fm.metric_key = 'goal_1' then fm.metric_value end) as goal_1,
                max(case when fm.metric_key = 'goal_2' then fm.metric_value end) as goal_2,
                max(case when fm.metric_key = 'goal_3' then fm.metric_value end) as goal_3,
                max(case when fm.metric_key = 'goal_4' then fm.metric_value end) as goal_4,
                max(case when fm.metric_key = 'goal_5' then fm.metric_value end) as goal_5,
                max(case when fm.metric_key = 'goal_6' then fm.metric_value end) as goal_6,
                max(case when fm.metric_key = 'goal_7' then fm.metric_value end) as goal_7,
                max(case when fm.metric_key = 'goal_8' then fm.metric_value end) as goal_8,
                max(case when fm.metric_key = 'goal_9' then fm.metric_value end) as goal_9,
                max(case when fm.metric_key = 'goal_10' then fm.metric_value end) as goal_10,
                max(case when fm.metric_key = 'goal_11' then fm.metric_value end) as goal_11,
                max(case when fm.metric_key = 'goal_12' then fm.metric_value end) as goal_12,
                max(case when fm.metric_key = 'goal_13' then fm.metric_value end) as goal_13,
                max(case when fm.metric_key = 'goal_14' then fm.metric_value end) as goal_14,
                max(case when fm.metric_key = 'goal_15' then fm.metric_value end) as goal_15,
                max(case when fm.metric_key = 'goal_16' then fm.metric_value end) as goal_16,
                max(case when fm.metric_key = 'goal_17' then fm.metric_value end) as goal_17,
                max(case when fm.metric_key = 'goal_18' then fm.metric_value end) as goal_18,
                max(case when fm.metric_key = 'goal_19' then fm.metric_value end) as goal_19,
                max(case when fm.metric_key = 'goal_20' then fm.metric_value end) as goal_20,
                max(case when fm.metric_key = 'goal_21' then fm.metric_value end) as goal_21,
                max(case when fm.metric_key = 'goal_22' then fm.metric_value end) as goal_22,
                max(case when fm.metric_key = 'goal_23' then fm.metric_value end) as goal_23,
                max(case when fm.metric_key = 'goal_24' then fm.metric_value end) as goal_24,
                max(case when fm.metric_key = 'goal_25' then fm.metric_value end) as goal_25
              from public.fact_metrics fm
              join target_rows tr
                on tr.fact_row_id = fm.fact_row_id
              group by fm.fact_row_id
            )
            insert into public.operator_export_rows (
              run_date,
              topic,
              report_date,
              report_date_from,
              report_date_to,
              utm_source,
              utm_medium,
              utm_campaign,
              utm_content,
              utm_term,
              visits,
              users,
              bounce_rate,
              page_depth,
              time_on_site_seconds,
              robot_rate,
              goal_1,
              goal_2,
              goal_3,
              goal_4,
              goal_5,
              goal_6,
              goal_7,
              goal_8,
              goal_9,
              goal_10,
              goal_11,
              goal_12,
              goal_13,
              goal_14,
              goal_15,
              goal_16,
              goal_17,
              goal_18,
              goal_19,
              goal_20,
              goal_21,
              goal_22,
              goal_23,
              goal_24,
              goal_25
            )
            select
              tr.run_date,
              tr.topic,
              tr.report_date,
              tr.report_date_from,
              tr.report_date_to,
              dp.utm_source,
              dp.utm_medium,
              dp.utm_campaign,
              'aggregated'::text as utm_content,
              'aggregated'::text as utm_term,
              sum(mp.visits) as visits,
              sum(mp.users) as users,
              sum(case when mp.bounce_rate is not null then mp.bounce_rate * mp.visits end) as bounce_rate,
              sum(case when mp.page_depth is not null then mp.page_depth * mp.visits end) as page_depth,
              sum(case when mp.time_on_site_seconds is not null then mp.time_on_site_seconds * mp.visits end) as time_on_site_seconds,
              sum(case when mp.robot_rate is not null then mp.robot_rate * mp.visits end) as robot_rate,
              sum(mp.goal_1) as goal_1,
              sum(mp.goal_2) as goal_2,
              sum(mp.goal_3) as goal_3,
              sum(mp.goal_4) as goal_4,
              sum(mp.goal_5) as goal_5,
              sum(mp.goal_6) as goal_6,
              sum(mp.goal_7) as goal_7,
              sum(mp.goal_8) as goal_8,
              sum(mp.goal_9) as goal_9,
              sum(mp.goal_10) as goal_10,
              sum(mp.goal_11) as goal_11,
              sum(mp.goal_12) as goal_12,
              sum(mp.goal_13) as goal_13,
              sum(mp.goal_14) as goal_14,
              sum(mp.goal_15) as goal_15,
              sum(mp.goal_16) as goal_16,
              sum(mp.goal_17) as goal_17,
              sum(mp.goal_18) as goal_18,
              sum(mp.goal_19) as goal_19,
              sum(mp.goal_20) as goal_20,
              sum(mp.goal_21) as goal_21,
              sum(mp.goal_22) as goal_22,
              sum(mp.goal_23) as goal_23,
              sum(mp.goal_24) as goal_24,
              sum(mp.goal_25) as goal_25
            from target_rows tr
            left join dimension_pivot dp
              on dp.fact_row_id = tr.fact_row_id
            left join metric_pivot mp
              on mp.fact_row_id = tr.fact_row_id
            group by
              tr.run_date,
              tr.topic,
              tr.report_date,
              tr.report_date_from,
              tr.report_date_to,
              dp.utm_source,
              dp.utm_medium,
              dp.utm_campaign
            """,
            (run_date,),
        )


def finalize_normalized_runs(
    normalized_results: Sequence[Dict[str, Any]],
    logger: Optional[Callable[[str, Dict[str, Any]], None]] = None,
) -> None:
    started_at = time.perf_counter()

    def phase(name: str, **payload: Any) -> None:
        emit_log(
            logger,
            name,
            {
                "elapsed_ms": round((time.perf_counter() - started_at) * 1000, 2),
                **payload,
            },
        )

    successful_results = [result for result in normalized_results if not str(result.get("error") or "").strip()]
    if not successful_results:
        return

    affected_row_keys = sorted(
        {
            (str(topic), str(row_hash))
            for result in successful_results
            for topic, row_hash in result.get("_affected_row_keys", [])
            if str(topic or "").strip() and str(row_hash or "").strip()
        }
    )
    run_dates = []
    seen_run_dates = set()
    for result in successful_results:
        run_date = str(result.get("run_date") or "").strip()
        if run_date and run_date not in seen_run_dates:
            run_dates.append(run_date)
            seen_run_dates.add(run_date)

    phase(
        "finalize_normalized_runs_started",
        run_dates=run_dates,
        run_count=len(run_dates),
        row_keys=len(affected_row_keys),
    )

    with connect_db() as conn:
        phase("finalize_refresh_flags_started", row_keys=len(affected_row_keys))
        refresh_current_flags_for_row_keys(conn, affected_row_keys)
        phase("finalize_refresh_flags_finished")

        for run_date in run_dates:
            phase("finalize_refresh_operator_export_started", run_date=run_date)
            refresh_operator_export_rows_for_run(conn, run_date)
            phase("finalize_refresh_operator_export_finished", run_date=run_date)

        for result in successful_results:
            run_date = str(result["run_date"])
            phase("finalize_mark_ready_started", run_date=run_date)
            mark_pipeline_run_ready(
                conn,
                run_date,
                files_count=int(result.get("files") or 0),
                fact_rows_count=int(result.get("fact_rows") or 0),
            )
            phase("finalize_mark_ready_finished", run_date=run_date)

        phase("finalize_commit_started")
        conn.commit()
        phase("finalize_commit_finished")


def normalize_run(
    run_date: str,
    logger: Optional[Callable[[str, Dict[str, Any]], None]] = None,
    *,
    defer_finalize: bool = False,
    skip_delete_existing: bool = False,
) -> Dict[str, int]:
    started_at = time.perf_counter()

    def phase(name: str, **payload: Any) -> None:
        emit_log(
            logger,
            name,
            {
                "run_date": run_date,
                "elapsed_ms": round((time.perf_counter() - started_at) * 1000, 2),
                **payload,
            },
        )

    phase("normalize_connecting")
    with connect_db() as conn:
        try:
            phase("normalize_fetch_ingested_files_started")
            files = fetch_ingested_files(conn, run_date)
            phase("normalize_fetch_ingested_files_finished", files=len(files))
            topics = sorted({file_row["matched_topic"] for file_row in files})
            file_ids = [str(file_row["id"]) for file_row in files]

            phase("normalize_fetch_rows_started", file_ids=len(file_ids))
            rows_by_file_id = fetch_ingest_rows(conn, file_ids)
            phase(
                "normalize_fetch_rows_finished",
                files_with_rows=len(rows_by_file_id),
                raw_rows=sum(len(rows) for rows in rows_by_file_id.values()),
            )
            phase("normalize_fetch_payloads_started", file_ids=len(file_ids))
            payloads_by_file_id = fetch_ingest_payloads(conn, file_ids)
            phase("normalize_fetch_payloads_finished", payloads=len(payloads_by_file_id))
            phase("normalize_fetch_goal_slots_started", topics=len(topics))
            existing_goal_slots = fetch_existing_goal_slots(conn, topics)
            phase("normalize_fetch_goal_slots_finished", topics_with_slots=len(existing_goal_slots))

            phase("normalize_collect_goal_slots_started")
            goal_slots_by_topic, first_seen_file_ids = collect_goal_slots(files, existing_goal_slots)
            phase(
                "normalize_collect_goal_slots_finished",
                topics=len(goal_slots_by_topic),
                slots=sum(len(topic_slots) for topic_slots in goal_slots_by_topic.values()),
            )
            phase("normalize_build_goal_slot_records_started")
            topic_goal_slot_records = build_topic_goal_slot_records(
                goal_slots_by_topic=goal_slots_by_topic,
                first_seen_file_ids=first_seen_file_ids,
            )
            phase("normalize_build_goal_slot_records_finished", goal_slot_records=len(topic_goal_slot_records))
            phase("normalize_build_payloads_started")
            fact_rows, fact_dimensions, fact_metrics, secondary_merge_stats = build_normalized_payloads(
                files,
                rows_by_file_id,
                payloads_by_file_id,
                goal_slots_by_topic,
            )
            phase(
                "normalize_build_payloads_finished",
                fact_rows=len(fact_rows),
                fact_dimensions=len(fact_dimensions),
                fact_metrics=len(fact_metrics),
                **secondary_merge_stats,
            )
            if skip_delete_existing:
                deleted_row_keys = []
                phase("normalize_delete_existing_skipped")
            else:
                phase("normalize_delete_existing_rows_started")
                deleted_row_keys = delete_existing_rows_for_run(conn, run_date)
                phase("normalize_delete_existing_rows_finished", row_keys=len(deleted_row_keys))
            affected_row_keys = build_affected_row_keys(
                existing_keys=deleted_row_keys,
                fact_rows=fact_rows,
            )
            phase("normalize_build_affected_row_keys_finished", row_keys=len(affected_row_keys))
            phase("normalize_upsert_goal_slots_started", goal_slot_records=len(topic_goal_slot_records))
            upsert_topic_goal_slots(conn, topic_goal_slot_records)
            phase("normalize_upsert_goal_slots_finished")
            phase("normalize_insert_fact_rows_started", fact_rows=len(fact_rows))
            insert_fact_rows(conn, fact_rows)
            phase("normalize_insert_fact_rows_finished")
            phase("normalize_insert_fact_dimensions_started", fact_dimensions=len(fact_dimensions))
            insert_fact_dimensions(conn, fact_dimensions)
            phase("normalize_insert_fact_dimensions_finished")
            phase("normalize_insert_fact_metrics_started", fact_metrics=len(fact_metrics))
            insert_fact_metrics(conn, fact_metrics)
            phase("normalize_insert_fact_metrics_finished")
            if not defer_finalize:
                phase("normalize_refresh_flags_started", row_keys=len(affected_row_keys))
                refresh_current_flags_for_row_keys(conn, affected_row_keys)
                phase("normalize_refresh_flags_finished")
                phase("normalize_refresh_operator_export_started")
                refresh_operator_export_rows_for_run(conn, run_date)
                phase("normalize_refresh_operator_export_finished")
                phase("normalize_mark_ready_started")
                mark_pipeline_run_ready(
                    conn,
                    run_date,
                    files_count=len(files),
                    fact_rows_count=len(fact_rows),
                )
                phase("normalize_mark_ready_finished")
            phase("normalize_commit_started")
            conn.commit()
            phase("normalize_commit_finished")
        except Exception as error:
            conn.rollback()
            mark_pipeline_run_error(conn, run_date, str(error))
            conn.commit()
            raise

    result = {
        "files": len(files),
        "topics": len(topics),
        "fact_rows": len(fact_rows),
        "fact_dimensions": len(fact_dimensions),
        "fact_metrics": len(fact_metrics),
        "goal_slots": len(topic_goal_slot_records),
        **secondary_merge_stats,
    }
    if defer_finalize:
        result["_affected_row_keys"] = affected_row_keys
    phase("normalize_finished", **public_payload(result))
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Normalize raw Supabase ingest rows into canonical fact tables.")
    parser.add_argument("--run-date", required=True, help="Target run date in YYYY-MM-DD format.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    def cli_logger(phase: str, payload: Dict[str, Any]) -> None:
        print(json.dumps({"phase": phase, **payload}, ensure_ascii=False), flush=True)

    result = normalize_run(args.run_date, logger=cli_logger)
    print(
        json.dumps({"ok": True, "run_date": args.run_date, **public_payload(result)}, ensure_ascii=False),
        flush=True,
    )


if __name__ == "__main__":
    main()
