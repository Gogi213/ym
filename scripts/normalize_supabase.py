from __future__ import annotations

import argparse
import base64
from collections import defaultdict
from datetime import datetime
from decimal import Decimal
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


def emit_log(
    logger: Optional[Callable[[str, Dict[str, Any]], None]],
    phase: str,
    payload: Dict[str, Any],
) -> None:
    if logger is None:
        return
    logger(phase, payload)


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
    return Decimal(value)


def parse_duration_to_seconds(raw_value: str) -> Optional[int]:
    value = str(raw_value or "").strip()
    if not value:
        return None
    parts = value.split(":")
    if len(parts) != 3:
        return None
    hours, minutes, seconds = (int(part) for part in parts)
    return hours * 3600 + minutes * 60 + seconds


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

    for header, raw_value in row.items():
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

    return psycopg.connect(load_connection_string(), row_factory=dict_row)


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
              matched_topic,
              attachment_name,
              attachment_type,
              header_json
            from public.ingest_files
            where run_date = %s
              and status = 'ingested'
            order by matched_topic, message_date, created_at, id
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
        topic = file_row["matched_topic"]
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


def build_normalized_payloads(
    files: Sequence[Dict[str, Any]],
    rows_by_file_id: Dict[str, List[Dict[str, Any]]],
    payloads_by_file_id: Dict[str, Dict[str, Any]],
    goal_slots_by_topic: Dict[str, Dict[str, int]],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    fact_rows: List[Dict[str, Any]] = []
    fact_dimensions: List[Dict[str, Any]] = []
    fact_metrics: List[Dict[str, Any]] = []

    for file_row in files:
        file_id = str(file_row["id"])
        topic = file_row["matched_topic"]
        headers = file_row.get("header_json") or []
        layout_signature = build_layout_signature(headers)
        message_date = file_row["message_date"].isoformat() if file_row.get("message_date") else ""
        goal_slots = goal_slots_by_topic.get(topic, {})
        payload_row = payloads_by_file_id.get(file_id, {})
        file_report_period = extract_report_period_from_payload(
            attachment_type=file_row.get("attachment_type") or "",
            file_base64=payload_row.get("file_base64"),
        )

        for raw_row in rows_by_file_id.get(file_id, []):
            payload = build_fact_payload(
                topic=topic,
                file_id=file_id,
                row_index=raw_row["row_index"],
                row=raw_row["row_json"],
                message_date=message_date,
                goal_slots=goal_slots,
                file_report_period=file_report_period,
            )

            if not payload["dimensions"] and not payload["metrics"] and not payload["goals"]:
                continue

            fact_row_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{file_id}:{raw_row['row_index']}"))

            fact_rows.append(
                {
                    "fact_row_id": fact_row_id,
                    "topic": topic,
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

    return fact_rows, fact_dimensions, fact_metrics


def replace_normalized_rows_for_run(conn, run_date: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            delete from public.fact_rows fr
            using public.ingest_files f
            where fr.source_file_id = f.id
              and f.run_date = %s
            """,
            (run_date,),
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


def refresh_current_flags(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("select public.refresh_fact_rows_current_flags()")


def normalize_run(
    run_date: str,
    logger: Optional[Callable[[str, Dict[str, Any]], None]] = None,
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
        fact_rows, fact_dimensions, fact_metrics = build_normalized_payloads(
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
        )

        phase("normalize_replace_rows_started")
        replace_normalized_rows_for_run(conn, run_date)
        phase("normalize_replace_rows_finished")
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
        phase("normalize_refresh_flags_started")
        refresh_current_flags(conn)
        phase("normalize_refresh_flags_finished")
        phase("normalize_commit_started")
        conn.commit()
        phase("normalize_commit_finished")

    result = {
        "files": len(files),
        "topics": len(topics),
        "fact_rows": len(fact_rows),
        "fact_dimensions": len(fact_dimensions),
        "fact_metrics": len(fact_metrics),
        "goal_slots": len(topic_goal_slot_records),
    }
    phase("normalize_finished", **result)
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
    print(json.dumps({"ok": True, "run_date": args.run_date, **result}, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
