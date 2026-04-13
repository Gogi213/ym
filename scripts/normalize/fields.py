from __future__ import annotations

import base64
from datetime import datetime
from decimal import Decimal
from decimal import InvalidOperation
import hashlib
import io
import json
import re
from typing import Dict, Iterable, List, Optional, Tuple
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
