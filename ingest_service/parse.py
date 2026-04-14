from __future__ import annotations

import csv
import io
import re
import zipfile
from xml.etree import ElementTree as ET

from ingest_service.types import ParseDebug, ParsedTable, ParseResult

UTM_HEADERS = {
    "utm_source",
    "utm_campaign",
    "utm_content",
    "utm_term",
}

NS = {
    "main": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
    "rel": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
    "pkg": "http://schemas.openxmlformats.org/package/2006/relationships",
}


def normalize_header_cell(value: object) -> str:
    return (
        str(value or "")
        .lower()
        .replace("ё", "е")
        .replace("-", "_")
        .replace(" ", "_")
    )


def count_non_empty_cells(cells: list[object]) -> int:
    return sum(1 for cell in cells if str(cell or "").strip())


def row_has_utm_header(cells: list[object]) -> bool:
    return any(normalize_header_cell(cell) in UTM_HEADERS for cell in cells)


def find_table_block(rows: list[list[str]]) -> tuple[list[str], int] | None:
    for index, row in enumerate(rows):
        if count_non_empty_cells(row) >= 2 and row_has_utm_header(row):
            return row, index
    return None


def is_summary_row(row: list[str]) -> bool:
    for cell in row:
        value = str(cell or "").strip().lower().replace("ё", "е")
        if not value:
            continue
        return value.startswith("итого") or value.startswith("total")
    return False


def extract_data_rows(rows: list[list[str]], header_index: int) -> list[list[str]]:
    data_rows: list[list[str]] = []
    for row in rows[header_index + 1 :]:
        if count_non_empty_cells(row) == 0:
            break
        if is_summary_row(row):
            continue
        if count_non_empty_cells(row) < 2:
            continue
        data_rows.append(row)
    return data_rows


def parse_csv_table(payload: bytes) -> ParseResult:
    text = payload.decode("utf-8-sig")
    lines = [line for line in text.splitlines() if line][:5]
    delimiter_scores = {}
    for candidate in [",", ";", "\t", "|"]:
        delimiter_scores[candidate] = sum(max(line.count(candidate), 0) for line in lines)
    delimiter = max(delimiter_scores, key=delimiter_scores.get) if delimiter_scores else ","
    rows = [list(row) for row in csv.reader(io.StringIO(text), delimiter=delimiter)]
    table_block = find_table_block(rows)
    if not table_block:
        return ParseResult(table=None, debug=ParseDebug(type="csv", summary={"rows": len(rows)}))
    header, header_index = table_block
    return ParseResult(
        table=ParsedTable(header=header, rows=extract_data_rows(rows, header_index)),
        debug=ParseDebug(type="csv", summary={"header_row_index": header_index + 1}),
    )


def _resolve_sheet_paths(workbook_xml: bytes, workbook_rels_xml: bytes | None) -> list[str]:
    workbook_root = ET.fromstring(workbook_xml)
    rels_root = ET.fromstring(workbook_rels_xml) if workbook_rels_xml else None
    rel_map: dict[str, str] = {}
    if rels_root is not None:
        for rel in rels_root.findall("{http://schemas.openxmlformats.org/package/2006/relationships}Relationship"):
            rel_id = rel.attrib.get("Id")
            target = rel.attrib.get("Target")
            if rel_id and target:
                rel_map[rel_id] = target
    paths: list[str] = []
    for sheet in workbook_root.findall("main:sheets/main:sheet", NS):
        rel_id = sheet.attrib.get("{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id")
        target = rel_map.get(rel_id or "")
        if not target:
            continue
        target = re.sub(r"^(\.\./)+", "", target.lstrip("/"))
        if not target.startswith("xl/"):
            target = f"xl/{target}"
        paths.append(target)
    return paths


def _extract_cells_from_row(row_elem: ET.Element) -> list[str]:
    values: list[str] = []
    for cell in row_elem.findall("main:c", NS):
        cell_type = cell.attrib.get("t")
        if cell_type == "inlineStr":
            texts = [node.text or "" for node in cell.findall(".//main:t", NS)]
            values.append("".join(texts))
            continue
        value_node = cell.find("main:v", NS)
        values.append(value_node.text if value_node is not None and value_node.text is not None else "")
    return values


def _parse_xlsx_rows(payload: bytes) -> list[list[str]]:
    with zipfile.ZipFile(io.BytesIO(payload), "r") as archive:
        workbook_xml = archive.read("xl/workbook.xml")
        workbook_rels_xml = archive.read("xl/_rels/workbook.xml.rels") if "xl/_rels/workbook.xml.rels" in archive.namelist() else None
        sheet_paths = _resolve_sheet_paths(workbook_xml, workbook_rels_xml)
        for path in sheet_paths:
            if path not in archive.namelist():
                continue
            sheet_root = ET.fromstring(archive.read(path))
            return [_extract_cells_from_row(row) for row in sheet_root.findall(".//main:row", NS)]
    return []


def parse_xlsx_table(payload: bytes) -> ParseResult:
    rows = _parse_xlsx_rows(payload)
    table_block = find_table_block(rows)
    if not table_block:
        return ParseResult(table=None, debug=ParseDebug(type="xlsx", summary={"rows": len(rows)}))
    header, header_index = table_block
    return ParseResult(
        table=ParsedTable(header=header, rows=extract_data_rows(rows, header_index)),
        debug=ParseDebug(type="xlsx", summary={"header_row_index": header_index + 1}),
    )


def parse_attachment(attachment_type: str, payload: bytes) -> ParseResult:
    return parse_csv_table(payload) if attachment_type == "csv" else parse_xlsx_table(payload)
