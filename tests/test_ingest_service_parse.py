import io
import unittest
import zipfile


def build_xlsx_bytes(sheet_xml: str) -> bytes:
    workbook_xml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
    <workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"
      xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
      <sheets><sheet name="Sheet1" sheetId="1" r:id="rId1"/></sheets>
    </workbook>"""
    workbook_rels_xml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
    <Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
      <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>
    </Relationships>"""
    content_types_xml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
    <Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
      <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
      <Default Extension="xml" ContentType="application/xml"/>
      <Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
      <Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
    </Types>"""

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("[Content_Types].xml", content_types_xml)
        archive.writestr("xl/workbook.xml", workbook_xml)
        archive.writestr("xl/_rels/workbook.xml.rels", workbook_rels_xml)
        archive.writestr("xl/worksheets/sheet1.xml", sheet_xml)
    return buffer.getvalue()


class IngestServiceParseTests(unittest.TestCase):
    def test_parse_attachment_detects_csv_table_block(self):
        from ingest_service.parse import parse_attachment

        payload = (
            "мусор;заголовок\n"
            "UTM Source;UTM Campaign;Визиты\n"
            "google;brand;10\n"
            "yandex;perf;20\n"
            "Итого;;30\n"
        ).encode("utf-8")

        parsed = parse_attachment("csv", payload)

        self.assertEqual(parsed.table.header, ["UTM Source", "UTM Campaign", "Визиты"])
        self.assertEqual(parsed.table.rows, [["google", "brand", "10"], ["yandex", "perf", "20"]])
        self.assertEqual(parsed.debug.type, "csv")

    def test_parse_attachment_detects_xlsx_table_block(self):
        from ingest_service.parse import parse_attachment

        sheet_xml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
        <worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
          <sheetData>
            <row r="1">
              <c r="A1" t="inlineStr"><is><t>noise</t></is></c>
            </row>
            <row r="2">
              <c r="A2" t="inlineStr"><is><t>UTM Source</t></is></c>
              <c r="B2" t="inlineStr"><is><t>UTM Campaign</t></is></c>
              <c r="C2" t="inlineStr"><is><t>Визиты</t></is></c>
            </row>
            <row r="3">
              <c r="A3" t="inlineStr"><is><t>google</t></is></c>
              <c r="B3" t="inlineStr"><is><t>brand</t></is></c>
              <c r="C3"><v>7</v></c>
            </row>
            <row r="4">
              <c r="A4" t="inlineStr"><is><t>Итого</t></is></c>
              <c r="C4"><v>7</v></c>
            </row>
          </sheetData>
        </worksheet>"""

        parsed = parse_attachment("xlsx", build_xlsx_bytes(sheet_xml))

        self.assertEqual(parsed.table.header, ["UTM Source", "UTM Campaign", "Визиты"])
        self.assertEqual(parsed.table.rows, [["google", "brand", "7"]])
        self.assertEqual(parsed.debug.type, "xlsx")

    def test_parse_attachment_returns_null_table_when_no_utm_header(self):
        from ingest_service.parse import parse_attachment

        payload = "a,b,c\n1,2,3\n".encode("utf-8")
        parsed = parse_attachment("csv", payload)

        self.assertIsNone(parsed.table)
        self.assertEqual(parsed.debug.type, "csv")


if __name__ == "__main__":
    unittest.main()
