import sqlite3
import unittest
from decimal import Decimal
from unittest import mock

from scripts.bootstrap_turso import load_bootstrap_sql


def build_bootstrap_connection():
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    connection.executescript(load_bootstrap_sql())
    return connection


class NormalizeOperatorExportRuntimeTests(unittest.TestCase):
    def test_prepare_files_for_operator_export_parses_raw_payloads_in_memory(self):
        from scripts.normalize.operator_export_runtime import prepare_files_for_operator_export

        files = [
            {
                "id": "file-1",
                "run_date": "2026-04-17",
                "message_date": "2026-04-17T10:00:00Z",
                "primary_topic": "Topic A",
                "matched_topic": "Topic A",
                "topic_role": "primary",
                "attachment_type": "csv",
                "header_json": [],
                "status": "raw_only",
            }
        ]
        payloads_by_file_id = {
            "file-1": {
                "file_base64": "VVRNIFNvdXJjZTtVVE0gQ2FtcGFpZ2470JLQuNC30LjRgtGLCmdvb2dsZTticmFuZDsxMAo="
            }
        }

        prepared_files, rows_by_file_id, metadata_updates, stats = prepare_files_for_operator_export(
            files,
            payloads_by_file_id,
        )

        self.assertEqual(stats["prepared_files"], 1)
        self.assertEqual(stats["ingested_files"], 1)
        self.assertEqual(stats["raw_rows"], 1)
        self.assertEqual(len(prepared_files), 1)
        self.assertEqual(prepared_files[0]["status"], "ingested")
        self.assertIsNone(prepared_files[0]["file_report_period"])
        self.assertEqual(prepared_files[0]["row_count"], 1)
        self.assertEqual(prepared_files[0]["header_json"], ["UTM Source", "UTM Campaign", "Визиты"])
        self.assertEqual(
            rows_by_file_id,
            {
                "file-1": [
                    {
                        "row_index": 1,
                        "row_json": {
                            "UTM Source": "google",
                            "UTM Campaign": "brand",
                            "Визиты": "10",
                        },
                    }
                ]
            },
        )
        self.assertEqual(
            metadata_updates,
            [
                {
                    "file_id": "file-1",
                    "status": "ingested",
                    "header_json": ["UTM Source", "UTM Campaign", "Визиты"],
                    "row_count": 1,
                    "error_text": None,
                }
            ],
        )

    def test_prepare_files_for_operator_export_uses_parse_result_report_period(self):
        from scripts.normalize.operator_export_runtime import prepare_files_for_operator_export

        files = [
            {
                "id": "file-1",
                "run_date": "2026-04-17",
                "message_date": "2026-04-17T10:00:00Z",
                "primary_topic": "Topic A",
                "matched_topic": "Topic A",
                "topic_role": "primary",
                "attachment_type": "csv",
                "header_json": [],
                "status": "raw_only",
            }
        ]
        payloads_by_file_id = {
            "file-1": {
                "file_base64": "0J7RgtGH0LXRgiDQt9CwINC/0LXRgNC40L7QtCDRgSAyMDI2LTA0LTEwINC/0L4gMjAyNi0wNC0xMApVVE0gU291cmNlO1VUTSBDYW1wYWlnbjtCaXR5Cmdvb2dsZTticmFuZDsxMAo="
            }
        }

        prepared_files, _rows_by_file_id, _metadata_updates, _stats = prepare_files_for_operator_export(
            files,
            payloads_by_file_id,
        )

        self.assertEqual(prepared_files[0]["file_report_period"], ("2026-04-10", "2026-04-10"))

    def test_build_operator_export_rows_uses_latest_primary_and_merges_secondary_goals(self):
        from scripts.normalize.operator_export_runtime import build_operator_export_rows

        files = [
            {
                "id": "file-old",
                "run_date": "2026-04-17",
                "message_date": "2026-04-17T09:00:00Z",
                "primary_topic": "Topic A",
                "matched_topic": "Topic A",
                "topic_role": "primary",
                "attachment_type": "csv",
                "header_json": ["UTM Source", "UTM Medium", "UTM Campaign", "Визиты"],
            },
            {
                "id": "file-new",
                "run_date": "2026-04-17",
                "message_date": "2026-04-17T10:00:00Z",
                "primary_topic": "Topic A",
                "matched_topic": "Topic A",
                "topic_role": "primary",
                "attachment_type": "csv",
                "header_json": ["UTM Source", "UTM Medium", "UTM Campaign", "Визиты"],
            },
            {
                "id": "file-secondary",
                "run_date": "2026-04-17",
                "message_date": "2026-04-17T10:05:00Z",
                "primary_topic": "Topic A",
                "matched_topic": "Topic A Secondary",
                "topic_role": "secondary",
                "attachment_type": "csv",
                "header_json": ["UTM Source", "UTM Medium", "UTM Campaign", "Достижения избранных целей"],
            },
        ]
        rows_by_file_id = {
            "file-old": [
                {
                    "row_index": 1,
                    "row_json": {
                        "UTM Source": "google",
                        "UTM Medium": "cpc",
                        "UTM Campaign": "brand",
                        "Визиты": "10",
                    },
                }
            ],
            "file-new": [
                {
                    "row_index": 1,
                    "row_json": {
                        "UTM Source": "google",
                        "UTM Medium": "cpc",
                        "UTM Campaign": "brand",
                        "Визиты": "20",
                    },
                }
            ],
            "file-secondary": [
                {
                    "row_index": 1,
                    "row_json": {
                        "UTM Source": "google",
                        "UTM Medium": "cpc",
                        "UTM Campaign": "brand",
                        "Достижения избранных целей": "3",
                    },
                }
            ],
        }
        payloads_by_file_id = {
            "file-old": {"file_base64": ""},
            "file-new": {"file_base64": ""},
            "file-secondary": {"file_base64": ""},
        }
        goal_slots_by_topic = {"Topic A": {"Достижения избранных целей": 1}}

        rows, stats = build_operator_export_rows(
            files,
            rows_by_file_id,
            payloads_by_file_id,
            goal_slots_by_topic,
        )

        self.assertEqual(stats["current_rows"], 1)
        self.assertEqual(stats["matched_secondary_rows"], 1)
        self.assertEqual(len(rows), 1)
        self.assertEqual(
            rows[0],
            {
                "run_date": "2026-04-17",
                "topic": "Topic A",
                "report_date": "2026-04-17",
                "report_date_from": "2026-04-17",
                "report_date_to": "2026-04-17",
                "utm_source": "google",
                "utm_medium": "cpc",
                "utm_campaign": "brand",
                "utm_content": "aggregated",
                "utm_term": "aggregated",
                "visits": Decimal("20"),
                "users": None,
                "bounce_rate": None,
                "page_depth": None,
                "time_on_site_seconds": None,
                "robot_rate": None,
                **{f"goal_{index}": (Decimal("3") if index == 1 else None) for index in range(1, 26)},
            },
        )

    def test_build_operator_export_rows_aggregates_publish_grain(self):
        from scripts.normalize.operator_export_runtime import build_operator_export_rows

        files = [
            {
                "id": "file-a",
                "run_date": "2026-04-17",
                "message_date": "2026-04-17T10:00:00Z",
                "primary_topic": "Topic A",
                "matched_topic": "Topic A",
                "topic_role": "primary",
                "attachment_type": "csv",
                "header_json": [
                    "UTM Source",
                    "UTM Medium",
                    "UTM Campaign",
                    "Визиты",
                    "Посетители",
                    "Отказы",
                    "Глубина просмотра",
                ],
            }
        ]
        rows_by_file_id = {
            "file-a": [
                {
                    "row_index": 1,
                    "row_json": {
                        "UTM Source": "google",
                        "UTM Medium": "cpc",
                        "UTM Campaign": "brand",
                        "Креатив": "A",
                        "Визиты": "10",
                        "Посетители": "8",
                        "Отказы": "0.5",
                        "Глубина просмотра": "2",
                    },
                },
                {
                    "row_index": 2,
                    "row_json": {
                        "UTM Source": "google",
                        "UTM Medium": "cpc",
                        "UTM Campaign": "brand",
                        "Креатив": "B",
                        "Визиты": "20",
                        "Посетители": "15",
                        "Отказы": "0.25",
                        "Глубина просмотра": "3",
                    },
                },
            ]
        }

        rows, stats = build_operator_export_rows(files, rows_by_file_id, {"file-a": {"file_base64": ""}}, {})

        self.assertEqual(stats["current_rows"], 2)
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["visits"], Decimal("30"))
        self.assertEqual(row["users"], Decimal("23"))
        self.assertEqual(row["bounce_rate"], Decimal("10"))
        self.assertEqual(row["page_depth"], Decimal("80"))

    def test_build_operator_export_rows_uses_precomputed_file_report_period(self):
        from scripts.normalize.operator_export_runtime import build_operator_export_rows

        files = [
            {
                "id": "file-a",
                "run_date": "2026-04-17",
                "message_date": "2026-04-17T10:00:00Z",
                "primary_topic": "Topic A",
                "matched_topic": "Topic A",
                "topic_role": "primary",
                "attachment_type": "csv",
                "header_json": ["UTM Source", "Визиты"],
                "file_report_period": ("2026-04-10", "2026-04-10"),
            }
        ]
        rows_by_file_id = {
            "file-a": [
                {
                    "row_index": 1,
                    "row_json": {
                        "UTM Source": "google",
                        "Визиты": "10",
                    },
                }
            ]
        }

        rows, _stats = build_operator_export_rows(files, rows_by_file_id, {"file-a": {}}, {})

        self.assertEqual(rows[0]["report_date"], "2026-04-10")
        self.assertEqual(rows[0]["report_date_from"], "2026-04-10")
        self.assertEqual(rows[0]["report_date_to"], "2026-04-10")


if __name__ == "__main__":
    unittest.main()
