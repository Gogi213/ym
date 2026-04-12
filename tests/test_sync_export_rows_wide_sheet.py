import unittest
from decimal import Decimal

from scripts.sync_export_rows_wide_sheet import (
    aggregate_operator_records,
    build_display_columns,
    build_export_rows_grid,
    chunk_grid_rows,
    filter_export_columns,
    transform_operator_record,
)


class BuildExportRowsGridTests(unittest.TestCase):
    def test_filter_export_columns_removes_service_fields(self):
        columns = [
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
            "utm_source",
            "visits",
            "goal_1",
            "source_row_json",
            "created_at",
        ]

        self.assertEqual(
            filter_export_columns(columns),
            [
                "topic",
                "report_date",
                "report_date_from",
                "report_date_to",
                "utm_source",
                "visits",
                "goal_1",
            ],
        )

    def test_builds_header_and_stringifies_values(self):
        columns = ["topic", "report_date", "visits", "bounce_rate", "time_on_site_seconds", "source_row_json"]
        records = [
            {
                "topic": "Solta_Nektar_2026",
                "report_date": "2026-04-11",
                "visits": "12.0",
                "bounce_rate": "0.5",
                "time_on_site_seconds": "10",
                "source_row_json": {"UTM Source": "Solta"},
            }
        ]

        grid = build_export_rows_grid(columns, records)

        self.assertEqual(
            grid[0],
            ["topic", "report_date", "visits", "bounce_visits", "time_on_site_total", "source_row_json"],
        )
        self.assertEqual(grid[1][0], "Solta_Nektar_2026")
        self.assertEqual(grid[1][1], "11.04.2026")
        self.assertEqual(grid[1][2], 12)
        self.assertEqual(grid[1][3], 6)
        self.assertAlmostEqual(float(grid[1][4]), 120 / 86400)
        self.assertEqual(grid[1][5], '{"UTM Source": "Solta"}')

    def test_transform_operator_record_converts_metrics_to_aggregatable_values(self):
        row = transform_operator_record(
            {
                "report_date": "2026-04-11",
                "report_date_from": "2026-04-11",
                "report_date_to": "2026-04-11",
                "visits": Decimal("10"),
                "bounce_rate": Decimal("0.3"),
                "page_depth": Decimal("2.5"),
                "time_on_site_seconds": Decimal("15"),
                "robot_rate": Decimal("0.2"),
            }
        )

        self.assertEqual(row["report_date"], "11.04.2026")
        self.assertEqual(row["report_date_from"], "11.04.2026")
        self.assertEqual(row["report_date_to"], "11.04.2026")
        self.assertEqual(row["bounce_rate"], Decimal("3.0"))
        self.assertEqual(row["page_depth"], Decimal("25.0"))
        self.assertEqual(row["time_on_site_seconds"], Decimal("150"))
        self.assertEqual(row["robot_rate"], Decimal("2.0"))

    def test_chunk_grid_rows_splits_large_grid_into_stable_batches(self):
        grid = [["header"], ["row1"], ["row2"], ["row3"], ["row4"]]
        self.assertEqual(
            chunk_grid_rows(grid, 2),
            [
                [["header"], ["row1"]],
                [["row2"], ["row3"]],
                [["row4"]],
            ],
        )

    def test_build_display_columns_renames_aggregated_metric_headers(self):
        self.assertEqual(
            build_display_columns(
                ["topic", "bounce_rate", "page_depth", "time_on_site_seconds", "robot_rate", "goal_1"]
            ),
            ["topic", "bounce_visits", "pageviews", "time_on_site_total", "robot_visits", "goal_1"],
        )

    def test_aggregate_operator_records_collapses_all_terms_into_aggregated(self):
        records = [
            {
                "topic": "TW",
                "report_date": "11.04.2026",
                "report_date_from": "11.04.2026",
                "report_date_to": "11.04.2026",
                "utm_source": "solta",
                "utm_medium": "cpm",
                "utm_campaign": "cmp",
                "utm_content": "banner",
                "utm_term": "term-1",
                "visits": Decimal("10"),
                "users": Decimal("9"),
                "bounce_rate": Decimal("3"),
                "page_depth": Decimal("25"),
                "time_on_site_seconds": Decimal("150"),
                "robot_rate": Decimal("2"),
                "goal_1": Decimal("5"),
            },
            {
                "topic": "TW",
                "report_date": "11.04.2026",
                "report_date_from": "11.04.2026",
                "report_date_to": "11.04.2026",
                "utm_source": "solta",
                "utm_medium": "cpm",
                "utm_campaign": "cmp",
                "utm_content": "banner",
                "utm_term": "term-2",
                "visits": Decimal("7"),
                "users": Decimal("6"),
                "bounce_rate": Decimal("4"),
                "page_depth": Decimal("14"),
                "time_on_site_seconds": Decimal("70"),
                "robot_rate": Decimal("1"),
                "goal_1": Decimal("3"),
            },
        ]

        aggregated = aggregate_operator_records(records)

        self.assertEqual(len(aggregated), 1)
        self.assertEqual(aggregated[0]["utm_term"], "aggregated")
        self.assertEqual(aggregated[0]["visits"], Decimal("17"))
        self.assertEqual(aggregated[0]["users"], Decimal("15"))
        self.assertEqual(aggregated[0]["bounce_rate"], Decimal("7"))
        self.assertEqual(aggregated[0]["page_depth"], Decimal("39"))
        self.assertEqual(aggregated[0]["time_on_site_seconds"], Decimal("220"))
        self.assertEqual(aggregated[0]["robot_rate"], Decimal("3"))
        self.assertEqual(aggregated[0]["goal_1"], Decimal("8"))

    def test_build_export_rows_grid_ignores_hidden_service_fields_when_collapsing_terms(self):
        columns = [
            "topic",
            "report_date",
            "report_date_from",
            "report_date_to",
            "utm_source",
            "utm_medium",
            "utm_campaign",
            "utm_content",
            "utm_term",
            "visits",
        ]
        records = [
            {
                "topic": "TW",
                "report_date": "2026-04-11",
                "report_date_from": "2026-04-11",
                "report_date_to": "2026-04-11",
                "utm_source": "solta",
                "utm_medium": "cpm",
                "utm_campaign": "cmp",
                "utm_content": "banner",
                "utm_term": "term-1",
                "visits": "10",
                "source_file_id": "file-1",
                "source_row_index": 1,
            },
            {
                "topic": "TW",
                "report_date": "2026-04-11",
                "report_date_from": "2026-04-11",
                "report_date_to": "2026-04-11",
                "utm_source": "solta",
                "utm_medium": "cpm",
                "utm_campaign": "cmp",
                "utm_content": "banner",
                "utm_term": "term-2",
                "visits": "7",
                "source_file_id": "file-2",
                "source_row_index": 2,
            },
        ]

        grid = build_export_rows_grid(columns, records)

        self.assertEqual(len(grid), 2)
        self.assertEqual(grid[1][8], "aggregated")
        self.assertEqual(grid[1][9], 17)


if __name__ == "__main__":
    unittest.main()
