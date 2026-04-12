import unittest
from decimal import Decimal

from scripts.sync_export_rows_wide_sheet import (
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
        columns = ["topic", "report_date", "bounce_rate", "time_on_site_seconds", "source_row_json"]
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
            ["topic", "report_date", "bounce_visits", "time_on_site_total", "source_row_json"],
        )
        self.assertEqual(grid[1][0], "Solta_Nektar_2026")
        self.assertEqual(grid[1][1], "11.04.2026")
        self.assertEqual(grid[1][2], "6")
        self.assertAlmostEqual(float(grid[1][3]), 120 / 86400)
        self.assertEqual(grid[1][4], '{"UTM Source": "Solta"}')

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
        self.assertEqual(row["bounce_rate"], 3)
        self.assertEqual(row["page_depth"], 25)
        self.assertAlmostEqual(row["time_on_site_seconds"], 150 / 86400)
        self.assertEqual(row["robot_rate"], 2)

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


if __name__ == "__main__":
    unittest.main()
