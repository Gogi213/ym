import unittest

from scripts.sync_export_rows_wide_sheet import build_export_rows_grid, filter_export_columns


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
        columns = ["topic", "report_date", "visits", "source_row_json"]
        records = [
            {
                "topic": "Solta_Nektar_2026",
                "report_date": "2026-04-11",
                "visits": "12.0",
                "source_row_json": {"UTM Source": "Solta"},
            }
        ]

        grid = build_export_rows_grid(columns, records)

        self.assertEqual(grid[0], columns)
        self.assertEqual(grid[1][0], "Solta_Nektar_2026")
        self.assertEqual(grid[1][1], "2026-04-11")
        self.assertEqual(grid[1][2], "12.0")
        self.assertEqual(grid[1][3], '{"UTM Source": "Solta"}')


if __name__ == "__main__":
    unittest.main()
