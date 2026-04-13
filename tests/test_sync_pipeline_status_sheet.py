import unittest
from datetime import date, datetime

from scripts.sync_pipeline_status_sheet import build_pipeline_status_grid, classify_pipeline_status


class SyncPipelineStatusSheetTests(unittest.TestCase):
    def test_classify_pipeline_status_prefers_explicit_ready_status(self):
        self.assertEqual(
            classify_pipeline_status(
                {
                    "normalize_status": "ready",
                    "ingested_files": 6,
                    "raw_rows": 473,
                    "normalized_rows": 100,
                }
            ),
            "ready",
        )

    def test_classify_pipeline_status_marks_raw_only_when_explicit(self):
        self.assertEqual(
            classify_pipeline_status(
                {
                    "normalize_status": "raw_only",
                    "ingested_files": 6,
                    "raw_rows": 473,
                    "normalized_rows": 473,
                }
            ),
            "raw_only",
        )

    def test_classify_pipeline_status_marks_pending_when_explicit(self):
        self.assertEqual(
            classify_pipeline_status(
                {
                    "normalize_status": "pending_normalize",
                    "ingested_files": 6,
                    "raw_rows": 473,
                    "normalized_rows": 473,
                }
            ),
            "pending_normalize",
        )

    def test_build_pipeline_status_grid_shapes_operator_status_table(self):
        grid = build_pipeline_status_grid(
            [
                {
                    "run_date": date(2026, 4, 11),
                    "pipeline_status": "ready",
                    "total_files": 12,
                    "ingested_files": 6,
                    "skipped_files": 6,
                    "error_files": 0,
                    "raw_rows": 473,
                    "normalized_files": 6,
                    "normalized_rows": 473,
                    "first_message_at": datetime(2026, 4, 12, 7, 0, 0),
                    "last_message_at": datetime(2026, 4, 12, 7, 30, 0),
                    "normalized_at": datetime(2026, 4, 13, 0, 5, 0),
                }
            ]
        )

        self.assertEqual(
            grid[0],
            [
                "run_date",
                "pipeline_status",
                "total_files",
                "ingested_files",
                "skipped_files",
                "error_files",
                "raw_rows",
                "normalized_files",
                "normalized_rows",
                "first_message_at",
                "last_message_at",
                "normalized_at",
            ],
        )
        self.assertEqual(grid[1][0], "11.04.2026")
        self.assertEqual(grid[1][1], "ready")
        self.assertEqual(grid[1][2], 12)
        self.assertEqual(grid[1][9], "12.04.2026 07:00:00")


if __name__ == "__main__":
    unittest.main()
