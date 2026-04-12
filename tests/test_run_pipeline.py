import unittest
from datetime import date

from scripts.run_pipeline import select_pending_run_dates


class RunPipelineTests(unittest.TestCase):
    def test_select_pending_run_dates_returns_only_pending_days(self):
        self.assertEqual(
            select_pending_run_dates(
                [
                    {"run_date": date(2026, 4, 11), "pipeline_status": "ready"},
                    {"run_date": date(2026, 4, 10), "pipeline_status": "pending_normalize"},
                    {"run_date": date(2026, 4, 9), "pipeline_status": "raw_only"},
                    {"run_date": date(2026, 4, 8), "pipeline_status": "pending_normalize"},
                ]
            ),
            ["2026-04-10", "2026-04-08"],
        )


if __name__ == "__main__":
    unittest.main()
