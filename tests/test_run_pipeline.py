import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch
import os
import sqlite3

from scripts.run_pipeline import (
    has_failed_runs,
    run_pipeline,
    select_pending_run_dates,
    should_use_bootstrap_mode,
    should_sync_full_operator_views,
)
from scripts.bootstrap_turso import load_bootstrap_sql


def build_bootstrap_connection():
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    connection.executescript(load_bootstrap_sql())
    return connection


class RunPipelineTests(unittest.TestCase):
    def test_select_pending_run_dates_returns_only_pending_days(self):
        self.assertEqual(
            select_pending_run_dates(
                [
                    {"run_date": date(2026, 4, 11), "normalize_status": "ready"},
                    {"run_date": date(2026, 4, 10), "normalize_status": "pending_normalize"},
                    {"run_date": date(2026, 4, 9), "pipeline_status": "raw_only"},
                    {"run_date": date(2026, 4, 8), "normalize_status": "normalize_error"},
                ]
            ),
            ["2026-04-10", "2026-04-08"],
        )

    def test_should_sync_full_operator_views_only_when_normalized_runs_exist(self):
        self.assertFalse(should_sync_full_operator_views([], []))
        self.assertTrue(
            should_sync_full_operator_views(
                [],
                [
                    {"run_date": "2026-04-10", "fact_rows": 100},
                ]
            )
        )

    def test_should_not_sync_full_operator_views_when_any_run_failed(self):
        self.assertFalse(
            should_sync_full_operator_views(
                [
                    {"run_date": "2026-04-09", "error": "boom"},
                ],
                [
                    {"run_date": "2026-04-10", "fact_rows": 100},
                ],
            )
        )

    def test_has_failed_runs_detects_any_error_result(self):
        self.assertFalse(has_failed_runs([]))
        self.assertFalse(has_failed_runs([{"run_date": "2026-04-10", "fact_rows": 100}]))
        self.assertTrue(
            has_failed_runs(
                [
                    {"run_date": "2026-04-10", "fact_rows": 100},
                    {"run_date": "2026-04-09", "error": "boom"},
                ]
            )
        )

    def test_should_use_bootstrap_mode_when_normalized_layer_is_empty(self):
        self.assertTrue(
            should_use_bootstrap_mode(
                [
                    {"run_date": date(2026, 4, 12), "normalized_rows": 0},
                    {"run_date": date(2026, 4, 11), "normalized_rows": 0},
                ],
                ["2026-04-12", "2026-04-11"],
            )
        )

    def test_should_not_use_bootstrap_mode_when_any_normalized_rows_exist(self):
        self.assertFalse(
            should_use_bootstrap_mode(
                [
                    {"run_date": date(2026, 4, 12), "normalized_rows": 10},
                    {"run_date": date(2026, 4, 11), "normalized_rows": 0},
                ],
                ["2026-04-12", "2026-04-11"],
            )
        )

    def test_normalize_run_prepares_raw_only_files_before_building_fact_rows(self):
        from scripts.normalize.pipeline import normalize_run

        connection = build_bootstrap_connection()
        connection.executescript(
            """
            insert into pipeline_runs (
              run_date, raw_revision, normalize_status, raw_files, raw_rows,
              normalized_files, normalized_rows, last_ingest_at, normalized_at,
              last_error, updated_at
            ) values (
              '2026-04-14', 1, 'pending_normalize', 1, 0, 0, 0,
              '2026-04-14T09:00:00Z', null, null, '2026-04-14T09:00:00Z'
            );
            insert into ingest_files (
              id, raw_file_key, file_hash, run_date, message_id, thread_id, message_date, message_subject,
              primary_topic, matched_topic, topic_role, attachment_name, attachment_type,
              status, header_json, row_count, error_text
            ) values (
              'file-1', 'rk-file-1', 'hash-file-1', '2026-04-14', 'm1', 't1', '2026-04-14T10:00:00Z',
              'Отчет за 2026-04-14', 'Topic A', 'Topic A', 'primary',
              'a.csv', 'csv', 'raw_only', '[]', 0, null
            );
            insert into ingest_file_payloads (file_id, content_type, file_size_bytes, file_base64)
            values ('file-1', 'text/csv', 53, 'VVRNIFNvdXJjZTtVVE0gQ2FtcGFpZ2470JLQuNC30LjRgtGLCmdvb2dsZTticmFuZDsxMAo=');
            """
        )
        connection.commit()

        with patch("scripts.normalize.pipeline.connect_db", return_value=connection):
            result = normalize_run("2026-04-14", defer_finalize=True)

        file_row = connection.execute(
            "select status, row_count from ingest_files where id = 'file-1'"
        ).fetchone()
        raw_rows = connection.execute(
            "select count(*) from ingest_rows where file_id = 'file-1'"
        ).fetchone()[0]

        self.assertEqual(result["files"], 1)
        self.assertEqual(result["fact_rows"], 1)
        self.assertEqual(dict(file_row), {"status": "ingested", "row_count": 1})
        self.assertEqual(raw_rows, 1)

    @patch("scripts.run_pipeline.sync_status_only")
    @patch("scripts.run_pipeline.sync_operator_views")
    @patch("scripts.run_pipeline.finalize_normalized_runs")
    @patch("scripts.run_pipeline.normalize_one_run_date")
    @patch("scripts.run_pipeline.fetch_pipeline_status_records")
    def test_run_pipeline_skips_full_sync_when_any_run_fails(
        self,
        fetch_status_mock,
        normalize_one_run_date_mock,
        finalize_normalized_runs_mock,
        sync_operator_views_mock,
        sync_status_only_mock,
    ):
        fetch_status_mock.side_effect = [
            [
                {"run_date": date(2026, 4, 12), "normalize_status": "pending_normalize"},
                {"run_date": date(2026, 4, 11), "normalize_status": "pending_normalize"},
            ],
            [
                {"run_date": date(2026, 4, 12), "normalize_status": "ready"},
                {"run_date": date(2026, 4, 11), "normalize_status": "normalize_error"},
            ],
        ]

        def normalize_side_effect(run_date, bootstrap_mode=False):
            if run_date == "2026-04-11":
                raise RuntimeError("boom")
            return {"run_date": run_date, "fact_rows": 10}

        normalize_one_run_date_mock.side_effect = normalize_side_effect
        finalize_normalized_runs_mock.return_value = None
        sync_status_only_mock.return_value = {"pipeline_status": {"ok": True}}

        result = run_pipeline(
            spreadsheet_id="sheet-id",
            service_account_path=Path("key.json"),
        )

        self.assertEqual(result["selected_run_dates"], ["2026-04-12", "2026-04-11"])
        self.assertEqual(result["normalized"], [{"run_date": "2026-04-12", "fact_rows": 10}])
        self.assertEqual(result["failed"], [{"run_date": "2026-04-11", "error": "boom"}])
        finalize_normalized_runs_mock.assert_called_once_with(
            [{"run_date": "2026-04-12", "fact_rows": 10}],
            logger=None,
        )
        sync_operator_views_mock.assert_not_called()
        sync_status_only_mock.assert_called_once()

    @patch("scripts.run_pipeline.sync_status_only")
    @patch("scripts.run_pipeline.sync_operator_views")
    @patch("scripts.run_pipeline.finalize_normalized_runs")
    @patch("scripts.run_pipeline.normalize_one_run_date")
    @patch("scripts.run_pipeline.fetch_pipeline_status_records")
    def test_run_pipeline_runs_full_sync_when_all_runs_succeed(
        self,
        fetch_status_mock,
        normalize_one_run_date_mock,
        finalize_normalized_runs_mock,
        sync_operator_views_mock,
        sync_status_only_mock,
    ):
        fetch_status_mock.side_effect = [
            [
                {"run_date": date(2026, 4, 12), "normalize_status": "pending_normalize"},
                {"run_date": date(2026, 4, 11), "normalize_status": "pending_normalize"},
            ],
            [
                {"run_date": date(2026, 4, 12), "normalize_status": "ready"},
                {"run_date": date(2026, 4, 11), "normalize_status": "ready"},
            ],
        ]
        normalize_one_run_date_mock.side_effect = [
            {"run_date": "2026-04-12", "fact_rows": 10},
            {"run_date": "2026-04-11", "fact_rows": 20},
        ]
        finalize_normalized_runs_mock.return_value = None
        sync_operator_views_mock.return_value = {"union": {"rows_written": 2}}

        result = run_pipeline(
            spreadsheet_id="sheet-id",
            service_account_path=Path("key.json"),
        )

        self.assertEqual(
            result["normalized"],
            [
                {"run_date": "2026-04-12", "fact_rows": 10},
                {"run_date": "2026-04-11", "fact_rows": 20},
            ],
        )
        self.assertEqual(result["failed"], [])
        finalize_normalized_runs_mock.assert_called_once_with(
            [
                {"run_date": "2026-04-12", "fact_rows": 10},
                {"run_date": "2026-04-11", "fact_rows": 20},
            ],
            logger=None,
        )
        sync_operator_views_mock.assert_called_once()
        sync_status_only_mock.assert_not_called()


if __name__ == "__main__":
    unittest.main()
