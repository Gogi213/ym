import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch
import os
import sqlite3
import tempfile

from scripts.run_pipeline import (
    PipelineLockError,
    acquire_pipeline_lock,
    has_failed_runs,
    run_pipeline,
    select_pending_run_dates,
    sync_operator_views,
    sync_status_only,
    should_sync_full_operator_views,
)
from scripts.bootstrap_turso import load_bootstrap_sql


def build_bootstrap_connection():
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    connection.executescript(load_bootstrap_sql())
    return connection


class RunPipelineTests(unittest.TestCase):
    def test_purge_skipped_raw_files_removes_unneeded_payloads(self):
        from scripts.normalize.pipeline import _purge_skipped_raw_files

        connection = build_bootstrap_connection()
        connection.executescript(
            """
            insert into pipeline_runs (
              run_date, raw_revision, normalize_status, raw_files, raw_rows,
              normalized_files, normalized_rows, last_ingest_at, normalized_at,
              last_error, updated_at
            ) values (
              '2026-04-14', 1, 'pending_normalize', 2, 0, 0, 0,
              '2026-04-14T09:00:00Z', null, null, '2026-04-14T09:00:00Z'
            );
            insert into ingest_files (
              id, raw_file_key, file_hash, run_date, message_id, thread_id, message_date, message_subject,
              primary_topic, matched_topic, topic_role, attachment_name, attachment_type,
              status, header_json, row_count, error_text
            ) values
              ('file-keep', 'rk-file-keep', 'hash-file-keep', '2026-04-14', 'm1', 't1', '2026-04-14T10:00:00Z', 's1', 'Topic A', 'Topic A', 'primary', 'a.csv', 'csv', 'ingested', '[]', 1, null),
              ('file-drop', 'rk-file-drop', 'hash-file-drop', '2026-04-14', 'm2', 't2', '2026-04-14T10:01:00Z', 's2', 'Topic A', 'Topic A', 'primary', 'b.csv', 'csv', 'skipped', '[]', 0, 'no table');
            insert into ingest_file_payloads (file_id, content_type, file_size_bytes, file_base64) values
              ('file-keep', 'text/csv', 3, 'YWJj'),
              ('file-drop', 'text/csv', 3, 'ZGVm');
            """
        )

        purged = _purge_skipped_raw_files(connection, "2026-04-14")

        self.assertEqual(purged, 1)
        remaining_files = connection.execute(
            "select id, status from ingest_files order by id"
        ).fetchall()
        remaining_payloads = connection.execute(
            "select file_id from ingest_file_payloads order by file_id"
        ).fetchall()
        self.assertEqual([tuple(row) for row in remaining_files], [("file-keep", "ingested")])
        self.assertEqual([tuple(row) for row in remaining_payloads], [("file-keep",)])

    def test_update_ingest_file_metadata_updates_rows_in_one_statement(self):
        from scripts.normalize.pipeline import _update_ingest_file_metadata

        connection = build_bootstrap_connection()
        connection.executescript(
            """
            insert into ingest_files (
              id, raw_file_key, file_hash, run_date, message_id, thread_id, message_date, message_subject,
              primary_topic, matched_topic, topic_role, attachment_name, attachment_type,
              status, header_json, row_count, error_text
            ) values
              ('file-1', 'rk-file-1', 'hash-file-1', '2026-04-14', 'm1', 't1', '2026-04-14T10:00:00Z', 's1', 'Topic A', 'Topic A', 'primary', 'a.csv', 'csv', 'raw_only', '[]', 0, null),
              ('file-2', 'rk-file-2', 'hash-file-2', '2026-04-14', 'm2', 't2', '2026-04-14T10:01:00Z', 's2', 'Topic B', 'Topic B', 'primary', 'b.csv', 'csv', 'raw_only', '[]', 0, null);
            """
        )

        _update_ingest_file_metadata(
            connection,
            [
                {
                    "file_id": "file-1",
                    "status": "ingested",
                    "header_json": ["UTM Source", "Визиты"],
                    "row_count": 10,
                    "error_text": None,
                },
                {
                    "file_id": "file-2",
                    "status": "skipped",
                    "header_json": [],
                    "row_count": 0,
                    "error_text": "no table",
                },
            ],
        )

        rows = connection.execute(
            "select id, status, header_json, row_count, error_text from ingest_files order by id"
        ).fetchall()
        self.assertEqual(
            [dict(row) for row in rows],
            [
                {
                    "id": "file-1",
                    "status": "ingested",
                    "header_json": '["UTM Source", "Визиты"]',
                    "row_count": 10,
                    "error_text": None,
                },
                {
                    "id": "file-2",
                    "status": "skipped",
                    "header_json": "[]",
                    "row_count": 0,
                    "error_text": "no table",
                },
            ],
        )

    def test_acquire_pipeline_lock_creates_and_releases_lock_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            lock_path = Path(tmpdir) / "run_pipeline.lock"
            with acquire_pipeline_lock(lock_path=lock_path):
                self.assertTrue(lock_path.exists())
            self.assertFalse(lock_path.exists())

    def test_acquire_pipeline_lock_rejects_running_process(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            lock_path = Path(tmpdir) / "run_pipeline.lock"
            lock_path.write_text('{"pid": 12345}', encoding="utf-8")
            with patch("scripts.run_pipeline._is_process_running", return_value=True):
                with self.assertRaises(PipelineLockError):
                    with acquire_pipeline_lock(lock_path=lock_path):
                        self.fail("lock acquisition should not succeed when another run is active")

    def test_acquire_pipeline_lock_reclaims_stale_lock_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            lock_path = Path(tmpdir) / "run_pipeline.lock"
            lock_path.write_text('{"pid": 12345}', encoding="utf-8")
            with patch("scripts.run_pipeline._is_process_running", return_value=False):
                with acquire_pipeline_lock(lock_path=lock_path):
                    self.assertTrue(lock_path.exists())
            self.assertFalse(lock_path.exists())

    def test_select_pending_run_dates_returns_raw_and_pending_days(self):
        self.assertEqual(
            select_pending_run_dates(
                [
                    {"run_date": date(2026, 4, 11), "normalize_status": "ready"},
                    {"run_date": date(2026, 4, 10), "normalize_status": "pending_normalize"},
                    {"run_date": date(2026, 4, 9), "pipeline_status": "raw_only"},
                    {"run_date": date(2026, 4, 8), "normalize_status": "normalize_error"},
                ]
            ),
            ["2026-04-10", "2026-04-09", "2026-04-08"],
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

    def test_normalize_run_parses_raw_only_files_without_writing_ingest_rows(self):
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
        self.assertEqual(raw_rows, 0)

    def test_normalize_run_purges_skipped_raw_files_after_prepare(self):
        from scripts.normalize.pipeline import normalize_run

        connection = build_bootstrap_connection()
        connection.executescript(
            """
            insert into pipeline_runs (
              run_date, raw_revision, normalize_status, raw_files, raw_rows,
              normalized_files, normalized_rows, last_ingest_at, normalized_at,
              last_error, updated_at
            ) values (
              '2026-04-14', 1, 'pending_normalize', 2, 0, 0, 0,
              '2026-04-14T09:00:00Z', null, null, '2026-04-14T09:00:00Z'
            );
            insert into ingest_files (
              id, raw_file_key, file_hash, run_date, message_id, thread_id, message_date, message_subject,
              primary_topic, matched_topic, topic_role, attachment_name, attachment_type,
              status, header_json, row_count, error_text
            ) values
              ('file-1', 'rk-file-1', 'hash-file-1', '2026-04-14', 'm1', 't1', '2026-04-14T10:00:00Z',
               'Отчет за 2026-04-14', 'Topic A', 'Topic A', 'primary',
               'a.csv', 'csv', 'raw_only', '[]', 0, null),
              ('file-2', 'rk-file-2', 'hash-file-2', '2026-04-14', 'm2', 't2', '2026-04-14T10:01:00Z',
               'Пустой файл', 'Topic A', 'Topic A', 'primary',
               'b.csv', 'csv', 'raw_only', '[]', 0, null);
            insert into ingest_file_payloads (file_id, content_type, file_size_bytes, file_base64) values
              ('file-1', 'text/csv', 53, 'VVRNIFNvdXJjZTtVVE0gQ2FtcGFpZ2470JLQuNC30LjRgtGLCmdvb2dsZTticmFuZDsxMAo='),
              ('file-2', 'text/csv', 13, 'bm90X2FfdGFibGUK');
            """
        )
        connection.commit()

        with patch("scripts.normalize.pipeline.connect_db", return_value=connection):
            result = normalize_run("2026-04-14", defer_finalize=True)

        remaining_files = connection.execute(
            "select id, status from ingest_files order by id"
        ).fetchall()
        remaining_payloads = connection.execute(
            "select file_id from ingest_file_payloads order by file_id"
        ).fetchall()

        self.assertEqual(result["files"], 1)
        self.assertEqual(result["fact_rows"], 1)
        self.assertEqual([tuple(row) for row in remaining_files], [("file-1", "ingested")])
        self.assertEqual([tuple(row) for row in remaining_payloads], [("file-1",)])

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

        def normalize_side_effect(run_date):
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

    @patch("scripts.run_pipeline.sync_pipeline_status_sheet")
    @patch("scripts.run_pipeline.sync_export_rows_wide_sheet")
    @patch("scripts.run_pipeline.sync_goal_mapping_sheet")
    def test_sync_operator_views_emits_per_sheet_progress(
        self,
        sync_goal_mapping_sheet_mock,
        sync_export_rows_wide_sheet_mock,
        sync_pipeline_status_sheet_mock,
    ):
        events = []
        sync_goal_mapping_sheet_mock.return_value = {"rows_written": 10}
        sync_export_rows_wide_sheet_mock.return_value = {"rows_written": 20}
        sync_pipeline_status_sheet_mock.return_value = {"rows_written": 30}

        result = sync_operator_views(
            spreadsheet_id="sheet-id",
            service_account_path=Path("key.json"),
            logger=lambda phase, payload: events.append((phase, payload)),
        )

        self.assertEqual(
            [phase for phase, _payload in events],
            [
                "sheet_sync_progress",
                "sheet_sync_progress",
                "sheet_sync_progress",
                "sheet_sync_progress",
                "sheet_sync_progress",
                "sheet_sync_progress",
            ],
        )
        self.assertEqual(
            events,
            [
                ("sheet_sync_progress", {"sheet": "отчеты", "status": "started", "sheet_index": 1, "sheet_count": 3}),
                ("sheet_sync_progress", {"sheet": "отчеты", "status": "finished", "sheet_index": 1, "sheet_count": 3, "rows_written": 10}),
                ("sheet_sync_progress", {"sheet": "union", "status": "started", "sheet_index": 2, "sheet_count": 3}),
                ("sheet_sync_progress", {"sheet": "union", "status": "finished", "sheet_index": 2, "sheet_count": 3, "rows_written": 20}),
                ("sheet_sync_progress", {"sheet": "pipeline_status", "status": "started", "sheet_index": 3, "sheet_count": 3}),
                ("sheet_sync_progress", {"sheet": "pipeline_status", "status": "finished", "sheet_index": 3, "sheet_count": 3, "rows_written": 30}),
            ],
        )
        self.assertEqual(result["goal_mapping"], {"rows_written": 10})
        self.assertEqual(result["union"], {"rows_written": 20})
        self.assertEqual(result["pipeline_status"], {"rows_written": 30})

    @patch("scripts.run_pipeline.sync_pipeline_status_sheet")
    def test_sync_status_only_emits_sheet_progress(
        self,
        sync_pipeline_status_sheet_mock,
    ):
        events = []
        sync_pipeline_status_sheet_mock.return_value = {"rows_written": 17}

        result = sync_status_only(
            spreadsheet_id="sheet-id",
            service_account_path=Path("key.json"),
            logger=lambda phase, payload: events.append((phase, payload)),
        )

        self.assertEqual(
            events,
            [
                ("sheet_sync_progress", {"sheet": "pipeline_status", "status": "started", "sheet_index": 1, "sheet_count": 1}),
                ("sheet_sync_progress", {"sheet": "pipeline_status", "status": "finished", "sheet_index": 1, "sheet_count": 1, "rows_written": 17}),
            ],
        )
        self.assertEqual(result["pipeline_status"], {"rows_written": 17})


if __name__ == "__main__":
    unittest.main()
