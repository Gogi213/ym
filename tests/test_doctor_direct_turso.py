import sqlite3
import unittest

from scripts.bootstrap_turso import load_bootstrap_sql


def build_bootstrap_connection():
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    connection.executescript(load_bootstrap_sql())
    return connection


class DirectTursoDoctorTests(unittest.TestCase):
    def test_run_connection_smoke_returns_scalar_one(self):
        from scripts.doctor_direct_turso import run_connection_smoke

        connection = build_bootstrap_connection()

        self.assertEqual(
            run_connection_smoke(connection),
            {"ok": True, "scalar": 1},
        )

    def test_fetch_recent_pipeline_runs_orders_desc_and_limits(self):
        from scripts.doctor_direct_turso import fetch_recent_pipeline_runs

        connection = build_bootstrap_connection()
        connection.executescript(
            """
            insert into pipeline_runs (
              run_date, raw_revision, normalize_status, raw_files, raw_rows,
              normalized_files, normalized_rows, last_ingest_at, normalized_at,
              last_error, updated_at
            ) values
              ('2026-04-14', 2, 'pending_normalize', 3, 30, 0, 0, '2026-04-14T10:00:00Z', null, null, '2026-04-14T10:00:00Z'),
              ('2026-04-13', 1, 'ready', 2, 20, 2, 20, '2026-04-13T10:00:00Z', '2026-04-13T11:00:00Z', null, '2026-04-13T11:00:00Z');
            """
        )
        connection.commit()

        self.assertEqual(
            fetch_recent_pipeline_runs(connection, limit=1),
            [
                {
                    "run_date": "2026-04-14",
                    "raw_revision": 2,
                    "normalize_status": "pending_normalize",
                    "raw_files": 3,
                    "raw_rows": 30,
                    "normalized_files": 0,
                    "normalized_rows": 0,
                    "last_error": None,
                }
            ],
        )

    def test_inspect_run_date_summarizes_raw_state(self):
        from scripts.doctor_direct_turso import inspect_run_date

        connection = build_bootstrap_connection()
        connection.executescript(
            """
            insert into pipeline_runs (
              run_date, raw_revision, normalize_status, raw_files, raw_rows,
              normalized_files, normalized_rows, last_ingest_at, normalized_at,
              last_error, updated_at
            ) values (
              '2026-04-14', 2, 'pending_normalize', 3, 10, 0, 0,
              '2026-04-14T10:00:00Z', null, null, '2026-04-14T10:00:00Z'
            );
            insert into ingest_files (
              id, raw_file_key, file_hash, run_date, message_id, thread_id, message_date, message_subject,
              primary_topic, matched_topic, topic_role, attachment_name, attachment_type,
              status, header_json, row_count, error_text
            ) values
              ('f-1', 'rk-f-1', 'hash-f-1', '2026-04-14', 'm1', 't1', '2026-04-14T10:00:00Z', 's1', 'Topic A', 'Topic A', 'primary', 'a.csv', 'csv', 'raw_only', '[]', 0, null),
              ('f-2', 'rk-f-2', 'hash-f-2', '2026-04-14', 'm2', 't2', '2026-04-14T11:00:00Z', 's2', 'Topic A', 'Topic A', 'primary', 'b.csv', 'csv', 'ingested', '[]', 10, null),
              ('f-3', 'rk-f-3', 'hash-f-3', '2026-04-14', 'm3', 't3', '2026-04-14T12:00:00Z', 's3', 'Topic A', 'Topic A', 'primary', 'c.csv', 'csv', 'error', '[]', 0, 'boom');
            insert into ingest_file_payloads (file_id, content_type, file_size_bytes, file_base64) values
              ('f-1', 'text/csv', 53, 'VVRNIFNvdXJjZTtVVE0gQ2FtcGFpZ2470JLQuNC30LjRgtGLCmdvb2dsZTticmFuZDsxMAo='),
              ('f-2', 'text/csv', 53, 'VVRNIFNvdXJjZTtVVE0gQ2FtcGFpZ2470JLQuNC30LjRgtGLCmdvb2dsZTticmFuZDsxMAo=');
            """
        )
        connection.commit()

        self.assertEqual(
            inspect_run_date(connection, "2026-04-14"),
            {
                "run_date": "2026-04-14",
                "pipeline_status": "pending_normalize",
                "raw_revision": 2,
                "files_total": 3,
                "raw_only_files": 1,
                "ingested_files": 1,
                "skipped_files": 0,
                "error_files": 1,
                "payload_files": 2,
                "raw_rows": 10,
                "normalized_files": 0,
                "normalized_rows": 0,
                "last_error": None,
            },
        )

    def test_validate_raw_payloads_reports_parseable_skipped_and_errors(self):
        from scripts.doctor_direct_turso import validate_raw_payloads

        connection = build_bootstrap_connection()
        connection.executescript(
            """
            insert into ingest_files (
              id, raw_file_key, file_hash, run_date, message_id, thread_id, message_date, message_subject,
              primary_topic, matched_topic, topic_role, attachment_name, attachment_type,
              status, header_json, row_count, error_text
            ) values
              ('f-1', 'rk-f-1', 'hash-f-1', '2026-04-14', 'm1', 't1', '2026-04-14T10:00:00Z', 's1', 'Topic A', 'Topic A', 'primary', 'parseable.csv', 'csv', 'raw_only', '[]', 0, null),
              ('f-2', 'rk-f-2', 'hash-f-2', '2026-04-14', 'm2', 't2', '2026-04-14T11:00:00Z', 's2', 'Topic A', 'Topic A', 'primary', 'skipped.csv', 'csv', 'raw_only', '[]', 0, null),
              ('f-3', 'rk-f-3', 'hash-f-3', '2026-04-14', 'm3', 't3', '2026-04-14T12:00:00Z', 's3', 'Topic A', 'Topic A', 'primary', 'broken.csv', 'csv', 'raw_only', '[]', 0, null);
            insert into ingest_file_payloads (file_id, content_type, file_size_bytes, file_base64) values
              ('f-1', 'text/csv', 53, 'VVRNIFNvdXJjZTtVVE0gQ2FtcGFpZ2470JLQuNC30LjRgtGLCmdvb2dsZTticmFuZDsxMAo='),
              ('f-2', 'text/csv', 12, 'bm90X2FfdXRtO2hlYWRlcgo='); 
            """
        )
        connection.commit()

        report = validate_raw_payloads(connection, "2026-04-14")

        self.assertEqual(
            {
                "validated_files": report["validated_files"],
                "parseable_files": report["parseable_files"],
                "skipped_files": report["skipped_files"],
                "error_files": report["error_files"],
            },
            {
                "validated_files": 3,
                "parseable_files": 1,
                "skipped_files": 1,
                "error_files": 1,
            },
        )
        self.assertEqual(
            report["details"],
            [
                {"file_id": "f-1", "attachment_name": "parseable.csv", "status": "parseable", "rows": 1},
                {"file_id": "f-2", "attachment_name": "skipped.csv", "status": "skipped", "rows": 0},
                {"file_id": "f-3", "attachment_name": "broken.csv", "status": "error", "error": "Missing payload"},
            ],
        )


if __name__ == "__main__":
    unittest.main()
