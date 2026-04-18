import os
import sqlite3
import unittest
from unittest import mock

from scripts.bootstrap_turso import load_bootstrap_sql


def build_bootstrap_connection():
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    connection.executescript(load_bootstrap_sql())
    return connection


class NormalizeTursoLayoutTests(unittest.TestCase):
    def test_turso_connection_delegates_to_shared_runtime(self):
        from scripts.normalize import turso_connection

        fake_connection = object()
        with mock.patch("scripts.normalize.turso_connection.connect_turso", return_value=fake_connection) as connect_mock:
            with mock.patch.dict(os.environ, {"TURSO_DATABASE_URL": "libsql://example", "TURSO_AUTH_TOKEN": "secret"}, clear=True):
                resolved = turso_connection.connect_db()

        self.assertIs(resolved, fake_connection)
        connect_mock.assert_called_once()

    def test_fetch_ingested_files_filters_status_and_preserves_topic_order(self):
        from scripts.normalize.turso_reads import fetch_ingested_files

        connection = build_bootstrap_connection()
        connection.executescript(
            """
            insert into ingest_files (
              id, raw_file_key, file_hash, run_date, message_id, thread_id, message_date, message_subject,
              primary_topic, matched_topic, topic_role, attachment_name, attachment_type,
              status, header_json, row_count, error_text
            ) values
              ('f2', 'rk-f2', 'hash-f2', '2026-04-14', 'm2', 't2', '2026-04-14T10:00:00Z', 's2', 'B Topic', 'B Topic', 'secondary', 'b.csv', 'csv', 'ingested', '["UTM Source","Goal"]', 1, null),
              ('f1', 'rk-f1', 'hash-f1', '2026-04-14', 'm1', 't1', '2026-04-14T09:00:00Z', 's1', 'A Topic', 'A Topic', 'primary', 'a.csv', 'csv', 'ingested', '["UTM Source","Visits"]', 2, null),
              ('f3', 'rk-f3', 'hash-f3', '2026-04-14', 'm3', 't3', '2026-04-14T11:00:00Z', 's3', 'C Topic', 'C Topic', 'primary', 'c.csv', 'csv', 'skipped', '[]', 0, null);
            """
        )
        connection.commit()

        records = fetch_ingested_files(connection, "2026-04-14")

        self.assertEqual([record["id"] for record in records], ["f1", "f2"])
        self.assertEqual([record["matched_topic"] for record in records], ["A Topic", "B Topic"])
        self.assertEqual(records[0]["header_json"], ["UTM Source", "Visits"])

    def test_fetch_ingest_rows_groups_rows_by_file_id(self):
        from scripts.normalize.turso_reads import fetch_ingest_rows

        connection = build_bootstrap_connection()
        connection.executescript(
            """
            insert into ingest_rows (file_id, run_date, row_index, row_json) values
              ('f1', '2026-04-14', 1, '{"a":"1"}'),
              ('f1', '2026-04-14', 2, '{"a":"2"}'),
              ('f2', '2026-04-14', 1, '{"b":"3"}');
            """
        )
        connection.commit()

        grouped = fetch_ingest_rows(connection, ["f1", "f2"])

        self.assertEqual([row["row_index"] for row in grouped["f1"]], [1, 2])
        self.assertEqual(grouped["f2"][0]["row_json"], {"b": "3"})

    def test_fetch_ingest_payloads_returns_map_by_file_id(self):
        from scripts.normalize.turso_reads import fetch_ingest_payloads

        connection = build_bootstrap_connection()
        connection.executescript(
            """
            insert into ingest_file_payloads (file_id, content_type, file_size_bytes, file_base64) values
              ('f1', 'text/csv', 3, 'YWJj'),
              ('f2', 'application/vnd.ms-excel', 4, 'ZGVmZw==');
            """
        )
        connection.commit()

        payloads = fetch_ingest_payloads(connection, ["f1", "f2"])

        self.assertEqual(payloads["f1"]["content_type"], "text/csv")
        self.assertEqual(payloads["f2"]["file_base64"], "ZGVmZw==")

    def test_fetch_existing_goal_slots_returns_nested_mapping(self):
        from scripts.normalize.turso_reads import fetch_existing_goal_slots

        connection = build_bootstrap_connection()
        connection.executescript(
            """
            insert into topic_goal_slots (topic, goal_slot, source_header, goal_label) values
              ('Topic A', 1, 'Goal A1', 'Goal A1'),
              ('Topic A', 2, 'Goal A2', 'Goal A2'),
              ('Topic B', 1, 'Goal B1', 'Goal B1');
            """
        )
        connection.commit()

        mapping = fetch_existing_goal_slots(connection, ["Topic A", "Topic B"])

        self.assertEqual(mapping["Topic A"]["Goal A1"], 1)
        self.assertEqual(mapping["Topic A"]["Goal A2"], 2)
        self.assertEqual(mapping["Topic B"]["Goal B1"], 1)

    def test_prepare_raw_ingest_files_parses_raw_only_payloads(self):
        from scripts.normalize.pipeline import prepare_raw_ingest_files

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
              'file-1', 'rk-file-1', 'hash-file-1', '2026-04-14', 'm1', 't1', '2026-04-14T10:00:00Z', 's1',
              'Topic A', 'Topic A', 'primary', 'a.csv', 'csv', 'raw_only', '[]', 0, null
            );
            insert into ingest_file_payloads (file_id, content_type, file_size_bytes, file_base64)
            values ('file-1', 'text/csv', 53, 'VVRNIFNvdXJjZTtVVE0gQ2FtcGFpZ2470JLQuNC30LjRgtGLCmdvb2dsZTticmFuZDsxMAo=');
            """
        )
        connection.commit()

        prepared = prepare_raw_ingest_files(connection, "2026-04-14")

        row = connection.execute(
            "select status, header_json, row_count, error_text from ingest_files where id = 'file-1'"
        ).fetchone()
        raw_rows = connection.execute(
            "select row_index, row_json from ingest_rows where file_id = 'file-1' order by row_index"
        ).fetchall()
        pipeline = connection.execute(
            "select raw_files, raw_rows, normalize_status from pipeline_runs where run_date = '2026-04-14'"
        ).fetchone()

        self.assertEqual(prepared, {"prepared_files": 1, "ingested_files": 1, "skipped_files": 0, "error_files": 0})
        self.assertEqual(dict(row), {
            "status": "ingested",
            "header_json": '["UTM Source", "UTM Campaign", "Визиты"]',
            "row_count": 1,
            "error_text": None,
        })
        self.assertEqual([(item["row_index"], item["row_json"]) for item in raw_rows], [(1, '{"UTM Source": "google", "UTM Campaign": "brand", "Визиты": "10"}')])
        self.assertEqual(dict(pipeline), {"raw_files": 1, "raw_rows": 1, "normalize_status": "pending_normalize"})

    def test_prepare_raw_ingest_files_parses_legacy_placeholder_ingested_file(self):
        from scripts.normalize.pipeline import prepare_raw_ingest_files

        connection = build_bootstrap_connection()
        connection.executescript(
            """
            insert into pipeline_runs (
              run_date, raw_revision, normalize_status, raw_files, raw_rows,
              normalized_files, normalized_rows, last_ingest_at, normalized_at,
              last_error, updated_at
            ) values (
              '2026-04-15', 1, 'pending_normalize', 1, 0, 0, 0,
              '2026-04-15T09:00:00Z', null, null, '2026-04-15T09:00:00Z'
            );
            insert into ingest_files (
              id, raw_file_key, file_hash, run_date, message_id, thread_id, message_date, message_subject,
              primary_topic, matched_topic, topic_role, attachment_name, attachment_type,
              status, header_json, row_count, error_text
            ) values (
              'file-legacy', 'rk-file-legacy', 'hash-file-legacy', '2026-04-15', 'm1', 't1', '2026-04-15T10:00:00Z', 's1',
              'Topic A', 'Topic A', 'primary', 'legacy.csv', 'csv', 'ingested', '[]', 0, null
            );
            insert into ingest_file_payloads (file_id, content_type, file_size_bytes, file_base64)
            values ('file-legacy', 'text/csv', 53, 'VVRNIFNvdXJjZTtVVE0gQ2FtcGFpZ2470JLQuNC30LjRgtGLCmdvb2dsZTticmFuZDsxMAo=');
            """
        )
        connection.commit()

        prepared = prepare_raw_ingest_files(connection, "2026-04-15")

        row = connection.execute(
            "select status, header_json, row_count, error_text from ingest_files where id = 'file-legacy'"
        ).fetchone()
        raw_rows = connection.execute(
            "select row_index, row_json from ingest_rows where file_id = 'file-legacy' order by row_index"
        ).fetchall()

        self.assertEqual(prepared, {"prepared_files": 1, "ingested_files": 1, "skipped_files": 0, "error_files": 0})
        self.assertEqual(dict(row), {
            "status": "ingested",
            "header_json": '["UTM Source", "UTM Campaign", "Визиты"]',
            "row_count": 1,
            "error_text": None,
        })
        self.assertEqual([(item["row_index"], item["row_json"]) for item in raw_rows], [(1, '{"UTM Source": "google", "UTM Campaign": "brand", "Визиты": "10"}')])


if __name__ == "__main__":
    unittest.main()
