import unittest

from test_ingest_service_storage import build_sqlite_connection


class TursoR2ManifestSchemaTests(unittest.TestCase):
    def test_ingest_files_supports_r2_manifest_columns(self):
        connection = build_sqlite_connection()

        columns = {
            row["name"]: row
            for row in connection.execute("pragma table_info('ingest_files')").fetchall()
        }

        self.assertIn("r2_key", columns)
        self.assertIn("file_size_bytes", columns)
        self.assertIn("parse_error", columns)
        self.assertIn("raw_revision", columns)
        self.assertIn("updated_at", columns)

    def test_pipeline_status_includes_manifest_summary_counts(self):
        from ingest_service.storage import (
            fetch_pipeline_run_status,
            insert_file_record,
            mark_pipeline_run_after_reset,
            refresh_pipeline_run_after_ingest,
        )

        connection = build_sqlite_connection()
        mark_pipeline_run_after_reset(connection, "2026-04-14")

        base_meta = {
            "run_date": "2026-04-14",
            "message_id": "message-1",
            "thread_id": "thread-1",
            "message_date": "2026-04-14T09:00:00Z",
            "message_subject": "Subject",
            "primary_topic": "topic-primary",
            "matched_topic": "topic-primary",
            "topic_role": "primary",
            "attachment_name": "report.xlsx",
        }

        insert_file_record(
            connection,
            base_meta,
            attachment_type="xlsx",
            status="uploaded",
            header=["UTM Source"],
            row_count=0,
            error_text=None,
        )
        insert_file_record(
            connection,
            {**base_meta, "message_id": "message-2", "attachment_name": "parsed.xlsx"},
            attachment_type="xlsx",
            status="parsed",
            header=["UTM Source"],
            row_count=10,
            error_text=None,
        )
        insert_file_record(
            connection,
            {**base_meta, "message_id": "message-3", "attachment_name": "failed.xlsx"},
            attachment_type="xlsx",
            status="failed",
            header=["UTM Source"],
            row_count=0,
            error_text="boom",
        )

        refresh_pipeline_run_after_ingest(connection, "2026-04-14")

        status = fetch_pipeline_run_status(connection, "2026-04-14")

        self.assertEqual(status["raw_files"], 3)
        self.assertEqual(status["uploaded_files"], 1)
        self.assertEqual(status["parsed_files"], 1)
        self.assertEqual(status["failed_files"], 1)


if __name__ == "__main__":
    unittest.main()
