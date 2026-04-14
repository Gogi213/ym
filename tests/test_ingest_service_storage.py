import sqlite3
import unittest

from scripts.bootstrap_turso import load_bootstrap_sql


def build_sqlite_connection():
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    connection.executescript(load_bootstrap_sql())
    return connection


class IngestServiceStorageTests(unittest.TestCase):
    def test_mark_pipeline_run_after_reset_initializes_run_state(self):
        from ingest_service.storage import mark_pipeline_run_after_reset

        connection = build_sqlite_connection()
        mark_pipeline_run_after_reset(connection, "2026-04-14")

        row = connection.execute(
            "select run_date, raw_revision, normalize_status, raw_files, raw_rows, normalized_files, normalized_rows "
            "from pipeline_runs where run_date = ?",
            ("2026-04-14",),
        ).fetchone()

        self.assertIsNotNone(row)
        self.assertEqual(row["run_date"], "2026-04-14")
        self.assertEqual(row["raw_revision"], 1)
        self.assertEqual(row["normalize_status"], "pending_normalize")
        self.assertEqual(row["raw_files"], 0)
        self.assertEqual(row["raw_rows"], 0)
        self.assertEqual(row["normalized_files"], 0)
        self.assertEqual(row["normalized_rows"], 0)

    def test_insert_file_record_persists_ingest_file(self):
        from ingest_service.storage import insert_file_record

        connection = build_sqlite_connection()
        file_id = insert_file_record(
            connection,
            {
                "run_date": "2026-04-14",
                "message_id": "message-1",
                "thread_id": "thread-1",
                "message_date": "2026-04-14T09:00:00Z",
                "message_subject": "Subject",
                "primary_topic": "topic-primary",
                "matched_topic": "topic-secondary",
                "topic_role": "secondary",
                "attachment_name": "report.xlsx",
            },
            attachment_type="xlsx",
            status="ingested",
            header=["UTM Source", "Визиты"],
            row_count=3,
            error_text=None,
        )

        row = connection.execute("select * from ingest_files where id = ?", (file_id,)).fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row["run_date"], "2026-04-14")
        self.assertEqual(row["primary_topic"], "topic-primary")
        self.assertEqual(row["matched_topic"], "topic-secondary")
        self.assertEqual(row["topic_role"], "secondary")
        self.assertEqual(row["attachment_type"], "xlsx")
        self.assertEqual(row["status"], "ingested")
        self.assertEqual(row["row_count"], 3)
        self.assertEqual(row["header_json"], '["UTM Source", "Визиты"]')

    def test_insert_file_payload_record_persists_base64_payload(self):
        from ingest_service.storage import insert_file_payload_record

        connection = build_sqlite_connection()
        connection.execute(
            """
            insert into ingest_files (
              id, run_date, message_id, thread_id, message_date, message_subject,
              primary_topic, matched_topic, topic_role, attachment_name, attachment_type,
              status, header_json, row_count, error_text
            ) values (
              'file-1', '2026-04-14', 'message-1', 'thread-1', '2026-04-14T09:00:00Z', 'Subject',
              'topic-primary', 'topic-primary', 'primary', 'report.xlsx', 'xlsx',
              'ingested', '[]', 0, null
            )
            """
        )
        connection.commit()

        insert_file_payload_record(
            connection,
            file_id="file-1",
            file_content_type="application/vnd.ms-excel",
            bytes_payload=b"abc",
        )

        row = connection.execute("select * from ingest_file_payloads where file_id = 'file-1'").fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row["content_type"], "application/vnd.ms-excel")
        self.assertEqual(row["file_size_bytes"], 3)
        self.assertEqual(row["file_base64"], "YWJj")

    def test_insert_row_records_and_refresh_pipeline_run_after_ingest(self):
        from ingest_service.storage import (
            insert_file_record,
            insert_row_records,
            mark_pipeline_run_after_reset,
            refresh_pipeline_run_after_ingest,
        )

        connection = build_sqlite_connection()
        mark_pipeline_run_after_reset(connection, "2026-04-14")
        file_id = insert_file_record(
            connection,
            {
                "run_date": "2026-04-14",
                "message_id": "message-1",
                "thread_id": "thread-1",
                "message_date": "2026-04-14T09:00:00Z",
                "message_subject": "Subject",
                "primary_topic": "topic-primary",
                "matched_topic": "topic-primary",
                "topic_role": "primary",
                "attachment_name": "report.xlsx",
            },
            attachment_type="xlsx",
            status="ingested",
            header=["UTM Source", "Визиты"],
            row_count=2,
            error_text=None,
        )
        insert_row_records(
            connection,
            file_id=file_id,
            run_date="2026-04-14",
            rows=[
                {"utm_source": "google", "visits": "1"},
                {"utm_source": "yandex", "visits": "2"},
            ],
        )

        refresh_pipeline_run_after_ingest(connection, "2026-04-14")

        rows = connection.execute(
            "select row_index, row_json from ingest_rows where file_id = ? order by row_index",
            (file_id,),
        ).fetchall()
        pipeline = connection.execute(
            "select raw_files, raw_rows, normalize_status from pipeline_runs where run_date = ?",
            ("2026-04-14",),
        ).fetchone()

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["row_index"], 1)
        self.assertEqual(rows[1]["row_index"], 2)
        self.assertEqual(pipeline["raw_files"], 1)
        self.assertEqual(pipeline["raw_rows"], 2)
        self.assertEqual(pipeline["normalize_status"], "pending_normalize")


if __name__ == "__main__":
    unittest.main()
