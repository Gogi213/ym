import io
import json
import sqlite3
import unittest

from fastapi.testclient import TestClient

from scripts.bootstrap_turso import load_bootstrap_sql


def build_connection():
    connection = sqlite3.connect(":memory:", check_same_thread=False)
    connection.row_factory = sqlite3.Row
    connection.executescript(load_bootstrap_sql())
    return connection


class IngestServiceHandlersTests(unittest.TestCase):
    def test_reset_and_ingest_write_raw_rows_and_pipeline_state(self):
        from ingest_service.app import create_app
        from ingest_service.handlers import create_runtime_handlers

        connection = build_connection()
        handlers = create_runtime_handlers(connection)
        client = TestClient(
            create_app(
                ingest_token="secret",
                reset_handler=handlers.reset_handler,
                ingest_handler=handlers.ingest_handler,
            )
        )

        reset_response = client.post(
            "/reset",
            headers={"x-ingest-token": "secret"},
            json={"action": "reset", "run_date": "2026-04-14"},
        )
        self.assertEqual(reset_response.status_code, 200)
        self.assertEqual(reset_response.json(), {"ok": True, "action": "reset", "run_date": "2026-04-14"})

        meta = {
            "action": "ingest",
            "run_date": "2026-04-14",
            "primary_topic": "topic-primary",
            "matched_topic": "topic-primary",
            "topic_role": "primary",
            "message_subject": "Subject",
            "message_date": "2026-04-14T09:00:00Z",
            "message_id": "message-1",
            "thread_id": "thread-1",
            "attachment_name": "report.csv",
            "attachment_type": "csv",
        }
        csv_payload = (
            "noise\n"
            "UTM Source;UTM Campaign;Визиты\n"
            "google;brand;10\n"
            "yandex;perf;20\n"
        ).encode("utf-8")
        ingest_response = client.post(
            "/ingest",
            headers={"x-ingest-token": "secret"},
            data={"meta": json.dumps(meta)},
            files={"file": ("report.csv", io.BytesIO(csv_payload), "text/csv")},
        )

        self.assertEqual(ingest_response.status_code, 200)
        body = ingest_response.json()
        self.assertEqual(body["ok"], True)
        self.assertEqual(body["status"], "ingested")
        self.assertEqual(body["rows"], 2)

        files_count = connection.execute("select count(*) from ingest_files").fetchone()[0]
        rows_count = connection.execute("select count(*) from ingest_rows").fetchone()[0]
        payloads_count = connection.execute("select count(*) from ingest_file_payloads").fetchone()[0]
        pipeline = connection.execute(
            "select raw_files, raw_rows, normalize_status from pipeline_runs where run_date = ?",
            ("2026-04-14",),
        ).fetchone()

        self.assertEqual(files_count, 1)
        self.assertEqual(rows_count, 2)
        self.assertEqual(payloads_count, 1)
        self.assertEqual(pipeline["raw_files"], 1)
        self.assertEqual(pipeline["raw_rows"], 2)
        self.assertEqual(pipeline["normalize_status"], "pending_normalize")

    def test_reset_clears_existing_raw_data_for_run_date(self):
        from ingest_service.app import create_app
        from ingest_service.handlers import create_runtime_handlers

        connection = build_connection()
        handlers = create_runtime_handlers(connection)
        client = TestClient(
            create_app(
                ingest_token="secret",
                reset_handler=handlers.reset_handler,
                ingest_handler=handlers.ingest_handler,
            )
        )

        meta = {
            "action": "ingest",
            "run_date": "2026-04-14",
            "primary_topic": "topic-primary",
            "matched_topic": "topic-primary",
            "topic_role": "primary",
            "message_subject": "Subject",
            "message_date": "2026-04-14T09:00:00Z",
            "message_id": "message-1",
            "thread_id": "thread-1",
            "attachment_name": "report.csv",
            "attachment_type": "csv",
        }
        csv_payload = "UTM Source;UTM Campaign;Визиты\ngoogle;brand;10\n".encode("utf-8")

        client.post(
            "/reset",
            headers={"x-ingest-token": "secret"},
            json={"action": "reset", "run_date": "2026-04-14"},
        )
        client.post(
            "/ingest",
            headers={"x-ingest-token": "secret"},
            data={"meta": json.dumps(meta)},
            files={"file": ("report.csv", io.BytesIO(csv_payload), "text/csv")},
        )

        before_reset_files = connection.execute(
            "select count(*) from ingest_files where run_date = ?",
            ("2026-04-14",),
        ).fetchone()[0]
        self.assertEqual(before_reset_files, 1)

        reset_response = client.post(
            "/reset",
            headers={"x-ingest-token": "secret"},
            json={"action": "reset", "run_date": "2026-04-14"},
        )

        files_after_reset = connection.execute(
            "select count(*) from ingest_files where run_date = ?",
            ("2026-04-14",),
        ).fetchone()[0]
        rows_after_reset = connection.execute(
            "select count(*) from ingest_rows where run_date = ?",
            ("2026-04-14",),
        ).fetchone()[0]
        pipeline = connection.execute(
            "select raw_revision, raw_files, raw_rows, normalize_status from pipeline_runs where run_date = ?",
            ("2026-04-14",),
        ).fetchone()

        self.assertEqual(reset_response.status_code, 200)
        self.assertEqual(files_after_reset, 0)
        self.assertEqual(rows_after_reset, 0)
        self.assertEqual(pipeline["raw_revision"], 2)
        self.assertEqual(pipeline["raw_files"], 0)
        self.assertEqual(pipeline["raw_rows"], 0)
        self.assertEqual(pipeline["normalize_status"], "pending_normalize")

    def test_pipeline_run_handler_returns_run_state(self):
        from ingest_service.handlers import create_runtime_handlers

        connection = build_connection()
        handlers = create_runtime_handlers(connection)

        connection.execute(
            """
            insert into pipeline_runs (
              run_date, raw_revision, normalize_status, raw_files, raw_rows,
              normalized_files, normalized_rows, last_ingest_at, normalized_at,
              last_error, updated_at
            ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("2026-04-14", 3, "pending_normalize", 4, 120, 0, 0, "2026-04-14T09:00:00Z", None, None, "2026-04-14T09:00:00Z"),
        )
        connection.commit()

        self.assertEqual(
            handlers.pipeline_run_handler("2026-04-14"),
            {
                "ok": True,
                "run_date": "2026-04-14",
                "exists": True,
                "normalize_status": "pending_normalize",
                "raw_files": 4,
                "raw_rows": 120,
                "normalized_files": 0,
                "normalized_rows": 0,
                "raw_revision": 3,
                "last_error": None,
            },
        )


if __name__ == "__main__":
    unittest.main()
