import sqlite3
import unittest

from scripts.bootstrap_turso import load_bootstrap_sql


def build_bootstrap_connection():
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    connection.executescript(load_bootstrap_sql())
    return connection


class NormalizeTursoWritePathTests(unittest.TestCase):
    def test_replace_operator_export_rows_for_run_rebuilds_only_target_day(self):
        from scripts.normalize.turso_operator_export import replace_operator_export_rows_for_run

        connection = build_bootstrap_connection()
        connection.executescript(
            """
            insert into operator_export_rows (
              run_date, topic, report_date, report_date_from, report_date_to,
              utm_source, utm_medium, utm_campaign, utm_content, utm_term,
              visits, users, bounce_rate, page_depth, time_on_site_seconds, robot_rate,
              goal_1
            ) values
              ('2026-04-16', 'Topic A', '2026-04-16', '2026-04-16', '2026-04-16', 'google', 'cpc', 'brand', 'aggregated', 'aggregated', 10, 8, 5, 20, null, null, 1),
              ('2026-04-17', 'Topic B', '2026-04-17', '2026-04-17', '2026-04-17', 'yandex', 'cpc', 'campaign', 'aggregated', 'aggregated', 3, 2, null, null, null, null, null);
            """
        )

        replace_operator_export_rows_for_run(
            connection,
            "2026-04-17",
            [
                {
                    "run_date": "2026-04-17",
                    "topic": "Topic A",
                    "report_date": "2026-04-17",
                    "report_date_from": "2026-04-17",
                    "report_date_to": "2026-04-17",
                    "utm_source": "google",
                    "utm_medium": "cpc",
                    "utm_campaign": "brand",
                    "utm_content": "aggregated",
                    "utm_term": "aggregated",
                    "visits": 30,
                    "users": 23,
                    "bounce_rate": 10,
                    "page_depth": 80,
                    "time_on_site_seconds": None,
                    "robot_rate": None,
                    **{f"goal_{index}": (5 if index == 1 else None) for index in range(1, 26)},
                }
            ],
        )

        rows = connection.execute(
            """
            select run_date, topic, visits, users, goal_1
            from operator_export_rows
            order by run_date, topic
            """
        ).fetchall()
        self.assertEqual(
            [tuple(row) for row in rows],
            [
                ("2026-04-16", "Topic A", 10, 8, 1),
                ("2026-04-17", "Topic A", 30, 23, 5),
            ],
        )

    def test_replace_operator_export_rows_for_run_uses_bulk_insert_statement(self):
        from scripts.normalize.turso_operator_export import replace_operator_export_rows_for_run

        class FakeCursor:
            def fetchall(self):
                return []

        class FakeConnection:
            def __init__(self):
                self.execute_calls = []
                self.executemany_calls = []

            def execute(self, sql, params=()):
                self.execute_calls.append((" ".join(str(sql).split()).lower(), tuple(params)))
                return FakeCursor()

            def executemany(self, sql, seq_of_params):
                self.executemany_calls.append((sql, list(seq_of_params)))
                return FakeCursor()

        connection = FakeConnection()

        replace_operator_export_rows_for_run(
            connection,
            "2026-04-17",
            [
                {
                    "run_date": "2026-04-17",
                    "topic": "Topic A",
                    "report_date": "2026-04-17",
                    "report_date_from": "2026-04-17",
                    "report_date_to": "2026-04-17",
                    "utm_source": "google",
                    "utm_medium": "cpc",
                    "utm_campaign": "brand",
                    "utm_content": "aggregated",
                    "utm_term": "aggregated",
                    "visits": 30,
                    "users": 23,
                    "bounce_rate": 10,
                    "page_depth": 80,
                    "time_on_site_seconds": None,
                    "robot_rate": None,
                    **{f"goal_{index}": (5 if index == 1 else None) for index in range(1, 26)},
                },
                {
                    "run_date": "2026-04-17",
                    "topic": "Topic B",
                    "report_date": "2026-04-17",
                    "report_date_from": "2026-04-17",
                    "report_date_to": "2026-04-17",
                    "utm_source": "yandex",
                    "utm_medium": "cpc",
                    "utm_campaign": "perf",
                    "utm_content": "aggregated",
                    "utm_term": "aggregated",
                    "visits": 11,
                    "users": 9,
                    "bounce_rate": None,
                    "page_depth": None,
                    "time_on_site_seconds": None,
                    "robot_rate": None,
                    **{f"goal_{index}": None for index in range(1, 26)},
                },
            ],
        )

        self.assertEqual(connection.executemany_calls, [])
        insert_calls = [call for call in connection.execute_calls if call[0].startswith("insert into operator_export_rows")]
        self.assertEqual(len(insert_calls), 1)

    def test_mark_pipeline_run_ready_and_error_update_status_fields(self):
        from scripts.normalize.turso_writes import mark_pipeline_run_error, mark_pipeline_run_ready

        connection = build_bootstrap_connection()
        connection.execute(
            """
            insert into pipeline_runs (
              run_date, raw_revision, normalize_status, raw_files, raw_rows,
              normalized_files, normalized_rows, last_ingest_at, normalized_at,
              last_error, updated_at
            ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("2026-04-14", 1, "pending_normalize", 2, 10, 0, 0, "2026-04-14T10:00:00Z", None, None, "2026-04-14T10:00:00Z"),
        )
        connection.commit()

        mark_pipeline_run_ready(connection, "2026-04-14", files_count=2, fact_rows_count=9)
        ready_row = connection.execute(
            "select normalized_files, normalized_rows, normalize_status, last_error, normalized_at from pipeline_runs where run_date = ?",
            ("2026-04-14",),
        ).fetchone()
        self.assertEqual(dict(ready_row)["normalized_files"], 2)
        self.assertEqual(dict(ready_row)["normalized_rows"], 9)
        self.assertEqual(dict(ready_row)["normalize_status"], "ready")
        self.assertIsNone(dict(ready_row)["last_error"])
        self.assertTrue(dict(ready_row)["normalized_at"])

        mark_pipeline_run_error(connection, "2026-04-14", "boom")
        error_row = connection.execute(
            "select normalize_status, last_error from pipeline_runs where run_date = ?",
            ("2026-04-14",),
        ).fetchone()
        self.assertEqual(dict(error_row), {"normalize_status": "normalize_error", "last_error": "boom"})

    def test_upsert_topic_goal_slots_preserves_existing_manual_goal_label(self):
        from scripts.normalize.turso_writes import upsert_topic_goal_slots

        connection = build_bootstrap_connection()
        connection.execute(
            """
            insert into topic_goal_slots (topic, goal_slot, source_header, goal_label)
            values (?, ?, ?, ?)
            """,
            ("Topic A", 1, "Goal A", "Manual Label"),
        )
        connection.commit()

        upsert_topic_goal_slots(
            connection,
            [
                {
                    "topic": "Topic A",
                    "goal_slot": 1,
                    "source_header": "Goal A Updated",
                    "goal_label": "Auto Label",
                    "first_seen_file_id": None,
                },
                {
                    "topic": "Topic A",
                    "goal_slot": 2,
                    "source_header": "Goal B",
                    "goal_label": "Goal B",
                    "first_seen_file_id": None,
                },
            ],
        )

        rows = connection.execute(
            "select goal_slot, source_header, goal_label from topic_goal_slots where topic = ? order by goal_slot",
            ("Topic A",),
        ).fetchall()
        self.assertEqual(
            [tuple(row) for row in rows],
            [
                (1, "Goal A Updated", "Manual Label"),
                (2, "Goal B", "Goal B"),
            ],
        )

    def test_upsert_topic_goal_slots_uses_bulk_insert_statement(self):
        from scripts.normalize.turso_writes import upsert_topic_goal_slots

        class FakeCursor:
            def fetchall(self):
                return []

        class FakeConnection:
            def __init__(self):
                self.execute_calls = []
                self.executemany_calls = []

            def execute(self, sql, params=()):
                self.execute_calls.append((" ".join(str(sql).split()).lower(), tuple(params)))
                return FakeCursor()

            def executemany(self, sql, seq_of_params):
                self.executemany_calls.append((sql, list(seq_of_params)))
                return FakeCursor()

        connection = FakeConnection()

        upsert_topic_goal_slots(
            connection,
            [
                {
                    "topic": "Topic A",
                    "goal_slot": 1,
                    "source_header": "Goal A",
                    "goal_label": "Goal A",
                    "first_seen_file_id": "file-a",
                },
                {
                    "topic": "Topic A",
                    "goal_slot": 2,
                    "source_header": "Goal B",
                    "goal_label": "Goal B",
                    "first_seen_file_id": "file-a",
                },
            ],
        )

        self.assertEqual(connection.executemany_calls, [])
        insert_calls = [call for call in connection.execute_calls if call[0].startswith("insert into topic_goal_slots")]
        self.assertEqual(len(insert_calls), 1)


if __name__ == "__main__":
    unittest.main()
