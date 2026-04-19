import sqlite3
import unittest

from scripts.bootstrap_turso import load_bootstrap_sql


def build_bootstrap_connection():
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    connection.executescript(load_bootstrap_sql())
    return connection


class NormalizeTursoWritePathTests(unittest.TestCase):
    def test_delete_existing_rows_for_run_deletes_only_target_run_and_returns_distinct_row_keys(self):
        from scripts.normalize.turso_writes import delete_existing_rows_for_run

        connection = build_bootstrap_connection()
        connection.executescript(
            """
            insert into ingest_files (
              id, raw_file_key, file_hash, run_date, message_id, thread_id, message_date, message_subject,
              primary_topic, matched_topic, topic_role, attachment_name, attachment_type,
              status, header_json, row_count, error_text
            ) values
              ('file-a', 'rk-file-a', 'hash-file-a', '2026-04-14', 'm1', 't1', '2026-04-14T10:00:00Z', 's1', 'Topic A', 'Topic A', 'primary', 'a.csv', 'csv', 'ingested', '[]', 2, null),
              ('file-b', 'rk-file-b', 'hash-file-b', '2026-04-13', 'm2', 't2', '2026-04-13T10:00:00Z', 's2', 'Topic B', 'Topic B', 'primary', 'b.csv', 'csv', 'ingested', '[]', 1, null);
            insert into fact_rows (
              fact_row_id, topic, source_file_id, source_row_index, report_date, report_date_from,
              report_date_to, message_date, layout_signature, row_hash, is_current, source_row_json
            ) values
              ('fr-1', 'Topic A', 'file-a', 1, '2026-04-14', '2026-04-14', '2026-04-14', '2026-04-14T10:00:00Z', 'sig', 'hash-1', 1, '{}'),
              ('fr-2', 'Topic A', 'file-a', 2, '2026-04-14', '2026-04-14', '2026-04-14', '2026-04-14T10:00:00Z', 'sig', 'hash-1', 1, '{}'),
              ('fr-3', 'Topic B', 'file-b', 1, '2026-04-13', '2026-04-13', '2026-04-13', '2026-04-13T10:00:00Z', 'sig', 'hash-2', 1, '{}');
            """
        )
        connection.commit()

        deleted = delete_existing_rows_for_run(connection, "2026-04-14")

        self.assertEqual(deleted, [("Topic A", "hash-1")])
        remaining = connection.execute("select fact_row_id from fact_rows order by fact_row_id").fetchall()
        self.assertEqual([row[0] for row in remaining], ["fr-3"])

    def test_delete_existing_rows_for_run_uses_explicit_file_ids_for_turso_safe_reruns(self):
        from scripts.normalize.turso_writes import delete_existing_rows_for_run

        class FakeCursor:
            def __init__(self, rows):
                self._rows = list(rows)

            def fetchall(self):
                return list(self._rows)

        class FakeConnection:
            def __init__(self):
                self.file_ids = ["file-a", "file-b"]
                self.row_keys = [("Topic A", "hash-1"), ("Topic A", "hash-2")]
                self.fact_rows = [
                    ("file-a", 1, "fr-1"),
                    ("file-a", 2, "fr-2"),
                    ("file-b", 1, "fr-3"),
                ]

            def execute(self, sql, params=()):
                normalized_sql = " ".join(str(sql).split()).lower()
                if normalized_sql.startswith("select id from ingest_files where run_date = ?"):
                    return FakeCursor([(file_id,) for file_id in self.file_ids])
                if "select distinct fr.topic, fr.row_hash" in normalized_sql:
                    return FakeCursor(self.row_keys)
                if normalized_sql.startswith("delete from fact_rows where source_file_id = ?"):
                    target = tuple(params)[0]
                    self.fact_rows = [row for row in self.fact_rows if row[0] != target]
                    return FakeCursor([])
                if normalized_sql.startswith("delete from fact_rows where source_file_id in ( select id from ingest_files"):
                    raise AssertionError("delete_existing_rows_for_run should not rely on Turso-buggy subquery delete")
                raise AssertionError(f"unexpected sql: {sql}")

            def executemany(self, sql, seq_of_params):
                for params in seq_of_params:
                    self.execute(sql, params)
                return FakeCursor([])

        connection = FakeConnection()

        deleted = delete_existing_rows_for_run(connection, "2026-04-14")

        self.assertEqual(deleted, [("Topic A", "hash-1"), ("Topic A", "hash-2")])
        self.assertEqual(connection.fact_rows, [])

    def test_delete_existing_rows_for_run_emits_chunk_progress(self):
        from scripts.normalize.turso_writes import delete_existing_rows_for_run

        connection = build_bootstrap_connection()
        connection.executescript(
            """
            insert into ingest_files (
              id, raw_file_key, file_hash, run_date, message_id, thread_id, message_date, message_subject,
              primary_topic, matched_topic, topic_role, attachment_name, attachment_type,
              status, header_json, row_count, error_text
            ) values
              ('file-a', 'rk-file-a', 'hash-file-a', '2026-04-14', 'm1', 't1', '2026-04-14T10:00:00Z', 's1', 'Topic A', 'Topic A', 'primary', 'a.csv', 'csv', 'ingested', '[]', 1, null),
              ('file-b', 'rk-file-b', 'hash-file-b', '2026-04-14', 'm2', 't2', '2026-04-14T10:01:00Z', 's2', 'Topic A', 'Topic A', 'primary', 'b.csv', 'csv', 'ingested', '[]', 1, null),
              ('file-c', 'rk-file-c', 'hash-file-c', '2026-04-14', 'm3', 't3', '2026-04-14T10:02:00Z', 's3', 'Topic A', 'Topic A', 'primary', 'c.csv', 'csv', 'ingested', '[]', 1, null);
            insert into fact_rows (
              fact_row_id, topic, source_file_id, source_row_index, report_date, report_date_from,
              report_date_to, message_date, layout_signature, row_hash, is_current, source_row_json
            ) values
              ('fr-1', 'Topic A', 'file-a', 1, '2026-04-14', '2026-04-14', '2026-04-14', '2026-04-14T10:00:00Z', 'sig', 'hash-1', 1, '{}'),
              ('fr-2', 'Topic A', 'file-b', 1, '2026-04-14', '2026-04-14', '2026-04-14', '2026-04-14T10:01:00Z', 'sig', 'hash-2', 1, '{}'),
              ('fr-3', 'Topic A', 'file-c', 1, '2026-04-14', '2026-04-14', '2026-04-14', '2026-04-14T10:02:00Z', 'sig', 'hash-3', 1, '{}');
            """
        )
        connection.commit()
        progress_events = []

        delete_existing_rows_for_run(
            connection,
            "2026-04-14",
            progress=progress_events.append,
            chunk_size=2,
        )

        self.assertEqual(
            progress_events,
            [
                {"processed": 2, "total": 3, "chunk_index": 1, "chunk_count": 2, "percent": 66.67},
                {"processed": 3, "total": 3, "chunk_index": 2, "chunk_count": 2, "percent": 100.0},
            ],
        )

    def test_insert_fact_rows_emits_chunk_progress(self):
        from scripts.normalize.turso_writes import insert_fact_rows

        connection = build_bootstrap_connection()
        connection.execute(
            """
            insert into ingest_files (
              id, raw_file_key, file_hash, run_date, message_id, thread_id, message_date, message_subject,
              primary_topic, matched_topic, topic_role, attachment_name, attachment_type,
              status, header_json, row_count, error_text
            ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("file-a", "rk-file-a", "hash-file-a", "2026-04-14", "m1", "t1", "2026-04-14T10:00:00Z", "s1", "Topic A", "Topic A", "primary", "a.csv", "csv", "ingested", "[]", 3, None),
        )
        progress_events = []

        insert_fact_rows(
            connection,
            [
                {
                    "fact_row_id": "fr-1",
                    "topic": "Topic A",
                    "source_file_id": "file-a",
                    "source_row_index": 1,
                    "report_date": "2026-04-14",
                    "report_date_from": "2026-04-14",
                    "report_date_to": "2026-04-14",
                    "message_date": "2026-04-14T10:00:00Z",
                    "layout_signature": "sig",
                    "row_hash": "hash-1",
                    "source_row_json": "{}",
                },
                {
                    "fact_row_id": "fr-2",
                    "topic": "Topic A",
                    "source_file_id": "file-a",
                    "source_row_index": 2,
                    "report_date": "2026-04-14",
                    "report_date_from": "2026-04-14",
                    "report_date_to": "2026-04-14",
                    "message_date": "2026-04-14T10:00:00Z",
                    "layout_signature": "sig",
                    "row_hash": "hash-2",
                    "source_row_json": "{}",
                },
                {
                    "fact_row_id": "fr-3",
                    "topic": "Topic A",
                    "source_file_id": "file-a",
                    "source_row_index": 3,
                    "report_date": "2026-04-14",
                    "report_date_from": "2026-04-14",
                    "report_date_to": "2026-04-14",
                    "message_date": "2026-04-14T10:00:00Z",
                    "layout_signature": "sig",
                    "row_hash": "hash-3",
                    "source_row_json": "{}",
                },
            ],
            progress=progress_events.append,
            chunk_size=2,
        )

        self.assertEqual(
            progress_events,
            [
                {"processed": 2, "total": 3, "chunk_index": 1, "chunk_count": 2, "percent": 66.67},
                {"processed": 3, "total": 3, "chunk_index": 2, "chunk_count": 2, "percent": 100.0},
            ],
        )

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

    def test_insert_fact_records_and_refresh_current_flags(self):
        from scripts.normalize.turso_operator_flags import refresh_current_flags_for_row_keys
        from scripts.normalize.turso_writes import insert_fact_dimensions, insert_fact_metrics, insert_fact_rows

        connection = build_bootstrap_connection()
        connection.execute(
            """
            insert into ingest_files (
              id, raw_file_key, file_hash, run_date, message_id, thread_id, message_date, message_subject,
              primary_topic, matched_topic, topic_role, attachment_name, attachment_type,
              status, header_json, row_count, error_text
            ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("file-a", "rk-file-a", "hash-file-a", "2026-04-14", "m1", "t1", "2026-04-14T10:00:00Z", "s1", "Topic A", "Topic A", "primary", "a.csv", "csv", "ingested", "[]", 2, None),
        )

        insert_fact_rows(
            connection,
            [
                {
                    "fact_row_id": "fr-1",
                    "topic": "Topic A",
                    "source_file_id": "file-a",
                    "source_row_index": 1,
                    "report_date": "2026-04-14",
                    "report_date_from": "2026-04-14",
                    "report_date_to": "2026-04-14",
                    "message_date": "2026-04-14T10:00:00Z",
                    "layout_signature": "sig",
                    "row_hash": "hash-1",
                    "source_row_json": "{}",
                },
                {
                    "fact_row_id": "fr-2",
                    "topic": "Topic A",
                    "source_file_id": "file-a",
                    "source_row_index": 2,
                    "report_date": "2026-04-14",
                    "report_date_from": "2026-04-14",
                    "report_date_to": "2026-04-14",
                    "message_date": "2026-04-14T11:00:00Z",
                    "layout_signature": "sig",
                    "row_hash": "hash-1",
                    "source_row_json": "{}",
                },
            ],
        )
        insert_fact_dimensions(
            connection,
            [
                {"fact_row_id": "fr-1", "dimension_key": "utm_source", "dimension_value": "google"},
                {"fact_row_id": "fr-2", "dimension_key": "utm_source", "dimension_value": "google"},
            ],
        )
        insert_fact_metrics(
            connection,
            [
                {"fact_row_id": "fr-1", "metric_key": "visits", "metric_value": 1},
                {"fact_row_id": "fr-2", "metric_key": "visits", "metric_value": 2},
            ],
        )

        refresh_current_flags_for_row_keys(connection, [("Topic A", "hash-1")])

        rows = connection.execute(
            "select fact_row_id, is_current from fact_rows order by fact_row_id",
        ).fetchall()
        self.assertEqual([tuple(row) for row in rows], [("fr-1", 0), ("fr-2", 1)])

    def test_refresh_operator_export_rows_for_run_aggregates_content_and_term(self):
        from scripts.normalize.turso_operator_export import refresh_operator_export_rows_for_run
        from scripts.normalize.turso_writes import insert_fact_dimensions, insert_fact_metrics, insert_fact_rows

        connection = build_bootstrap_connection()
        connection.execute(
            """
            insert into ingest_files (
              id, raw_file_key, file_hash, run_date, message_id, thread_id, message_date, message_subject,
              primary_topic, matched_topic, topic_role, attachment_name, attachment_type,
              status, header_json, row_count, error_text
            ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("file-a", "rk-file-a", "hash-file-a", "2026-04-14", "m1", "t1", "2026-04-14T10:00:00Z", "s1", "Topic A", "Topic A", "primary", "a.csv", "csv", "ingested", "[]", 2, None),
        )
        insert_fact_rows(
            connection,
            [
                {
                    "fact_row_id": "fr-1",
                    "topic": "Topic A",
                    "source_file_id": "file-a",
                    "source_row_index": 1,
                    "report_date": "2026-04-14",
                    "report_date_from": "2026-04-14",
                    "report_date_to": "2026-04-14",
                    "message_date": "2026-04-14T10:00:00Z",
                    "layout_signature": "sig",
                    "row_hash": "hash-1",
                    "source_row_json": "{}",
                },
                {
                    "fact_row_id": "fr-2",
                    "topic": "Topic A",
                    "source_file_id": "file-a",
                    "source_row_index": 2,
                    "report_date": "2026-04-14",
                    "report_date_from": "2026-04-14",
                    "report_date_to": "2026-04-14",
                    "message_date": "2026-04-14T10:00:00Z",
                    "layout_signature": "sig",
                    "row_hash": "hash-2",
                    "source_row_json": "{}",
                },
            ],
        )
        insert_fact_dimensions(
            connection,
            [
                {"fact_row_id": "fr-1", "dimension_key": "utm_source", "dimension_value": "google"},
                {"fact_row_id": "fr-1", "dimension_key": "utm_medium", "dimension_value": "cpc"},
                {"fact_row_id": "fr-1", "dimension_key": "utm_campaign", "dimension_value": "campaign-a"},
                {"fact_row_id": "fr-1", "dimension_key": "utm_content", "dimension_value": "creative-1"},
                {"fact_row_id": "fr-1", "dimension_key": "utm_term", "dimension_value": "term-1"},
                {"fact_row_id": "fr-2", "dimension_key": "utm_source", "dimension_value": "google"},
                {"fact_row_id": "fr-2", "dimension_key": "utm_medium", "dimension_value": "cpc"},
                {"fact_row_id": "fr-2", "dimension_key": "utm_campaign", "dimension_value": "campaign-a"},
                {"fact_row_id": "fr-2", "dimension_key": "utm_content", "dimension_value": "creative-2"},
                {"fact_row_id": "fr-2", "dimension_key": "utm_term", "dimension_value": "term-2"},
            ],
        )
        insert_fact_metrics(
            connection,
            [
                {"fact_row_id": "fr-1", "metric_key": "visits", "metric_value": 10},
                {"fact_row_id": "fr-1", "metric_key": "users", "metric_value": 8},
                {"fact_row_id": "fr-1", "metric_key": "bounce_rate", "metric_value": 0.5},
                {"fact_row_id": "fr-1", "metric_key": "goal_1", "metric_value": 2},
                {"fact_row_id": "fr-2", "metric_key": "visits", "metric_value": 20},
                {"fact_row_id": "fr-2", "metric_key": "users", "metric_value": 16},
                {"fact_row_id": "fr-2", "metric_key": "bounce_rate", "metric_value": 0.25},
                {"fact_row_id": "fr-2", "metric_key": "goal_1", "metric_value": 3},
            ],
        )
        connection.execute("update fact_rows set is_current = 1")
        connection.commit()

        refresh_operator_export_rows_for_run(connection, "2026-04-14")

        row = connection.execute(
            """
            select topic, report_date, utm_source, utm_medium, utm_campaign,
                   utm_content, utm_term, visits, users, bounce_rate, goal_1
            from operator_export_rows
            where run_date = ?
            """,
            ("2026-04-14",),
        ).fetchone()
        self.assertEqual(
            tuple(row),
            ("Topic A", "2026-04-14", "google", "cpc", "campaign-a", "aggregated", "aggregated", 30, 24, 10.0, 5),
        )


if __name__ == "__main__":
    unittest.main()
