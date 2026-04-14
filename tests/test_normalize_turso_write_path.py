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
              id, run_date, message_id, thread_id, message_date, message_subject,
              primary_topic, matched_topic, topic_role, attachment_name, attachment_type,
              status, header_json, row_count, error_text
            ) values
              ('file-a', '2026-04-14', 'm1', 't1', '2026-04-14T10:00:00Z', 's1', 'Topic A', 'Topic A', 'primary', 'a.csv', 'csv', 'ingested', '[]', 2, null),
              ('file-b', '2026-04-13', 'm2', 't2', '2026-04-13T10:00:00Z', 's2', 'Topic B', 'Topic B', 'primary', 'b.csv', 'csv', 'ingested', '[]', 1, null);
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
              id, run_date, message_id, thread_id, message_date, message_subject,
              primary_topic, matched_topic, topic_role, attachment_name, attachment_type,
              status, header_json, row_count, error_text
            ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("file-a", "2026-04-14", "m1", "t1", "2026-04-14T10:00:00Z", "s1", "Topic A", "Topic A", "primary", "a.csv", "csv", "ingested", "[]", 2, None),
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
              id, run_date, message_id, thread_id, message_date, message_subject,
              primary_topic, matched_topic, topic_role, attachment_name, attachment_type,
              status, header_json, row_count, error_text
            ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("file-a", "2026-04-14", "m1", "t1", "2026-04-14T10:00:00Z", "s1", "Topic A", "Topic A", "primary", "a.csv", "csv", "ingested", "[]", 2, None),
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
