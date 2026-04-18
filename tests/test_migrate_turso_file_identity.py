import sqlite3
import unittest

from scripts.migrate_turso_file_identity import (
    backfill_ingest_file_identity,
    build_file_identity,
    ensure_ingest_file_identity_columns,
    ensure_ingest_file_identity_indexes,
    fetch_table_columns,
)


def build_legacy_connection():
    connection = sqlite3.connect(":memory:")
    connection.executescript(
        """
        create table ingest_files (
          id text primary key,
          run_date text not null,
          message_id text not null,
          thread_id text,
          message_date text,
          message_subject text not null,
          primary_topic text not null,
          matched_topic text not null,
          topic_role text not null,
          attachment_name text not null,
          attachment_type text not null,
          status text not null,
          header_json text not null default '[]',
          row_count integer not null default 0,
          error_text text,
          created_at text not null default current_timestamp
        );

        create table ingest_file_payloads (
          file_id text primary key,
          content_type text,
          file_size_bytes integer not null,
          file_base64 text not null
        );
        """
    )
    return connection


class MigrateTursoFileIdentityTests(unittest.TestCase):
    def test_build_file_identity_uses_content_hash(self):
        raw_file_key, file_hash = build_file_identity("2026-04-14", "Topic A", "YWJj")
        self.assertEqual(file_hash, "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")
        self.assertEqual(raw_file_key, f"2026-04-14|Topic A|{file_hash}")

    def test_ensure_ingest_file_identity_columns_adds_missing_columns(self):
        connection = build_legacy_connection()

        altered = ensure_ingest_file_identity_columns(connection)
        columns = fetch_table_columns(connection, "ingest_files")

        self.assertIn("raw_file_key", columns)
        self.assertIn("file_hash", columns)
        self.assertIn("raw_revision", columns)
        self.assertIn("updated_at", columns)
        self.assertEqual(len(altered), 4)

    def test_backfill_ingest_file_identity_populates_existing_rows(self):
        connection = build_legacy_connection()
        ensure_ingest_file_identity_columns(connection)
        connection.executescript(
            """
            insert into ingest_files (
              id, run_date, message_id, thread_id, message_date, message_subject,
              primary_topic, matched_topic, topic_role, attachment_name, attachment_type,
              status, header_json, row_count, error_text
            ) values (
              'file-1', '2026-04-14', 'm1', 't1', '2026-04-14T10:00:00Z', 's1',
              'Topic A', 'Topic A', 'primary', 'a.csv', 'csv', 'raw_only', '[]', 0, null
            );
            insert into ingest_file_payloads (file_id, content_type, file_size_bytes, file_base64)
            values ('file-1', 'text/csv', 3, 'YWJj');
            """
        )

        result = backfill_ingest_file_identity(connection)
        row = connection.execute(
            "select raw_file_key, file_hash, raw_revision, updated_at from ingest_files where id = 'file-1'"
        ).fetchone()

        self.assertEqual(result, {"updated_files": 1})
        self.assertEqual(row[0], "2026-04-14|Topic A|ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")
        self.assertEqual(row[1], "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")
        self.assertEqual(row[2], 0)
        self.assertTrue(row[3])

    def test_ensure_ingest_file_identity_indexes_creates_unique_raw_key_index(self):
        connection = build_legacy_connection()
        ensure_ingest_file_identity_columns(connection)

        ensure_ingest_file_identity_indexes(connection)
        indexes = connection.execute("pragma index_list('ingest_files')").fetchall()
        index_names = {row[1] for row in indexes}

        self.assertIn("ingest_files_raw_file_key_idx", index_names)
        self.assertIn("ingest_files_file_hash_idx", index_names)


if __name__ == "__main__":
    unittest.main()
