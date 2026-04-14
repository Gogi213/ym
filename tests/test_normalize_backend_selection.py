import os
import unittest
from unittest import mock


class NormalizeBackendSelectionTests(unittest.TestCase):
    def test_connect_db_uses_postgres_backend_by_default(self):
        from scripts.normalize import db

        fake_connection = object()
        with mock.patch.dict(os.environ, {}, clear=False):
            with mock.patch("scripts.normalize.db.db_connection.connect_db", return_value=fake_connection) as connect_mock:
                resolved = db.connect_db()

        self.assertIs(resolved, fake_connection)
        connect_mock.assert_called_once_with()

    def test_connect_db_uses_turso_backend_when_requested(self):
        from scripts.normalize import db

        fake_connection = object()
        with mock.patch.dict(os.environ, {"NORMALIZE_DB_BACKEND": "turso"}, clear=False):
            with mock.patch("scripts.normalize.db.turso_connection.connect_db", return_value=fake_connection) as connect_mock:
                resolved = db.connect_db()

        self.assertIs(resolved, fake_connection)
        connect_mock.assert_called_once_with()

    def test_fetch_ingested_files_dispatches_to_turso_backend(self):
        from scripts.normalize import db

        fake_connection = object()
        fake_rows = [{"id": "file-1"}]
        with mock.patch.dict(os.environ, {"NORMALIZE_DB_BACKEND": "turso"}, clear=False):
            with mock.patch("scripts.normalize.db.turso_reads.fetch_ingested_files", return_value=fake_rows) as fetch_mock:
                resolved = db.fetch_ingested_files(fake_connection, "2026-04-14")

        self.assertEqual(resolved, fake_rows)
        fetch_mock.assert_called_once_with(fake_connection, "2026-04-14")

    def test_refresh_operator_export_rows_for_run_dispatches_to_turso_backend(self):
        from scripts.normalize import db

        fake_connection = object()
        with mock.patch.dict(os.environ, {"NORMALIZE_DB_BACKEND": "turso"}, clear=False):
            with mock.patch("scripts.normalize.db.turso_operator_export.refresh_operator_export_rows_for_run") as refresh_mock:
                db.refresh_operator_export_rows_for_run(fake_connection, "2026-04-14")

        refresh_mock.assert_called_once_with(fake_connection, "2026-04-14")


if __name__ == "__main__":
    unittest.main()
