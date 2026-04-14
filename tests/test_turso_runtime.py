import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock


class TursoRuntimeTests(unittest.TestCase):
    def test_load_turso_config_requires_url_and_token(self):
        from scripts.turso_runtime import load_turso_config

        with mock.patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(RuntimeError, "Turso connection is not configured"):
                load_turso_config()

    def test_load_turso_config_reads_env(self):
        from scripts.turso_runtime import load_turso_config

        with mock.patch.dict(
            os.environ,
            {
                "TURSO_DATABASE_URL": "libsql://example.turso.io",
                "TURSO_AUTH_TOKEN": "secret-token",
            },
            clear=True,
        ):
            config = load_turso_config()

        self.assertEqual(config.database_url, "libsql://example.turso.io")
        self.assertEqual(config.auth_token, "secret-token")
        self.assertTrue(config.local_replica_path.endswith("ym-local.db"))

    def test_connect_turso_uses_libsql_connect(self):
        from scripts.turso_runtime import connect_turso, load_turso_config

        fake_connection = mock.Mock()
        fake_libsql = mock.Mock()
        fake_libsql.connect.return_value = fake_connection

        with tempfile.TemporaryDirectory() as tmpdir:
            with mock.patch.dict(
                os.environ,
                {
                    "TURSO_DATABASE_URL": "libsql://example.turso.io",
                    "TURSO_AUTH_TOKEN": "secret-token",
                    "TURSO_LOCAL_REPLICA_PATH": str(Path(tmpdir) / "replica.db"),
                },
                clear=True,
            ):
                with mock.patch("importlib.import_module", return_value=fake_libsql):
                    conn = connect_turso(load_turso_config())

        self.assertIs(conn, fake_connection)
        fake_libsql.connect.assert_called_once()
        fake_connection.sync.assert_called_once()

    def test_load_bootstrap_sql_reads_schema_file(self):
        from scripts.bootstrap_turso import load_bootstrap_sql

        sql = load_bootstrap_sql()
        self.assertIn("create table if not exists ingest_files", sql)
        self.assertIn("create view if not exists export_rows_wide", sql)

    def test_split_sql_statements_separates_bootstrap_script(self):
        from scripts.bootstrap_turso import split_sql_statements

        statements = split_sql_statements(
            """
            create table test_a(id integer);
            create table test_b(id integer);
            """
        )

        self.assertEqual(len(statements), 2)
        self.assertTrue(statements[0].startswith("create table test_a"))
        self.assertTrue(statements[1].startswith("create table test_b"))

    def test_apply_bootstrap_executes_all_statements(self):
        from scripts.bootstrap_turso import apply_bootstrap

        executed = []

        class FakeConnection:
            def __init__(self):
                self.commits = 0
                self.syncs = 0

            def execute(self, sql):
                executed.append(sql.strip())

            def commit(self):
                self.commits += 1

            def sync(self):
                self.syncs += 1

        connection = FakeConnection()

        apply_bootstrap(
            connection,
            """
            create table test_a(id integer);
            create table test_b(id integer);
            """,
        )

        self.assertEqual(
            executed,
            [
                "create table test_a(id integer);",
                "create table test_b(id integer);",
            ],
        )
        self.assertEqual(connection.commits, 1)
        self.assertEqual(connection.syncs, 1)


if __name__ == "__main__":
    unittest.main()
