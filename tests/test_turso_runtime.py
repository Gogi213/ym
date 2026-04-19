import json
import os
import tempfile
import unittest
from urllib.error import HTTPError
from pathlib import Path
from unittest import mock


def write_turso_settings(path: Path, *, hostname: str = "ym-cache.aws-eu-west-1.turso.io", db_id: str = "db-1", db_token: str = "db-token") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "cache": {
                    "database_names": {
                        "data": [
                            {
                                "dbId": db_id,
                                "Hostname": hostname,
                            }
                        ]
                    },
                    "database_token": {
                        db_id: {
                            "data": db_token,
                        }
                    },
                }
            }
        ),
        encoding="utf-8",
    )


class TursoRuntimeTests(unittest.TestCase):
    class _FakeHttpResponse:
        def __init__(self, payload: dict, *, status: int = 200):
            self._payload = json.dumps(payload).encode("utf-8")
            self.status = status

        def read(self):
            return self._payload

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

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

    def test_load_turso_config_reads_turso_cli_settings_when_env_missing(self):
        from scripts.turso_runtime import load_turso_config

        with tempfile.TemporaryDirectory() as tmpdir:
            settings_path = Path(tmpdir) / "turso" / "settings.json"
            write_turso_settings(settings_path, hostname="ym-from-cache.aws-eu-west-1.turso.io", db_token="cache-db-token")

            with mock.patch.dict(
                os.environ,
                {
                    "APPDATA": tmpdir,
                },
                clear=True,
            ):
                config = load_turso_config()

        self.assertEqual(config.database_url, "libsql://ym-from-cache.aws-eu-west-1.turso.io")
        self.assertEqual(config.auth_token, "cache-db-token")
        self.assertTrue(config.local_replica_path.endswith("ym-local.db"))

    def test_load_turso_config_prefers_env_over_turso_cli_settings(self):
        from scripts.turso_runtime import load_turso_config

        with tempfile.TemporaryDirectory() as tmpdir:
            settings_path = Path(tmpdir) / "turso" / "settings.json"
            write_turso_settings(settings_path, hostname="ym-from-cache.aws-eu-west-1.turso.io", db_token="cache-db-token")

            with mock.patch.dict(
                os.environ,
                {
                    "APPDATA": tmpdir,
                    "TURSO_DATABASE_URL": "libsql://example.turso.io",
                    "TURSO_AUTH_TOKEN": "env-token",
                },
                clear=True,
            ):
                config = load_turso_config()

        self.assertEqual(config.database_url, "libsql://example.turso.io")
        self.assertEqual(config.auth_token, "env-token")

    def test_normalize_turso_pipeline_url_converts_libsql_to_pipeline_endpoint(self):
        from scripts.turso_runtime import normalize_turso_pipeline_url

        self.assertEqual(
            normalize_turso_pipeline_url("libsql://example.turso.io"),
            "https://example.turso.io/v2/pipeline",
        )
        self.assertEqual(
            normalize_turso_pipeline_url("https://example.turso.io"),
            "https://example.turso.io/v2/pipeline",
        )

    def test_connect_turso_execute_returns_cursor_rows_over_http(self):
        from scripts.turso_runtime import connect_turso, load_turso_config

        requests = []

        def fake_urlopen(request):
            requests.append(
                {
                    "url": request.full_url,
                    "headers": dict(request.header_items()),
                    "body": json.loads(request.data.decode("utf-8")),
                }
            )
            return self._FakeHttpResponse(
                {
                    "results": [
                        {
                            "type": "ok",
                            "response": {
                                "type": "execute",
                                "result": {
                                    "cols": ["scalar"],
                                    "rows": [[{"type": "integer", "value": "1"}]],
                                    "affected_row_count": 0,
                                },
                            },
                        },
                        {"type": "ok", "response": {"type": "close"}},
                    ]
                }
            )

        with mock.patch.dict(
            os.environ,
            {
                "TURSO_DATABASE_URL": "libsql://example.turso.io",
                "TURSO_AUTH_TOKEN": "secret-token",
            },
            clear=True,
        ):
            with mock.patch("urllib.request.urlopen", side_effect=fake_urlopen):
                conn = connect_turso(load_turso_config())
                cursor = conn.execute("select 1 as scalar")

        self.assertEqual(cursor.description, [("scalar", None, None, None, None, None, None)])
        self.assertEqual(cursor.fetchall(), [(1,)])
        self.assertEqual(requests[0]["url"], "https://example.turso.io/v2/pipeline")
        self.assertEqual(
            requests[0]["body"]["requests"],
            [
                {"type": "execute", "stmt": {"sql": "select 1 as scalar"}},
                {"type": "close"},
            ],
        )

    def test_connect_turso_decodes_null_cells_without_value_field(self):
        from scripts.turso_runtime import connect_turso, load_turso_config

        def fake_urlopen(_request):
            return self._FakeHttpResponse(
                {
                    "results": [
                        {
                            "type": "ok",
                            "response": {
                                "type": "execute",
                                "result": {
                                    "cols": ["goal_1"],
                                    "rows": [[{"type": "null"}]],
                                    "affected_row_count": 0,
                                },
                            },
                        },
                        {"type": "ok", "response": {"type": "close"}},
                    ]
                }
            )

        with mock.patch.dict(
            os.environ,
            {
                "TURSO_DATABASE_URL": "libsql://example.turso.io",
                "TURSO_AUTH_TOKEN": "secret-token",
            },
            clear=True,
        ):
            with mock.patch("urllib.request.urlopen", side_effect=fake_urlopen):
                conn = connect_turso(load_turso_config())
                cursor = conn.execute("select null as goal_1")

        self.assertEqual(cursor.fetchall(), [(None,)])

    def test_http_connection_reuses_baton_until_commit(self):
        from scripts.turso_runtime import connect_turso, load_turso_config

        requests = []
        responses = [
            self._FakeHttpResponse(
                {
                    "baton": "baton-1",
                    "base_url": "https://primary.example.turso.io",
                    "results": [
                        {
                            "type": "ok",
                            "response": {"type": "execute", "result": {"cols": [], "rows": []}},
                        }
                    ],
                }
            ),
            self._FakeHttpResponse(
                {
                    "baton": "baton-1",
                    "base_url": "https://primary.example.turso.io",
                    "results": [
                        {
                            "type": "ok",
                            "response": {"type": "execute", "result": {"cols": [], "rows": []}},
                        }
                    ],
                }
            ),
            self._FakeHttpResponse(
                {
                    "results": [
                        {
                            "type": "ok",
                            "response": {"type": "execute", "result": {"cols": [], "rows": []}},
                        },
                        {"type": "ok", "response": {"type": "close"}},
                    ]
                }
            ),
        ]

        def fake_urlopen(request):
            requests.append(
                {
                    "url": request.full_url,
                    "body": json.loads(request.data.decode("utf-8")),
                }
            )
            return responses[len(requests) - 1]

        with mock.patch.dict(
            os.environ,
            {
                "TURSO_DATABASE_URL": "libsql://example.turso.io",
                "TURSO_AUTH_TOKEN": "secret-token",
            },
            clear=True,
        ):
            with mock.patch("urllib.request.urlopen", side_effect=fake_urlopen):
                conn = connect_turso(load_turso_config())
                conn.begin()
                conn.execute("insert into test values (?)", ("x",))
                conn.commit()

        self.assertEqual(requests[0]["body"]["requests"], [{"type": "execute", "stmt": {"sql": "BEGIN"}}])
        self.assertEqual(requests[1]["url"], "https://primary.example.turso.io/v2/pipeline")
        self.assertEqual(requests[1]["body"]["baton"], "baton-1")
        self.assertEqual(
            requests[2]["body"]["requests"],
            [
                {"type": "execute", "stmt": {"sql": "COMMIT"}},
                {"type": "close"},
            ],
        )

    def test_executemany_splits_large_batches_across_multiple_http_requests(self):
        from scripts.turso_runtime import connect_turso, load_turso_config

        requests = []

        def fake_urlopen(request):
            requests.append(json.loads(request.data.decode("utf-8")))
            return self._FakeHttpResponse(
                {
                    "results": [
                        {
                            "type": "ok",
                            "response": {"type": "execute", "result": {"cols": [], "rows": []}},
                        },
                        {"type": "ok", "response": {"type": "close"}},
                    ]
                }
            )

        with mock.patch.dict(
            os.environ,
            {
                "TURSO_DATABASE_URL": "libsql://example.turso.io",
                "TURSO_AUTH_TOKEN": "secret-token",
            },
            clear=True,
        ):
            with mock.patch("urllib.request.urlopen", side_effect=fake_urlopen):
                conn = connect_turso(load_turso_config())
                conn.executemany(
                    "insert into ingest_rows values (?, ?, ?, ?)",
                    [("file-1", "2026-04-16", index, f"row-{index}") for index in range(120)],
                )

        execute_counts = [
            sum(1 for item in request["requests"] if item["type"] == "execute")
            for request in requests
        ]
        self.assertEqual(execute_counts, [50, 50, 20])

    def test_connect_turso_wraps_auth_http_failure_with_actionable_message(self):
        from scripts.turso_runtime import connect_turso, load_turso_config

        def fake_urlopen(_request):
            raise HTTPError(
                url="https://example.turso.io/v2/pipeline",
                code=401,
                msg="Unauthorized",
                hdrs=None,
                fp=None,
            )

        with mock.patch.dict(
            os.environ,
            {
                "TURSO_DATABASE_URL": "libsql://example.turso.io",
                "TURSO_AUTH_TOKEN": "expired-token",
            },
            clear=True,
        ):
            with mock.patch("urllib.request.urlopen", side_effect=fake_urlopen):
                conn = connect_turso(load_turso_config())
                with self.assertRaisesRegex(
                    RuntimeError,
                    "TURSO_AUTH_TOKEN.*401 Unauthorized.*refresh the DB token",
                ):
                    conn.execute("select 1")

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
