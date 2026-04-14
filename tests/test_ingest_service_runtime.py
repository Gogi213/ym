import os
import unittest
from unittest import mock

from fastapi.testclient import TestClient


class IngestServiceRuntimeTests(unittest.TestCase):
    def test_create_runtime_app_requires_ingest_token(self):
        from ingest_service.runtime import create_runtime_app

        with mock.patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(RuntimeError, "INGEST_TOKEN"):
                create_runtime_app()

    def test_create_runtime_app_wires_turso_connection_and_closes_on_shutdown(self):
        from ingest_service.runtime import create_runtime_app

        fake_connection = mock.Mock()
        fake_handlers = mock.Mock()
        fake_handlers.reset_handler = mock.Mock(return_value={"ok": True, "action": "reset", "run_date": "2026-04-14"})
        fake_handlers.ingest_handler = mock.Mock(return_value={"ok": True, "status": "accepted", "rows": 0})

        with mock.patch.dict(os.environ, {"INGEST_TOKEN": "secret"}, clear=True):
            with mock.patch("ingest_service.runtime.connect_turso", return_value=fake_connection) as connect_mock:
                with mock.patch("ingest_service.runtime.create_runtime_handlers", return_value=fake_handlers) as handlers_mock:
                    app = create_runtime_app()
                    with TestClient(app) as client:
                        health = client.get("/health")
                        self.assertEqual(health.status_code, 200)

                    connect_mock.assert_called_once()
                    handlers_mock.assert_called_once_with(fake_connection)
                    fake_connection.close.assert_called_once()

    def test_runtime_app_maps_value_error_to_json_response(self):
        from ingest_service.runtime import create_runtime_app

        fake_connection = mock.Mock()
        fake_handlers = mock.Mock()
        fake_handlers.reset_handler = mock.Mock(side_effect=ValueError("bad payload"))
        fake_handlers.ingest_handler = mock.Mock(return_value={"ok": True, "status": "accepted", "rows": 0})

        with mock.patch.dict(os.environ, {"INGEST_TOKEN": "secret"}, clear=True):
            with mock.patch("ingest_service.runtime.connect_turso", return_value=fake_connection):
                with mock.patch("ingest_service.runtime.create_runtime_handlers", return_value=fake_handlers):
                    app = create_runtime_app()
                    with TestClient(app) as client:
                        response = client.post(
                            "/reset",
                            headers={"x-ingest-token": "secret"},
                            json={"action": "reset", "run_date": "2026-04-14"},
                        )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json(), {"ok": False, "error": "bad payload"})


if __name__ == "__main__":
    unittest.main()
