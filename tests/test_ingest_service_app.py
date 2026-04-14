import io
import json
import unittest

from fastapi.testclient import TestClient


class IngestServiceAppTests(unittest.TestCase):
    def test_health_route_returns_ok(self):
        from ingest_service.app import create_app

        client = TestClient(create_app(ingest_token="secret"))
        response = client.get("/health")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"ok": True})

    def test_reset_requires_ingest_token(self):
        from ingest_service.app import create_app

        client = TestClient(create_app(ingest_token="secret"))
        response = client.post("/reset", json={"action": "reset", "run_date": "2026-04-14"})

        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.json(), {"ok": False, "error": "Unauthorized"})

    def test_reset_calls_injected_handler(self):
        from ingest_service.app import create_app

        received = {}

        def reset_handler(payload):
            received["action"] = payload.action
            received["run_date"] = payload.run_date
            return {"ok": True, "action": payload.action, "run_date": payload.run_date}

        client = TestClient(create_app(ingest_token="secret", reset_handler=reset_handler))
        response = client.post(
            "/reset",
            headers={"x-ingest-token": "secret"},
            json={"action": "reset", "run_date": "2026-04-14"},
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"ok": True, "action": "reset", "run_date": "2026-04-14"})
        self.assertEqual(received, {"action": "reset", "run_date": "2026-04-14"})

    def test_ingest_requires_meta_and_file(self):
        from ingest_service.app import create_app

        client = TestClient(create_app(ingest_token="secret"))
        response = client.post("/ingest", headers={"x-ingest-token": "secret"})

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json(), {"ok": False, "error": "Missing multipart meta or file"})

    def test_ingest_calls_injected_handler(self):
        from ingest_service.app import create_app

        received = {}

        def ingest_handler(meta, upload):
            received["meta"] = meta
            received["filename"] = upload.filename
            received["content_type"] = upload.content_type
            return {"ok": True, "status": "accepted", "rows": 0}

        client = TestClient(create_app(ingest_token="secret", ingest_handler=ingest_handler))
        meta = {
            "action": "ingest",
            "run_date": "2026-04-14",
            "primary_topic": "topic-primary",
            "matched_topic": "topic-primary",
            "topic_role": "primary",
            "attachment_name": "report.xlsx",
            "attachment_type": "xlsx",
        }
        response = client.post(
            "/ingest",
            headers={"x-ingest-token": "secret"},
            data={"meta": json.dumps(meta)},
            files={"file": ("report.xlsx", io.BytesIO(b"fake-bytes"), "application/vnd.ms-excel")},
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"ok": True, "status": "accepted", "rows": 0})
        self.assertEqual(received["meta"], meta)
        self.assertEqual(received["filename"], "report.xlsx")
        self.assertEqual(received["content_type"], "application/vnd.ms-excel")


if __name__ == "__main__":
    unittest.main()
