from __future__ import annotations

from pathlib import Path
import unittest


ROOT_DIR = Path(__file__).resolve().parents[1]


class IngestServiceDeploymentTests(unittest.TestCase):
    def test_dockerfile_exists_for_ingest_service_runtime(self):
        dockerfile_path = ROOT_DIR / "Dockerfile.ingest-service"
        self.assertTrue(dockerfile_path.exists(), "Expected Dockerfile.ingest-service to exist")

    def test_dockerfile_exposes_ingest_service_uvicorn_entrypoint(self):
        dockerfile_path = ROOT_DIR / "Dockerfile.ingest-service"
        contents = dockerfile_path.read_text(encoding="utf-8")

        self.assertIn("uvicorn ingest_service.main:app", contents)
        self.assertIn("EXPOSE 8000", contents)
        self.assertIn("PORT", contents)
        self.assertIn("requirements.txt", contents)


if __name__ == "__main__":
    unittest.main()
