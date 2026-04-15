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

    def test_env_example_declares_required_ingest_service_variables(self):
        env_example_path = ROOT_DIR / ".env.ingest-service.example"
        self.assertTrue(env_example_path.exists(), "Expected .env.ingest-service.example to exist")

        contents = env_example_path.read_text(encoding="utf-8")
        self.assertIn("INGEST_TOKEN=", contents)
        self.assertIn("TURSO_DATABASE_URL=", contents)
        self.assertIn("TURSO_AUTH_TOKEN=", contents)
        self.assertIn("PORT=", contents)

    def test_compose_file_exposes_ingest_service_runtime(self):
        compose_path = ROOT_DIR / "docker-compose.ingest-service.yml"
        self.assertTrue(compose_path.exists(), "Expected docker-compose.ingest-service.yml to exist")

        contents = compose_path.read_text(encoding="utf-8")
        self.assertIn("Dockerfile.ingest-service", contents)
        self.assertIn(".env.ingest-service", contents)
        self.assertIn("8000:8000", contents)
        self.assertIn("/health", contents)


if __name__ == "__main__":
    unittest.main()
