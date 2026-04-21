from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]
APPS_SCRIPT_DIR = ROOT / "appsscript-src"


class DirectTursoOnlyLayoutTests(unittest.TestCase):
    def test_legacy_runtime_paths_are_removed(self):
        self.assertFalse((APPS_SCRIPT_DIR / "21_transport_http_ingest.js").exists())
        self.assertFalse((ROOT / "scripts" / "run_local_stack.py").exists())

    def test_apps_script_is_direct_turso_only(self):
        config_source = (APPS_SCRIPT_DIR / "00_config_and_topics.js").read_text(encoding="utf-8")
        runtime_source = (APPS_SCRIPT_DIR / "23_runtime_settings.js").read_text(encoding="utf-8")

        self.assertNotIn("INGEST_BASE_URL", config_source)
        self.assertNotIn("INGEST_TOKEN", config_source)
        self.assertNotIn("http_ingest", runtime_source)

    def test_normalizer_db_boundary_is_turso_only(self):
        db_source = (ROOT / "scripts" / "normalize" / "db.py").read_text(encoding="utf-8")
        self.assertNotIn("postgres", db_source.lower())


if __name__ == "__main__":
    unittest.main()
