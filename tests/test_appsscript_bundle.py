import pathlib
import unittest

from scripts.build_appsscript_bundle import build_bundle


ROOT = pathlib.Path(__file__).resolve().parents[1]


class AppsScriptBundleTests(unittest.TestCase):
    def test_code_js_matches_generated_bundle(self):
        actual = (ROOT / "Code.js").read_text(encoding="utf-8-sig")
        self.assertEqual(actual, build_bundle())


if __name__ == "__main__":
    unittest.main()
