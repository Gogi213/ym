import pathlib
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
EDGE_DIR = ROOT / "supabase" / "functions" / "mail-ingest"


class MailIngestLayoutTests(unittest.TestCase):
    def test_edge_function_is_split_into_focused_modules(self):
        expected = {
            "index.ts",
            "auth.ts",
            "handlers.ts",
            "parse.ts",
            "shared.ts",
            "supabase.ts",
        }
        actual = {path.name for path in EDGE_DIR.glob("*.ts")}
        self.assertTrue(expected.issubset(actual))

    def test_index_ts_is_thin_entrypoint(self):
        index_text = (EDGE_DIR / "index.ts").read_text(encoding="utf-8")
        self.assertIn("handleIngest", index_text)
        self.assertIn("handleReset", index_text)
        self.assertLessEqual(len(index_text.splitlines()), 40)

    def test_parse_and_handler_modules_hold_logic(self):
        parse_lines = len((EDGE_DIR / "parse.ts").read_text(encoding="utf-8").splitlines())
        handler_lines = len((EDGE_DIR / "handlers.ts").read_text(encoding="utf-8").splitlines())
        self.assertGreater(parse_lines, 200)
        self.assertGreater(handler_lines, 150)


if __name__ == "__main__":
    unittest.main()
