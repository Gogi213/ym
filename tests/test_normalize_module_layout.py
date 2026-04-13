import unittest


class NormalizeModuleLayoutTests(unittest.TestCase):
    def test_fields_module_exposes_header_normalization_helpers(self):
        from scripts.normalize import fields

        self.assertEqual(fields.normalize_header("UTM Source"), "utm_source")
        self.assertEqual(
            fields.canonical_field_for_header("Роботность Про"),
            ("metric", "robot_rate"),
        )

    def test_pipeline_module_exposes_normalize_entrypoints(self):
        from scripts.normalize import pipeline

        self.assertTrue(callable(pipeline.normalize_run))
        self.assertTrue(callable(pipeline.finalize_normalized_runs))


if __name__ == "__main__":
    unittest.main()
