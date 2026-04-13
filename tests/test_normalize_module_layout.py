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

    def test_db_submodules_expose_storage_boundaries(self):
        from scripts.normalize import db_operator, db_reads, db_writes

        self.assertTrue(callable(db_reads.fetch_ingested_files))
        self.assertTrue(callable(db_writes.insert_fact_rows))
        self.assertTrue(callable(db_operator.refresh_operator_export_rows_for_run))


if __name__ == "__main__":
    unittest.main()
