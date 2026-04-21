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

    def test_turso_submodules_expose_storage_boundaries(self):
        from scripts.normalize import db, raw_parse, turso_operator_export, turso_reads, turso_writes

        self.assertEqual(db.backend_name(), "turso")
        self.assertTrue(callable(raw_parse.parse_attachment))
        self.assertTrue(callable(turso_reads.fetch_ingested_files))
        self.assertTrue(callable(turso_writes.upsert_topic_goal_slots))
        self.assertTrue(callable(turso_operator_export.replace_operator_export_rows_for_run))
        self.assertEqual(len(turso_operator_export.GOAL_COLUMNS), 25)


if __name__ == "__main__":
    unittest.main()
