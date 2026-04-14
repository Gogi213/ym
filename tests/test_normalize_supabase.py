import unittest
from decimal import Decimal

from scripts.normalize_supabase import (
    assign_goal_slots,
    build_affected_row_keys,
    build_fact_payload,
    build_layout_signature,
    build_merge_key,
    build_normalized_payloads,
    build_pipeline_run_error_update,
    build_pipeline_run_ready_update,
    build_topic_goal_slot_records,
    canonical_field_for_header,
    extract_report_period_from_text,
    extract_report_date,
    merge_secondary_payloads_into_primary,
    normalize_header,
    parse_duration_to_seconds,
    parse_metric_value,
)


class NormalizeSupabaseTests(unittest.TestCase):
    def test_normalize_header_normalizes_common_header_shapes(self):
        self.assertEqual(normalize_header("UTM Source"), "utm_source")
        self.assertEqual(normalize_header("Достижения цели (tw 1. Клик Купить)"), "достижения_цели_tw_1_клик_купить")

    def test_canonical_field_for_header_maps_known_headers(self):
        self.assertEqual(canonical_field_for_header("UTM Source"), ("dimension", "utm_source"))
        self.assertEqual(canonical_field_for_header("Визиты"), ("metric", "visits"))
        self.assertEqual(canonical_field_for_header("Роботность"), ("metric", "robot_rate"))
        self.assertEqual(canonical_field_for_header("Роботность PRO"), ("metric", "robot_rate"))
        self.assertIsNone(canonical_field_for_header("Конверсия посетителей по избранным целям"))
        self.assertEqual(canonical_field_for_header("Доход по избранным целям"), ("goal", "доход_по_избранным_целям"))
        self.assertEqual(canonical_field_for_header("Товаров куплено"), ("goal", "товаров_куплено"))
        self.assertEqual(canonical_field_for_header("Посетители, купившие товар"), ("goal", "посетители_купившие_товар"))
        self.assertEqual(canonical_field_for_header("Достижения избранных целей"), ("goal", "достижения_избранных_целей"))

    def test_parse_metric_value_handles_numbers_and_empty_values(self):
        self.assertEqual(parse_metric_value("1.0"), Decimal("1.0"))
        self.assertEqual(parse_metric_value("0"), Decimal("0"))
        self.assertIsNone(parse_metric_value(""))

    def test_parse_duration_to_seconds_handles_hh_mm_ss(self):
        self.assertEqual(parse_duration_to_seconds("00:04:26"), 266)
        self.assertEqual(parse_duration_to_seconds("01:00:00"), 3600)
        self.assertIsNone(parse_duration_to_seconds(""))

    def test_extract_report_date_prefers_row_date(self):
        row = {"Дата визита": "2026-04-05"}
        self.assertEqual(
            extract_report_date(row=row, message_date="2026-04-06T07:14:35+00:00"),
            "2026-04-05",
        )

    def test_extract_report_date_falls_back_to_message_date_when_row_date_missing(self):
        self.assertEqual(
            extract_report_date(row={}, message_date="2026-04-06T07:14:35+00:00"),
            "2026-04-06",
        )

    def test_extract_report_period_from_text_parses_iso_period(self):
        self.assertEqual(
            extract_report_period_from_text("Отчет за период с 2026-04-10 по 2026-04-10"),
            ("2026-04-10", "2026-04-10"),
        )

    def test_extract_report_period_from_text_parses_ru_period(self):
        self.assertEqual(
            extract_report_period_from_text("Отчет за период с 10.04.2026 по 10.04.2026"),
            ("2026-04-10", "2026-04-10"),
        )

    def test_assign_goal_slots_keeps_existing_slots_and_appends_new_ones(self):
        existing = {
            "TW // Назонекс Аллерджи // Solta": {
                "Достижения цели (tw 1. Клик Купить)": 1,
            }
        }
        assigned = assign_goal_slots(
            topic="TW // Назонекс Аллерджи // Solta",
            goal_headers=[
                "Достижения цели (tw 1. Клик Купить)",
                "Достижения цели (tw 7. Переход в аптеки - сумма)",
            ],
            existing_slots=existing,
        )
        self.assertEqual(
            assigned,
            {
                "Достижения цели (tw 1. Клик Купить)": 1,
                "Достижения цели (tw 7. Переход в аптеки - сумма)": 2,
            },
        )

    def test_build_fact_payload_splits_row_into_dimensions_metrics_and_goals(self):
        payload = build_fact_payload(
            topic="TW // Назонекс Аллерджи // Solta",
            file_id="file-1",
            row_index=1,
            row={
                "UTM Source": "solta",
                "UTM Medium": "cpm",
                "UTM Campaign": "organon_tw_solta_cpm_banner",
                "UTM Content": "banner",
                "UTM Term": "term-1",
                "Дата визита": "2026-04-05",
                "Визиты": "1.0",
                "Посетители": "1.0",
                "Отказы": "0.0",
                "Глубина просмотра": "1.0",
                "Время на сайте": "00:00:14",
                "Роботность": "0.0",
                "Достижения цели (tw 1. Клик Купить)": "2.0",
                "Достижения цели (tw 7. Переход в аптеки - сумма)": "5.0",
            },
            message_date="2026-04-06T07:14:35+00:00",
            goal_slots={
                "Достижения цели (tw 1. Клик Купить)": 1,
                "Достижения цели (tw 7. Переход в аптеки - сумма)": 2,
            },
        )

        self.assertEqual(payload["report_date"], "2026-04-05")
        self.assertEqual(
            payload["dimensions"],
            {
                "utm_source": "solta",
                "utm_medium": "cpm",
                "utm_campaign": "organon_tw_solta_cpm_banner",
                "utm_content": "banner",
                "utm_term": "term-1",
            },
        )
        self.assertEqual(payload["metrics"]["visits"], Decimal("1.0"))
        self.assertEqual(payload["metrics"]["time_on_site_seconds"], Decimal("14"))
        self.assertEqual(payload["metrics"]["robot_rate"], Decimal("0.0"))
        self.assertEqual(payload["goals"], {"goal_1": Decimal("2.0"), "goal_2": Decimal("5.0")})
        self.assertTrue(payload["row_hash"])

    def test_build_fact_payload_row_hash_changes_for_unmapped_text_dimension(self):
        left = build_fact_payload(
            topic="Solaris HC_feb'26_Solta OLV",
            file_id="file-1",
            row_index=1,
            row={
                "UTM Source": "solta_olv",
                "UTM Content": "video_credit_pv",
                "Дата визита": "2026-04-09",
                "Визиты": "3.0",
                "Домен реферера": "away.vk.com",
            },
            message_date="2026-04-10T01:32:05+00:00",
            goal_slots={},
        )
        right = build_fact_payload(
            topic="Solaris HC_feb'26_Solta OLV",
            file_id="file-1",
            row_index=2,
            row={
                "UTM Source": "solta_olv",
                "UTM Content": "video_credit_pv",
                "Дата визита": "2026-04-09",
                "Визиты": "3.0",
                "Домен реферера": "Не определено",
            },
            message_date="2026-04-10T01:32:05+00:00",
            goal_slots={},
        )

        self.assertNotEqual(left["row_hash"], right["row_hash"])

    def test_build_fact_payload_row_hash_ignores_target_visit_metrics(self):
        left = build_fact_payload(
            topic="Solaris HC_feb'26_Solta OLV",
            file_id="file-1",
            row_index=1,
            row={
                "UTM Source": "solta_olv",
                "UTM Content": "video_credit_pv",
                "Дата визита": "2026-04-09",
                "Визиты": "3.0",
                "Целевые визиты (SA Форма отправлена - Фин.программа)": "0.0",
            },
            message_date="2026-04-10T01:32:05+00:00",
            goal_slots={},
        )
        right = build_fact_payload(
            topic="Solaris HC_feb'26_Solta OLV",
            file_id="file-1",
            row_index=2,
            row={
                "UTM Source": "solta_olv",
                "UTM Content": "video_credit_pv",
                "Дата визита": "2026-04-09",
                "Визиты": "3.0",
                "Целевые визиты (SA Форма отправлена - Фин.программа)": "1.0",
            },
            message_date="2026-04-10T01:32:05+00:00",
            goal_slots={},
        )

        self.assertEqual(left["row_hash"], right["row_hash"])

    def test_build_affected_row_keys_unions_existing_and_new_keys(self):
        affected = build_affected_row_keys(
            existing_keys=[
                ("Topic A", "hash-1"),
                ("Topic A", "hash-2"),
            ],
            fact_rows=[
                {"topic": "Topic A", "row_hash": "hash-2"},
                {"topic": "Topic B", "row_hash": "hash-3"},
                {"topic": "Topic B", "row_hash": "hash-3"},
            ],
        )

        self.assertEqual(
            affected,
            [
                ("Topic A", "hash-1"),
                ("Topic A", "hash-2"),
                ("Topic B", "hash-3"),
            ],
        )

    def test_build_layout_signature_depends_on_normalized_header_order(self):
        self.assertEqual(
            build_layout_signature(["UTM Source", "Визиты", "Посетители"]),
            "utm_source|визиты|посетители",
        )

    def test_build_topic_goal_slot_records_emits_stable_slot_rows(self):
        records = build_topic_goal_slot_records(
            goal_slots_by_topic={
                "TW // Назонекс Аллерджи // Solta": {
                    "Достижения цели (tw 1. Клик Купить)": 1,
                    "Достижения цели (tw 7. Переход в аптеки - сумма)": 2,
                }
            },
            first_seen_file_ids={
                "TW // Назонекс Аллерджи // Solta": {
                    "Достижения цели (tw 1. Клик Купить)": "file-1",
                    "Достижения цели (tw 7. Переход в аптеки - сумма)": "file-1",
                }
            },
        )
        self.assertEqual(
            records,
            [
                {
                    "topic": "TW // Назонекс Аллерджи // Solta",
                    "goal_slot": 1,
                    "source_header": "Достижения цели (tw 1. Клик Купить)",
                    "goal_label": "Достижения цели (tw 1. Клик Купить)",
                    "first_seen_file_id": "file-1",
                },
                {
                    "topic": "TW // Назонекс Аллерджи // Solta",
                    "goal_slot": 2,
                    "source_header": "Достижения цели (tw 7. Переход в аптеки - сумма)",
                    "goal_label": "Достижения цели (tw 7. Переход в аптеки - сумма)",
                    "first_seen_file_id": "file-1",
                },
            ],
        )

    def test_merge_secondary_payloads_into_primary_attaches_only_on_exact_grain(self):
        primary_entry = {
            "payload": {
                "topic": "Primary Topic",
                "report_date": "2026-04-10",
                "report_date_from": "2026-04-10",
                "report_date_to": "2026-04-10",
                "dimensions": {
                    "utm_source": "solta",
                    "utm_medium": "cpm",
                    "utm_campaign": "cmp",
                    "utm_content": "banner",
                    "utm_term": "term-1",
                },
                "metrics": {
                    "visits": Decimal("10"),
                },
                "goals": {
                    "goal_1": Decimal("2"),
                },
            }
        }
        matching_secondary = {
            "payload": {
                "topic": "Primary Topic",
                "report_date": "2026-04-10",
                "report_date_from": "2026-04-10",
                "report_date_to": "2026-04-10",
                "dimensions": {
                    "utm_source": "solta",
                    "utm_medium": "cpm",
                    "utm_campaign": "cmp",
                    "utm_content": "banner",
                    "utm_term": "term-1",
                },
                "metrics": {
                    "visits": Decimal("999"),
                },
                "goals": {
                    "goal_2": Decimal("5"),
                },
            }
        }
        unmatched_secondary = {
            "payload": {
                "topic": "Primary Topic",
                "report_date": "2026-04-10",
                "report_date_from": "2026-04-10",
                "report_date_to": "2026-04-10",
                "dimensions": {
                    "utm_source": "solta",
                    "utm_medium": "cpm",
                    "utm_campaign": "cmp",
                    "utm_content": "banner-2",
                    "utm_term": "term-1",
                },
                "metrics": {},
                "goals": {
                    "goal_3": Decimal("7"),
                },
            }
        }

        stats = merge_secondary_payloads_into_primary(
            [primary_entry],
            [matching_secondary, unmatched_secondary],
        )

        self.assertEqual(
            build_merge_key(primary_entry["payload"]),
            (
                "Primary Topic",
                "2026-04-10",
                "2026-04-10",
                "2026-04-10",
                "solta",
                "cpm",
                "cmp",
                "banner",
                "term-1",
            ),
        )
        self.assertEqual(primary_entry["payload"]["metrics"]["visits"], Decimal("10"))
        self.assertEqual(primary_entry["payload"]["goals"]["goal_1"], Decimal("2"))
        self.assertEqual(primary_entry["payload"]["goals"]["goal_2"], Decimal("5"))
        self.assertNotIn("goal_3", primary_entry["payload"]["goals"])
        self.assertEqual(
            stats,
            {
                "matched_secondary_rows": 1,
                "unmatched_secondary_rows": 1,
                "ambiguous_secondary_rows": 0,
            },
        )

    def test_build_normalized_payloads_accepts_string_message_date_and_decoded_raw_json(self):
        fact_rows, fact_dimensions, fact_metrics, secondary_merge_stats = build_normalized_payloads(
            files=[
                {
                    "id": "file-1",
                    "matched_topic": "Topic A",
                    "primary_topic": "Topic A",
                    "topic_role": "primary",
                    "header_json": ["UTM Source", "Визиты"],
                    "message_date": "2026-04-14T10:00:00Z",
                    "attachment_type": "csv",
                }
            ],
            rows_by_file_id={
                "file-1": [
                    {
                        "row_index": 1,
                        "row_json": {
                            "UTM Source": "google",
                            "Визиты": "2",
                        },
                    }
                ]
            },
            payloads_by_file_id={"file-1": {"file_base64": None}},
            goal_slots_by_topic={},
        )

        self.assertEqual(len(fact_rows), 1)
        self.assertEqual(len(fact_dimensions), 1)
        self.assertEqual(len(fact_metrics), 1)
        self.assertEqual(fact_rows[0]["message_date"], "2026-04-14T10:00:00Z")
        self.assertEqual(secondary_merge_stats["matched_secondary_rows"], 0)

    def test_build_pipeline_run_ready_update_shapes_explicit_ready_state(self):
        self.assertEqual(
            build_pipeline_run_ready_update(
                files_count=6,
                fact_rows_count=473,
            ),
            {
                "normalized_files": 6,
                "normalized_rows": 473,
                "normalize_status": "ready",
                "last_error": None,
            },
        )

    def test_build_pipeline_run_error_update_shapes_explicit_error_state(self):
        self.assertEqual(
            build_pipeline_run_error_update("boom"),
            {
                "normalize_status": "normalize_error",
                "last_error": "boom",
            },
        )


if __name__ == "__main__":
    unittest.main()
