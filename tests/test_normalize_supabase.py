import unittest
from decimal import Decimal

from scripts.normalize_supabase import (
    assign_goal_slots,
    build_fact_payload,
    build_layout_signature,
    build_topic_goal_slot_records,
    canonical_field_for_header,
    extract_report_period_from_text,
    extract_report_date,
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
                "visit_date": "2026-04-05",
            },
        )
        self.assertEqual(payload["metrics"]["visits"], Decimal("1.0"))
        self.assertEqual(payload["metrics"]["time_on_site_seconds"], Decimal("14"))
        self.assertEqual(payload["metrics"]["robot_rate"], Decimal("0.0"))
        self.assertEqual(payload["goals"], {"goal_1": Decimal("2.0"), "goal_2": Decimal("5.0")})
        self.assertTrue(payload["row_hash"])

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


if __name__ == "__main__":
    unittest.main()
