import unittest

from scripts.sync_goal_mapping_sheet import (
    apply_goal_mapping_to_sheet_values,
    build_goal_mapping_grid,
)


class BuildGoalMappingGridTests(unittest.TestCase):
    def test_builds_stable_wide_grid(self):
        grid = build_goal_mapping_grid(
            [
                {
                    "topic": "TW // Назонекс Аллерджи // Solta",
                    "goal_1": "Достижения цели (tw 1. Клик Купить)",
                    "goal_2": "Достижения цели (tw 7. Переход в аптеки - сумма)",
                },
                {
                    "topic": "Solta_Nektar_2026",
                    "goal_1": "Достижения избранных целей",
                },
            ]
        )

        self.assertEqual(
            grid[0],
            [
                "Отчёт",
                "goal_1",
                "goal_2",
                "goal_3",
                "goal_4",
                "goal_5",
                "goal_6",
                "goal_7",
                "goal_8",
                "goal_9",
                "goal_10",
                "goal_11",
                "goal_12",
                "goal_13",
                "goal_14",
                "goal_15",
                "goal_16",
                "goal_17",
                "goal_18",
                "goal_19",
                "goal_20",
                "goal_21",
                "goal_22",
                "goal_23",
                "goal_24",
                "goal_25",
            ],
        )
        self.assertEqual(grid[1][0], "Solta_Nektar_2026")
        self.assertEqual(grid[1][1], "Достижения избранных целей")
        self.assertEqual(grid[2][0], "TW // Назонекс Аллерджи // Solta")
        self.assertEqual(grid[2][1], "Достижения цели (tw 1. Клик Купить)")
        self.assertEqual(grid[2][2], "Достижения цели (tw 7. Переход в аптеки - сумма)")

    def test_applies_goal_mapping_without_rewriting_non_goal_columns(self):
        existing = [
            ["Отчёт", "", "", "goal_1", "goal_2", "goal_3"],
            ["Solta_Nektar_2026", "x", "y", "old1", "old2", ""],
            ["TW // Назонекс Аллерджи // Solta", "meta", "", "", "", ""],
            ["_SenSoy_", "", "", "stale", "stale", "stale"],
        ]
        records = [
            {
                "topic": "Solta_Nektar_2026",
                "goal_1": "Достижения избранных целей",
            },
            {
                "topic": "TW // Назонекс Аллерджи // Solta",
                "goal_1": "Достижения цели (tw 1. Клик Купить)",
                "goal_2": "Достижения цели (tw 7. Переход в аптеки - сумма)",
            },
            {
                "topic": "_SenSoy_",
            },
        ]

        updated = apply_goal_mapping_to_sheet_values(existing, records)

        self.assertEqual(updated[0], existing[0])
        self.assertEqual(updated[1][:3], ["Solta_Nektar_2026", "x", "y"])
        self.assertEqual(updated[1][3:], ["Достижения избранных целей", "", ""])
        self.assertEqual(updated[2][:3], ["TW // Назонекс Аллерджи // Solta", "meta", ""])
        self.assertEqual(
            updated[2][3:],
            ["Достижения цели (tw 1. Клик Купить)", "Достижения цели (tw 7. Переход в аптеки - сумма)", ""],
        )
        self.assertEqual(updated[3][:3], ["_SenSoy_", "", ""])
        self.assertEqual(updated[3][3:], ["", "", ""])


if __name__ == "__main__":
    unittest.main()
