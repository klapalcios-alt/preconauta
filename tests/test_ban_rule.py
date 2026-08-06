import unittest

import pandas as pd

from scripts.sync_topdeck import annotate_deck_stats_with_ban_rule, ban_rule_for_league


class BanRuleTests(unittest.TestCase):
    def test_online2x2_uses_fifty_percent_threshold(self) -> None:
        deck_stats = pd.DataFrame(
            [
                {"deck_key": "Deck A", "partidas_jogadas": 12, "win_rate": 0.5},
                {"deck_key": "Deck B", "partidas_jogadas": 12, "win_rate": 0.49},
                {"deck_key": "Deck C", "partidas_jogadas": 11, "win_rate": 0.8},
            ]
        )

        result = annotate_deck_stats_with_ban_rule(deck_stats, "online2x2")

        self.assertTrue(result.loc[result["deck_key"] == "Deck A", "is_banned"].iloc[0])
        self.assertFalse(result.loc[result["deck_key"] == "Deck B", "is_banned"].iloc[0])
        self.assertFalse(result.loc[result["deck_key"] == "Deck C", "is_banned"].iloc[0])

    def test_presencial2x2_uses_two_thirds_threshold(self) -> None:
        deck_stats = pd.DataFrame(
            [
                {"deck_key": "Deck A", "partidas_jogadas": 12, "win_rate": 2 / 3},
                {"deck_key": "Deck B", "partidas_jogadas": 12, "win_rate": 0.66},
                {"deck_key": "Deck C", "partidas_jogadas": 12, "win_rate": 0.65},
            ]
        )

        result = annotate_deck_stats_with_ban_rule(deck_stats, "presencial2x2")

        self.assertTrue(result.loc[result["deck_key"] == "Deck A", "is_banned"].iloc[0])
        self.assertTrue(result.loc[result["deck_key"] == "Deck B", "is_banned"].iloc[0])
        self.assertFalse(result.loc[result["deck_key"] == "Deck C", "is_banned"].iloc[0])

    def test_regular_online_keeps_old_threshold(self) -> None:
        rule = ban_rule_for_league("online")
        self.assertEqual(rule["min_matches"], 12)
        self.assertEqual(rule["min_win_rate"], 1 / 3)


if __name__ == "__main__":
    unittest.main()
