"""Юнит-тесты разбора статистики Mobz и периода (без реального API и без .env)."""

from __future__ import annotations

import unittest
from datetime import date
from unittest.mock import AsyncMock, patch

from config import DeeplinkConfig, MarketplaceConfig, MobzApiSettings
from mobz_http import HttpMobzClient


def _sample_deeplink() -> DeeplinkConfig:
    return DeeplinkConfig(
        id="main",
        label="Main",
        api_key_env="MOBZ_API_KEY_MAIN",
        default_domain="example.com",
        marketplaces=[
            MarketplaceConfig(
                id="wb",
                label="WB",
                suffix="wb",
                notification_label="WB",
                folders=[],
            )
        ],
    )


class StatsPageRowsTest(unittest.TestCase):
    def setUp(self) -> None:
        api = MobzApiSettings(
            origin="https://mobz.io",
            auth_header="Authorization",
            editlink_token_field="erid",
            default_deeplink_id="main",
            marketplace_link_types={},
            stats_unique_only=True,
        )
        self.client = HttpMobzClient([_sample_deeplink()], api)

    def test_rows_in_message_when_result_empty_list(self) -> None:
        payload = {"status": "success", "result": [], "message": [{"id": 1}]}
        rows = self.client._stats_page_rows(payload)
        self.assertEqual(rows, [{"id": 1}])

    def test_rows_prefer_non_empty_result(self) -> None:
        payload = {"status": "success", "result": [{"a": 1}], "message": []}
        rows = self.client._stats_page_rows(payload)
        self.assertEqual(rows, [{"a": 1}])


class StatsForLinkTest(unittest.IsolatedAsyncioTestCase):
    async def test_onelink_dict_message_stats_all(self) -> None:
        api = MobzApiSettings(
            origin="https://mobz.io",
            auth_header="Authorization",
            editlink_token_field="erid",
            default_deeplink_id="main",
            marketplace_link_types={},
            stats_unique_only=True,
        )
        client = HttpMobzClient([_sample_deeplink()], api)
        fake = {
            "status": "success",
            "message": {"stats": {"all": 42}},
        }
        with patch.object(client, "_get_json", new_callable=AsyncMock, return_value=fake):
            out = await client.stats_for_link(
                {
                    "deeplink_id": "main",
                    "external_id": "999",
                    "short_code": "x",
                }
            )
        self.assertEqual(out["clicks"], 42)


class ParsePeriodTest(unittest.TestCase):
    def test_range(self) -> None:
        import main

        p = main.parse_period("01.04.2026-13.04.2026")
        self.assertIsNotNone(p)
        assert p is not None
        self.assertEqual(p[0], date(2026, 4, 1))
        self.assertEqual(p[1], date(2026, 4, 13))


if __name__ == "__main__":
    unittest.main()
