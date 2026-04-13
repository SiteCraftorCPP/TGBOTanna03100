from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any


@dataclass(slots=True)
class CreateLinkRequest:
    deeplink_id: str
    deeplink_label: str
    marketplace_id: str
    marketplace_label: str
    folder_name: str
    source_url: str
    short_code: str
    domain: str
    link_note: str = ""


@dataclass(slots=True)
class CreateLinkResult:
    external_id: str
    short_url: str


class MobzClient:
    supports_live_stats = False

    async def create_short_link(self, request: CreateLinkRequest) -> CreateLinkResult:
        raise NotImplementedError

    async def attach_marking_token(self, link_record: dict[str, Any], token: str) -> dict[str, Any]:
        raise NotImplementedError

    async def stats_for_period(self, start_date: date, end_date: date) -> list[dict[str, Any]]:
        raise NotImplementedError

    async def stats_for_link(self, link_record: dict[str, Any]) -> dict[str, Any]:
        raise NotImplementedError


class MockMobzClient(MobzClient):
    async def create_short_link(self, request: CreateLinkRequest) -> CreateLinkResult:
        short_url = f"https://{request.domain}/{request.short_code}"
        external_id = f"mock::{request.short_code}"
        return CreateLinkResult(external_id=external_id, short_url=short_url)

    async def attach_marking_token(self, link_record: dict[str, Any], token: str) -> dict[str, Any]:
        return {
            "token": token,
            "token_status": "applied",
        }

    async def stats_for_period(self, start_date: date, end_date: date) -> list[dict[str, Any]]:
        raise RuntimeError(
            "Статистика кликов недоступна в mock-режиме. Нужны точные эндпоинты Mobz API."
        )

    async def stats_for_link(self, link_record: dict[str, Any]) -> dict[str, Any]:
        raise RuntimeError(
            "Статистика кликов недоступна в mock-режиме. Нужны точные эндпоинты Mobz API."
        )
