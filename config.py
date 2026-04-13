from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


@dataclass(slots=True)
class FormatOption:
    id: str
    label: str
    slug: str


@dataclass(slots=True)
class MarketplaceConfig:
    id: str
    label: str
    suffix: str
    notification_label: str
    folders: list[str]


@dataclass(slots=True)
class DeeplinkConfig:
    id: str
    label: str
    api_key_env: str
    default_domain: str
    marketplaces: list[MarketplaceConfig]


@dataclass(slots=True)
class MobzApiSettings:
    origin: str
    auth_header: str
    editlink_token_field: str
    default_deeplink_id: str | None
    marketplace_link_types: dict[str, dict[str, str]]
    stats_unique_only: bool


@dataclass(slots=True)
class AppConfig:
    project_dir: Path
    token: str
    proxy_url: str | None
    mobz_provider: str
    formats: list[FormatOption]
    deeplinks: list[DeeplinkConfig]
    mobz_api: MobzApiSettings


def _normalize_proxy(raw_proxy: str | None) -> str | None:
    if not raw_proxy:
        return None

    if "://" in raw_proxy:
        return raw_proxy

    parts = raw_proxy.split(":")
    if len(parts) == 4:
        host, port, username, password = parts
        return f"socks5://{username}:{password}@{host}:{port}"

    if len(parts) == 2:
        host, port = parts
        return f"socks5://{host}:{port}"

    raise ValueError("TELEGRAM_PROXY должен быть socks5 URL или host:port[:user:pass].")


def _default_mobz_api() -> MobzApiSettings:
    return MobzApiSettings(
        origin="https://mobz.io",
        auth_header="Authorization",
        editlink_token_field="erid",
        default_deeplink_id="main",
        marketplace_link_types={
            "wb": {"type": "wildberries", "url_field": "wildberries"},
            "ozon": {"type": "ozon", "url_field": "ozon"},
            "golden_apple": {"type": "goldapple", "url_field": "goldapple"},
            "letual": {"type": "letual", "url_field": "letual"},
        },
        stats_unique_only=True,
    )


def _parse_mobz_api(raw: dict) -> MobzApiSettings:
    defaults = _default_mobz_api()
    block = raw.get("mobz_api")
    if not block or not isinstance(block, dict):
        return defaults

    cleaned = {k: v for k, v in block.items() if not str(k).startswith("_")}

    mlt = {k: dict(v) for k, v in defaults.marketplace_link_types.items()}
    raw_mlt = cleaned.get("marketplace_link_types")
    if isinstance(raw_mlt, dict):
        for key, value in raw_mlt.items():
            if isinstance(value, dict):
                mlt[str(key)] = {str(k2): str(v2) for k2, v2 in value.items()}

    raw_dd = cleaned.get("default_deeplink_id", defaults.default_deeplink_id)
    if raw_dd is None or (isinstance(raw_dd, str) and not raw_dd.strip()):
        default_deeplink_id = None
    else:
        default_deeplink_id = str(raw_dd).strip()

    stats_flag = cleaned.get("stats_unique_only", defaults.stats_unique_only)
    if isinstance(stats_flag, str):
        stats_unique_only = stats_flag.strip().lower() in {"1", "true", "yes", "on"}
    else:
        stats_unique_only = bool(stats_flag)

    return MobzApiSettings(
        origin=str(cleaned.get("origin") or defaults.origin).rstrip("/"),
        auth_header=str(cleaned.get("auth_header") or defaults.auth_header).strip(),
        editlink_token_field=str(
            cleaned.get("editlink_token_field") or defaults.editlink_token_field
        ).strip(),
        default_deeplink_id=default_deeplink_id,
        marketplace_link_types=mlt,
        stats_unique_only=stats_unique_only,
    )


def deeplink_from_raw(deeplink: dict) -> DeeplinkConfig:
    """Собирает DeeplinkConfig из одного объекта как в settings.json → deeplinks[]."""
    if not isinstance(deeplink, dict):
        raise ValueError("Диплинк должен быть JSON-объектом.")

    m_raw = deeplink.get("marketplaces")
    if not isinstance(m_raw, list) or not m_raw:
        raise ValueError("Поле marketplaces должно быть непустым массивом.")

    marketplaces: list[MarketplaceConfig] = []
    for item in m_raw:
        if not isinstance(item, dict):
            raise ValueError("Каждый элемент marketplaces должен быть объектом.")
        folders_raw = item.get("folders")
        if folders_raw is None:
            folders: list[str] = []
        elif isinstance(folders_raw, list):
            folders = [str(f) for f in folders_raw]
        else:
            raise ValueError("Поле folders должно быть массивом строк.")

        marketplaces.append(
            MarketplaceConfig(
                id=str(item["id"]),
                label=str(item["label"]),
                suffix=str(item["suffix"]),
                notification_label=str(item["notification_label"]),
                folders=folders,
            )
        )

    return DeeplinkConfig(
        id=str(deeplink["id"]).strip(),
        label=str(deeplink["label"]).strip(),
        api_key_env=str(deeplink["api_key_env"]).strip(),
        default_domain=str(deeplink["default_domain"]).strip(),
        marketplaces=marketplaces,
    )


def _load_settings(project_dir: Path) -> tuple[list[FormatOption], list[DeeplinkConfig], MobzApiSettings]:
    settings_path = project_dir / "settings.json"
    raw = json.loads(settings_path.read_text(encoding="utf-8"))

    formats = [
        FormatOption(
            id=item["id"],
            label=item["label"],
            slug=item["slug"],
        )
        for item in raw["formats"]
    ]

    deeplinks = [deeplink_from_raw(d) for d in raw["deeplinks"]]

    mobz_api = _parse_mobz_api(raw)
    return formats, deeplinks, mobz_api


def load_config() -> AppConfig:
    project_dir = Path(__file__).resolve().parent
    load_dotenv(project_dir / ".env")

    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    if not token:
        raise RuntimeError("Не задан TELEGRAM_BOT_TOKEN в .env")

    formats, deeplinks, mobz_api = _load_settings(project_dir)
    proxy_url = _normalize_proxy(os.getenv("TELEGRAM_PROXY"))

    return AppConfig(
        project_dir=project_dir,
        token=token,
        proxy_url=proxy_url,
        mobz_provider=os.getenv("MOBZ_PROVIDER", "mock").strip().lower() or "mock",
        formats=formats,
        deeplinks=deeplinks,
        mobz_api=mobz_api,
    )
