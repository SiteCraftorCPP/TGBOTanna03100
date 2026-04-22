from __future__ import annotations

import asyncio
import json
import os
from datetime import date, datetime, time, timezone
from typing import Any, Iterator
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit

import aiohttp

from config import DeeplinkConfig, MobzApiSettings
from mobz_client import CreateLinkRequest, CreateLinkResult, MobzClient


def _parse_mobz_response_text(text: str) -> dict[str, Any]:
    text = text.strip()
    if not text:
        return {}

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    if "}{" in text:
        first, _rest = text.split("}{", 1)
        first = first + "}"
        return json.loads(first)

    raise ValueError(f"Ответ Mobz не JSON: {text[:300]!r}")


def _normalize_url(url: str) -> str:
    u = url.strip()
    if u.startswith("http://") or u.startswith("https://"):
        return u
    return f"https://{u}"


def _url_with_query_value(url: str, key: str, value: str) -> str:
    parsed = urlsplit(_normalize_url(url))
    items = [(k, v) for k, v in parse_qsl(parsed.query, keep_blank_values=True) if k != key]
    items.append((key, value))
    query = urlencode(items, doseq=True)
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, query, parsed.fragment))


class HttpMobzClient(MobzClient):
    supports_live_stats = True

    def __init__(self, deeplinks: list[DeeplinkConfig], api: MobzApiSettings) -> None:
        self._deeplink_index = {item.id: item for item in deeplinks}
        self.api = api

    def _api_key_for(self, deeplink_id: str) -> str:
        deeplink = self._deeplink_index.get(deeplink_id)
        if not deeplink:
            raise RuntimeError(f"Неизвестный диплинк: {deeplink_id}")

        key = os.getenv(deeplink.api_key_env, "").strip()
        if not key:
            key = os.getenv("MOBZ_API_KEY", "").strip()
        if not key:
            raise RuntimeError(
                f"Не задан API-ключ Mobz: переменная {deeplink.api_key_env} или MOBZ_API_KEY"
            )
        return key

    def _headers(self, api_key: str) -> dict[str, str]:
        return {self.api.auth_header: api_key}

    def _origin(self) -> str:
        return self.api.origin.rstrip("/")

    async def _post_form(
        self,
        deeplink_id: str,
        path: str,
        data: dict[str, Any],
    ) -> dict[str, Any]:
        api_key = self._api_key_for(deeplink_id)
        url = urljoin(self._origin() + "/", path.lstrip("/"))
        timeout = aiohttp.ClientTimeout(total=120)
        form = {k: v for k, v in data.items() if v is not None and v != ""}
        async with aiohttp.ClientSession(timeout=timeout, trust_env=True) as session:
            async with session.post(
                url,
                data=form,
                headers=self._headers(api_key),
            ) as response:
                text = await response.text()

        if response.status >= 400:
            raise RuntimeError(
                f"Mobz HTTP {response.status} для POST {path}: {text[:500]!r}"
            )

        try:
            payload = _parse_mobz_response_text(text)
        except ValueError as exc:
            raise RuntimeError(
                f"Mobz не JSON для POST {path} (HTTP {response.status}): {text[:400]!r}"
            ) from exc
        if payload.get("status") == "error":
            msg = payload.get("message")
            if isinstance(msg, list):
                msg = "; ".join(str(x) for x in msg)
            raise RuntimeError(f"Mobz API: {msg or text[:400]}")
        return payload

    async def _get_json(
        self,
        deeplink_id: str,
        path: str,
        params: dict[str, Any] | None = None,
        *,
        timeout_total: int = 120,
    ) -> dict[str, Any]:
        api_key = self._api_key_for(deeplink_id)
        url = urljoin(self._origin() + "/", path.lstrip("/"))
        timeout = aiohttp.ClientTimeout(total=timeout_total)
        clean_params = {k: v for k, v in (params or {}).items() if v is not None and v != ""}
        async with aiohttp.ClientSession(timeout=timeout, trust_env=True) as session:
            async with session.get(
                url,
                params=clean_params,
                headers=self._headers(api_key),
            ) as response:
                text = await response.text()

        if response.status >= 400:
            raise RuntimeError(
                f"Mobz HTTP {response.status} для GET {path}: {text[:500]!r}"
            )

        try:
            payload = _parse_mobz_response_text(text)
        except ValueError as exc:
            raise RuntimeError(
                f"Mobz не JSON для GET {path} (HTTP {response.status}): {text[:400]!r}"
            ) from exc
        if payload.get("status") == "error":
            msg = payload.get("message")
            if isinstance(msg, list):
                msg = "; ".join(str(x) for x in msg)
            raise RuntimeError(f"Mobz API: {msg or text[:400]}")
        return payload

    def _marketplace_rule(self, marketplace_id: str) -> tuple[str, str]:
        rule = self.api.marketplace_link_types.get(marketplace_id)
        if not rule:
            raise RuntimeError(
                f"Площадка {marketplace_id!r} не настроена для Mobz API. "
                "Добавьте запись в mobz_api.marketplace_link_types (type и url_field)."
            )
        link_type = str(rule.get("type") or "").strip()
        url_field = str(rule.get("url_field") or "").strip()
        if not link_type or not url_field:
            raise RuntimeError(
                f"Для площадки {marketplace_id!r} задайте type и url_field в mobz_api.marketplace_link_types."
            )
        return link_type, url_field

    def _iter_folder_entries(self, raw: list[Any]) -> Iterator[dict[str, Any]]:
        for item in raw:
            if not isinstance(item, dict):
                continue
            if any(k in item for k in ("folder_name", "folder_id", "name")):
                yield item
                continue
            for value in item.values():
                if not isinstance(value, dict):
                    continue
                if "folder_name" in value or "name" in value or "folder_id" in value:
                    if "links" in value and "folder_name" not in value and "folder_id" not in value:
                        continue
                    yield value

    async def _resolve_folder_id(self, deeplink_id: str, folder_name: str) -> str | None:
        payload = await self._get_json(deeplink_id, "/api/public/folders")
        raw: Any = payload.get("message")
        if raw is None:
            raw = payload.get("result")
        if isinstance(raw, dict):
            raw = raw.get("folders") or raw.get("items") or raw.get("list")
        if not isinstance(raw, list):
            return None

        name_norm = folder_name.strip().casefold()
        for item in self._iter_folder_entries(raw):
            title = str(
                item.get("folder_name")
                or item.get("name")
                or item.get("title")
                or "",
            ).strip()
            if title.casefold() == name_norm:
                fid = item.get("folder_id") or item.get("id")
                if fid is not None:
                    return str(fid)
        return None

    async def create_short_link(self, request: CreateLinkRequest) -> CreateLinkResult:
        link_type, url_field = self._marketplace_rule(request.marketplace_id)
        folder_id = await self._resolve_folder_id(request.deeplink_id, request.folder_name)

        data: dict[str, Any] = {
            "shortcode": request.short_code,
            "type": link_type,
            "agree": "2",
            url_field: request.source_url,
        }
        if folder_id:
            data["folder_id"] = folder_id

        if request.link_note.strip():
            data["urlnote"] = request.link_note.strip()

        payload = await self._post_form(request.deeplink_id, "/api/public/addlink", data)

        msg = payload.get("message")
        if not isinstance(msg, str) or not msg.startswith("http"):
            raise RuntimeError(f"Mobz addlink: неожиданный ответ: {payload!r}")

        short_url = _normalize_url(msg)
        info = payload.get("info") or {}
        if not isinstance(info, dict):
            raise RuntimeError(f"Mobz addlink: нет info.link_id: {payload!r}")
        link_id = info.get("link_id")
        if link_id is None:
            raise RuntimeError(f"Mobz addlink: нет info.link_id: {payload!r}")

        return CreateLinkResult(external_id=str(link_id), short_url=short_url)

    async def attach_marking_token(self, link_record: dict[str, Any], token: str) -> dict[str, Any]:
        deeplink_id = str(link_record["deeplink_id"])
        shortcode = str(link_record["short_code"])
        field = self.api.editlink_token_field.strip() or "detail_erid"

        data: dict[str, Any] = {"shortcode": shortcode, field: token}
        source = str(link_record.get("source_url") or "").strip()
        if source:
            data["some_url"] = source
        payload = await self._post_form(deeplink_id, "/api/public/editlink", data)

        raw_url = str(payload.get("message") or "").strip()
        if raw_url.startswith("http"):
            base_url = _normalize_url(raw_url)
        else:
            base_url = str(link_record.get("short_url") or "").strip()
        short_url = _url_with_query_value(base_url, field, token) if base_url else base_url
        updates: dict[str, Any] = {"token_status": "applied"}
        if short_url:
            updates["short_url"] = short_url
        return updates

    def _period_timestamps(self, start_date: date, end_date: date) -> tuple[int, int]:
        tz = timezone.utc
        start_dt = datetime.combine(start_date, time.min, tzinfo=tz)
        end_dt = datetime.combine(end_date, time.max, tzinfo=tz)
        return int(start_dt.timestamp()), int(end_dt.timestamp())

    def _stats_page_rows(self, payload: dict[str, Any]) -> list[Any]:
        # у stats иногда event-ы в message, result пустой
        result = payload.get("result")
        message = payload.get("message")

        msg_rows: list[Any] = []
        if isinstance(message, list) and (not message or isinstance(message[0], dict)):
            msg_rows = message

        if isinstance(result, list):
            if result:
                return result
            return msg_rows

        return msg_rows

    async def stats_for_period(
        self,
        start_date: date,
        end_date: date,
        *,
        link_records: list[dict[str, Any]] | None = None,
    ) -> list[dict[str, Any]]:
        # за период: отдельный stats на link_id. link_records — только бот, иначе mylinks целиком
        default_dl = self.api.default_deeplink_id or next(iter(self._deeplink_index))
        ts_from, ts_to = self._period_timestamps(start_date, end_date)

        triples: list[tuple[str, str, str]] = []

        if link_records is not None:
            for rec in link_records:
                ext = str(rec.get("external_id") or "").strip()
                if not ext:
                    continue
                did = str(rec.get("deeplink_id") or "").strip() or default_dl
                surl = str(rec.get("short_url") or "").strip()
                if surl and not surl.startswith("http"):
                    surl = _normalize_url(surl)
                triples.append((did, ext, surl or ext))
        else:
            listing = await self._get_json(
                default_dl,
                "/api/public/mylinks",
                {},
                timeout_total=300,
            )
            links_raw = listing.get("message")
            if links_raw is None:
                links_raw = listing.get("result")
            if not isinstance(links_raw, list):
                raise RuntimeError(f"Mobz mylinks: ожидался список ссылок: {listing!r}")

            def _iter_mylink_entries(raw: list[Any]) -> Iterator[dict[str, Any]]:
                for item in raw:
                    if not isinstance(item, dict):
                        continue
                    if item.get("link_id") is not None:
                        yield item
                        continue
                    for value in item.values():
                        if isinstance(value, dict) and value.get("link_id") is not None:
                            yield value

            for item in _iter_mylink_entries(links_raw):
                link_id = item.get("link_id")
                if link_id is None:
                    continue
                link_label = item.get("link") or item.get("shortcode") or str(link_id)
                raw_link = str(link_label).strip()
                url = raw_link if raw_link.startswith("http") else _normalize_url(raw_link)
                triples.append((default_dl, str(link_id), url))

        if not triples:
            return []

        concurrency = 16
        sem = asyncio.Semaphore(concurrency)
        timeout = aiohttp.ClientTimeout(total=180)

        async with aiohttp.ClientSession(timeout=timeout, trust_env=True) as session:

            async def _clicks_triple(deeplink_id: str, link_id: str, url: str) -> dict[str, Any]:
                async with sem:
                    headers = self._headers(self._api_key_for(deeplink_id))
                    clicks = await self._stats_clicks_for_link_period(
                        session,
                        headers,
                        link_id,
                        ts_from,
                        ts_to,
                    )
                return {"short_url": url, "clicks": clicks}

            rows = await asyncio.gather(
                *(_clicks_triple(did, lid, u) for did, lid, u in triples),
            )

        return list(rows)

    async def _stats_clicks_for_link_period(
        self,
        session: aiohttp.ClientSession,
        headers: dict[str, str],
        link_id: str,
        ts_from: int,
        ts_to: int,
    ) -> int:
        stats_url = urljoin(self._origin() + "/", "api/public/stats")
        total = 0
        page = 1
        while True:
            params: dict[str, str] = {
                "link_id": link_id,
                "page": str(page),
                "dateFrom": str(ts_from),
                "dateTo": str(ts_to),
            }
            if self.api.stats_unique_only:
                params["clean"] = "1"

            async with session.get(stats_url, params=params, headers=headers) as response:
                text = await response.text()

            if response.status >= 400:
                break

            try:
                stats_payload = _parse_mobz_response_text(text)
            except ValueError:
                break

            if stats_payload.get("status") == "error":
                break

            rows = self._stats_page_rows(stats_payload)
            chunk = len(rows)
            total += chunk
            if chunk < 100:
                break
            page += 1

        return total

    async def stats_for_link(self, link_record: dict[str, Any]) -> dict[str, Any]:
        deeplink_id = str(link_record["deeplink_id"])
        link_id = str(link_record.get("external_id") or "").strip()
        if not link_id:
            raise RuntimeError("Нет external_id (link_id Mobz) для этой записи.")

        params: dict[str, str] = {"link_id": link_id, "stats": "1"}
        if self.api.stats_unique_only:
            params["clean"] = "1"

        payload = await self._get_json(deeplink_id, "/api/public/onelink", params)
        msg = payload.get("message")
        if isinstance(msg, dict):
            stats = msg.get("stats") or {}
            if isinstance(stats, dict):
                raw = stats.get("all") or stats.get("today") or 0
                clicks = int(str(raw).strip() or 0)
                return {"clicks": clicks}

        if isinstance(msg, list) and msg and isinstance(msg[0], dict):
            stats = msg[0].get("stats") or {}
            if isinstance(stats, dict):
                raw = stats.get("all") or 0
                return {"clicks": int(str(raw).strip() or 0)}

        raise RuntimeError(f"Mobz onelink: не удалось разобрать статистику: {payload!r}")
