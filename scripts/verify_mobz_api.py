r"""
Полная проверка Mobz Public API (Authorization + эндпоинты, используемые ботом).
Запуск из корня проекта:

  .venv\Scripts\python scripts\verify_mobz_api.py
"""

from __future__ import annotations

import asyncio
import sys
import time
from datetime import date, datetime, time as dt_time, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import load_config
from mobz_client import CreateLinkRequest
from mobz_http import HttpMobzClient


def ok(name: str, detail: str = "") -> None:
    print(f"[OK] {name}" + (f" — {detail}" if detail else ""), flush=True)


def fail(name: str, exc: BaseException) -> None:
    print(f"[FAIL] {name}: {exc}", flush=True)


def _first_link_id_from_mylinks(payload: dict) -> tuple[str, str] | None:
    raw = payload.get("message")
    if raw is None:
        raw = payload.get("result")
    if not isinstance(raw, list) or not raw:
        return None
    for item in raw:
        if not isinstance(item, dict):
            continue
        if item.get("link_id") is not None:
            lid = str(item["link_id"])
            link = str(item.get("link") or item.get("shortcode") or lid)
            return lid, link
        for v in item.values():
            if not isinstance(v, dict):
                continue
            lid = v.get("link_id")
            if lid is not None:
                link = str(v.get("link") or v.get("shortcode") or lid)
                return str(lid), link
    return None


async def main() -> int:
    print("verify_mobz_api: старт проверки Mobz Public API…", flush=True)
    config = load_config()
    if config.mobz_provider != "http":
        print("В .env задайте MOBZ_PROVIDER=http для проверки реального API.")
        return 1

    client = HttpMobzClient(config.deeplinks, config.mobz_api)
    deeplink_id = "main"
    errors = 0

    # 1) Folders
    try:
        r = await client._get_json(deeplink_id, "/api/public/folders")
        assert r.get("status") == "success"
        ok("GET /api/public/folders", f"status={r.get('status')}")
    except Exception as e:
        fail("GET /api/public/folders", e)
        errors += 1

    # 2) My links (повтор при 504 / таймауте)
    first = None
    last_err: BaseException | None = None
    for attempt in range(1, 4):
        try:
            r = await client._get_json(
                deeplink_id,
                "/api/public/mylinks",
                {},
                timeout_total=300,
            )
            assert r.get("status") == "success"
            first = _first_link_id_from_mylinks(r)
            ok(
                "GET /api/public/mylinks",
                f"без stats (легче для nginx), попытка {attempt}"
                + (f", пример link_id={first[0]}" if first else ""),
            )
            last_err = None
            break
        except Exception as e:
            last_err = e
            if attempt < 3:
                await asyncio.sleep(5.0 * attempt)
    if last_err is not None:
        fail("GET /api/public/mylinks", last_err)
        errors += 1

    # 3) One link + stats (если есть существующая ссылка)
    if first:
        link_id, _link_label = first
        try:
            r = await client._get_json(
                deeplink_id,
                "/api/public/onelink",
                {"link_id": link_id, "stats": "1", "clean": "1"},
                timeout_total=120,
            )
            assert r.get("status") == "success"
            ok("GET /api/public/onelink", f"link_id={link_id}")
        except Exception as e:
            fail("GET /api/public/onelink", e)
            errors += 1

        try:
            d0 = date.today() - timedelta(days=7)
            d1 = date.today()
            ts0 = int(datetime.combine(d0, dt_time.min, tzinfo=timezone.utc).timestamp())
            ts1 = int(datetime.combine(d1, dt_time.max, tzinfo=timezone.utc).timestamp())
            r = await client._get_json(
                deeplink_id,
                "/api/public/stats",
                {
                    "link_id": link_id,
                    "page": "1",
                    "dateFrom": str(ts0),
                    "dateTo": str(ts1),
                    "clean": "1",
                },
                timeout_total=120,
            )
            assert r.get("status") == "success"
            res = r.get("result")
            extra = f"type(result)={type(res).__name__}"
            if isinstance(res, list):
                extra += f", len={len(res)}"
            ok("GET /api/public/stats", extra)
        except Exception as e:
            fail("GET /api/public/stats", e)
            errors += 1

    # 4) addlink (ozon + поле ozon, уникальный шорткод)
    short_code = f"tgc{int(time.time())}"
    created_id: str | None = None
    try:
        req = CreateLinkRequest(
            deeplink_id=deeplink_id,
            deeplink_label="verify",
            marketplace_id="ozon",
            marketplace_label="OZON",
            folder_name="__verify_no_folder_match__",
            source_url="https://www.ozon.ru/product/",
            short_code=short_code,
            domain="sprey.mobz.link",
            link_note="cursor_api_verify",
        )
        out = await client.create_short_link(req)
        created_id = out.external_id
        ok("POST /api/public/addlink (HttpMobzClient.create_short_link)", f"id={created_id}, url={out.short_url[:50]}...")
    except Exception as e:
        fail("POST /api/public/addlink", e)
        errors += 1

    # 5) HttpMobzClient.stats_for_link по созданной записи
    if created_id:
        record = {
            "deeplink_id": deeplink_id,
            "external_id": created_id,
            "short_code": short_code,
            "source_url": "https://www.ozon.ru/product/",
        }
        try:
            s = await client.stats_for_link(record)
            ok("stats_for_link (onelink)", f"clicks={s.get('clicks')}")
        except Exception as e:
            fail("stats_for_link", e)
            errors += 1

        try:
            await client.attach_marking_token(record, "VERIFY_ERID_PLACEHOLDER")
            ok("POST /api/public/editlink (attach_marking_token)", "urlnote + some_url")
        except Exception as e:
            fail("POST /api/public/editlink", e)
            errors += 1

    # 6) stats за период: выборка первых 5 ссылок (полный обход аккаунта может занять минуты)
    def _iter_links(raw: list) -> list[dict]:
        out: list[dict] = []
        for item in raw:
            if not isinstance(item, dict):
                continue
            if item.get("link_id") is not None:
                out.append(item)
                continue
            for value in item.values():
                if isinstance(value, dict) and value.get("link_id") is not None:
                    out.append(value)
        return out

    try:
        t0 = time.perf_counter()
        listing = await client._get_json(deeplink_id, "/api/public/mylinks", {}, timeout_total=300)
        links_raw = listing.get("message") or listing.get("result")
        if not isinstance(links_raw, list):
            raise RuntimeError(f"ожидался список: {listing!r}")
        flat = _iter_links(links_raw)
        d0 = date.today() - timedelta(days=3)
        d1 = date.today()
        ts0 = int(datetime.combine(d0, dt_time.min, tzinfo=timezone.utc).timestamp())
        ts1 = int(datetime.combine(d1, dt_time.max, tzinfo=timezone.utc).timestamp())
        for idx, row in enumerate(flat[:5], start=1):
            await client._get_json(
                deeplink_id,
                "/api/public/stats",
                {
                    "link_id": str(row["link_id"]),
                    "page": "1",
                    "dateFrom": str(ts0),
                    "dateTo": str(ts1),
                    "clean": "1",
                },
                timeout_total=120,
            )
        dt = time.perf_counter() - t0
        ok("GET /api/public/stats (выборка до 5 ссылок)", f"ссылок в выборке={min(5, len(flat))}, за {dt:.1f}s")
    except Exception as e:
        fail("stats период (выборка)", e)
        errors += 1

    # 7) stats_for_period — тот же вызов, что в боте (main._answer_stats_period)
    try:
        d0 = date.today() - timedelta(days=7)
        d1 = date.today()
        t0 = time.perf_counter()
        rows = await client.stats_for_period(d0, d1)
        dt = time.perf_counter() - t0
        total_clicks = sum(int(r.get("clicks", 0) or 0) for r in rows)
        ok(
            "HttpMobzClient.stats_for_period (бот: статистика за период)",
            f"ссылок={len(rows)}, сумма кликов={total_clicks}, {dt:.1f}s",
        )
    except Exception as e:
        fail("HttpMobzClient.stats_for_period", e)
        errors += 1

    print("---", flush=True)
    if errors:
        print(f"Итого: {errors} ошибок", flush=True)
        return 1
    print("Итого: все проверки прошли", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
