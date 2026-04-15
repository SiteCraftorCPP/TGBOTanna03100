from __future__ import annotations

import asyncio
import re
from datetime import date, datetime
from typing import Any
from urllib.parse import urlparse

from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import CallbackQuery, InlineKeyboardButton, Message, ReplyKeyboardRemove
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder

from config import AppConfig, DeeplinkConfig, FormatOption, MarketplaceConfig, load_config
from deeplinks_extra import load_extra_deeplinks
from formats_extra import load_extra_format_rows
from mobz_client import CreateLinkRequest, MockMobzClient, MobzClient
from mobz_http import HttpMobzClient
from storage import JsonStorage


class CreateLinkStates(StatesGroup):
    entering_target_url = State()
    entering_blogger = State()
    entering_date = State()


class TokenStates(StatesGroup):
    entering_token = State()


class StatsStates(StatesGroup):
    entering_period = State()


router = Router()
CONFIG: AppConfig
STORE: JsonStorage
MOBZ: MobzClient


def can_use_bot(user_id: int | None) -> bool:
    """Бот доступен любому пользователю Telegram (с известным id)."""
    return user_id is not None


async def deny_access(target: Message | CallbackQuery) -> None:
    text = "Откройте бот из личного чата."
    if isinstance(target, Message):
        await target.answer(text, reply_markup=ReplyKeyboardRemove())
    else:
        await target.answer(text, show_alert=True)


def main_menu() -> Any:
    builder = ReplyKeyboardBuilder()
    builder.button(text="Создать ссылку")
    builder.button(text="Мои ссылки")
    builder.button(text="Статистика")
    builder.button(text="Справка")
    builder.adjust(2)
    return builder.as_markup(resize_keyboard=True)


def merged_deeplinks() -> list[DeeplinkConfig]:
    base = list(CONFIG.deeplinks)
    base_ids = {d.id for d in base}
    seen: set[str] = set()
    extras: list[DeeplinkConfig] = []
    for dl in load_extra_deeplinks(CONFIG.project_dir):
        if dl.id in base_ids or dl.id in seen:
            continue
        seen.add(dl.id)
        extras.append(dl)
    return base + extras


def refresh_mobz_client() -> None:
    global MOBZ
    if CONFIG.mobz_provider == "mock":
        MOBZ = MockMobzClient()
    elif CONFIG.mobz_provider == "http":
        MOBZ = HttpMobzClient(merged_deeplinks(), CONFIG.mobz_api)
    else:
        raise RuntimeError(
            f"Неизвестный MOBZ_PROVIDER={CONFIG.mobz_provider!r}. Используйте mock или http."
        )


def deeplink_by_id(deeplink_id: str) -> DeeplinkConfig:
    for item in merged_deeplinks():
        if item.id == deeplink_id:
            return item
    raise KeyError(f"Не найден диплинк: {deeplink_id}")


def marketplace_by_id(deeplink: DeeplinkConfig, marketplace_id: str) -> MarketplaceConfig:
    for item in deeplink.marketplaces:
        if item.id == marketplace_id:
            return item
    raise KeyError(f"Не найдена площадка: {marketplace_id}")


def merged_formats() -> list[FormatOption]:
    base = list(CONFIG.formats)
    base_ids = {f.id for f in base}
    extras = [
        FormatOption(id=r["id"], label=r["label"], slug=r["slug"])
        for r in load_extra_format_rows(CONFIG.project_dir)
        if r["id"] not in base_ids
    ]
    return base + extras


def format_by_id(format_id: str) -> FormatOption:
    for item in merged_formats():
        if item.id == format_id:
            return item
    raise KeyError(f"Не найден формат: {format_id}")


def normalize_blogger(value: str) -> str:
    return "".join(char for char in value.lower() if char.isalnum())


def is_valid_url(value: str) -> bool:
    parsed = urlparse(value.strip())
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def parse_day_month(value: str) -> str | None:
    value = value.strip()
    if not re.fullmatch(r"\d{2}\.\d{2}", value):
        return None

    try:
        datetime.strptime(value, "%d.%m")
    except ValueError:
        return None

    return value


def _parse_date_dmY(part: str) -> date | None:
    """ДД.ММ.ГГГГ с допуском ведущих нулей (например 013.04.2026 → 13.04.2026)."""
    chunks = [c.strip() for c in part.strip().split(".") if c.strip()]
    if len(chunks) != 3:
        return None
    try:
        d, m, y = int(chunks[0]), int(chunks[1]), int(chunks[2])
    except ValueError:
        return None
    if y < 2000 or y > 2100:
        return None
    try:
        return date(y, m, d)
    except ValueError:
        return None


def parse_period(value: str) -> tuple[date, date] | None:
    """Две даты Д.М.ГГГГ; между ними — дефис, длинное тире, пробелы и т.п."""
    cleaned = (value or "").strip().replace(" ", "")
    m = re.search(
        r"(\d{1,3}\.\d{1,2}\.\d{4})\s*[^\d.]+\s*(\d{1,3}\.\d{1,2}\.\d{4})",
        cleaned,
    )
    if m:
        a, b = m.group(1), m.group(2)
    else:
        for ch in (
            "\u2010",
            "\u2011",
            "\u2012",
            "\u2013",
            "\u2014",
            "\u2015",
            "\u2212",
            "\ufe58",
            "\ufe63",
            "\uff0d",
        ):
            cleaned = cleaned.replace(ch, "-")
        parts = cleaned.split("-", maxsplit=1)
        if len(parts) != 2:
            return None
        a, b = parts[0], parts[1]

    start_date = _parse_date_dmY(a)
    end_date = _parse_date_dmY(b)
    if start_date is None or end_date is None:
        return None
    if start_date > end_date:
        return None
    return start_date, end_date


def build_short_code(blogger_slug: str, date_value: str, format_slug: str, marketplace_suffix: str) -> str:
    date_slug = date_value.replace(".", "")
    return f"{blogger_slug}{date_slug}{format_slug}{marketplace_suffix}"


def short_status(record: dict[str, Any]) -> str:
    return "ЕРИД вшит" if record.get("token_status") == "applied" else "без ЕРИД"


def render_link_card(record: dict[str, Any]) -> str:
    return (
        f"<b>{record['short_code']}</b>\n"
        f"Площадка: {record['marketplace_label']}\n"
        f"Папка: {record['folder_name']}\n"
        f"Формат: {record['format_label']}\n"
        f"Исходная ссылка: {record['source_url']}\n"
        f"Короткая ссылка: {record['short_url']}\n"
        f"Статус: {short_status(record)}"
    )


def links_keyboard(records: list[dict[str, Any]]):
    builder = InlineKeyboardBuilder()
    for record in records:
        label = f"{record['short_code']} | {short_status(record)}"
        builder.row(
            InlineKeyboardButton(
                text=label[:64],
                callback_data=f"link:{record['id']}",
            )
        )
    return builder.as_markup()


def link_actions_keyboard(link_id: str):
    builder = InlineKeyboardBuilder()
    builder.button(text="Вшить ЕРИД", callback_data=f"token:{link_id}")
    builder.button(text="Стата по ссылке", callback_data=f"stats_link:{link_id}")
    builder.button(text="К списку ссылок", callback_data="links:list")
    builder.adjust(1)
    return builder.as_markup()


def deeplink_keyboard():
    builder = InlineKeyboardBuilder()
    for item in merged_deeplinks():
        builder.row(
            InlineKeyboardButton(
                text=item.label,
                callback_data=f"deeplink:{item.id}",
            )
        )
    return builder.as_markup()


def marketplace_keyboard_filtered(deeplink: DeeplinkConfig, *, exclude_ids: set[str] | None = None):
    exclude_ids = exclude_ids or set()
    builder = InlineKeyboardBuilder()
    for item in deeplink.marketplaces:
        if item.id in exclude_ids:
            continue
        builder.row(
            InlineKeyboardButton(
                text=item.label,
                callback_data=f"marketplace:{item.id}",
            )
        )
    return builder.as_markup()


def after_create_keyboard(link_id: str):
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="➕ Ещё ссылка", callback_data=f"create:more:{link_id}"))
    builder.row(InlineKeyboardButton(text="✅ Завершить", callback_data="create:finish"))
    return builder.as_markup()


def marketplace_keyboard(deeplink: DeeplinkConfig):
    builder = InlineKeyboardBuilder()
    for item in deeplink.marketplaces:
        builder.row(
            InlineKeyboardButton(
                text=item.label,
                callback_data=f"marketplace:{item.id}",
            )
        )
    return builder.as_markup()


def folder_keyboard(marketplace: MarketplaceConfig):
    builder = InlineKeyboardBuilder()
    for folder in marketplace.folders:
        builder.row(
            InlineKeyboardButton(
                text=folder,
                callback_data=f"folder:{folder}",
            )
        )
    return builder.as_markup()


def format_keyboard():
    builder = InlineKeyboardBuilder()
    for item in merged_formats():
        builder.row(
            InlineKeyboardButton(
                text=item.label,
                callback_data=f"format:{item.id}",
            )
        )
    return builder.as_markup()


def stats_keyboard():
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="За период", callback_data="stats:period"))
    builder.row(InlineKeyboardButton(text="По конкретной ссылке", callback_data="stats:hint"))
    return builder.as_markup()


async def prompt_marketplaces(target: Message | CallbackQuery, deeplink_id: str, state: FSMContext) -> None:
    await state.update_data(deeplink_id=deeplink_id)
    deeplink = deeplink_by_id(deeplink_id)
    text = f"Выберите площадку для диплинка <b>{deeplink.label}</b>."
    if isinstance(target, Message):
        await target.answer(text, reply_markup=marketplace_keyboard(deeplink))
    else:
        await target.message.answer(text, reply_markup=marketplace_keyboard(deeplink))


async def prompt_marketplaces_for_more(
    target: Message | CallbackQuery,
    deeplink_id: str,
    state: FSMContext,
    *,
    exclude_marketplace_ids: set[str],
) -> None:
    await state.update_data(deeplink_id=deeplink_id)
    deeplink = deeplink_by_id(deeplink_id)
    text = f"Ещё ссылка для <b>{deeplink.label}</b>. Выберите площадку."
    markup = marketplace_keyboard_filtered(deeplink, exclude_ids=exclude_marketplace_ids)
    if isinstance(target, Message):
        await target.answer(text, reply_markup=markup)
    else:
        await target.message.answer(text, reply_markup=markup)


async def _create_link_with_format(
    target: Message | CallbackQuery,
    state: FSMContext,
    *,
    format_option: FormatOption,
) -> None:
    data = await state.get_data()
    required = (
        "deeplink_id",
        "marketplace_id",
        "folder_name",
        "source_url",
        "blogger_raw",
        "blogger_slug",
        "date_value",
    )
    if not all(data.get(k) for k in required):
        if isinstance(target, CallbackQuery):
            await target.answer("Сессия устарела. Начните с «Создать ссылку».", show_alert=True)
        else:
            await target.answer("Сессия устарела. Нажмите «Создать ссылку».", reply_markup=main_menu())
        return

    try:
        deeplink = deeplink_by_id(str(data["deeplink_id"]))
        marketplace = marketplace_by_id(deeplink, str(data["marketplace_id"]))
    except KeyError:
        if isinstance(target, CallbackQuery) and target.message:
            await target.message.answer("Диплинк или площадка не найдены. Начните создание ссылки заново.")
        elif isinstance(target, Message):
            await target.answer("Диплинк или площадка не найдены. Начните создание ссылки заново.")
        await state.clear()
        return

    short_code = build_short_code(
        blogger_slug=str(data["blogger_slug"]),
        date_value=str(data["date_value"]),
        format_slug=format_option.slug,
        marketplace_suffix=marketplace.suffix,
    )

    request = CreateLinkRequest(
        deeplink_id=deeplink.id,
        deeplink_label=deeplink.label,
        marketplace_id=marketplace.id,
        marketplace_label=marketplace.label,
        folder_name=str(data["folder_name"]),
        source_url=str(data["source_url"]),
        short_code=short_code,
        domain=deeplink.default_domain,
        link_note=(
            f"{data['blogger_slug']} {data['date_value']} {format_option.slug} {marketplace.suffix}"
        ).strip(),
    )

    try:
        result = await MOBZ.create_short_link(request)
    except RuntimeError as exc:
        text = f"Не удалось создать ссылку в Mobz: {exc}"
        if isinstance(target, CallbackQuery) and target.message:
            await target.message.answer(text)
        elif isinstance(target, Message):
            await target.answer(text)
        return

    record = STORE.create_link(
        {
            "deeplink_id": deeplink.id,
            "deeplink_label": deeplink.label,
            "marketplace_id": marketplace.id,
            "marketplace_label": marketplace.label,
            "marketplace_notification_label": marketplace.notification_label,
            "folder_name": data["folder_name"],
            "source_url": data["source_url"],
            "blogger_raw": data["blogger_raw"],
            "blogger_slug": data["blogger_slug"],
            "date_value": data["date_value"],
            "format_id": format_option.id,
            "format_label": format_option.label,
            "format_slug": format_option.slug,
            "short_code": short_code,
            "short_url": result.short_url,
            "external_id": result.external_id,
            "token_status": "pending",
        }
    )

    # Сохраняем контекст последнего создания (для кнопки "Ещё ссылка")
    await state.clear()
    label = record.get("marketplace_notification_label") or record.get("marketplace_label") or "Ссылка"
    url = record["short_url"]
    msg_text = f"{label}: {url}"
    if isinstance(target, CallbackQuery) and target.message:
        await target.message.answer(msg_text)
        await target.message.answer("Дальше:", reply_markup=after_create_keyboard(record["id"]))
    elif isinstance(target, Message):
        await target.answer(msg_text)
        await target.answer("Дальше:", reply_markup=after_create_keyboard(record["id"]))



async def begin_create_flow(message: Message, state: FSMContext) -> None:
    await state.clear()
    all_dl = merged_deeplinks()
    if len(all_dl) == 1:
        await prompt_marketplaces(message, all_dl[0].id, state)
        return

    await message.answer("Диплинк:", reply_markup=deeplink_keyboard())


async def show_links(message: Message | CallbackQuery) -> None:
    records = STORE.list_links(limit=20)
    if not records:
        text = "Ссылок пока нет. Сначала создайте первую через кнопку «Создать ссылку»."
        if isinstance(message, Message):
            await message.answer(text)
        else:
            await message.message.answer(text)
        return

    text = "Последние созданные ссылки:"
    markup = links_keyboard(records)
    if isinstance(message, Message):
        await message.answer(text, reply_markup=markup)
    else:
        await message.message.answer(text, reply_markup=markup)


async def show_help(message: Message) -> None:
    await message.answer(
        "Создать ссылку → площадка → папка (если есть) → URL → ник → дата ДД.ММ → формат.\n"
        "Потом «Мои ссылки» → вшить ЕРИД.\n"
        "/cancel — сброс."
    )


@router.message(CommandStart())
async def start_handler(message: Message, state: FSMContext) -> None:
    if not can_use_bot(message.from_user.id if message.from_user else None):
        await deny_access(message)
        return

    await state.clear()
    await message.answer("Готово. Меню ниже.", reply_markup=main_menu())


@router.message(Command("cancel"))
async def cancel_handler(message: Message, state: FSMContext) -> None:
    if not can_use_bot(message.from_user.id if message.from_user else None):
        await deny_access(message)
        return

    await state.clear()
    await message.answer("Сброс.", reply_markup=main_menu())


@router.message(F.text == "Справка")
async def help_handler(message: Message) -> None:
    if not can_use_bot(message.from_user.id if message.from_user else None):
        await deny_access(message)
        return

    await show_help(message)


@router.message(F.text == "Создать ссылку")
async def create_link_menu_handler(message: Message, state: FSMContext) -> None:
    if not can_use_bot(message.from_user.id if message.from_user else None):
        await deny_access(message)
        return

    await begin_create_flow(message, state)


@router.message(F.text == "Мои ссылки")
async def my_links_handler(message: Message) -> None:
    if not can_use_bot(message.from_user.id if message.from_user else None):
        await deny_access(message)
        return

    await show_links(message)


@router.message(F.text == "Статистика")
async def stats_handler(message: Message) -> None:
    if not can_use_bot(message.from_user.id if message.from_user else None):
        await deny_access(message)
        return

    await message.answer("Статистика:", reply_markup=stats_keyboard())


@router.callback_query(F.data.startswith("deeplink:"))
async def deeplink_selected(callback: CallbackQuery, state: FSMContext) -> None:
    if not can_use_bot(callback.from_user.id if callback.from_user else None):
        await deny_access(callback)
        return

    deeplink_id = callback.data.split(":", maxsplit=1)[1]
    await callback.answer()
    await prompt_marketplaces(callback, deeplink_id, state)


@router.callback_query(F.data.startswith("marketplace:"))
async def marketplace_selected(callback: CallbackQuery, state: FSMContext) -> None:
    if not can_use_bot(callback.from_user.id if callback.from_user else None):
        await deny_access(callback)
        return

    marketplace_id = callback.data.split(":", maxsplit=1)[1]
    data = await state.get_data()
    deeplink_id = data["deeplink_id"]
    deeplink = deeplink_by_id(deeplink_id)
    marketplace = marketplace_by_id(deeplink, marketplace_id)
    await state.update_data(marketplace_id=marketplace_id)
    await callback.answer()
    if marketplace.folders:
        await callback.message.answer(
            f"Площадка <b>{marketplace.label}</b>. Теперь выберите папку.",
            reply_markup=folder_keyboard(marketplace),
        )
        return

    await state.update_data(folder_name="Без папки")
    await state.set_state(CreateLinkStates.entering_target_url)
    await callback.message.answer(
        f"Площадка <b>{marketplace.label}</b>. Для нее папки не используются.\n"
        "Отправьте ссылку на товар или бренд."
    )


@router.callback_query(F.data.startswith("folder:"))
async def folder_selected(callback: CallbackQuery, state: FSMContext) -> None:
    if not can_use_bot(callback.from_user.id if callback.from_user else None):
        await deny_access(callback)
        return

    folder_name = callback.data.split(":", maxsplit=1)[1]
    await state.update_data(folder_name=folder_name)
    await state.set_state(CreateLinkStates.entering_target_url)
    await callback.answer()
    await callback.message.answer("Отправьте ссылку на товар или бренд.")


@router.message(CreateLinkStates.entering_target_url)
async def target_url_received(message: Message, state: FSMContext) -> None:
    if not can_use_bot(message.from_user.id if message.from_user else None):
        await deny_access(message)
        return

    raw_url = (message.text or "").strip()
    if not is_valid_url(raw_url):
        await message.answer("Ссылка должна начинаться с http:// или https://")
        return

    await state.update_data(source_url=raw_url)
    data = await state.get_data()
    # Быстрый режим "Ещё ссылка": блогер/дата/формат уже известны, остаётся только URL.
    if data.get("quick_more") and data.get("format_id"):
        try:
            fmt = format_by_id(str(data["format_id"]))
        except KeyError:
            await state.clear()
            await message.answer("Не найден формат. Начните с «Создать ссылку».", reply_markup=main_menu())
            return
        await _create_link_with_format(message, state, format_option=fmt)
        return

    await state.set_state(CreateLinkStates.entering_blogger)
    await message.answer(
        "Отправьте ник блогера. Бот автоматически удалит пробелы, подчёркивания и пунктуацию."
    )


@router.message(CreateLinkStates.entering_blogger)
async def blogger_received(message: Message, state: FSMContext) -> None:
    if not can_use_bot(message.from_user.id if message.from_user else None):
        await deny_access(message)
        return

    raw_nick = (message.text or "").strip()
    blogger_slug = normalize_blogger(raw_nick)
    if not blogger_slug:
        await message.answer("Не удалось получить ник. Отправьте текст, где есть буквы или цифры.")
        return

    await state.update_data(blogger_raw=raw_nick, blogger_slug=blogger_slug)
    await state.set_state(CreateLinkStates.entering_date)
    await message.answer("Укажите дату выхода в формате ДД.ММ, например 03.09")


@router.message(CreateLinkStates.entering_date)
async def date_received(message: Message, state: FSMContext) -> None:
    if not can_use_bot(message.from_user.id if message.from_user else None):
        await deny_access(message)
        return

    date_value = parse_day_month(message.text or "")
    if not date_value:
        await message.answer("Дата должна быть в формате ДД.ММ, например 03.09")
        return

    await state.update_data(date_value=date_value)
    await message.answer("Выберите формат ссылки.", reply_markup=format_keyboard())


@router.callback_query(F.data.startswith("format:"))
async def format_selected(callback: CallbackQuery, state: FSMContext) -> None:
    if not can_use_bot(callback.from_user.id if callback.from_user else None):
        await deny_access(callback)
        return

    format_id = callback.data.split(":", maxsplit=1)[1]
    try:
        format_option = format_by_id(format_id)
    except KeyError:
        await callback.answer("Неизвестный формат.", show_alert=True)
        return

    await callback.answer()
    await _create_link_with_format(callback, state, format_option=format_option)


@router.callback_query(F.data == "create:finish")
async def create_finish_callback(callback: CallbackQuery, state: FSMContext) -> None:
    if not can_use_bot(callback.from_user.id if callback.from_user else None):
        await deny_access(callback)
        return
    await callback.answer()
    await state.clear()
    if callback.message:
        await callback.message.answer("Готово.", reply_markup=main_menu())


@router.callback_query(F.data.startswith("create:more:"))
async def create_more_callback(callback: CallbackQuery, state: FSMContext) -> None:
    if not can_use_bot(callback.from_user.id if callback.from_user else None):
        await deny_access(callback)
        return

    link_id = callback.data.split(":", maxsplit=2)[2]
    record = STORE.get_link(link_id)
    await callback.answer()
    if not record:
        if callback.message:
            await callback.message.answer("Ссылка не найдена. Начните с «Создать ссылку».", reply_markup=main_menu())
        return

    # Контекст (блогер/дата/формат) берём из первой ссылки, чтобы не спрашивать снова.
    await state.clear()
    await state.update_data(
        blogger_raw=record.get("blogger_raw"),
        blogger_slug=record.get("blogger_slug"),
        date_value=record.get("date_value"),
        format_id=record.get("format_id"),
        quick_more=True,
    )
    # Исключаем уже использованную площадку, чтобы удобнее выбрать вторую.
    deeplink_id = str(record.get("deeplink_id") or "main")
    exclude = {str(record.get("marketplace_id") or "")} if record.get("marketplace_id") else set()
    await prompt_marketplaces_for_more(callback, deeplink_id, state, exclude_marketplace_ids=exclude)


@router.callback_query(F.data == "links:list")
async def links_list_callback(callback: CallbackQuery) -> None:
    if not can_use_bot(callback.from_user.id if callback.from_user else None):
        await deny_access(callback)
        return

    await callback.answer()
    await show_links(callback)


@router.callback_query(F.data.startswith("link:"))
async def link_card_callback(callback: CallbackQuery) -> None:
    if not can_use_bot(callback.from_user.id if callback.from_user else None):
        await deny_access(callback)
        return

    link_id = callback.data.split(":", maxsplit=1)[1]
    record = STORE.get_link(link_id)
    await callback.answer()
    if not record:
        await callback.message.answer("Ссылка не найдена.")
        return

    await callback.message.answer(
        render_link_card(record),
        reply_markup=link_actions_keyboard(link_id),
    )


@router.callback_query(F.data.startswith("token:"))
async def token_start_callback(callback: CallbackQuery, state: FSMContext) -> None:
    if not can_use_bot(callback.from_user.id if callback.from_user else None):
        await deny_access(callback)
        return

    link_id = callback.data.split(":", maxsplit=1)[1]
    record = STORE.get_link(link_id)
    await callback.answer()
    if not record:
        await callback.message.answer("Ссылка не найдена.")
        return

    await state.set_state(TokenStates.entering_token)
    await state.update_data(link_id=link_id)
    await callback.message.answer(
        f"Отправьте токен для ссылки:\n{record['short_url']}"
    )


@router.message(TokenStates.entering_token)
async def token_received(message: Message, state: FSMContext) -> None:
    if not can_use_bot(message.from_user.id if message.from_user else None):
        await deny_access(message)
        return

    token = (message.text or "").strip()
    if not token:
        await message.answer("Токен не должен быть пустым.")
        return

    data = await state.get_data()
    record = STORE.get_link(data["link_id"])
    if not record:
        await state.clear()
        await message.answer("Ссылка не найдена. Попробуйте открыть её заново из списка.")
        return

    updates = await MOBZ.attach_marking_token(record, token)
    updated = STORE.update_link(
        record["id"],
        {
            **updates,
            "token_value": token,
        },
    )
    await state.clear()

    if not updated:
        await message.answer("Не удалось сохранить токен.")
        return

    marketplace_label = updated["marketplace_notification_label"]
    await message.answer(
        "❗️Подготовили вам ссылки для публикации, ерид вшит!\n"
        f"Ссылка {marketplace_label}: “{updated['short_url']}”"
    )


async def _answer_stats_period(message: Message, start_date: date, end_date: date) -> None:
    if MOBZ.supports_live_stats:
        await message.answer(
            f"⏳ Считаю клики за {start_date:%d.%m.%Y}\u2014{end_date:%d.%m.%Y} через Mobz: "
            "обычно 1\u20133 минуты. Пожалуйста, подождите."
        )
    try:
        bot_links = [
            r
            for r in STORE.list_all_links()
            if str(r.get("external_id") or "").strip()
        ]
        if bot_links:
            rows = await MOBZ.stats_for_period(
                start_date, end_date, link_records=bot_links
            )
        else:
            rows = await MOBZ.stats_for_period(start_date, end_date)
    except RuntimeError as exc:
        await message.answer(str(exc))
        return
    except Exception as exc:
        await message.answer(f"Не удалось получить статистику за период: {exc}")
        return

    with_clicks = [item for item in rows if item.get("clicks", 0) > 0]
    if not with_clicks:
        await message.answer(
            "За этот период по данным Mobz нет кликов ни по одной из ваших ссылок"
        )
        return

    with_clicks.sort(key=lambda item: item.get("clicks", 0), reverse=True)
    max_lines = 40
    chunk = with_clicks[:max_lines]
    lines = [f"{item['short_url']} — {item['clicks']}" for item in chunk]
    extra = len(with_clicks) - len(chunk)
    if extra > 0:
        lines.append(f"… ещё {extra}")
    await message.answer("\n".join(lines))


@router.callback_query(F.data == "stats:period")
async def stats_period_callback(callback: CallbackQuery, state: FSMContext) -> None:
    if not can_use_bot(callback.from_user.id if callback.from_user else None):
        await deny_access(callback)
        return

    await callback.answer()
    await state.set_state(StatsStates.entering_period)
    if callback.message:
        await callback.message.answer("Введите период: ДД.ММ.ГГГГ-ДД.ММ.ГГГГ")


@router.message(StatsStates.entering_period)
async def stats_period_received(message: Message, state: FSMContext) -> None:
    if not can_use_bot(message.from_user.id if message.from_user else None):
        await deny_access(message)
        return

    period = parse_period(message.text or "")
    if not period:
        await message.answer("Период: ДД.ММ.ГГГГ-ДД.ММ.ГГГГ")
        return

    start_date, end_date = period
    await state.clear()
    await _answer_stats_period(message, start_date, end_date)


@router.callback_query(F.data == "stats:hint")
async def stats_hint_callback(callback: CallbackQuery) -> None:
    if not can_use_bot(callback.from_user.id if callback.from_user else None):
        await deny_access(callback)
        return

    await callback.answer()
    await callback.message.answer("Мои ссылки → карточка → «Стата по ссылке».")


@router.callback_query(F.data.startswith("stats_link:"))
async def stats_link_callback(callback: CallbackQuery) -> None:
    if not can_use_bot(callback.from_user.id if callback.from_user else None):
        await deny_access(callback)
        return

    link_id = callback.data.split(":", maxsplit=1)[1]
    record = STORE.get_link(link_id)
    await callback.answer()
    if not record:
        await callback.message.answer("Ссылка не найдена.")
        return

    try:
        stats = await MOBZ.stats_for_link(record)
    except RuntimeError as exc:
        await callback.message.answer(str(exc))
        return

    await callback.message.answer(
        f"{record['short_url']} - {stats['clicks']} кликов"
    )


@router.message()
async def fallback_handler(message: Message) -> None:
    uid = message.from_user.id if message.from_user else None
    if not can_use_bot(uid):
        await deny_access(message)
        return

    text = (message.text or "").strip()
    period = parse_period(text)
    if period:
        start_date, end_date = period
        await _answer_stats_period(message, start_date, end_date)
        return

    await message.answer("Меню или /start.", reply_markup=main_menu())


async def build_bot() -> Bot:
    session = AiohttpSession(proxy=CONFIG.proxy_url) if CONFIG.proxy_url else AiohttpSession()
    return Bot(
        token=CONFIG.token,
        session=session,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )


async def main() -> None:
    bot = await build_bot()
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)
    await dp.start_polling(bot)


if __name__ == "__main__":
    CONFIG = load_config()
    STORE = JsonStorage(CONFIG.project_dir / "data" / "storage.json")
    if CONFIG.mobz_provider == "mock":
        MOBZ: MobzClient = MockMobzClient()
    elif CONFIG.mobz_provider == "http":
        MOBZ = HttpMobzClient(merged_deeplinks(), CONFIG.mobz_api)
    else:
        raise RuntimeError(
            f"Неизвестный MOBZ_PROVIDER={CONFIG.mobz_provider!r}. Используйте mock или http."
        )
    asyncio.run(main())
