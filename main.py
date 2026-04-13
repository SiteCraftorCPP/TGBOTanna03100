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
from aiogram.types import CallbackQuery, InlineKeyboardButton, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder

from config import AppConfig, DeeplinkConfig, FormatOption, MarketplaceConfig, load_config
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


def is_admin(user_id: int | None) -> bool:
    # Bot is open for all users.
    return True


async def deny_access(target: Message | CallbackQuery) -> None:
    text = "Этот бот доступен только администратору."
    if isinstance(target, Message):
        await target.answer(text)
    else:
        await target.answer(text, show_alert=True)


def main_menu():
    builder = ReplyKeyboardBuilder()
    builder.button(text="Создать ссылку")
    builder.button(text="Мои ссылки")
    builder.button(text="Статистика")
    builder.button(text="Справка")
    builder.adjust(2)
    return builder.as_markup(resize_keyboard=True)


def deeplink_by_id(deeplink_id: str) -> DeeplinkConfig:
    for item in CONFIG.deeplinks:
        if item.id == deeplink_id:
            return item
    raise KeyError(f"Не найден диплинк: {deeplink_id}")


def marketplace_by_id(deeplink: DeeplinkConfig, marketplace_id: str) -> MarketplaceConfig:
    for item in deeplink.marketplaces:
        if item.id == marketplace_id:
            return item
    raise KeyError(f"Не найдена площадка: {marketplace_id}")


def format_by_id(format_id: str) -> FormatOption:
    for item in CONFIG.formats:
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


def parse_period(value: str) -> tuple[date, date] | None:
    cleaned = value.strip().replace(" ", "")
    parts = cleaned.split("-")
    if len(parts) != 2:
        return None

    try:
        start_date = datetime.strptime(parts[0], "%d.%m.%Y").date()
        end_date = datetime.strptime(parts[1], "%d.%m.%Y").date()
    except ValueError:
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
    for item in CONFIG.deeplinks:
        builder.row(
            InlineKeyboardButton(
                text=item.label,
                callback_data=f"deeplink:{item.id}",
            )
        )
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
    for item in CONFIG.formats:
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


async def begin_create_flow(message: Message, state: FSMContext) -> None:
    await state.clear()
    if len(CONFIG.deeplinks) == 1:
        await prompt_marketplaces(message, CONFIG.deeplinks[0].id, state)
        return

    await message.answer(
        "Выберите, с каким диплинком работаем.",
        reply_markup=deeplink_keyboard(),
    )


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
        "Сценарий работы:\n"
        "1. Нажмите «Создать ссылку».\n"
        "2. Выберите площадку и папку.\n"
        "3. Отправьте ссылку на товар или бренд.\n"
        "4. Отправьте ник блогера.\n"
        "5. Укажите дату в формате ДД.ММ.\n"
        "6. Выберите формат.\n"
        "7. После создания откройте ссылку в разделе «Мои ссылки» и вшейте ЕРИД.\n\n"
        "Команда /cancel сбрасывает текущий сценарий."
    )


@router.message(CommandStart())
async def start_handler(message: Message, state: FSMContext) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await deny_access(message)
        return

    await state.clear()
    await message.answer(
        "Бот готов к работе. Через меню можно создать короткую ссылку, вшить ЕРИД и открыть список ранее созданных ссылок.",
        reply_markup=main_menu(),
    )


@router.message(Command("cancel"))
async def cancel_handler(message: Message, state: FSMContext) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await deny_access(message)
        return

    await state.clear()
    await message.answer("Текущий сценарий сброшен.", reply_markup=main_menu())


@router.message(F.text == "Справка")
async def help_handler(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await deny_access(message)
        return

    await show_help(message)


@router.message(F.text == "Создать ссылку")
async def create_link_menu_handler(message: Message, state: FSMContext) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await deny_access(message)
        return

    await begin_create_flow(message, state)


@router.message(F.text == "Мои ссылки")
async def my_links_handler(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await deny_access(message)
        return

    await show_links(message)


@router.message(F.text == "Статистика")
async def stats_handler(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await deny_access(message)
        return

    await message.answer("Выберите вариант выгрузки статистики.", reply_markup=stats_keyboard())


@router.callback_query(F.data.startswith("deeplink:"))
async def deeplink_selected(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_admin(callback.from_user.id if callback.from_user else None):
        await deny_access(callback)
        return

    deeplink_id = callback.data.split(":", maxsplit=1)[1]
    await callback.answer()
    await prompt_marketplaces(callback, deeplink_id, state)


@router.callback_query(F.data.startswith("marketplace:"))
async def marketplace_selected(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_admin(callback.from_user.id if callback.from_user else None):
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
    if not is_admin(callback.from_user.id if callback.from_user else None):
        await deny_access(callback)
        return

    folder_name = callback.data.split(":", maxsplit=1)[1]
    await state.update_data(folder_name=folder_name)
    await state.set_state(CreateLinkStates.entering_target_url)
    await callback.answer()
    await callback.message.answer("Отправьте ссылку на товар или бренд.")


@router.message(CreateLinkStates.entering_target_url)
async def target_url_received(message: Message, state: FSMContext) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await deny_access(message)
        return

    raw_url = (message.text or "").strip()
    if not is_valid_url(raw_url):
        await message.answer("Ссылка должна начинаться с http:// или https://")
        return

    await state.update_data(source_url=raw_url)
    await state.set_state(CreateLinkStates.entering_blogger)
    await message.answer(
        "Отправьте ник блогера. Бот автоматически удалит пробелы, подчёркивания и пунктуацию."
    )


@router.message(CreateLinkStates.entering_blogger)
async def blogger_received(message: Message, state: FSMContext) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
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
    if not is_admin(message.from_user.id if message.from_user else None):
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
    if not is_admin(callback.from_user.id if callback.from_user else None):
        await deny_access(callback)
        return

    format_id = callback.data.split(":", maxsplit=1)[1]
    format_option = format_by_id(format_id)
    data = await state.get_data()

    deeplink = deeplink_by_id(data["deeplink_id"])
    marketplace = marketplace_by_id(deeplink, data["marketplace_id"])
    short_code = build_short_code(
        blogger_slug=data["blogger_slug"],
        date_value=data["date_value"],
        format_slug=format_option.slug,
        marketplace_suffix=marketplace.suffix,
    )

    request = CreateLinkRequest(
        deeplink_id=deeplink.id,
        deeplink_label=deeplink.label,
        marketplace_id=marketplace.id,
        marketplace_label=marketplace.label,
        folder_name=data["folder_name"],
        source_url=data["source_url"],
        short_code=short_code,
        domain=deeplink.default_domain,
        link_note=(
            f"{data['blogger_slug']} {data['date_value']} {format_option.slug} {marketplace.suffix}"
        ).strip(),
    )

    result = await MOBZ.create_short_link(request)
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

    await callback.answer()
    await state.clear()
    await callback.message.answer(
        f"Короткая ссылка готова - {record['short_url']}\n\n"
        f"Код: <b>{record['short_code']}</b>",
        reply_markup=link_actions_keyboard(record["id"]),
    )


@router.callback_query(F.data == "links:list")
async def links_list_callback(callback: CallbackQuery) -> None:
    if not is_admin(callback.from_user.id if callback.from_user else None):
        await deny_access(callback)
        return

    await callback.answer()
    await show_links(callback)


@router.callback_query(F.data.startswith("link:"))
async def link_card_callback(callback: CallbackQuery) -> None:
    if not is_admin(callback.from_user.id if callback.from_user else None):
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
    if not is_admin(callback.from_user.id if callback.from_user else None):
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
    if not is_admin(message.from_user.id if message.from_user else None):
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


@router.callback_query(F.data == "stats:period")
async def stats_period_callback(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_admin(callback.from_user.id if callback.from_user else None):
        await deny_access(callback)
        return

    await callback.answer()
    await state.set_state(StatsStates.entering_period)
    await callback.message.answer(
        "Введите период в формате ДД.ММ.ГГГГ-ДД.ММ.ГГГГ, например 01.10.2026-03.10.2026"
    )


@router.message(StatsStates.entering_period)
async def stats_period_received(message: Message, state: FSMContext) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await deny_access(message)
        return

    period = parse_period(message.text or "")
    if not period:
        await message.answer("Неверный формат периода. Пример: 01.10.2026-03.10.2026")
        return

    start_date, end_date = period
    await state.clear()

    try:
        rows = await MOBZ.stats_for_period(start_date, end_date)
    except RuntimeError as exc:
        await message.answer(str(exc))
        return

    if not rows:
        await message.answer("За выбранный период кликов не найдено.")
        return

    lines = [
        f"{item['short_url']} - {item['clicks']} кликов"
        for item in rows
    ]
    await message.answer("\n".join(lines))


@router.callback_query(F.data == "stats:hint")
async def stats_hint_callback(callback: CallbackQuery) -> None:
    if not is_admin(callback.from_user.id if callback.from_user else None):
        await deny_access(callback)
        return

    await callback.answer()
    await callback.message.answer(
        "Для статистики по одной ссылке откройте её через раздел «Мои ссылки» и нажмите «Стата по ссылке»."
    )


@router.callback_query(F.data.startswith("stats_link:"))
async def stats_link_callback(callback: CallbackQuery) -> None:
    if not is_admin(callback.from_user.id if callback.from_user else None):
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
    if not is_admin(message.from_user.id if message.from_user else None):
        await deny_access(message)
        return

    await message.answer(
        "Используйте кнопки меню ниже или команду /start.",
        reply_markup=main_menu(),
    )


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
        MOBZ = HttpMobzClient(CONFIG.deeplinks, CONFIG.mobz_api)
    else:
        raise RuntimeError(
            f"Неизвестный MOBZ_PROVIDER={CONFIG.mobz_provider!r}. Используйте mock или http."
        )
    asyncio.run(main())
