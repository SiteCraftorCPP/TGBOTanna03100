from __future__ import annotations

import asyncio
import json
import re
from datetime import date, datetime
from typing import Any
from urllib.parse import urlparse

from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart, or_f
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import CallbackQuery, InlineKeyboardButton, Message, ReplyKeyboardRemove
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder

from config import AppConfig, DeeplinkConfig, FormatOption, MarketplaceConfig, load_config
from deeplinks_extra import add_extra_deeplink, load_extra_deeplinks, remove_extra_deeplink
from formats_extra import (
    add_extra_format,
    load_extra_format_rows,
    parse_format_line,
    remove_extra_format,
)
from mobz_client import CreateLinkRequest, MockMobzClient, MobzClient
from mobz_http import HttpMobzClient
from admins_extra import add_extra_admin, load_extra_admin_ids, remove_extra_admin
from storage import JsonStorage


class CreateLinkStates(StatesGroup):
    entering_target_url = State()
    entering_blogger = State()
    entering_date = State()


class TokenStates(StatesGroup):
    entering_token = State()


class StatsStates(StatesGroup):
    entering_period = State()


class FormatManageStates(StatesGroup):
    entering_line = State()


class AdminAddAdminStates(StatesGroup):
    waiting_telegram_id = State()


class AdminDeeplinkStates(StatesGroup):
    waiting_json = State()


router = Router()
CONFIG: AppConfig
STORE: JsonStorage
MOBZ: MobzClient


def is_admin(user_id: int | None) -> bool:
    """Администратор: TELEGRAM_ADMIN_IDS в .env и/или data/extra_admins.json (равные права)."""
    if user_id is None:
        return False
    if user_id in CONFIG.admin_ids:
        return True
    return user_id in load_extra_admin_ids(CONFIG.project_dir)


def can_use_bot(user_id: int | None) -> bool:
    """Бот доступен любому пользователю Telegram (с известным id)."""
    return user_id is not None


async def deny_access(target: Message | CallbackQuery) -> None:
    text = "Не удалось определить пользователя Telegram. Откройте бот из личного чата."
    if isinstance(target, Message):
        await target.answer(text, reply_markup=ReplyKeyboardRemove())
    else:
        await target.answer(text, show_alert=True)


async def deny_admin_only(target: Message | CallbackQuery) -> None:
    text = "Эта настройка доступна только администраторам (раздел «Админ»)."
    uid = target.from_user.id if target.from_user else None
    if isinstance(target, Message):
        await target.answer(text, reply_markup=main_menu(uid))
    else:
        await target.answer(text, show_alert=True)


def main_menu(for_user_id: int | None = None) -> Any:
    builder = ReplyKeyboardBuilder()
    builder.button(text="Создать ссылку")
    builder.button(text="Мои ссылки")
    builder.button(text="Статистика")
    builder.button(text="Справка")
    if for_user_id is not None and is_admin(for_user_id):
        builder.button(text="Админ")
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
    cleaned = value.strip().replace(" ", "")
    cleaned = cleaned.replace("–", "-").replace("—", "-").replace("−", "-")
    parts = cleaned.split("-")
    if len(parts) != 2:
        return None

    start_date = _parse_date_dmY(parts[0])
    end_date = _parse_date_dmY(parts[1])
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


def admin_panel_keyboard():
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="Форматы публикаций", callback_data="admin:formats"))
    builder.row(InlineKeyboardButton(text="Администраторы", callback_data="admin:admins"))
    builder.row(InlineKeyboardButton(text="Диплинки (доп.)", callback_data="admin:deeplinks"))
    return builder.as_markup()


def admins_manage_keyboard():
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="➕ Добавить администратора", callback_data="admins:add"))
    for uid in sorted(load_extra_admin_ids(CONFIG.project_dir)):
        builder.row(
            InlineKeyboardButton(
                text=f"🗑 {uid}",
                callback_data=f"admins:del:{uid}",
            )
        )
    return builder.as_markup()


def deeplinks_manage_keyboard():
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(
            text="➕ Добавить диплинк (JSON)",
            callback_data="deeplinks:add",
        )
    )
    for dl in load_extra_deeplinks(CONFIG.project_dir):
        label = (dl.label or dl.id)[:48]
        builder.row(
            InlineKeyboardButton(
                text=f"🗑 {label}",
                callback_data=f"deeplinks:del:{dl.id}",
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
    for item in merged_formats():
        builder.row(
            InlineKeyboardButton(
                text=item.label,
                callback_data=f"format:{item.id}",
            )
        )
    return builder.as_markup()


def formats_manage_keyboard():
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="➕ Добавить формат", callback_data="formats:add"))
    for row in load_extra_format_rows(CONFIG.project_dir):
        label = row["label"][:52]
        builder.row(
            InlineKeyboardButton(
                text=f"🗑 {label}",
                callback_data=f"formats:del:{row['id']}",
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
    all_dl = merged_deeplinks()
    if len(all_dl) == 1:
        await prompt_marketplaces(message, all_dl[0].id, state)
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
        "6. Выберите формат публикации из списка.\n"
        "7. После создания откройте ссылку в разделе «Мои ссылки» и вшейте ЕРИД.\n\n"
        "Команда /cancel сбрасывает текущий сценарий."
    )


@router.message(CommandStart())
async def start_handler(message: Message, state: FSMContext) -> None:
    if not can_use_bot(message.from_user.id if message.from_user else None):
        await deny_access(message)
        return

    await state.clear()
    uid = message.from_user.id if message.from_user else None
    await message.answer(
        "Бот готов к работе. Через меню можно создать короткую ссылку, вшить ЕРИД и открыть список ранее созданных ссылок.",
        reply_markup=main_menu(uid),
    )


@router.message(Command("cancel"))
async def cancel_handler(message: Message, state: FSMContext) -> None:
    if not can_use_bot(message.from_user.id if message.from_user else None):
        await deny_access(message)
        return

    await state.clear()
    uid = message.from_user.id if message.from_user else None
    await message.answer("Текущий сценарий сброшен.", reply_markup=main_menu(uid))


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

    await message.answer("Выберите вариант выгрузки статистики.", reply_markup=stats_keyboard())


def _formats_manage_text() -> str:
    base = ", ".join(f.id for f in CONFIG.formats)
    return (
        "<b>Форматы публикаций</b>\n\n"
        "Базовый список задаётся в <code>settings.json</code> "
        f"(сейчас: {base}).\n\n"
        "Дополнительные форматы сохраняются в "
        "<code>data/formats_extra.json</code> и сразу появляются в шаге "
        "«Выберите формат» при создании ссылки (для всех, кто может пользоваться ботом).\n\n"
        "<b>Добавить:</b> нажмите кнопку ниже и отправьте одну строку "
        "(три поля через символ |):\n"
        "<code>код|подпись в кнопке|суффикс в шорткоде</code>\n"
        "Пример: <code>reels|Reels|reels</code>\n\n"
        "Код и суффикс — латиница, цифры, подчёркивание; суффикс может быть пустым "
        "(как у «без формата» в базовом списке). Код не должен совпадать с уже "
        "существующим в settings.json.\n\n"
        "Удалить свой формат можно кнопкой 🗑 под списком."
    )


def _admins_manage_text() -> str:
    env_admins = ", ".join(str(i) for i in sorted(CONFIG.admin_ids)) or "—"
    extra = sorted(load_extra_admin_ids(CONFIG.project_dir))
    extra_txt = ", ".join(str(i) for i in extra) if extra else "—"
    return (
        "<b>Администраторы</b>\n\n"
        "Бот <b>доступен всем</b> пользователям Telegram. Администраторы — это те, "
        "у кого есть раздел «Админ» и настройки (форматы, диплинки, список админов). "
        "Права у всех админов <b>равные</b>.\n\n"
        f"Из <code>.env</code> (TELEGRAM_ADMIN_IDS): <code>{env_admins}</code>\n"
        f"Добавлены в боте (<code>data/extra_admins.json</code>): <code>{extra_txt}</code>\n\n"
        "Удалить кнопкой 🗑 можно только администраторов из файла бота; из .env — только вручную в .env.\n"
        "ID можно узнать через @userinfobot."
    )


def _deeplinks_manage_text() -> str:
    base = ", ".join(d.id for d in CONFIG.deeplinks)
    return (
        "<b>Дополнительные диплинки</b>\n\n"
        f"Из <code>settings.json</code> сейчас: <code>{base}</code>.\n\n"
        "Сюда можно добавить ещё диплинки (отдельный API-ключ в .env по полю "
        "<code>api_key_env</code>). Данные пишутся в "
        "<code>data/deeplinks_extra.json</code>.\n\n"
        "<b>Добавить:</b> один JSON-объект — как один элемент массива "
        "<code>deeplinks</code> в settings.json "
        "(поля id, label, api_key_env, default_domain, marketplaces).\n\n"
        "<b>Удалить</b> можно только дополнительные диплинки, не из settings.json.\n\n"
        "После изменений убедитесь, что в .env заданы переменные для ключей."
    )


@router.message(or_f(F.text == "Админ", Command("admin")))
async def admin_panel_handler(message: Message) -> None:
    uid = message.from_user.id if message.from_user else None
    if not can_use_bot(uid):
        await deny_access(message)
        return
    if not is_admin(uid):
        await deny_admin_only(message)
        return

    await message.answer(
        "<b>Админ-панель</b>\n\n"
        "Форматы публикаций, администраторы и дополнительные диплинки.",
        reply_markup=admin_panel_keyboard(),
    )


@router.callback_query(F.data == "admin:formats")
async def admin_formats_callback(callback: CallbackQuery) -> None:
    if not can_use_bot(callback.from_user.id if callback.from_user else None):
        await deny_access(callback)
        return
    if not is_admin(callback.from_user.id if callback.from_user else None):
        await deny_admin_only(callback)
        return

    await callback.answer()
    await callback.message.answer(
        _formats_manage_text(),
        reply_markup=formats_manage_keyboard(),
    )


@router.message(Command("formats"))
async def formats_command_handler(message: Message) -> None:
    uid = message.from_user.id if message.from_user else None
    if not can_use_bot(uid):
        await deny_access(message)
        return
    if not is_admin(uid):
        await deny_admin_only(message)
        return

    await message.answer(
        _formats_manage_text(),
        reply_markup=formats_manage_keyboard(),
    )


@router.callback_query(F.data == "admin:admins")
async def admin_admins_callback(callback: CallbackQuery) -> None:
    if not can_use_bot(callback.from_user.id if callback.from_user else None):
        await deny_access(callback)
        return
    if not is_admin(callback.from_user.id if callback.from_user else None):
        await deny_admin_only(callback)
        return

    await callback.answer()
    await callback.message.answer(
        _admins_manage_text(),
        reply_markup=admins_manage_keyboard(),
    )


@router.callback_query(F.data == "admin:deeplinks")
async def admin_deeplinks_callback(callback: CallbackQuery) -> None:
    if not can_use_bot(callback.from_user.id if callback.from_user else None):
        await deny_access(callback)
        return
    if not is_admin(callback.from_user.id if callback.from_user else None):
        await deny_admin_only(callback)
        return

    await callback.answer()
    await callback.message.answer(
        _deeplinks_manage_text(),
        reply_markup=deeplinks_manage_keyboard(),
    )


@router.callback_query(F.data == "admins:add")
async def admins_add_start(callback: CallbackQuery, state: FSMContext) -> None:
    if not can_use_bot(callback.from_user.id if callback.from_user else None):
        await deny_access(callback)
        return
    if not is_admin(callback.from_user.id if callback.from_user else None):
        await deny_admin_only(callback)
        return

    await callback.answer()
    await state.set_state(AdminAddAdminStates.waiting_telegram_id)
    await callback.message.answer(
        "Отправьте числовой Telegram ID нового администратора (только цифры, 5–15 символов).\n"
        "Права будут такие же, как у вас.\n"
        "/cancel — отмена."
    )


@router.callback_query(F.data.startswith("admins:del:"))
async def admins_delete_callback(callback: CallbackQuery) -> None:
    if not can_use_bot(callback.from_user.id if callback.from_user else None):
        await deny_access(callback)
        return
    if not is_admin(callback.from_user.id if callback.from_user else None):
        await deny_admin_only(callback)
        return

    raw_id = callback.data.split(":", maxsplit=2)[2]
    await callback.answer()
    try:
        uid_del = int(raw_id)
    except ValueError:
        await callback.message.answer("Некорректный ID.")
        return

    err = remove_extra_admin(
        CONFIG.project_dir,
        uid_del,
        env_admin_ids=CONFIG.admin_ids,
    )
    if err:
        await callback.message.answer(err)
    else:
        await callback.message.answer(f"Администратор <code>{uid_del}</code> удалён из списка бота.")

    await callback.message.answer(
        _admins_manage_text(),
        reply_markup=admins_manage_keyboard(),
    )


@router.message(AdminAddAdminStates.waiting_telegram_id)
async def admins_add_id_received(message: Message, state: FSMContext) -> None:
    uid = message.from_user.id if message.from_user else None
    if not can_use_bot(uid):
        await deny_access(message)
        return
    if not is_admin(uid):
        await deny_admin_only(message)
        return

    text = (message.text or "").strip()
    if not re.fullmatch(r"\d{5,15}", text):
        await message.answer("Нужен только числовой ID (5–15 цифр). Повторите или /cancel.")
        return

    new_id = int(text)
    err = add_extra_admin(
        CONFIG.project_dir,
        new_id,
        env_admin_ids=CONFIG.admin_ids,
    )
    if err:
        await message.answer(f"{err}\nПовторите или /cancel.")
        return

    await state.clear()
    await message.answer(
        f"Администратор <code>{new_id}</code> добавлен.",
        reply_markup=main_menu(uid),
    )
    await message.answer(
        _admins_manage_text(),
        reply_markup=admins_manage_keyboard(),
    )


@router.callback_query(F.data == "deeplinks:add")
async def deeplinks_add_start(callback: CallbackQuery, state: FSMContext) -> None:
    if not can_use_bot(callback.from_user.id if callback.from_user else None):
        await deny_access(callback)
        return
    if not is_admin(callback.from_user.id if callback.from_user else None):
        await deny_admin_only(callback)
        return

    await callback.answer()
    await state.set_state(AdminDeeplinkStates.waiting_json)
    await callback.message.answer(
        "Пришлите <b>один</b> JSON-объект диплинка (как элемент массива "
        "<code>deeplinks</code> в settings.json). Можно одним сообщением.\n"
        "/cancel — отмена."
    )


@router.callback_query(F.data.startswith("deeplinks:del:"))
async def deeplinks_delete_callback(callback: CallbackQuery) -> None:
    if not can_use_bot(callback.from_user.id if callback.from_user else None):
        await deny_access(callback)
        return
    if not is_admin(callback.from_user.id if callback.from_user else None):
        await deny_admin_only(callback)
        return

    dl_id = callback.data.split(":", maxsplit=2)[2]
    await callback.answer()
    if remove_extra_deeplink(CONFIG.project_dir, dl_id):
        refresh_mobz_client()
        await callback.message.answer(f"Диплинк <code>{dl_id}</code> удалён из дополнительных.")
    else:
        await callback.message.answer(
            "Не удалось удалить: такого id нет в дополнительных или это диплинк из settings.json."
        )

    await callback.message.answer(
        _deeplinks_manage_text(),
        reply_markup=deeplinks_manage_keyboard(),
    )


@router.message(AdminDeeplinkStates.waiting_json, F.text)
async def deeplinks_json_received(message: Message, state: FSMContext) -> None:
    uid = message.from_user.id if message.from_user else None
    if not can_use_bot(uid):
        await deny_access(message)
        return
    if not is_admin(uid):
        await deny_admin_only(message)
        return

    raw_text = (message.text or "").strip()
    try:
        obj: Any = json.loads(raw_text)
    except json.JSONDecodeError:
        await message.answer("Не удалось разобрать JSON. Проверьте кавычки и запятые или /cancel.")
        return

    if isinstance(obj, list):
        if len(obj) == 1 and isinstance(obj[0], dict):
            obj = obj[0]
        else:
            await message.answer("Нужен один объект {...} или массив из одного объекта.")
            return

    if not isinstance(obj, dict):
        await message.answer("Нужен JSON-объект с полями диплинка.")
        return

    base_ids = {d.id for d in CONFIG.deeplinks}
    err = add_extra_deeplink(CONFIG.project_dir, obj, base_ids)
    if err:
        await message.answer(f"{err}\nИсправьте и отправьте снова или /cancel.")
        return

    await state.clear()
    refresh_mobz_client()
    await message.answer(
        f"Диплинк <code>{obj.get('id', '')}</code> добавлен. Клиент Mobz обновлён.",
        reply_markup=main_menu(uid),
    )
    await message.answer(
        _deeplinks_manage_text(),
        reply_markup=deeplinks_manage_keyboard(),
    )


@router.callback_query(F.data == "formats:add")
async def formats_add_start(callback: CallbackQuery, state: FSMContext) -> None:
    if not can_use_bot(callback.from_user.id if callback.from_user else None):
        await deny_access(callback)
        return
    if not is_admin(callback.from_user.id if callback.from_user else None):
        await deny_admin_only(callback)
        return

    await callback.answer()
    await state.set_state(FormatManageStates.entering_line)
    await callback.message.answer(
        "Отправьте одну строку в формате:\n"
        "<code>код|подпись|суффикс</code>\n"
        "Например: <code>tgads|TG реклама|tgads</code>\n\n"
        "/cancel — отмена."
    )


@router.callback_query(F.data.startswith("formats:del:"))
async def formats_delete(callback: CallbackQuery) -> None:
    if not can_use_bot(callback.from_user.id if callback.from_user else None):
        await deny_access(callback)
        return
    if not is_admin(callback.from_user.id if callback.from_user else None):
        await deny_admin_only(callback)
        return

    format_id = callback.data.split(":", maxsplit=2)[2]
    await callback.answer()
    if remove_extra_format(CONFIG.project_dir, format_id):
        await callback.message.answer(f"Формат «{format_id}» удалён из дополнительных.")
    else:
        await callback.message.answer("Такого дополнительного формата нет (базовые из settings.json здесь не удаляются).")

    await callback.message.answer(
        _formats_manage_text(),
        reply_markup=formats_manage_keyboard(),
    )


@router.message(FormatManageStates.entering_line)
async def formats_add_line(message: Message, state: FSMContext) -> None:
    uid = message.from_user.id if message.from_user else None
    if not can_use_bot(uid):
        await deny_access(message)
        return
    if not is_admin(uid):
        await deny_admin_only(message)
        return

    line = (message.text or "").strip()
    parsed = parse_format_line(line)
    if not parsed:
        await message.answer(
            "Нужны три части через | : <code>код|подпись|суффикс</code>\n"
            "Повторите ввод или /cancel."
        )
        return

    format_id, label, slug = parsed
    reserved = {f.id for f in CONFIG.formats}
    err = add_extra_format(
        CONFIG.project_dir,
        format_id,
        label,
        slug,
        reserved_ids=reserved,
    )
    if err:
        await message.answer(f"{err}\nПовторите ввод или /cancel.")
        return

    await state.clear()
    await message.answer(
        f"Формат «{label}» (<code>{format_id}</code>) добавлен.\n"
        "Он уже доступен при выборе формата при создании ссылки.",
        reply_markup=main_menu(uid),
    )
    await message.answer(
        _formats_manage_text(),
        reply_markup=formats_manage_keyboard(),
    )


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
        await callback.answer("Сессия устарела. Начните с «Создать ссылку».", show_alert=True)
        return

    await callback.answer()

    try:
        deeplink = deeplink_by_id(str(data["deeplink_id"]))
        marketplace = marketplace_by_id(deeplink, str(data["marketplace_id"]))
    except KeyError:
        if callback.message:
            await callback.message.answer("Диплинк или площадка не найдены. Начните создание ссылки заново.")
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
        if callback.message:
            await callback.message.answer(f"Не удалось создать ссылку в Mobz: {exc}")
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

    await state.clear()
    if callback.message:
        await callback.message.answer(
            f"Короткая ссылка готова - {record['short_url']}\n\n"
            f"Код: <b>{record['short_code']}</b>",
            reply_markup=link_actions_keyboard(record["id"]),
        )


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


@router.callback_query(F.data == "stats:period")
async def stats_period_callback(callback: CallbackQuery, state: FSMContext) -> None:
    if not can_use_bot(callback.from_user.id if callback.from_user else None):
        await deny_access(callback)
        return

    await callback.answer()
    await state.set_state(StatsStates.entering_period)
    await callback.message.answer(
        "Введите период: <code>ДД.ММ.ГГГГ-ДД.ММ.ГГГГ</code> (дефис между датами).\n"
        "Пример: <code>01.10.2026-03.10.2026</code> или <code>13.04.2026-13.04.2026</code>.\n"
        "День и месяц — числами через точку; лишний ноль в дне (013) тоже поймётся."
    )


@router.message(StatsStates.entering_period)
async def stats_period_received(message: Message, state: FSMContext) -> None:
    if not can_use_bot(message.from_user.id if message.from_user else None):
        await deny_access(message)
        return

    period = parse_period(message.text or "")
    if not period:
        await message.answer(
            "Не получилось разобрать период. Нужно: <code>ДД.ММ.ГГГГ-ДД.ММ.ГГГГ</code>, "
            "например <code>13.04.2026-13.04.2026</code> (проверьте дефис и две даты)."
        )
        return

    start_date, end_date = period
    await state.clear()

    try:
        rows = await MOBZ.stats_for_period(start_date, end_date)
    except RuntimeError as exc:
        await message.answer(str(exc))
        return

    with_clicks = [item for item in rows if item.get("clicks", 0) > 0]
    if not with_clicks:
        await message.answer("За выбранный период кликов не найдено.")
        return

    with_clicks.sort(key=lambda item: item.get("clicks", 0), reverse=True)
    max_lines = 40
    chunk = with_clicks[:max_lines]
    lines = [f"{item['short_url']} - {item['clicks']} кликов" for item in chunk]
    extra = len(with_clicks) - len(chunk)
    if extra > 0:
        lines.append(f"… и ещё {extra} ссылок с кликами (лимит вывода {max_lines}).")
    await message.answer("\n".join(lines))


@router.callback_query(F.data == "stats:hint")
async def stats_hint_callback(callback: CallbackQuery) -> None:
    if not can_use_bot(callback.from_user.id if callback.from_user else None):
        await deny_access(callback)
        return

    await callback.answer()
    await callback.message.answer(
        "Для статистики по одной ссылке откройте её через раздел «Мои ссылки» и нажмите «Стата по ссылке»."
    )


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

    await message.answer(
        "Используйте кнопки меню ниже или команду /start.",
        reply_markup=main_menu(uid),
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
        MOBZ = HttpMobzClient(merged_deeplinks(), CONFIG.mobz_api)
    else:
        raise RuntimeError(
            f"Неизвестный MOBZ_PROVIDER={CONFIG.mobz_provider!r}. Используйте mock или http."
        )
    asyncio.run(main())
