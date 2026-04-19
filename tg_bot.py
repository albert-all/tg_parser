from __future__ import annotations

import asyncio
from contextlib import suppress
import logging
import re
import shlex
import traceback
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

from aiogram import Bot, Dispatcher, F, Router
from aiogram.exceptions import TelegramBadRequest, TelegramNotFound, TelegramUnauthorizedError
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    BufferedInputFile,
    CallbackQuery,
    FSInputFile,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
    ReplyKeyboardRemove,
)

from bot_backend.auth import AuthManager
from bot_backend.config import Settings, load_settings
from bot_backend.db import Database, ThemeDTO, ThemeWatchDTO, UserSettingsDTO
from bot_backend.search import SearchError, SearchParams, SearchService, parse_date_from, parse_date_to

router = Router()
logger = logging.getLogger("tg_bot")

settings: Optional[Settings] = None
db: Optional[Database] = None
auth_manager: Optional[AuthManager] = None
search_service: Optional[SearchService] = None
active_search_tasks: dict[int, asyncio.Task] = {}
active_search_themes_by_user: dict[int, set[str]] = {}
active_theme_by_user: dict[int, str] = {}
watch_scheduler_task: Optional[asyncio.Task] = None
watch_ui_message_by_user: dict[int, tuple[int, int]] = {}
nav_keyboard_mode_by_user: dict[int, bool] = {}

MIN_WATCH_INTERVAL_MINUTES = 1
MAX_WATCH_INTERVAL_MINUTES = 60 * 24 * 7
WATCH_SCHEDULER_SLEEP_SECONDS = 20
WATCH_INTERVAL_OPTIONS = (15, 30, 60, 180, 360, 720, 1440)
SEARCH_LIMIT_OPTIONS = (20, 50, 100, 200, 500)
DEFAULT_WATCH_INTERVAL_MINUTES = 60
ACTIVE_SEARCH_ALL_MARKER = "__all__"


class AuthStates(StatesGroup):
    waiting_2fa = State()


class ThemeUiStates(StatesGroup):
    waiting_create_theme_name = State()
    waiting_bulk_payload = State()
    waiting_new_theme_keywords = State()
    waiting_new_theme_chats = State()


class WatchUiStates(StatesGroup):
    waiting_custom_interval = State()


class SearchUiStates(StatesGroup):
    waiting_custom_limit = State()
    waiting_custom_dates = State()


class SettingsUiStates(StatesGroup):
    waiting_watch_interval = State()


THEME_UI_ACTION_MAP = {
    "sa": "set_active",
    "ac": "add_chat",
    "dc": "del_chat",
    "ak": "add_kw",
    "dk": "del_kw",
    "dt": "delete_theme",
}

SEARCH_FORMAT_LABELS = {
    "both": "Текст + CSV",
    "text": "Только текст",
    "csv": "Только CSV",
}

SEARCH_COMMENTS_MODE_LABELS = {
    "std": "Стандартный",
    "deep": "Глубокий",
}

SILENT_UI_TEXT = "\u2060"


@dataclass
class ParsedSearchCommand:
    theme_name: Optional[str]
    date_from: Optional[datetime]
    date_to: Optional[datetime]
    limit: Optional[int]
    output_format: str
    deep_comments: bool


def _main_menu(*, show_auth_actions: bool = True) -> InlineKeyboardMarkup:
    if show_auth_actions:
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text="👤 Авторизация", callback_data="menu_auth"),
                    InlineKeyboardButton(text="❓ Помощь", callback_data="menu_help"),
                ],
            ]
        )
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="🔔 Подписки", callback_data="menu_watch"),
                InlineKeyboardButton(text="🔍 Поиск", callback_data="menu_search"),
            ],
            [
                InlineKeyboardButton(text="☰ Темы", callback_data="menu_themes"),
                InlineKeyboardButton(text="❓ Помощь", callback_data="menu_help"),
            ],
            [InlineKeyboardButton(text="⚙️ Общие настройки", callback_data="menu_settings")],
        ]
    )


async def _main_menu_for_user(user_id: int) -> InlineKeyboardMarkup:
    _, _, auth, _ = _require_services()
    is_auth = await auth.is_authorized(user_id)
    return _main_menu(show_auth_actions=not is_auth)

async def _ensure_nav_keyboard(
    message: Message,
    user_id: int,
    *,
    show_auth_actions: Optional[bool] = None,
    force: bool = False,
) -> None:
    del show_auth_actions
    if not force and nav_keyboard_mode_by_user.get(user_id):
        return
    nav_keyboard_mode_by_user[user_id] = True
    await message.answer(
        SILENT_UI_TEXT,
        reply_markup=ReplyKeyboardRemove(),
    )


def _auth_actions_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Отсканировал", callback_data="menu_auth_check"),
                InlineKeyboardButton(text="🔄 Обновить QR", callback_data="menu_auth_refresh"),
            ],
            [InlineKeyboardButton(text="❓ Помощь", callback_data="menu_help")],
        ]
    )


def _help_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="menu_home")],
        ]
    )


def _themes_back_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="⬅️ К темам", callback_data="menu_themes"),
                InlineKeyboardButton(text="🏠 Главное меню", callback_data="menu_home"),
            ],
        ]
    )


def _themes_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="➕ Добавить тему", callback_data="themes:new")],
            [InlineKeyboardButton(text="🔎 Чаты поиска", callback_data="themes:search_chats")],
            [
                InlineKeyboardButton(text="⬅️ Назад", callback_data="menu_home"),
                InlineKeyboardButton(text="🏠 Главное меню", callback_data="menu_home"),
            ],
        ]
    )


def _theme_wizard_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Отменить", callback_data="menu_cancel")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="menu_home")],
        ]
    )


def _themes_panel_keyboard(
    themes: list[ThemeDTO],
    active_theme_name: Optional[str],
    status_labels: Optional[dict[int, str]] = None,
) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for theme in themes:
        is_active = bool(active_theme_name and active_theme_name.casefold() == theme.name.casefold())
        if status_labels and theme.id in status_labels:
            label = status_labels[theme.id]
        else:
            label = f"✅ {theme.name}" if is_active else theme.name
        rows.append([InlineKeyboardButton(text=label, callback_data=f"themes:open:{theme.id}")])
    rows.append([InlineKeyboardButton(text="➕ Добавить тему", callback_data="themes:new")])
    rows.append([InlineKeyboardButton(text="🔎 Чаты поиска", callback_data="themes:search_chats")])
    rows.append(
        [
            InlineKeyboardButton(text="⬅️ Назад", callback_data="menu_home"),
            InlineKeyboardButton(text="🏠 Главное меню", callback_data="menu_home"),
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _theme_detail_keyboard(theme_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="🔑 Изменить ключи", callback_data=f"themes:keys:{theme_id}"),
                InlineKeyboardButton(text="💬 Изменить чаты", callback_data=f"themes:chats:{theme_id}"),
            ],
            [
                InlineKeyboardButton(text="🔍 Начать поиск", callback_data=f"searchui:th:{theme_id}"),
                InlineKeyboardButton(text="🔔 Отслеживать", callback_data=f"watch:set:th:{theme_id}"),
            ],
            [InlineKeyboardButton(text="🗑️ Удалить тему", callback_data=f"themes:delete:ask:{theme_id}")],
            [
                InlineKeyboardButton(text="⬅️ Назад", callback_data="menu_themes"),
                InlineKeyboardButton(text="🏠 Главное меню", callback_data="menu_home"),
            ],
        ]
    )


def _theme_keywords_keyboard(theme_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="➕ Добавить ключи", callback_data=f"themes:keys:add:{theme_id}"),
                InlineKeyboardButton(text="✂️ Удалить выборочно", callback_data=f"themes:keys:del:{theme_id}"),
            ],
            [InlineKeyboardButton(text="🗑️ Удалить все ключи", callback_data=f"themes:keys:clear:{theme_id}")],
            [
                InlineKeyboardButton(text="⬅️ Назад", callback_data=f"themes:open:{theme_id}"),
                InlineKeyboardButton(text="🏠 Главное меню", callback_data="menu_home"),
            ],
        ]
    )


def _theme_chats_keyboard(theme_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="➕ Добавить чаты", callback_data=f"themes:chats:add:{theme_id}"),
                InlineKeyboardButton(text="✂️ Удалить выборочно", callback_data=f"themes:chats:del:{theme_id}"),
            ],
            [InlineKeyboardButton(text="⚡ Добавить все чаты аккаунта", callback_data=f"themes:add_all_chats:th:{theme_id}")],
            [InlineKeyboardButton(text="🗑️ Удалить все чаты", callback_data=f"themes:chats:clear:{theme_id}")],
            [
                InlineKeyboardButton(text="⬅️ Назад", callback_data=f"themes:open:{theme_id}"),
                InlineKeyboardButton(text="🏠 Главное меню", callback_data="menu_home"),
            ],
        ]
    )


def _theme_delete_confirm_keyboard(theme_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ Да, удалить тему", callback_data=f"themes:delete:yes:{theme_id}")],
            [
                InlineKeyboardButton(text="⬅️ Назад", callback_data=f"themes:open:{theme_id}"),
                InlineKeyboardButton(text="🏠 Главное меню", callback_data="menu_home"),
            ],
        ]
    )


def _themes_add_section_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✨ Создать тему", callback_data="themes:new")],
            [
                InlineKeyboardButton(text="💬 Добавить чаты", callback_data="themes:quick:ac"),
                InlineKeyboardButton(text="🔑 Добавить ключи", callback_data="themes:quick:ak"),
            ],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="menu_themes")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="menu_home")],
        ]
    )


def _themes_manage_section_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🎯 Выбрать тему", callback_data="themes:pick:sa")],
            [
                InlineKeyboardButton(text="🗑 Удалить ключи", callback_data="themes:pick:dk"),
                InlineKeyboardButton(text="🗑 Удалить чаты", callback_data="themes:pick:dc"),
            ],
            [InlineKeyboardButton(text="❌ Удалить тему", callback_data="themes:pick:dt")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="menu_themes")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="menu_home")],
        ]
    )


def _themes_info_section_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📚 Список тем", callback_data="themes:list")],
            [InlineKeyboardButton(text="🔎 Чаты поиска", callback_data="themes:search_chats")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="menu_themes")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="menu_home")],
        ]
    )


def _theme_add_chat_keyboard(theme_id: int, back_callback: Optional[str] = None) -> InlineKeyboardMarkup:
    back_target = back_callback or f"themes:open:{theme_id}"
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="⚡ Добавить ВСЕ чаты аккаунта", callback_data=f"themes:add_all_chats:th:{theme_id}")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data=back_target)],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="menu_home")],
        ]
    )


def _theme_picker_keyboard(themes: list[ThemeDTO], action_code: str, back_callback: str = "menu_themes") -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for theme in themes:
        rows.append([InlineKeyboardButton(text=theme.name, callback_data=f"themes:do:{action_code}:{theme.id}")])
    rows.append([InlineKeyboardButton(text="Назад", callback_data=back_callback)])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _theme_picker_for_all_chats_keyboard(themes: list[ThemeDTO], back_callback: str = "themes:section:add") -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for theme in themes:
        rows.append([InlineKeyboardButton(text=theme.name, callback_data=f"themes:add_all_chats:th:{theme.id}")])
    rows.append([InlineKeyboardButton(text="Назад", callback_data=back_callback)])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _search_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="🎯 Выбрать тему", callback_data="searchui:pick_theme"),
                InlineKeyboardButton(text="🌐 Все чаты", callback_data="searchui:all"),
            ],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="menu_home")],
        ]
    )


def _search_theme_picker_keyboard(themes: list[ThemeDTO]) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for theme in themes:
        rows.append([InlineKeyboardButton(text=theme.name, callback_data=f"searchui:th:{theme.id}")])
    rows.append(
        [
            InlineKeyboardButton(text="⬅️ Назад", callback_data="menu_search"),
            InlineKeyboardButton(text="🏠 Главное меню", callback_data="menu_home"),
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _search_format_keyboard(scope: str, theme_id: int = 0) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton(text=SEARCH_FORMAT_LABELS["both"], callback_data=f"searchui:run:both:{scope}:{theme_id}"),
            InlineKeyboardButton(text=SEARCH_FORMAT_LABELS["text"], callback_data=f"searchui:run:text:{scope}:{theme_id}"),
        ],
        [
            InlineKeyboardButton(text=SEARCH_FORMAT_LABELS["csv"], callback_data=f"searchui:run:csv:{scope}:{theme_id}"),
        ],
        [InlineKeyboardButton(text="Назад", callback_data="menu_search")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _search_limit_keyboard(output_format: str, scope: str, theme_id: int, default_limit: int) -> InlineKeyboardMarkup:
    options: list[int] = []
    seen: set[int] = set()
    for value in (default_limit, *SEARCH_LIMIT_OPTIONS):
        if value <= 0 or value in seen:
            continue
        seen.add(value)
        options.append(value)

    rows: list[list[InlineKeyboardButton]] = []
    chunk: list[InlineKeyboardButton] = []
    for value in options:
        chunk.append(
            InlineKeyboardButton(
                text=str(value),
                callback_data=f"searchui:limit:{output_format}:{scope}:{theme_id}:{value}",
            )
        )
        if len(chunk) == 3:
            rows.append(chunk)
            chunk = []
    if chunk:
        rows.append(chunk)

    rows.append(
        [
            InlineKeyboardButton(
                text="Без лимита",
                callback_data=f"searchui:limit:{output_format}:{scope}:{theme_id}:nolimit",
            ),
            InlineKeyboardButton(
                text="Свой лимит",
                callback_data=f"searchui:limit:{output_format}:{scope}:{theme_id}:custom",
            ),
        ]
    )
    rows.append([InlineKeyboardButton(text="Назад", callback_data="menu_search")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _search_date_keyboard(output_format: str, scope: str, theme_id: int, limit_token: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="За всё время",
                    callback_data=f"searchui:date:{output_format}:{scope}:{theme_id}:{limit_token}:all",
                ),
                InlineKeyboardButton(
                    text="Сегодня",
                    callback_data=f"searchui:date:{output_format}:{scope}:{theme_id}:{limit_token}:today",
                ),
            ],
            [
                InlineKeyboardButton(
                    text="Последние 7 дней",
                    callback_data=f"searchui:date:{output_format}:{scope}:{theme_id}:{limit_token}:7d",
                ),
                InlineKeyboardButton(
                    text="Последние 30 дней",
                    callback_data=f"searchui:date:{output_format}:{scope}:{theme_id}:{limit_token}:30d",
                ),
            ],
            [
                InlineKeyboardButton(
                    text="Свой диапазон дат",
                    callback_data=f"searchui:date:{output_format}:{scope}:{theme_id}:{limit_token}:custom",
                )
            ],
            [InlineKeyboardButton(text="Назад", callback_data="menu_search")],
        ]
    )


def _search_comments_mode_keyboard(
    output_format: str,
    scope: str,
    theme_id: int,
    limit_token: str,
    date_token: str,
) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="⚖️ Стандартный",
                    callback_data=f"searchui:cm:{output_format}:{scope}:{theme_id}:{limit_token}:{date_token}:std",
                ),
                InlineKeyboardButton(
                    text="🔎 Глубокий",
                    callback_data=f"searchui:cm:{output_format}:{scope}:{theme_id}:{limit_token}:{date_token}:deep",
                ),
            ],
            [InlineKeyboardButton(text="Назад", callback_data="menu_search")],
        ]
    )


def _watch_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Список подписок", callback_data="watch:list"),
                InlineKeyboardButton(text="Включить", callback_data="watch:set"),
            ],
            [
                InlineKeyboardButton(text="Отключить", callback_data="watch:off"),
                InlineKeyboardButton(text="Как включить", callback_data="watch:help"),
            ],
            [InlineKeyboardButton(text="Главное меню", callback_data="menu_home")],
        ]
    )


def _watch_theme_picker_keyboard(themes: list[ThemeDTO], mode: str) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for theme in themes:
        rows.append([InlineKeyboardButton(text=theme.name, callback_data=f"watch:{mode}:th:{theme.id}")])
    rows.append([InlineKeyboardButton(text="Назад", callback_data="menu_watch")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _watch_off_picker_keyboard(watches: list[ThemeWatchDTO]) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for watch in watches:
        rows.append([InlineKeyboardButton(text=f"Отключить: {watch.theme_name}", callback_data=f"watch:off:th:{watch.theme_id}")])
    rows.append([InlineKeyboardButton(text="Назад", callback_data="menu_watch")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _watch_interval_keyboard(theme_id: int) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    chunk: list[InlineKeyboardButton] = []
    for minutes in WATCH_INTERVAL_OPTIONS:
        chunk.append(InlineKeyboardButton(text=f"{minutes} мин", callback_data=f"watch:set:int:{theme_id}:{minutes}"))
        if len(chunk) == 3:
            rows.append(chunk)
            chunk = []
    if chunk:
        rows.append(chunk)
    rows.append([InlineKeyboardButton(text="Свой период", callback_data=f"watch:set:custom:{theme_id}")])
    rows.append([InlineKeyboardButton(text="Назад", callback_data="watch:set")])
    rows.append([InlineKeyboardButton(text="Главное меню", callback_data="menu_home")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _require_services() -> tuple[Settings, Database, AuthManager, SearchService]:
    if settings is None or db is None or auth_manager is None or search_service is None:
        raise RuntimeError("Services are not initialized")
    return settings, db, auth_manager, search_service


async def _ensure_user(message: Message) -> int:
    user = message.from_user
    if user is None:
        raise RuntimeError("Unknown user")
    return await _upsert_user(user.id, user.username, user.first_name)


async def _upsert_user(user_id: int, username: Optional[str], first_name: Optional[str]) -> int:
    _, database, _, _ = _require_services()
    await database.upsert_user(user_id, username, first_name)
    return user_id


async def _clear_settings_state_if_needed(state: FSMContext) -> None:
    current_state = await state.get_state()
    if current_state and current_state.startswith(f"{SettingsUiStates.__name__}:"):
        await state.clear()


def _set_active_search_themes(user_id: int, theme_names: Optional[list[str]] = None, *, all_themes: bool = False) -> None:
    if all_themes:
        active_search_themes_by_user[user_id] = {ACTIVE_SEARCH_ALL_MARKER}
        return
    names = {(name or "").strip().casefold() for name in (theme_names or []) if (name or "").strip()}
    if names:
        active_search_themes_by_user[user_id] = names
    else:
        active_search_themes_by_user.pop(user_id, None)


def _clear_active_search_themes(user_id: int) -> None:
    active_search_themes_by_user.pop(user_id, None)


def _short_text(text: str, max_len: int = 280) -> str:
    text = (text or "").replace("\n", " ").strip()
    if len(text) <= max_len:
        return text
    return text[: max_len - 3] + "..."


def _format_theme(theme: ThemeDTO) -> str:
    chats = "\n".join(f"  - {x}" for x in theme.chats) if theme.chats else "  - (нет)"
    keywords = "\n".join(f"  - {x}" for x in theme.keywords) if theme.keywords else "  - (нет)"
    return f"Тема: {theme.name}\nЧаты:\n{chats}\nКлючевые слова:\n{keywords}"


def _get_command_body(text: str) -> str:
    parts = text.split(maxsplit=1)
    if len(parts) == 1:
        return ""
    return parts[1].strip()


def _parse_theme_and_value(body: str) -> tuple[str, str]:
    parts = body.split(maxsplit=1)
    if len(parts) < 2:
        raise ValueError("Нужно указать два аргумента")
    theme_name = parts[0].strip()
    value = parts[1].strip()
    if not theme_name:
        raise ValueError("Имя темы пустое")
    if not value:
        raise ValueError("Значение пустое")
    return theme_name, value


def _parse_search_command(message_text: str, default_limit: int, default_output_format: str = "both") -> ParsedSearchCommand:
    tokens = shlex.split(message_text)
    theme_name: Optional[str] = None
    if len(tokens) >= 2 and not tokens[1].startswith("--"):
        theme_name = tokens[1].strip()
        idx = 2
    else:
        idx = 1

    date_from: Optional[datetime] = None
    date_to: Optional[datetime] = None
    limit: Optional[int] = default_limit
    output_format = default_output_format
    deep_comments = False
    no_limit_tokens = {
        "0",
        "none",
        "no",
        "nolimit",
        "no-limit",
        "all",
        "unlimited",
        "безлимит",
        "без-лимита",
        "без_лимита",
    }

    while idx < len(tokens):
        token = tokens[idx]
        if token == "--date-from":
            idx += 1
            if idx >= len(tokens):
                raise ValueError("После --date-from требуется значение даты")
            date_from = parse_date_from(tokens[idx])
        elif token == "--date-to":
            idx += 1
            if idx >= len(tokens):
                raise ValueError("После --date-to требуется значение даты")
            date_to = parse_date_to(tokens[idx])
        elif token == "--limit":
            idx += 1
            if idx >= len(tokens):
                raise ValueError("После --limit требуется число, 0 или none")
            raw_limit = tokens[idx].strip()
            if raw_limit.casefold() in no_limit_tokens:
                limit = None
            else:
                try:
                    limit = int(raw_limit)
                except ValueError as exc:
                    raise ValueError("--limit должен быть числом, 0 или none") from exc
                if limit <= 0:
                    raise ValueError("--limit должен быть > 0, либо 0/none для без лимита")
        elif token == "--no-limit":
            limit = None
        elif token == "--format":
            idx += 1
            if idx >= len(tokens):
                raise ValueError("После --format требуется значение")
            output_format = tokens[idx].lower()
            if output_format not in {"both", "text", "csv"}:
                raise ValueError("--format должен быть одним из: both, text, csv")
        elif token == "--deep-comments":
            deep_comments = True
        else:
            raise ValueError(f"Неизвестный параметр: {token}")
        idx += 1

    if date_from and date_to and date_from > date_to:
        raise ValueError("date-from не может быть позже date-to")

    return ParsedSearchCommand(
        theme_name=theme_name,
        date_from=date_from,
        date_to=date_to,
        limit=limit,
        output_format=output_format,
        deep_comments=deep_comments,
    )


async def _get_user_settings(user_id: int) -> UserSettingsDTO:
    _, database, _, _ = _require_services()
    return await database.get_user_settings(user_id, DEFAULT_WATCH_INTERVAL_MINUTES)


def _settings_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="🔔 Подписки", callback_data="settings:watch"),
                InlineKeyboardButton(text="🔍 Поиск", callback_data="settings:search"),
            ],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="menu_home")],
        ]
    )


def _settings_search_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📄 Формат выгрузки", callback_data="settings:search:format")],
            [
                InlineKeyboardButton(text="⬅️ Назад", callback_data="menu_settings"),
                InlineKeyboardButton(text="🏠 Главное меню", callback_data="menu_home"),
            ],
        ]
    )


def _settings_watch_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="📄 Формат выгрузки", callback_data="settings:watch:format"),
                InlineKeyboardButton(text="🕒 Период проверки", callback_data="settings:watch:period"),
            ],
            [
                InlineKeyboardButton(text="⬅️ Назад", callback_data="menu_settings"),
                InlineKeyboardButton(text="🏠 Главное меню", callback_data="menu_home"),
            ],
        ]
    )


def _settings_format_picker_keyboard(scope: str, current_format: str) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for value in ("both", "text", "csv"):
        label = SEARCH_FORMAT_LABELS[value]
        if value == current_format:
            label = f"✅ {label}"
        rows.append([InlineKeyboardButton(text=label, callback_data=f"settings:{scope}:fmt:{value}")])
    rows.append(
        [
            InlineKeyboardButton(text="⬅️ Назад", callback_data=f"settings:{scope}"),
            InlineKeyboardButton(text="🏠 Главное меню", callback_data="menu_home"),
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _settings_watch_period_keyboard(current_interval: int) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    chunk: list[InlineKeyboardButton] = []
    seen: set[int] = set()
    for minutes in (current_interval, *WATCH_INTERVAL_OPTIONS):
        if minutes in seen:
            continue
        seen.add(minutes)
        label = f"{minutes} мин"
        if minutes == current_interval:
            label = f"✅ {label}"
        chunk.append(InlineKeyboardButton(text=label, callback_data=f"settings:watch:period:set:{minutes}"))
        if len(chunk) == 3:
            rows.append(chunk)
            chunk = []
    if chunk:
        rows.append(chunk)
    rows.append([InlineKeyboardButton(text="✍️ Свой период", callback_data="settings:watch:period:custom")])
    rows.append(
        [
            InlineKeyboardButton(text="⬅️ Назад", callback_data="settings:watch"),
            InlineKeyboardButton(text="🏠 Главное меню", callback_data="menu_home"),
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _format_output_format_value(output_format: str) -> str:
    return SEARCH_FORMAT_LABELS.get(output_format, output_format)


def _format_watch_interval_value(interval_minutes: int) -> str:
    return f"{interval_minutes} мин"


def _settings_root_text() -> str:
    return (
        "⚙️ Общие настройки\n\n"
        "Здесь задаются значения по умолчанию для поиска и автопроверки.\n"
        "После сохранения бот будет использовать их автоматически."
    )


def _settings_search_text(user_settings: UserSettingsDTO) -> str:
    return (
        "🔍 Настройки поиска\n\n"
        f"📄 Формат выгрузки: {_format_output_format_value(user_settings.search_output_format)}\n\n"
        "Этот формат будет использоваться во всех новых поисках."
    )


def _settings_watch_text(user_settings: UserSettingsDTO) -> str:
    return (
        "🔔 Настройки подписок\n\n"
        f"📄 Формат выгрузки: {_format_output_format_value(user_settings.watch_output_format)}\n"
        f"🕒 Период проверки: {_format_watch_interval_value(user_settings.watch_interval_minutes)}\n\n"
        "Эти значения используются для новых и уже активных автопроверок."
    )


def _format_limit_value(limit: Optional[int]) -> str:
    return "без лимита" if limit is None else str(limit)


def _format_comments_mode_value(deep_comments: bool) -> str:
    return SEARCH_COMMENTS_MODE_LABELS["deep"] if deep_comments else SEARCH_COMMENTS_MODE_LABELS["std"]


def _parse_limit_token(limit_token: str) -> Optional[int]:
    raw = (limit_token or "").strip().casefold()
    if raw in {"nolimit", "none", "0", "all", "unlimited"}:
        return None
    limit = int(raw)
    if limit <= 0:
        raise ValueError("Лимит должен быть больше нуля.")
    return limit


def _limit_to_token(limit: Optional[int]) -> str:
    return "nolimit" if limit is None else str(limit)


def _preset_date_range(preset: str) -> tuple[Optional[datetime], Optional[datetime]]:
    key = (preset or "").strip().casefold()
    now = datetime.now(timezone.utc)
    today = now.date()

    if key == "all":
        return None, None
    if key == "today":
        day_str = today.isoformat()
        return parse_date_from(day_str), parse_date_to(day_str)
    if key == "7d":
        start = (today - timedelta(days=6)).isoformat()
        return parse_date_from(start), parse_date_to(today.isoformat())
    if key == "30d":
        start = (today - timedelta(days=29)).isoformat()
        return parse_date_from(start), parse_date_to(today.isoformat())
    raise ValueError("Неизвестный пресет дат.")


def _parse_custom_dates_input(text: str) -> tuple[Optional[datetime], Optional[datetime]]:
    value = (text or "").strip()
    if not value:
        raise ValueError("Введите даты в формате YYYY-MM-DD YYYY-MM-DD.")

    if value.casefold() in {"all", "none", "no", "все", "за все время", "без даты"}:
        return None, None

    date_parts = re.findall(r"\d{4}-\d{2}-\d{2}", value)
    if not date_parts:
        raise ValueError("Не распознал даты. Формат: YYYY-MM-DD YYYY-MM-DD.")
    if len(date_parts) == 1:
        day = date_parts[0]
        return parse_date_from(day), parse_date_to(day)

    date_from = parse_date_from(date_parts[0])
    date_to = parse_date_to(date_parts[1])
    if date_from > date_to:
        raise ValueError("Дата начала не может быть позже даты конца.")
    return date_from, date_to


def _format_search_date_range(date_from: Optional[datetime], date_to: Optional[datetime]) -> str:
    if date_from is None and date_to is None:
        return "за всё время"
    from_text = date_from.astimezone(timezone.utc).strftime("%Y-%m-%d") if date_from else "начало"
    to_text = date_to.astimezone(timezone.utc).strftime("%Y-%m-%d") if date_to else "сейчас"
    return f"{from_text} .. {to_text}"


def _date_range_to_token(date_from: Optional[datetime], date_to: Optional[datetime]) -> str:
    if date_from is None and date_to is None:
        return "all"
    left = date_from.astimezone(timezone.utc).strftime("%Y%m%d") if date_from else "start"
    right = date_to.astimezone(timezone.utc).strftime("%Y%m%d") if date_to else "now"
    return f"{left}-{right}"


def _parse_date_range_token(token: str) -> tuple[Optional[datetime], Optional[datetime]]:
    raw = (token or "").strip().casefold()
    if raw == "all":
        return None, None
    parts = raw.split("-", maxsplit=1)
    if len(parts) != 2:
        raise ValueError("Некорректный диапазон дат.")
    left, right = parts
    date_from = None if left == "start" else parse_date_from(f"{left[:4]}-{left[4:6]}-{left[6:8]}")
    date_to = None if right == "now" else parse_date_to(f"{right[:4]}-{right[4:6]}-{right[6:8]}")
    return date_from, date_to


def _search_comments_mode_prompt(limit: Optional[int], date_from: Optional[datetime], date_to: Optional[datetime]) -> str:
    return (
        f"Лимит: {_format_limit_value(limit)}\n"
        f"Период: {_format_search_date_range(date_from, date_to)}\n\n"
        "Выберите режим поиска по комментариям:\n"
        "⚖️ Стандартный — быстрее\n"
        "🔎 Глубокий — тщательнее ищет в комментариях, но работает дольше"
    )


def _format_search_start_text(
    target_label: str,
    output_format: str,
    limit: Optional[int],
    date_from: Optional[datetime],
    date_to: Optional[datetime],
    deep_comments: bool,
) -> str:
    return (
        "🔎 Поиск запущен\n"
        f"📌 Где искать: {target_label}\n"
        f"📄 Формат: {SEARCH_FORMAT_LABELS[output_format]}\n"
        f"🔢 Лимит: {_format_limit_value(limit)}\n"
        f"📅 Период: {_format_search_date_range(date_from, date_to)}\n"
        f"💬 Комментарии: {_format_comments_mode_value(deep_comments)}"
    )


def _format_auth_status_text(status: str, fallback: str) -> str:
    if status == "authorized":
        return "✅ Авторизация успешно завершена."
    if status == "missing":
        return "Нет активной авторизации. Нажмите «👤 Авторизация»."
    if status == "expired":
        return "⌛ QR-код истёк. Нажмите «🔄 Обновить QR»."
    if status == "pending":
        return (
            "QR ещё не подтвержден.\n\n"
            "1. Отсканируйте код в Telegram.\n"
            "2. Нажмите «✅ Отсканировал».\n"
            "3. Если потребуется пароль 2FA, бот попросит его автоматически."
        )
    return fallback


def _validate_watch_interval(interval_minutes: int) -> None:
    if interval_minutes < MIN_WATCH_INTERVAL_MINUTES or interval_minutes > MAX_WATCH_INTERVAL_MINUTES:
        raise ValueError(
            f"Интервал должен быть от {MIN_WATCH_INTERVAL_MINUTES} до {MAX_WATCH_INTERVAL_MINUTES} минут."
        )


def _parse_watch_set_command(body: str) -> tuple[Optional[str], int]:
    text = body.strip()
    if not text:
        raise ValueError("Использование: /watch_set [theme] <minutes>")
    parts = shlex.split(text)
    if not parts:
        raise ValueError("Использование: /watch_set [theme] <minutes>")
    interval_raw = parts[-1]
    if not interval_raw.isdigit():
        raise ValueError("Последний аргумент должен быть числом минут.")
    interval_minutes = int(interval_raw)
    theme_name = " ".join(parts[:-1]).strip() or None
    _validate_watch_interval(interval_minutes)
    return theme_name, interval_minutes


def _format_dt_utc(value: Optional[datetime]) -> str:
    if value is None:
        return "—"
    dt = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def _watch_usage_text() -> str:
    return (
        "🔔 Автопроверка тем\n"
        "Бот может сам регулярно проверять ваши темы и присылать новые совпадения.\n\n"
        "Как это работает:\n"
        "1. Откройте раздел «Подписки».\n"
        "2. Нажмите «Включить».\n"
        "3. Выберите тему.\n"
        "4. Бот включит автопроверку с параметрами из «⚙️ Общие настройки».\n\n"
        "Что ещё можно сделать:\n"
        "• посмотреть активные подписки\n"
        "• отключить ненужную подписку\n"
        "• поменять формат выгрузки и период проверки в «⚙️ Общие настройки»\n\n"
        f"⏱ Доступный интервал: от {MIN_WATCH_INTERVAL_MINUTES} до {MAX_WATCH_INTERVAL_MINUTES} минут."
    )


def _render_watch_list_text(watches: list[ThemeWatchDTO]) -> str:
    if not watches:
        return (
            "🔕 Активных подписок пока нет.\n\n"
            "Чтобы бот сам проверял новые сообщения:\n"
            "• откройте раздел «Подписки»\n"
            "• нажмите «Включить»\n"
            "• выберите тему\n"
            "Период и формат настраиваются в «⚙️ Общие настройки»."
        )

    lines = ["🔔 Активные подписки:"]
    for idx, watch in enumerate(watches, start=1):
        lines.append(f"{idx}. {watch.theme_name}")
        lines.append(f"   ⏱ Интервал: каждые {watch.interval_minutes} мин")
        lines.append(f"   ⏭ Следующая проверка: {_format_dt_utc(watch.next_check_at)}")
        if watch.last_checked_at:
            lines.append(f"   🕓 Последняя проверка: {_format_dt_utc(watch.last_checked_at)}")
        if watch.last_match_at:
            lines.append(f"   ✅ Последнее совпадение: {_format_dt_utc(watch.last_match_at)}")
        if watch.last_error:
            lines.append(f"   ⚠️ Последняя ошибка: {_short_text(watch.last_error, max_len=150)}")
    return "\n".join(lines)


def _remember_watch_ui_message(user_id: int, message: Message) -> None:
    watch_ui_message_by_user[user_id] = (message.chat.id, message.message_id)


async def _cleanup_previous_watch_ui_message(
    bot: Bot,
    user_id: int,
    chat_id: int,
    *,
    keep_message_id: Optional[int] = None,
) -> None:
    previous = watch_ui_message_by_user.get(user_id)
    if previous is None:
        return
    prev_chat_id, prev_message_id = previous
    if prev_chat_id != chat_id:
        watch_ui_message_by_user.pop(user_id, None)
        return
    if keep_message_id and prev_message_id == keep_message_id:
        return
    with suppress(TelegramBadRequest, TelegramNotFound):
        await bot.delete_message(chat_id=prev_chat_id, message_id=prev_message_id)
    if watch_ui_message_by_user.get(user_id) == previous:
        watch_ui_message_by_user.pop(user_id, None)


async def _send_watch_ui_message(
    message: Message,
    user_id: int,
    text: str,
    reply_markup: Optional[InlineKeyboardMarkup] = None,
    *,
    nav_show_auth_actions: Optional[bool] = None,
    nav_force: bool = False,
) -> Message:
    await _ensure_nav_keyboard(
        message,
        user_id,
        show_auth_actions=nav_show_auth_actions,
        force=nav_force,
    )
    await _cleanup_previous_watch_ui_message(message.bot, user_id, message.chat.id)
    sent = await message.answer(text, reply_markup=reply_markup, disable_web_page_preview=True)
    _remember_watch_ui_message(user_id, sent)
    return sent


async def _edit_or_replace_watch_ui(
    callback: CallbackQuery,
    user_id: int,
    text: str,
    reply_markup: Optional[InlineKeyboardMarkup] = None,
) -> None:
    if callback.message is None:
        return
    current = callback.message
    if not nav_keyboard_mode_by_user.get(user_id):
        nav_keyboard_mode_by_user[user_id] = True
        await current.answer(
            SILENT_UI_TEXT,
            reply_markup=ReplyKeyboardRemove(),
        )
    await _cleanup_previous_watch_ui_message(
        current.bot,
        user_id,
        current.chat.id,
        keep_message_id=current.message_id,
    )
    try:
        await current.edit_text(text, reply_markup=reply_markup, disable_web_page_preview=True)
        _remember_watch_ui_message(user_id, current)
        return
    except TelegramBadRequest:
        pass

    await _cleanup_previous_watch_ui_message(
        current.bot,
        user_id,
        current.chat.id,
        keep_message_id=current.message_id,
    )
    sent = await current.answer(text, reply_markup=reply_markup, disable_web_page_preview=True)
    _remember_watch_ui_message(user_id, sent)
    with suppress(TelegramBadRequest, TelegramNotFound):
        await current.delete()


async def _send_watch_ui_photo_message(
    message: Message,
    user_id: int,
    photo: BufferedInputFile,
    caption: str,
    reply_markup: Optional[InlineKeyboardMarkup] = None,
    *,
    nav_show_auth_actions: Optional[bool] = None,
    nav_force: bool = False,
) -> Message:
    await _ensure_nav_keyboard(
        message,
        user_id,
        show_auth_actions=nav_show_auth_actions,
        force=nav_force,
    )
    await _cleanup_previous_watch_ui_message(message.bot, user_id, message.chat.id)
    sent = await message.answer_photo(photo, caption=caption, reply_markup=reply_markup)
    _remember_watch_ui_message(user_id, sent)
    return sent


def _split_bulk_items(raw: str, allow_comma: bool = False) -> list[str]:
    value = raw.strip()
    if not value:
        return []
    separators_pattern = r"[;\n]+"
    if re.search(separators_pattern, value):
        parts = re.split(separators_pattern, value)
    elif allow_comma and "," in value:
        parts = value.split(",")
    else:
        parts = [value]
    return [part.strip() for part in parts if part.strip()]


async def _resolve_theme_for_user(user_id: int, provided_name: Optional[str]) -> ThemeDTO:
    _, database, _, _ = _require_services()

    if provided_name:
        theme = await database.get_theme(user_id, provided_name)
        if theme is None:
            raise ValueError(f"Тема '{provided_name}' не найдена.")
        active_theme_by_user[user_id] = theme.name
        return theme

    remembered = active_theme_by_user.get(user_id)
    if remembered:
        theme = await database.get_theme(user_id, remembered)
        if theme is not None:
            return theme
        active_theme_by_user.pop(user_id, None)

    themes = await database.list_themes(user_id)
    if not themes:
        raise ValueError("Тем нет. Создайте тему: /theme_new <name>")
    if len(themes) == 1:
        active_theme_by_user[user_id] = themes[0].name
        return themes[0]
    names = ", ".join(theme.name for theme in themes)
    raise ValueError(
        "Тема не указана. Выберите активную: /theme_use <name> "
        f"или укажите тему в команде. Доступные: {names}"
    )


async def _resolve_theme_and_value_from_body(
    user_id: int,
    body: str,
    *,
    allow_multiple: bool,
    allow_comma: bool = False,
) -> tuple[ThemeDTO, list[str]]:
    _, database, _, _ = _require_services()
    text = body.strip()
    if not text:
        raise ValueError("Не хватает аргументов.")

    first, rest = (text.split(maxsplit=1) + [""])[:2]
    explicit_theme = await database.get_theme(user_id, first)
    if explicit_theme is not None:
        if not rest.strip():
            raise ValueError("После имени темы нужно указать значение.")
        theme = await _resolve_theme_for_user(user_id, explicit_theme.name)
        payload = rest.strip()
    else:
        theme = await _resolve_theme_for_user(user_id, None)
        payload = text

    items = _split_bulk_items(payload, allow_comma=allow_comma) if allow_multiple else [payload.strip()]
    if not items:
        raise ValueError("Список значений пуст.")
    return theme, items


def _theme_action_prompt(action: str, theme_name: str) -> str:
    if action == "add_kw":
        return (
            f"Тема: {theme_name}\n"
            "Отправьте ключевые слова одним сообщением.\n"
            "Разделители: ';', ',' или перенос строки.\n"
            "Пример: Москва; погода; дрон"
        )
    if action == "del_kw":
        return (
            f"Тема: {theme_name}\n"
            "Отправьте ключевые слова для удаления.\n"
            "Разделители: ';', ',' или перенос строки."
        )
    if action == "add_chat":
        return (
            f"Тема: {theme_name}\n"
            "Отправьте чаты для добавления.\n"
            "Разделители: ';' или перенос строки.\n"
            "Поддерживается: @username, id, точное имя, t.me-ссылка.\n"
            "Или нажмите кнопку 'Добавить ВСЕ чаты аккаунта'."
        )
    if action == "del_chat":
        return (
            f"Тема: {theme_name}\n"
            "Отправьте чаты для удаления.\n"
            "Разделители: ';' или перенос строки."
        )
    return f"Тема: {theme_name}"


def _theme_wizard_keywords_prompt(theme_name: str) -> str:
    return (
        f"✅ Тема '{theme_name}' создана и выбрана активной.\n\n"
        "🔑 Шаг 2/3 — Ключевые слова\n"
        "Отправьте слова или фразы для поиска одним сообщением.\n\n"
        "Поддерживаются разделители:\n"
        "• ';'\n"
        "• ','\n"
        "• перенос строки\n\n"
        "Пример:\n"
        "Москва; погода; дрон\n\n"
        "⏭ Если хотите пропустить шаг, отправьте /skip"
    )


def _theme_wizard_chats_prompt(theme_name: str) -> str:
    return (
        f"✅ Тема: {theme_name}\n\n"
        "💬 Шаг 3/3 — Чаты и каналы\n"
        "Отправьте чаты, в которых нужно искать сообщения.\n\n"
        "Поддерживается формат:\n"
        "• @username\n"
        "• id\n"
        "• точное имя\n"
        "• ссылка t.me\n\n"
        "Разделители:\n"
        "• ';'\n"
        "• перенос строки\n\n"
        "⏭ Если хотите пропустить шаг, отправьте /skip"
    )


async def _build_theme_status_map(themes: list[ThemeDTO], user_id: int) -> dict[int, tuple[str, str]]:
    _, database, _, _ = _require_services()
    watches = await database.list_theme_watches(user_id)
    latest_runs = await database.get_latest_search_run_statuses(user_id)
    watched_names = {item.theme_name.casefold() for item in watches}
    active_names = active_search_themes_by_user.get(user_id, set())
    all_active = ACTIVE_SEARCH_ALL_MARKER in active_names

    result: dict[int, tuple[str, str]] = {}
    for theme in themes:
        key = theme.name.casefold()
        latest_status = latest_runs.get(key)
        if all_active or key in active_names or latest_status == "running":
            result[theme.id] = ("⏳", "поиск в процессе")
        elif key in watched_names:
            result[theme.id] = ("🔔", "отслеживается")
        elif latest_status == "completed":
            result[theme.id] = ("🎯", "поиск завершен")
        else:
            result[theme.id] = ("•", "без статуса")
    return result


def _render_value_list(items: list[str], *, max_items: int = 200) -> str:
    if not items:
        return "(нет)"
    lines: list[str] = []
    for idx, value in enumerate(items[:max_items], start=1):
        lines.append(f"{idx}. {_short_text(value, max_len=140)}")
    if len(items) > max_items:
        lines.append(f"... и еще {len(items) - max_items}")
    return "\n".join(lines)


async def _send_themes_panel(message: Message, user_id: int) -> None:
    summary = await _build_themes_panel_text(user_id)
    await _send_watch_ui_message(
        message,
        user_id,
        summary,
        await _themes_panel_markup_for_user(user_id),
    )


async def _themes_panel_markup_for_user(user_id: int) -> InlineKeyboardMarkup:
    _, database, _, _ = _require_services()
    themes = await database.list_themes(user_id)
    if not themes:
        return _themes_menu_keyboard()
    status_map = await _build_theme_status_map(themes, user_id)
    status_labels = {theme.id: f"{status_map[theme.id][0]} {theme.name}" for theme in themes if theme.id in status_map}
    return _themes_panel_keyboard(themes, active_theme_by_user.get(user_id), status_labels)


async def _build_themes_panel_text(user_id: int) -> str:
    _, database, _, _ = _require_services()
    themes = await database.list_themes(user_id)
    if not themes:
        return (
            "☰ Темы\n\n"
            "Тем пока нет.\n"
            "Нажмите «➕ Добавить тему», чтобы создать первую тему."
        )
    status_map = await _build_theme_status_map(themes, user_id)
    lines = [
        "☰ Темы",
        "",
        "Все темы:",
    ]
    for theme in themes:
        icon, status_text = status_map.get(theme.id, ("•", "без статуса"))
        lines.append(
            f"{icon} {theme.name}  |  {status_text}  |  ключи: {len(theme.keywords)}  |  чаты: {len(theme.chats)}"
        )
    lines.append("")
    lines.append("Выберите тему кнопкой ниже или создайте новую.")
    return "\n".join(lines)


async def _get_active_theme_for_user(user_id: int) -> Optional[ThemeDTO]:
    _, database, _, _ = _require_services()
    current = active_theme_by_user.get(user_id)
    if not current:
        return None
    theme = await database.get_theme(user_id, current)
    if theme is None:
        active_theme_by_user.pop(user_id, None)
        return None
    return theme


def _render_themes_compact_list(themes: list[ThemeDTO]) -> str:
    if not themes:
        return "Тем пока нет. Нажмите 'Создать тему'."
    lines = ["Список тем:"]
    for idx, theme in enumerate(themes, start=1):
        lines.append(
            f"{idx}. {theme.name} (чаты: {len(theme.chats)}, ключи: {len(theme.keywords)})"
        )
    lines.append("\nДля деталей по теме: /theme_show <name>")
    return "\n".join(lines)


def _format_theme_card(theme: ThemeDTO, *, status_icon: str, status_text: str) -> str:
    return (
        f"☰ Тема: {theme.name}\n"
        f"Статус: {status_icon} {status_text}\n"
        f"Ключевых слов: {len(theme.keywords)}\n"
        f"Чатов: {len(theme.chats)}\n\n"
        "Выберите действие:"
    )


def _render_chat_list(items: list[str], *, max_items: int = 200) -> str:
    return _render_value_list(items, max_items=max_items)


def _format_theme_keywords_screen(theme: ThemeDTO) -> str:
    return (
        f"🔑 Ключевые слова темы '{theme.name}'\n"
        f"Всего: {len(theme.keywords)}\n\n"
        f"{_render_value_list(theme.keywords)}"
    )


def _format_theme_chats_screen(theme: ThemeDTO) -> str:
    return (
        f"💬 Чаты темы '{theme.name}'\n"
        f"Всего: {len(theme.chats)}\n\n"
        f"{_render_value_list(theme.chats)}"
    )


async def _build_search_chats_text(user_id: int) -> str:
    _, database, _, _ = _require_services()
    themes = await database.list_themes(user_id)
    if not themes:
        return "Тем пока нет. Создайте тему в разделе 'Темы'."

    all_chats: list[str] = []
    by_name: dict[str, ThemeDTO] = {}
    for theme in themes:
        by_name[theme.name.casefold()] = theme
        all_chats.extend(theme.chats)
    all_unique = _dedupe_preserve_order(all_chats)

    active_name = active_theme_by_user.get(user_id)
    active_theme: Optional[ThemeDTO] = None
    if active_name:
        active_theme = by_name.get(active_name.casefold())
    if active_theme is None and len(themes) == 1:
        active_theme = themes[0]

    active_header = "Активная тема: (не выбрана)"
    active_block = "(чаты активной темы недоступны)"
    if active_theme is not None:
        active_header = f"Активная тема: {active_theme.name} ({len(active_theme.chats)} чатов)"
        active_block = _render_chat_list(active_theme.chats)

    return (
        f"Чаты для поиска\n"
        f"Тем: {len(themes)}\n"
        f"Уникальных чатов (все темы): {len(all_unique)}\n"
        f"{active_header}\n\n"
        f"Чаты активной темы:\n{active_block}\n\n"
        f"Все чаты (объединено):\n{_render_chat_list(all_unique)}"
    )


async def _build_watch_panel_text(user_id: int) -> str:
    _, database, _, _ = _require_services()
    watches = await database.list_theme_watches(user_id)
    return f"{_render_watch_list_text(watches)}\n\n{_watch_usage_text()}"


async def _send_watch_panel(message: Message, user_id: int) -> None:
    text = await _build_watch_panel_text(user_id)
    await _send_watch_ui_message(message, user_id, text, _watch_menu_keyboard())


def _render_results_messages(theme_name: str, items) -> list[str]:
    if not items:
        return [f"По теме '{theme_name}' ничего не найдено."]

    lines = [f"Результаты по теме '{theme_name}': {len(items)} шт.\n"]
    for idx, item in enumerate(items, start=1):
        lines.append(
            f"[{idx}] {item.date.isoformat()} | {item.chat} | msg_id={item.msg_id}\n"
            f"matched: {', '.join(item.matched_keywords)}\n"
            f"{_short_text(item.text)}\n"
            f"{item.link or '(ссылка недоступна)'}\n"
        )

    chunks: list[str] = []
    current = ""
    for line in lines:
        if len(current) + len(line) + 1 > 3800:
            chunks.append(current)
            current = line
        else:
            current = f"{current}\n{line}" if current else line
    if current:
        chunks.append(current)
    return chunks


def _search_error_ui(theme: Optional[ThemeDTO], error_text: str) -> tuple[str, Optional[InlineKeyboardMarkup]]:
    text = (error_text or "").strip()
    lowered = text.casefold()

    if "в теме нет чатов" in lowered:
        if theme is not None and theme.id > 0:
            return (
                f"Поиск по теме '{theme.name}' пока невозможен: в теме нет чатов.\n\n"
                "Выберите действие:",
                InlineKeyboardMarkup(
                    inline_keyboard=[
                        [InlineKeyboardButton(text="Открыть чаты темы", callback_data=f"themes:chats:{theme.id}")],
                        [InlineKeyboardButton(text="Открыть Темы", callback_data="menu_themes")],
                    ]
                ),
            )
        return (
            "Поиск пока невозможен: в выбранном наборе нет чатов.\n\n"
            "Откройте темы и добавьте чаты, затем запустите поиск снова.",
            InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="☰ Открыть темы", callback_data="menu_themes")],
                    [InlineKeyboardButton(text="🏠 Главное меню", callback_data="menu_home")],
                ]
            ),
        )

    if "во всех темах пуст список чатов" in lowered:
        return (
            "Поиск по всем чатам пока невозможен: во всех темах нет чатов.\n\n"
            "Откройте темы и добавьте чаты, затем запустите поиск снова.",
            InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="☰ Открыть темы", callback_data="menu_themes")],
                    [InlineKeyboardButton(text="🏠 Главное меню", callback_data="menu_home")],
                ]
            ),
        )

    if "в теме нет ключевых слов" in lowered:
        if theme is not None and theme.id > 0:
            return (
                f"Поиск по теме '{theme.name}' пока невозможен: в теме нет ключевых слов.\n\n"
                "Выберите действие:",
                InlineKeyboardMarkup(
                    inline_keyboard=[
                        [InlineKeyboardButton(text="Открыть ключи темы", callback_data=f"themes:keys:{theme.id}")],
                        [InlineKeyboardButton(text="Открыть Темы", callback_data="menu_themes")],
                    ]
                ),
            )
        return (
            "Поиск пока невозможен: не заданы ключевые слова.\n\n"
            "Откройте темы и добавьте ключевые слова.",
            InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="☰ Открыть темы", callback_data="menu_themes")],
                    [InlineKeyboardButton(text="🏠 Главное меню", callback_data="menu_home")],
                ]
            ),
        )

    return f"Ошибка поиска: {text}", None


async def _run_search_task(
    bot: Bot,
    user_id: int,
    chat_id: int,
    theme_name: str,
    date_from: Optional[datetime],
    date_to: Optional[datetime],
    limit: Optional[int],
    output_format: str,
    deep_comments: bool,
) -> None:
    _, database, _, _ = _require_services()
    theme = await database.get_theme(user_id, theme_name)
    if theme is None:
        await bot.send_message(chat_id, f"Ошибка поиска: Тема '{theme_name}' не найдена.")
        task = active_search_tasks.get(user_id)
        if task is asyncio.current_task():
            active_search_tasks.pop(user_id, None)
        return
    await _run_search_with_theme(
        bot=bot,
        user_id=user_id,
        chat_id=chat_id,
        run_theme_name=theme.name,
        theme=theme,
        date_from=date_from,
        date_to=date_to,
        limit=limit,
        output_format=output_format,
        deep_comments=deep_comments,
    )


def _dedupe_preserve_order(values: list[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        raw = (value or "").strip()
        if not raw:
            continue
        key = raw.casefold()
        if key in seen:
            continue
        seen.add(key)
        result.append(raw)
    return result


async def _build_all_chats_theme(user_id: int) -> ThemeDTO:
    _, database, _, _ = _require_services()
    themes = await database.list_themes(user_id)
    if not themes:
        raise SearchError("У вас нет тем. Создайте тему в разделе 'Темы'.")

    chats: list[str] = []
    keywords: list[str] = []
    for theme in themes:
        chats.extend(theme.chats)
        keywords.extend(theme.keywords)

    chats = _dedupe_preserve_order(chats)
    keywords = _dedupe_preserve_order(keywords)
    if not chats:
        raise SearchError("Во всех темах пуст список чатов. Добавьте чаты в разделe 'Темы'.")
    if not keywords:
        raise SearchError("Во всех темах пуст список ключевых слов. Добавьте ключевые слова в разделe 'Темы'.")

    return ThemeDTO(id=0, name="all_chats", chats=chats, keywords=keywords)


async def _run_search_all_chats_task(
    bot: Bot,
    user_id: int,
    chat_id: int,
    limit: Optional[int],
    output_format: str,
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None,
    deep_comments: bool = False,
) -> None:
    try:
        pseudo_theme = await _build_all_chats_theme(user_id)
    except Exception as e:
        text, reply_markup = _search_error_ui(None, str(e))
        await bot.send_message(chat_id, text, reply_markup=reply_markup, disable_web_page_preview=True)
        task = active_search_tasks.get(user_id)
        if task is asyncio.current_task():
            active_search_tasks.pop(user_id, None)
        return
    await _run_search_with_theme(
        bot=bot,
        user_id=user_id,
        chat_id=chat_id,
        run_theme_name=pseudo_theme.name,
        theme=pseudo_theme,
        date_from=date_from,
        date_to=date_to,
        limit=limit,
        output_format=output_format,
        deep_comments=deep_comments,
    )


async def _create_ui_search_task(
    *,
    bot: Bot,
    user_id: int,
    chat_id: int,
    scope: str,
    payload_id: int,
    output_format: str,
    limit: Optional[int],
    date_from: Optional[datetime],
    date_to: Optional[datetime],
    deep_comments: bool,
) -> tuple[str, asyncio.Task]:
    _, database, _, _ = _require_services()
    if scope == "th":
        theme = await database.get_theme_by_id(user_id, payload_id)
        if theme is None:
            raise ValueError("Тема не найдена.")
        active_theme_by_user[user_id] = theme.name
        _set_active_search_themes(user_id, [theme.name])
        text = _format_search_start_text(
            f"Тема '{theme.name}'",
            output_format,
            limit,
            date_from,
            date_to,
            deep_comments,
        )
        task = asyncio.create_task(
            _run_search_task(
                bot=bot,
                user_id=user_id,
                chat_id=chat_id,
                theme_name=theme.name,
                date_from=date_from,
                date_to=date_to,
                limit=limit,
                output_format=output_format,
                deep_comments=deep_comments,
            )
        )
        return text, task

    _set_active_search_themes(user_id, all_themes=True)
    text = _format_search_start_text(
        "Все чаты из всех тем",
        output_format,
        limit,
        date_from,
        date_to,
        deep_comments,
    )
    task = asyncio.create_task(
        _run_search_all_chats_task(
            bot=bot,
            user_id=user_id,
            chat_id=chat_id,
            limit=limit,
            output_format=output_format,
            date_from=date_from,
            date_to=date_to,
            deep_comments=deep_comments,
        )
    )
    return text, task


async def _run_search_with_theme(
    bot: Bot,
    user_id: int,
    chat_id: int,
    run_theme_name: str,
    theme: ThemeDTO,
    date_from: Optional[datetime],
    date_to: Optional[datetime],
    limit: Optional[int],
    output_format: str,
    deep_comments: bool,
) -> None:
    _, database, _, search = _require_services()
    run_id = await database.create_search_run(user_id=user_id, theme_name=run_theme_name)
    try:
        await search.cleanup_old_files()
        await database.cleanup_old_runs(retention_days=_require_services()[0].retention_days)

        last_progress = 0.0

        async def on_progress(text: str) -> None:
            nonlocal last_progress
            now = asyncio.get_event_loop().time()
            if now - last_progress < 5:
                return
            last_progress = now
            await bot.send_message(chat_id, text)

        items, csv_path = await search.run_theme_search(
            user_id=user_id,
            theme=theme,
            params=SearchParams(limit=limit, date_from=date_from, date_to=date_to, deep_comments=deep_comments),
            progress_cb=on_progress,
        )

        if output_format in {"both", "text"}:
            for chunk in _render_results_messages(theme.name, items):
                await bot.send_message(chat_id, chunk, disable_web_page_preview=True)

        if output_format in {"both", "csv"}:
            await bot.send_document(
                chat_id,
                FSInputFile(path=str(csv_path), filename=csv_path.name),
                caption=f"CSV по теме '{theme.name}', найдено: {len(items)}",
            )

        await database.finish_search_run(run_id, status="completed", result_count=len(items))
    except asyncio.CancelledError:
        await database.finish_search_run(run_id, status="cancelled")
        await bot.send_message(chat_id, "Поиск отменен.")
        raise
    except Exception as e:
        await database.finish_search_run(run_id, status="failed", error_text=str(e))
        logger.error("Search failed: %s\n%s", e, traceback.format_exc())
        text, reply_markup = _search_error_ui(theme, str(e))
        await bot.send_message(chat_id, text, reply_markup=reply_markup, disable_web_page_preview=True)
    finally:
        task = active_search_tasks.get(user_id)
        if task is asyncio.current_task():
            active_search_tasks.pop(user_id, None)
        _clear_active_search_themes(user_id)


def _watch_window_start(last_checked_at: Optional[datetime]) -> Optional[datetime]:
    if last_checked_at is None:
        return None
    dt = last_checked_at if last_checked_at.tzinfo else last_checked_at.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc) + timedelta(microseconds=1)


async def _notify_watch_hits(
    bot: Bot,
    watch: ThemeWatchDTO,
    theme_name: str,
    date_from: Optional[datetime],
    date_to: datetime,
    items,
    csv_path,
    output_format: str,
) -> None:
    range_from = _format_dt_utc(date_from) if date_from else "начало истории"
    header = (
        f"Автопроверка темы '{theme_name}'\n"
        f"Период: {range_from} - {_format_dt_utc(date_to)}\n"
        f"Найдено новых сообщений: {len(items)}"
    )

    try:
        await bot.send_message(watch.chat_id, header, disable_web_page_preview=True)
        if output_format in {"both", "text"}:
            for chunk in _render_results_messages(theme_name, items):
                await bot.send_message(watch.chat_id, chunk, disable_web_page_preview=True)
        if output_format in {"both", "csv"}:
            await bot.send_document(
                watch.chat_id,
                FSInputFile(path=str(csv_path), filename=csv_path.name),
                caption=f"CSV автопроверки по теме '{theme_name}', найдено: {len(items)}",
            )
    except Exception as notify_error:
        logger.warning(
            "Failed to send watch notification user_id=%s theme=%s: %s",
            watch.user_id,
            theme_name,
            notify_error,
        )


async def _run_theme_watch_check(bot: Bot, watch: ThemeWatchDTO) -> None:
    app_settings, database, auth, search = _require_services()
    user_settings = await _get_user_settings(watch.user_id)
    current_task = active_search_tasks.get(watch.user_id)
    if current_task and current_task.done():
        if active_search_tasks.get(watch.user_id) is current_task:
            active_search_tasks.pop(watch.user_id, None)
        current_task = None
    if current_task and not current_task.done():
        logger.info(
            "Skip watch check user_id=%s theme='%s': manual search is running",
            watch.user_id,
            watch.theme_name,
        )
        return

    checked_at = datetime.now(timezone.utc)
    theme = await database.get_theme_by_id(watch.user_id, watch.theme_id)
    if theme is None:
        logger.info(
            "Skip watch check user_id=%s theme_id=%s: theme does not exist",
            watch.user_id,
            watch.theme_id,
        )
        return

    if not await auth.is_authorized(watch.user_id):
        await database.mark_theme_watch_checked(
            watch.id,
            checked_at,
            had_matches=False,
            error_text="Account is not authorized",
        )
        logger.info(
            "Skip watch check user_id=%s theme='%s': account is not authorized",
            watch.user_id,
            watch.theme_name,
        )
        return

    date_from = _watch_window_start(watch.last_checked_at)
    date_to = checked_at
    logger.info(
        "Run watch check user_id=%s theme='%s' range=[%s..%s]",
        watch.user_id,
        theme.name,
        _format_dt_utc(date_from),
        _format_dt_utc(date_to),
    )
    try:
        items, csv_path = await search.run_theme_search(
            user_id=watch.user_id,
            theme=theme,
            params=SearchParams(limit=app_settings.default_limit, date_from=date_from, date_to=date_to),
            progress_cb=None,
        )
        await database.mark_theme_watch_checked(watch.id, checked_at, had_matches=bool(items), error_text=None)
        logger.info(
            "Watch check completed user_id=%s theme='%s': matches=%s",
            watch.user_id,
            theme.name,
            len(items),
        )
        if not items:
            with suppress(OSError):
                csv_path.unlink(missing_ok=True)
            return
        await _notify_watch_hits(
            bot=bot,
            watch=watch,
            theme_name=theme.name,
            date_from=date_from,
            date_to=date_to,
            items=items,
            csv_path=csv_path,
            output_format=user_settings.watch_output_format,
        )
    except asyncio.CancelledError:
        raise
    except Exception as e:
        await database.mark_theme_watch_checked(watch.id, checked_at, had_matches=False, error_text=str(e))
        logger.error("Theme watch failed user_id=%s theme='%s': %s", watch.user_id, watch.theme_name, e)


async def _watch_scheduler_iteration(bot: Bot) -> None:
    _, database, _, _ = _require_services()
    due_watches = await database.list_due_theme_watches(limit=30)
    if not due_watches:
        return
    logger.info("Watch scheduler: due watches=%s", len(due_watches))
    for watch in due_watches:
        await _run_theme_watch_check(bot, watch)


async def _watch_scheduler_loop(bot: Bot) -> None:
    logger.info("Theme watch scheduler started")
    try:
        while True:
            try:
                await _watch_scheduler_iteration(bot)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error("Watch scheduler iteration failed: %s\n%s", e, traceback.format_exc())
            await asyncio.sleep(WATCH_SCHEDULER_SLEEP_SECONDS)
    finally:
        logger.info("Theme watch scheduler stopped")


@router.message(Command("start"))
async def cmd_start(message: Message) -> None:
    user_id = await _ensure_user(message)
    _, _, auth, _ = _require_services()
    is_auth = await auth.is_authorized(user_id)
    status = "Авторизован" if is_auth else "Не авторизован"
    if is_auth:
        steps = (
            "• откройте 'Темы' и настройте тему\n"
            "• откройте 'Поиск' и запустите поиск\n"
            "• откройте 'Подписки', если нужна регулярная проверка"
        )
    else:
        steps = (
            "• нажмите «👤 Авторизация» и отсканируйте QR\n"
            "• если включена 2FA, бот попросит пароль\n"
            "• если нужна инструкция, нажмите «❓ Помощь»"
        )

    await _send_watch_ui_message(
        message,
        user_id,
        "👋 Бот для поиска по вашему Telegram-аккаунту\n"
        f"🔐 Статус: {status}\n\n"
        "Что можно сделать дальше:\n"
        f"{steps}",
        _main_menu(show_auth_actions=not is_auth),
        nav_show_auth_actions=not is_auth,
        nav_force=True,
    )


def _help_text() -> str:
    return (
        "❓ Помощь\n\n"
        "1. Авторизуйтесь через QR-код.\n"
        "2. Откройте раздел «Темы» и создайте тему.\n"
        "3. Добавьте ключевые слова и чаты.\n"
        "4. Запустите разовый поиск или включите отслеживание.\n\n"
        "Полезно знать:\n"
        "• чаты можно указывать как @username, id, точное имя или ссылку t.me\n"
        "• комментарии к постам ищутся автоматически, если они доступны\n"
        "• параметры поиска и подписок по умолчанию задаются в «⚙️ Общие настройки»"
    )


@router.message(Command("help"))
async def cmd_help(message: Message) -> None:
    user_id = await _ensure_user(message)
    await _send_watch_ui_message(message, user_id, _help_text(), _help_keyboard())


@router.callback_query(F.data == "menu_home")
async def cb_home(callback: CallbackQuery, state: FSMContext) -> None:
    if callback.message and callback.from_user:
        await state.clear()
        user_id = await _upsert_user(callback.from_user.id, callback.from_user.username, callback.from_user.first_name)
        current = callback.message
        tracked = watch_ui_message_by_user.get(user_id)
        current_ref = (current.chat.id, current.message_id)
        main_menu = await _main_menu_for_user(user_id)
        if tracked == current_ref:
            await _edit_or_replace_watch_ui(callback, user_id, "Главное меню:", main_menu)
        else:
            await _ensure_nav_keyboard(current, user_id)
            await _cleanup_previous_watch_ui_message(current.bot, user_id, current.chat.id)
            sent = await current.answer("Главное меню:", reply_markup=main_menu, disable_web_page_preview=True)
            _remember_watch_ui_message(user_id, sent)
            with suppress(TelegramBadRequest, TelegramNotFound):
                await current.delete()
    await callback.answer()


@router.callback_query(F.data == "menu_help")
async def cb_help(callback: CallbackQuery) -> None:
    if callback.message and callback.from_user:
        user_id = await _upsert_user(callback.from_user.id, callback.from_user.username, callback.from_user.first_name)
        await _edit_or_replace_watch_ui(callback, user_id, _help_text(), _help_keyboard())
    await callback.answer()


@router.callback_query(F.data == "menu_settings")
async def cb_settings(callback: CallbackQuery, state: FSMContext) -> None:
    if callback.message and callback.from_user:
        await _clear_settings_state_if_needed(state)
        user_id = await _upsert_user(callback.from_user.id, callback.from_user.username, callback.from_user.first_name)
        await _edit_or_replace_watch_ui(callback, user_id, _settings_root_text(), _settings_menu_keyboard())
    await callback.answer()


@router.callback_query(F.data == "settings:search")
async def cb_settings_search(callback: CallbackQuery, state: FSMContext) -> None:
    if callback.message and callback.from_user:
        await _clear_settings_state_if_needed(state)
        user_id = await _upsert_user(callback.from_user.id, callback.from_user.username, callback.from_user.first_name)
        user_settings = await _get_user_settings(user_id)
        await _edit_or_replace_watch_ui(callback, user_id, _settings_search_text(user_settings), _settings_search_keyboard())
    await callback.answer()


@router.callback_query(F.data == "settings:watch")
async def cb_settings_watch(callback: CallbackQuery, state: FSMContext) -> None:
    if callback.message and callback.from_user:
        await _clear_settings_state_if_needed(state)
        user_id = await _upsert_user(callback.from_user.id, callback.from_user.username, callback.from_user.first_name)
        user_settings = await _get_user_settings(user_id)
        await _edit_or_replace_watch_ui(callback, user_id, _settings_watch_text(user_settings), _settings_watch_keyboard())
    await callback.answer()


@router.callback_query(F.data == "settings:search:format")
async def cb_settings_search_format(callback: CallbackQuery, state: FSMContext) -> None:
    if callback.message and callback.from_user:
        await _clear_settings_state_if_needed(state)
        user_id = await _upsert_user(callback.from_user.id, callback.from_user.username, callback.from_user.first_name)
        user_settings = await _get_user_settings(user_id)
        await _edit_or_replace_watch_ui(
            callback,
            user_id,
            "🔍 Настройки поиска\n\nВыберите формат выгрузки по умолчанию:",
            _settings_format_picker_keyboard("search", user_settings.search_output_format),
        )
    await callback.answer()


@router.callback_query(F.data == "settings:watch:format")
async def cb_settings_watch_format(callback: CallbackQuery, state: FSMContext) -> None:
    if callback.message and callback.from_user:
        await _clear_settings_state_if_needed(state)
        user_id = await _upsert_user(callback.from_user.id, callback.from_user.username, callback.from_user.first_name)
        user_settings = await _get_user_settings(user_id)
        await _edit_or_replace_watch_ui(
            callback,
            user_id,
            "🔔 Настройки подписок\n\nВыберите формат выгрузки по умолчанию:",
            _settings_format_picker_keyboard("watch", user_settings.watch_output_format),
        )
    await callback.answer()


@router.callback_query(F.data.startswith("settings:search:fmt:"))
async def cb_settings_search_format_save(callback: CallbackQuery) -> None:
    if callback.message and callback.from_user:
        _, database, _, _ = _require_services()
        output_format = (callback.data or "").split(":")[-1]
        if output_format not in SEARCH_FORMAT_LABELS:
            await callback.answer("Некорректный формат", show_alert=False)
            return
        user_id = await _upsert_user(callback.from_user.id, callback.from_user.username, callback.from_user.first_name)
        user_settings = await database.set_search_output_format(user_id, output_format, DEFAULT_WATCH_INTERVAL_MINUTES)
        await _edit_or_replace_watch_ui(callback, user_id, _settings_search_text(user_settings), _settings_search_keyboard())
    await callback.answer("Сохранено")


@router.callback_query(F.data.startswith("settings:watch:fmt:"))
async def cb_settings_watch_format_save(callback: CallbackQuery) -> None:
    if callback.message and callback.from_user:
        _, database, _, _ = _require_services()
        output_format = (callback.data or "").split(":")[-1]
        if output_format not in SEARCH_FORMAT_LABELS:
            await callback.answer("Некорректный формат", show_alert=False)
            return
        user_id = await _upsert_user(callback.from_user.id, callback.from_user.username, callback.from_user.first_name)
        user_settings = await database.set_watch_output_format(user_id, output_format, DEFAULT_WATCH_INTERVAL_MINUTES)
        await _edit_or_replace_watch_ui(callback, user_id, _settings_watch_text(user_settings), _settings_watch_keyboard())
    await callback.answer("Сохранено")


@router.callback_query(F.data == "settings:watch:period")
async def cb_settings_watch_period(callback: CallbackQuery, state: FSMContext) -> None:
    if callback.message and callback.from_user:
        await _clear_settings_state_if_needed(state)
        user_id = await _upsert_user(callback.from_user.id, callback.from_user.username, callback.from_user.first_name)
        user_settings = await _get_user_settings(user_id)
        await _edit_or_replace_watch_ui(
            callback,
            user_id,
            (
                "🔔 Настройки подписок\n\n"
                f"Текущий период: {_format_watch_interval_value(user_settings.watch_interval_minutes)}\n\n"
                "Выберите период проверки по умолчанию:"
            ),
            _settings_watch_period_keyboard(user_settings.watch_interval_minutes),
        )
    await callback.answer()


@router.callback_query(F.data.startswith("settings:watch:period:set:"))
async def cb_settings_watch_period_save(callback: CallbackQuery, state: FSMContext) -> None:
    if not callback.message or not callback.from_user:
        await callback.answer()
        return
    raw = (callback.data or "").split(":")[-1]
    try:
        interval_minutes = int(raw)
        _validate_watch_interval(interval_minutes)
    except ValueError:
        await callback.answer("Некорректный интервал", show_alert=False)
        return

    _, database, _, _ = _require_services()
    await state.clear()
    user_id = await _upsert_user(callback.from_user.id, callback.from_user.username, callback.from_user.first_name)
    user_settings = await database.set_watch_interval_minutes(user_id, interval_minutes, DEFAULT_WATCH_INTERVAL_MINUTES)
    await database.update_all_theme_watches_interval(user_id, interval_minutes)
    await _edit_or_replace_watch_ui(callback, user_id, _settings_watch_text(user_settings), _settings_watch_keyboard())
    await callback.answer("Сохранено")


@router.callback_query(F.data == "settings:watch:period:custom")
async def cb_settings_watch_period_custom(callback: CallbackQuery, state: FSMContext) -> None:
    if callback.message and callback.from_user:
        user_id = await _upsert_user(callback.from_user.id, callback.from_user.username, callback.from_user.first_name)
        await state.set_state(SettingsUiStates.waiting_watch_interval)
        await _edit_or_replace_watch_ui(
            callback,
            user_id,
            (
                "🔔 Настройки подписок\n\n"
                f"Введите период проверки в минутах.\n"
                f"Диапазон: {MIN_WATCH_INTERVAL_MINUTES}..{MAX_WATCH_INTERVAL_MINUTES}\n"
                "Пример: 45\n"
                "Для отмены: /cancel"
            ),
            _settings_watch_keyboard(),
        )
    await callback.answer()


@router.message(SettingsUiStates.waiting_watch_interval, F.text, ~F.text.startswith("/"))
async def st_settings_watch_interval(message: Message, state: FSMContext) -> None:
    user_id = await _ensure_user(message)
    raw = (message.text or "").strip()
    if not raw:
        await _send_watch_ui_message(message, user_id, "Введите число минут.", _settings_watch_keyboard())
        return
    if not raw.isdigit():
        await _send_watch_ui_message(
            message,
            user_id,
            "Период должен быть целым числом минут. Пример: 45",
            _settings_watch_keyboard(),
        )
        return

    interval_minutes = int(raw)
    try:
        _validate_watch_interval(interval_minutes)
    except ValueError as e:
        await _send_watch_ui_message(message, user_id, str(e), _settings_watch_keyboard())
        return

    _, database, _, _ = _require_services()
    user_settings = await database.set_watch_interval_minutes(user_id, interval_minutes, DEFAULT_WATCH_INTERVAL_MINUTES)
    await database.update_all_theme_watches_interval(user_id, interval_minutes)
    await state.clear()
    await _send_watch_ui_message(message, user_id, _settings_watch_text(user_settings), _settings_watch_keyboard())


@router.message(StateFilter(None), F.text == "Главное меню")
async def kb_home(message: Message) -> None:
    user_id = await _ensure_user(message)
    await _send_watch_ui_message(message, user_id, "Главное меню:", await _main_menu_for_user(user_id))


@router.message(StateFilter(None), F.text == "Помощь")
async def kb_help(message: Message) -> None:
    user_id = await _ensure_user(message)
    await _send_watch_ui_message(message, user_id, _help_text(), _help_keyboard())


@router.message(StateFilter(None), F.text == "Темы")
async def kb_themes(message: Message) -> None:
    user_id = await _ensure_user(message)
    await _send_themes_panel(message, user_id)


@router.message(StateFilter(None), F.text == "Подписки")
async def kb_watch(message: Message) -> None:
    user_id = await _ensure_user(message)
    await _send_watch_panel(message, user_id)


@router.message(StateFilter(None), F.text == "Поиск")
async def kb_search(message: Message) -> None:
    _, _, auth, _ = _require_services()
    user_id = await _ensure_user(message)
    if not await auth.is_authorized(user_id):
        await _send_watch_ui_message(message, user_id, "Сначала авторизуйте аккаунт через /auth.", await _main_menu_for_user(user_id))
        return
    user_settings = await _get_user_settings(user_id)
    await _send_watch_ui_message(
        message,
        user_id,
        "🔍 Быстрый поиск\n\n"
        f"📄 Формат по умолчанию: {_format_output_format_value(user_settings.search_output_format)}\n\n"
        "Дальше бот попросит выбрать:\n"
        "• тему или все чаты\n"
        "• лимит\n"
        "• период\n"
        "• режим комментариев",
        _search_menu_keyboard(),
    )


@router.message(StateFilter(None), F.text == "Авторизация")
async def kb_auth(message: Message) -> None:
    user = message.from_user
    if user is None:
        return
    await _send_qr(message, user.id, user.username, user.first_name, refresh=False)


@router.message(StateFilter(None), F.text == "Проверить QR")
async def kb_auth_check(message: Message, state: FSMContext) -> None:
    user = message.from_user
    if user is None:
        return
    await _check_auth(message, state, user.id, user.username, user.first_name)


@router.message(StateFilter(None), F.text == "Обновить QR")
async def kb_auth_refresh(message: Message) -> None:
    user = message.from_user
    if user is None:
        return
    await _send_qr(message, user.id, user.username, user.first_name, refresh=True)


@router.message(StateFilter(None), F.text == "Отменить поиск")
async def kb_cancel_search(message: Message) -> None:
    user_id = await _ensure_user(message)
    await _cancel_user_task(message.chat.id, user_id, message.bot)


@router.callback_query(F.data == "menu_watch")
async def cb_menu_watch(callback: CallbackQuery) -> None:
    if callback.message and callback.from_user:
        user_id = await _upsert_user(callback.from_user.id, callback.from_user.username, callback.from_user.first_name)
        text = await _build_watch_panel_text(user_id)
        await _edit_or_replace_watch_ui(callback, user_id, text, _watch_menu_keyboard())
    await callback.answer()


@router.callback_query(F.data == "watch:list")
async def cb_watch_list(callback: CallbackQuery) -> None:
    if callback.message and callback.from_user:
        user_id = await _upsert_user(callback.from_user.id, callback.from_user.username, callback.from_user.first_name)
        text = await _build_watch_panel_text(user_id)
        await _edit_or_replace_watch_ui(callback, user_id, text, _watch_menu_keyboard())
    await callback.answer()


@router.callback_query(F.data == "watch:help")
async def cb_watch_help(callback: CallbackQuery) -> None:
    if callback.message and callback.from_user:
        user_id = await _upsert_user(callback.from_user.id, callback.from_user.username, callback.from_user.first_name)
        await _edit_or_replace_watch_ui(callback, user_id, _watch_usage_text(), _watch_menu_keyboard())
    await callback.answer()


@router.callback_query(F.data == "watch:set")
async def cb_watch_set(callback: CallbackQuery, state: FSMContext) -> None:
    if not callback.message or not callback.from_user:
        await callback.answer()
        return

    _, database, auth, _ = _require_services()
    current_state = await state.get_state()
    if current_state and current_state.startswith(f"{WatchUiStates.__name__}:"):
        await state.clear()
    user_id = await _upsert_user(callback.from_user.id, callback.from_user.username, callback.from_user.first_name)
    if not await auth.is_authorized(user_id):
        await _edit_or_replace_watch_ui(
            callback,
            user_id,
            "Сначала авторизуйте аккаунт через /auth.",
            _watch_menu_keyboard(),
        )
        await callback.answer()
        return

    themes = await database.list_themes(user_id)
    if not themes:
        await _edit_or_replace_watch_ui(
            callback,
            user_id,
            "У вас нет тем. Сначала создайте тему в разделе 'Темы'.",
            _watch_menu_keyboard(),
        )
        await callback.answer()
        return

    user_settings = await _get_user_settings(user_id)
    await _edit_or_replace_watch_ui(
        callback,
        user_id,
        (
            "Выберите тему для автопроверки.\n"
            f"Период: {_format_watch_interval_value(user_settings.watch_interval_minutes)}\n"
            f"Формат: {_format_output_format_value(user_settings.watch_output_format)}"
        ),
        _watch_theme_picker_keyboard(themes, mode="set"),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("watch:set:th:"))
async def cb_watch_set_pick_theme(callback: CallbackQuery) -> None:
    if not callback.message or not callback.from_user:
        await callback.answer()
        return

    parts = (callback.data or "").split(":")
    if len(parts) != 4:
        await callback.answer("Некорректные данные", show_alert=False)
        return
    try:
        theme_id = int(parts[3])
    except ValueError:
        await callback.answer("Некорректная тема", show_alert=False)
        return

    _, database, _, _ = _require_services()
    user_id = await _upsert_user(callback.from_user.id, callback.from_user.username, callback.from_user.first_name)
    theme = await database.get_theme_by_id(user_id, theme_id)
    if theme is None:
        await _edit_or_replace_watch_ui(
            callback,
            user_id,
            "Тема не найдена или уже удалена.",
            _watch_menu_keyboard(),
        )
        await callback.answer()
        return

    user_settings = await _get_user_settings(user_id)
    active_theme_by_user[user_id] = theme.name
    watch = await database.set_theme_watch(
        user_id=user_id,
        theme_name=theme.name,
        chat_id=callback.message.chat.id,
        interval_minutes=user_settings.watch_interval_minutes,
    )
    panel_text = await _build_watch_panel_text(user_id)
    await _edit_or_replace_watch_ui(
        callback,
        user_id,
        (
            f"Автопроверка включена для темы '{watch.theme_name}'.\n"
            f"⏱ Период: {_format_watch_interval_value(watch.interval_minutes)}\n"
            f"📄 Формат: {_format_output_format_value(user_settings.watch_output_format)}\n"
            f"⏭ Следующая проверка: {_format_dt_utc(watch.next_check_at)}\n\n"
            f"{panel_text}"
        ),
        _watch_menu_keyboard(),
    )
    await callback.answer("Сохранено")


@router.callback_query(F.data.startswith("watch:set:custom:"))
async def cb_watch_set_custom_interval(callback: CallbackQuery, state: FSMContext) -> None:
    if not callback.message or not callback.from_user:
        await callback.answer()
        return
    await state.clear()
    user_id = await _upsert_user(callback.from_user.id, callback.from_user.username, callback.from_user.first_name)
    user_settings = await _get_user_settings(user_id)
    await _edit_or_replace_watch_ui(
        callback,
        user_id,
        (
            "Старая форма выбора периода больше не используется.\n\n"
            f"{_settings_watch_text(user_settings)}"
        ),
        _settings_watch_keyboard(),
    )
    await callback.answer("Открыты актуальные настройки")


@router.callback_query(F.data.startswith("watch:set:int:"))
async def cb_watch_set_interval(callback: CallbackQuery, state: FSMContext) -> None:
    if not callback.message or not callback.from_user:
        await callback.answer()
        return
    await state.clear()
    user_id = await _upsert_user(callback.from_user.id, callback.from_user.username, callback.from_user.first_name)
    user_settings = await _get_user_settings(user_id)
    await _edit_or_replace_watch_ui(
        callback,
        user_id,
        (
            "Старая форма выбора периода больше не используется.\n\n"
            f"{_settings_watch_text(user_settings)}"
        ),
        _settings_watch_keyboard(),
    )
    await callback.answer("Открыты актуальные настройки")


@router.message(WatchUiStates.waiting_custom_interval, F.text, ~F.text.startswith("/"))
async def st_watch_custom_interval(message: Message, state: FSMContext) -> None:
    user_id = await _ensure_user(message)
    _, database, auth, _ = _require_services()
    raw = (message.text or "").strip()

    if not raw:
        await _send_watch_ui_message(message, user_id, "Введите число минут.", _watch_menu_keyboard())
        return

    if raw.startswith("/cancel"):
        await state.clear()
        await _send_watch_ui_message(message, user_id, "Ввод периода отменен.", _watch_menu_keyboard())
        return

    if raw.startswith("/"):
        await _send_watch_ui_message(
            message,
            user_id,
            "Сейчас ожидается число минут. Для отмены используйте /cancel.",
            _watch_menu_keyboard(),
        )
        return

    if not raw.isdigit():
        await _send_watch_ui_message(
            message,
            user_id,
            "Период должен быть целым числом минут. Пример: 45",
            _watch_menu_keyboard(),
        )
        return

    interval_minutes = int(raw)
    try:
        _validate_watch_interval(interval_minutes)
    except ValueError as e:
        await _send_watch_ui_message(message, user_id, str(e), _watch_menu_keyboard())
        return

    if not await auth.is_authorized(user_id):
        await state.clear()
        await _send_watch_ui_message(message, user_id, "Сначала авторизуйте аккаунт через /auth.", _watch_menu_keyboard())
        return

    data = await state.get_data()
    theme_id = data.get("watch_theme_id")
    if not isinstance(theme_id, int):
        await state.clear()
        await _send_watch_ui_message(
            message,
            user_id,
            "Сессия выбора темы устарела. Откройте раздел 'Подписки' и выберите тему заново.",
            _watch_menu_keyboard(),
        )
        return

    theme = await database.get_theme_by_id(user_id, theme_id)
    if theme is None:
        await state.clear()
        await _send_watch_ui_message(message, user_id, "Тема не найдена или уже удалена.", _watch_menu_keyboard())
        return

    watch = await database.set_theme_watch(
        user_id=user_id,
        theme_name=theme.name,
        chat_id=message.chat.id,
        interval_minutes=interval_minutes,
    )
    active_theme_by_user[user_id] = theme.name
    await state.clear()

    panel_text = await _build_watch_panel_text(user_id)
    await _send_watch_ui_message(
        message,
        user_id,
        (
            f"Автопроверка включена для темы '{watch.theme_name}'.\n"
            f"Интервал: {watch.interval_minutes} мин.\n"
            f"Следующая проверка: {_format_dt_utc(watch.next_check_at)}\n\n"
            f"{panel_text}"
        ),
        _watch_menu_keyboard(),
    )


@router.callback_query(F.data == "watch:off")
async def cb_watch_off(callback: CallbackQuery) -> None:
    if not callback.message or not callback.from_user:
        await callback.answer()
        return

    _, database, _, _ = _require_services()
    user_id = await _upsert_user(callback.from_user.id, callback.from_user.username, callback.from_user.first_name)
    watches = await database.list_theme_watches(user_id)
    if not watches:
        await _edit_or_replace_watch_ui(
            callback,
            user_id,
            "У вас нет активных подписок для отключения.",
            _watch_menu_keyboard(),
        )
        await callback.answer()
        return

    await _edit_or_replace_watch_ui(
        callback,
        user_id,
        "Выберите подписку, которую нужно отключить:",
        _watch_off_picker_keyboard(watches),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("watch:off:th:"))
async def cb_watch_off_theme(callback: CallbackQuery) -> None:
    if not callback.message or not callback.from_user:
        await callback.answer()
        return

    parts = (callback.data or "").split(":")
    if len(parts) != 4:
        await callback.answer("Некорректные данные", show_alert=False)
        return
    try:
        theme_id = int(parts[3])
    except ValueError:
        await callback.answer("Некорректная тема", show_alert=False)
        return

    _, database, _, _ = _require_services()
    user_id = await _upsert_user(callback.from_user.id, callback.from_user.username, callback.from_user.first_name)
    theme = await database.get_theme_by_id(user_id, theme_id)
    if theme is None:
        await _edit_or_replace_watch_ui(
            callback,
            user_id,
            "Тема не найдена или уже удалена.",
            _watch_menu_keyboard(),
        )
        await callback.answer()
        return

    ok = await database.delete_theme_watch(user_id, theme.name)
    panel_text = await _build_watch_panel_text(user_id)
    status = f"Автопроверка для темы '{theme.name}' отключена." if ok else f"Для темы '{theme.name}' нет активной автопроверки."
    await _edit_or_replace_watch_ui(
        callback,
        user_id,
        f"{status}\n\n{panel_text}",
        _watch_menu_keyboard(),
    )
    await callback.answer("Готово")


async def _send_qr(
    message: Message,
    user_id: int,
    username: Optional[str],
    first_name: Optional[str],
    refresh: bool = False,
) -> None:
    await _upsert_user(user_id, username, first_name)
    _, _, auth, _ = _require_services()
    status = await (auth.refresh_qr(user_id) if refresh else auth.start_qr_auth(user_id))
    if status.status == "authorized":
        await _send_watch_ui_message(
            message,
            user_id,
            _format_auth_status_text(status.status, status.message),
            _main_menu(show_auth_actions=False),
            nav_show_auth_actions=False,
        )
        return
    if not status.qr_png:
        await _send_watch_ui_message(
            message,
            user_id,
            _format_auth_status_text(status.status, status.message),
            _auth_actions_menu(),
            nav_show_auth_actions=True,
        )
        return

    await _send_watch_ui_photo_message(
        message=message,
        user_id=user_id,
        photo=BufferedInputFile(status.qr_png, filename="qr.png"),
        caption=(
            f"{status.message}\n\n"
            "После сканирования нажмите «✅ Отсканировал».\n"
            "Если включена 2FA, бот сразу попросит пароль."
        ),
        reply_markup=_auth_actions_menu(),
        nav_show_auth_actions=True,
    )


@router.message(Command("auth"))
async def cmd_auth(message: Message) -> None:
    user = message.from_user
    if user is None:
        return
    await _send_qr(message, user.id, user.username, user.first_name, refresh=False)


@router.message(Command("auth_refresh"))
async def cmd_auth_refresh(message: Message) -> None:
    user = message.from_user
    if user is None:
        return
    await _send_qr(message, user.id, user.username, user.first_name, refresh=True)


@router.callback_query(F.data == "menu_auth")
async def cb_auth(callback: CallbackQuery) -> None:
    if callback.message and callback.from_user:
        await _send_qr(
            callback.message,
            callback.from_user.id,
            callback.from_user.username,
            callback.from_user.first_name,
            refresh=False,
        )
    await callback.answer()


@router.callback_query(F.data == "menu_auth_refresh")
async def cb_auth_refresh(callback: CallbackQuery) -> None:
    if callback.message and callback.from_user:
        await _send_qr(
            callback.message,
            callback.from_user.id,
            callback.from_user.username,
            callback.from_user.first_name,
            refresh=True,
        )
    await callback.answer()


async def _check_auth(
    message: Message,
    state: FSMContext,
    user_id: int,
    username: Optional[str],
    first_name: Optional[str],
) -> None:
    await _upsert_user(user_id, username, first_name)
    _, _, auth, _ = _require_services()
    result = await auth.check_qr(user_id)
    if result.status == "need_2fa":
        await state.set_state(AuthStates.waiting_2fa)
        await _send_watch_ui_message(
            message,
            user_id,
            "🔐 На аккаунте включен пароль 2FA.\n\nВведите пароль следующим сообщением.",
            _auth_actions_menu(),
            nav_show_auth_actions=True,
        )
        return
    await state.clear()
    await _send_watch_ui_message(
        message,
        user_id,
        _format_auth_status_text(result.status, result.message),
        _main_menu(show_auth_actions=False) if result.status == "authorized" else _auth_actions_menu(),
        nav_show_auth_actions=result.status != "authorized",
    )


async def _open_2fa_prompt(
    message: Message,
    state: FSMContext,
    user_id: int,
    username: Optional[str],
    first_name: Optional[str],
) -> None:
    await _upsert_user(user_id, username, first_name)
    _, _, auth, _ = _require_services()

    if await auth.is_authorized(user_id):
        await state.clear()
        await _send_watch_ui_message(
            message,
            user_id,
            "Вы уже авторизованы.",
            _main_menu(show_auth_actions=False),
            nav_show_auth_actions=False,
        )
        return

    if not auth.has_pending(user_id):
        await _send_watch_ui_message(
            message,
            user_id,
            "Нет активной авторизации. Нажмите «👤 Авторизация» и отсканируйте QR.",
            _main_menu(show_auth_actions=True),
            nav_show_auth_actions=True,
        )
        return

    await state.set_state(AuthStates.waiting_2fa)
    await _send_watch_ui_message(
        message,
        user_id,
        "🔐 Отправьте пароль 2FA следующим сообщением.\nДля отмены: /cancel",
        _auth_actions_menu(),
        nav_show_auth_actions=True,
    )


@router.message(Command("auth_check"))
async def cmd_auth_check(message: Message, state: FSMContext) -> None:
    user = message.from_user
    if user is None:
        return
    await _check_auth(message, state, user.id, user.username, user.first_name)


@router.message(Command("auth_2fa"))
async def cmd_auth_2fa(message: Message, state: FSMContext) -> None:
    user = message.from_user
    if user is None:
        return
    await _open_2fa_prompt(message, state, user.id, user.username, user.first_name)


@router.callback_query(F.data == "menu_auth_check")
async def cb_auth_check(callback: CallbackQuery, state: FSMContext) -> None:
    if callback.message and callback.from_user:
        await _check_auth(
            callback.message,
            state,
            callback.from_user.id,
            callback.from_user.username,
            callback.from_user.first_name,
        )
    await callback.answer()


@router.message(AuthStates.waiting_2fa)
async def handle_2fa_password(message: Message, state: FSMContext) -> None:
    user_id = await _ensure_user(message)
    _, _, auth, _ = _require_services()
    password = (message.text or "").strip()
    if not password:
        await _send_watch_ui_message(
            message,
            user_id,
            "Пароль пуст. Попробуйте снова.",
            _auth_actions_menu(),
            nav_show_auth_actions=True,
        )
        return
    result = await auth.submit_2fa(user_id, password)
    if result.status == "authorized":
        await state.clear()
    await _send_watch_ui_message(
        message,
        user_id,
        _format_auth_status_text(result.status, result.message),
        _main_menu(show_auth_actions=result.status != "authorized"),
        nav_show_auth_actions=result.status != "authorized",
    )


@router.message(Command("logout"))
async def cmd_logout(message: Message) -> None:
    user_id = await _ensure_user(message)
    _, _, auth, _ = _require_services()
    ok = await auth.logout(user_id)
    if ok:
        await _send_watch_ui_message(
            message,
            user_id,
            "Сессия удалена. Для входа снова используйте /auth.",
            _main_menu(show_auth_actions=True),
            nav_show_auth_actions=True,
        )
    else:
        await _send_watch_ui_message(
            message,
            user_id,
            "Не удалось удалить сессию или сессия уже отсутствует.",
            await _main_menu_for_user(user_id),
        )


@router.message(Command("themes"))
async def cmd_themes(message: Message) -> None:
    user_id = await _ensure_user(message)
    await _send_themes_panel(message, user_id)


@router.callback_query(F.data == "menu_themes")
async def cb_themes(callback: CallbackQuery) -> None:
    if callback.message and callback.from_user:
        user_id = await _upsert_user(callback.from_user.id, callback.from_user.username, callback.from_user.first_name)
        text = await _build_themes_panel_text(user_id)
        await _edit_or_replace_watch_ui(
            callback,
            user_id,
            text,
            await _themes_panel_markup_for_user(user_id),
        )
    await callback.answer()


@router.callback_query(F.data.startswith("themes:open:"))
async def cb_themes_open(callback: CallbackQuery) -> None:
    if not callback.message or not callback.from_user:
        await callback.answer()
        return

    parts = (callback.data or "").split(":")
    if len(parts) != 3:
        await callback.answer("Некорректные данные", show_alert=False)
        return
    try:
        theme_id = int(parts[2])
    except ValueError:
        await callback.answer("Некорректная тема", show_alert=False)
        return

    _, database, _, _ = _require_services()
    user_id = await _upsert_user(callback.from_user.id, callback.from_user.username, callback.from_user.first_name)
    theme = await database.get_theme_by_id(user_id, theme_id)
    if theme is None:
        await _edit_or_replace_watch_ui(
            callback,
            user_id,
            "Тема не найдена или уже удалена.",
            await _themes_panel_markup_for_user(user_id),
        )
        await callback.answer()
        return

    active_theme_by_user[user_id] = theme.name
    status_map = await _build_theme_status_map([theme], user_id)
    status_icon, status_text = status_map.get(theme.id, ("•", "без статуса"))
    await _edit_or_replace_watch_ui(
        callback,
        user_id,
        _format_theme_card(theme, status_icon=status_icon, status_text=status_text),
        _theme_detail_keyboard(theme.id),
    )
    await callback.answer()


@router.callback_query(F.data.regexp(r"^themes:keys:(add|del|clear):\d+$"))
async def cb_theme_keys(callback: CallbackQuery, state: FSMContext) -> None:
    if not callback.message or not callback.from_user:
        await callback.answer()
        return

    parts = (callback.data or "").split(":")
    if len(parts) < 3:
        await callback.answer("Некорректные данные", show_alert=False)
        return

    action = parts[2]
    if action not in {"add", "del", "clear"} or len(parts) != 4:
        await callback.answer("Некорректное действие", show_alert=False)
        return

    try:
        theme_id = int(parts[3])
    except ValueError:
        await callback.answer("Некорректная тема", show_alert=False)
        return

    _, database, _, _ = _require_services()
    user_id = await _upsert_user(callback.from_user.id, callback.from_user.username, callback.from_user.first_name)
    theme = await database.get_theme_by_id(user_id, theme_id)
    if theme is None:
        await _edit_or_replace_watch_ui(callback, user_id, "Тема не найдена или уже удалена.", await _themes_panel_markup_for_user(user_id))
        await callback.answer()
        return

    active_theme_by_user[user_id] = theme.name
    if action == "clear":
        removed = await database.clear_keywords(user_id, theme.name)
        theme = await database.get_theme_by_id(user_id, theme_id)
        if theme is None:
            await _edit_or_replace_watch_ui(callback, user_id, "Тема не найдена или уже удалена.", await _themes_panel_markup_for_user(user_id))
            await callback.answer()
            return
        await _edit_or_replace_watch_ui(
            callback,
            user_id,
            f"Ключевые слова очищены: {removed}.\n\n{_format_theme_keywords_screen(theme)}",
            _theme_keywords_keyboard(theme_id),
        )
        await callback.answer("Готово")
        return

    await state.set_state(ThemeUiStates.waiting_bulk_payload)
    await state.update_data(theme_ui_action="add_kw" if action == "add" else "del_kw", theme_name=theme.name)
    await _edit_or_replace_watch_ui(callback, user_id, _theme_action_prompt("add_kw" if action == "add" else "del_kw", theme.name), _theme_keywords_keyboard(theme.id))
    await callback.answer()

@router.callback_query(F.data.regexp(r"^themes:chats:(add|del|clear):\d+$"))
async def cb_theme_chats(callback: CallbackQuery, state: FSMContext) -> None:
    if not callback.message or not callback.from_user:
        await callback.answer()
        return

    parts = (callback.data or "").split(":")
    if len(parts) < 3:
        await callback.answer("Некорректные данные", show_alert=False)
        return

    action = parts[2]
    if action not in {"add", "del", "clear"} or len(parts) != 4:
        await callback.answer("Некорректное действие", show_alert=False)
        return

    try:
        theme_id = int(parts[3])
    except ValueError:
        await callback.answer("Некорректная тема", show_alert=False)
        return

    _, database, _, _ = _require_services()
    user_id = await _upsert_user(callback.from_user.id, callback.from_user.username, callback.from_user.first_name)
    theme = await database.get_theme_by_id(user_id, theme_id)
    if theme is None:
        await _edit_or_replace_watch_ui(callback, user_id, "Тема не найдена или уже удалена.", await _themes_panel_markup_for_user(user_id))
        await callback.answer()
        return

    active_theme_by_user[user_id] = theme.name
    if action == "clear":
        removed = await database.clear_chats(user_id, theme.name)
        theme = await database.get_theme_by_id(user_id, theme_id)
        if theme is None:
            await _edit_or_replace_watch_ui(callback, user_id, "Тема не найдена или уже удалена.", await _themes_panel_markup_for_user(user_id))
            await callback.answer()
            return
        await _edit_or_replace_watch_ui(
            callback,
            user_id,
            f"Чаты очищены: {removed}.\n\n{_format_theme_chats_screen(theme)}",
            _theme_chats_keyboard(theme_id),
        )
        await callback.answer("Готово")
        return

    await state.set_state(ThemeUiStates.waiting_bulk_payload)
    await state.update_data(theme_ui_action="add_chat" if action == "add" else "del_chat", theme_name=theme.name)
    markup = _theme_add_chat_keyboard(theme.id, back_callback=f"themes:chats:{theme.id}") if action == "add" else _theme_chats_keyboard(theme.id)
    await _edit_or_replace_watch_ui(callback, user_id, _theme_action_prompt("add_chat" if action == "add" else "del_chat", theme.name), markup)
    await callback.answer()


@router.callback_query(F.data.regexp(r"^themes:keys:\d+$"))
async def cb_theme_keys_view(callback: CallbackQuery) -> None:
    if not callback.message or not callback.from_user:
        await callback.answer()
        return

    try:
        theme_id = int((callback.data or "").split(":")[2])
    except ValueError:
        await callback.answer("Некорректная тема", show_alert=False)
        return

    _, database, _, _ = _require_services()
    user_id = await _upsert_user(callback.from_user.id, callback.from_user.username, callback.from_user.first_name)
    theme = await database.get_theme_by_id(user_id, theme_id)
    if theme is None:
        await _edit_or_replace_watch_ui(callback, user_id, "Тема не найдена или уже удалена.", await _themes_panel_markup_for_user(user_id))
        await callback.answer()
        return

    active_theme_by_user[user_id] = theme.name
    await _edit_or_replace_watch_ui(callback, user_id, _format_theme_keywords_screen(theme), _theme_keywords_keyboard(theme.id))
    await callback.answer()


@router.callback_query(F.data.regexp(r"^themes:chats:\d+$"))
async def cb_theme_chats_view(callback: CallbackQuery) -> None:
    if not callback.message or not callback.from_user:
        await callback.answer()
        return

    try:
        theme_id = int((callback.data or "").split(":")[2])
    except ValueError:
        await callback.answer("Некорректная тема", show_alert=False)
        return

    _, database, _, _ = _require_services()
    user_id = await _upsert_user(callback.from_user.id, callback.from_user.username, callback.from_user.first_name)
    theme = await database.get_theme_by_id(user_id, theme_id)
    if theme is None:
        await _edit_or_replace_watch_ui(callback, user_id, "Тема не найдена или уже удалена.", await _themes_panel_markup_for_user(user_id))
        await callback.answer()
        return

    active_theme_by_user[user_id] = theme.name
    await _edit_or_replace_watch_ui(callback, user_id, _format_theme_chats_screen(theme), _theme_chats_keyboard(theme.id))
    await callback.answer()


@router.callback_query(F.data.startswith("themes:delete:ask:"))
async def cb_theme_delete_ask(callback: CallbackQuery) -> None:
    if not callback.message or not callback.from_user:
        await callback.answer()
        return

    try:
        theme_id = int((callback.data or "").split(":")[3])
    except Exception:
        await callback.answer("Некорректная тема", show_alert=False)
        return

    _, database, _, _ = _require_services()
    user_id = await _upsert_user(callback.from_user.id, callback.from_user.username, callback.from_user.first_name)
    theme = await database.get_theme_by_id(user_id, theme_id)
    if theme is None:
        await _edit_or_replace_watch_ui(callback, user_id, "Тема не найдена или уже удалена.", await _themes_panel_markup_for_user(user_id))
        await callback.answer()
        return

    await _edit_or_replace_watch_ui(
        callback,
        user_id,
        (
            f"🗑️ Удаление темы '{theme.name}'\n\n"
            "Будут удалены ключи, чаты и связанные подписки.\n"
            "Подтвердите удаление."
        ),
        _theme_delete_confirm_keyboard(theme.id),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("themes:delete:yes:"))
async def cb_theme_delete_yes(callback: CallbackQuery) -> None:
    if not callback.message or not callback.from_user:
        await callback.answer()
        return

    try:
        theme_id = int((callback.data or "").split(":")[3])
    except Exception:
        await callback.answer("Некорректная тема", show_alert=False)
        return

    _, database, _, _ = _require_services()
    user_id = await _upsert_user(callback.from_user.id, callback.from_user.username, callback.from_user.first_name)
    theme = await database.get_theme_by_id(user_id, theme_id)
    if theme is None:
        await _edit_or_replace_watch_ui(callback, user_id, "Тема не найдена или уже удалена.", await _themes_panel_markup_for_user(user_id))
        await callback.answer()
        return

    ok = await database.delete_theme(user_id, theme.name)
    if ok:
        current = active_theme_by_user.get(user_id)
        if current and current.casefold() == theme.name.casefold():
            active_theme_by_user.pop(user_id, None)
        await _edit_or_replace_watch_ui(
            callback,
            user_id,
            f"Тема '{theme.name}' удалена.\n\n{await _build_themes_panel_text(user_id)}",
            await _themes_panel_markup_for_user(user_id),
        )
    else:
        await _edit_or_replace_watch_ui(callback, user_id, "Не удалось удалить тему.", _theme_delete_confirm_keyboard(theme.id))
    await callback.answer("Готово")

@router.callback_query(F.data == "themes:section:add")
async def cb_themes_section_add(callback: CallbackQuery) -> None:
    if callback.message and callback.from_user:
        user_id = await _upsert_user(callback.from_user.id, callback.from_user.username, callback.from_user.first_name)
        await _edit_or_replace_watch_ui(
            callback,
            user_id,
            f"Открыт актуальный экран «Темы».\n\n{await _build_themes_panel_text(user_id)}",
            await _themes_panel_markup_for_user(user_id),
        )
    await callback.answer()


@router.callback_query(F.data == "themes:section:manage")
async def cb_themes_section_manage(callback: CallbackQuery) -> None:
    if callback.message and callback.from_user:
        user_id = await _upsert_user(callback.from_user.id, callback.from_user.username, callback.from_user.first_name)
        await _edit_or_replace_watch_ui(
            callback,
            user_id,
            f"Открыт актуальный экран «Темы».\n\n{await _build_themes_panel_text(user_id)}",
            await _themes_panel_markup_for_user(user_id),
        )
    await callback.answer()


@router.callback_query(F.data == "themes:section:info")
async def cb_themes_section_info(callback: CallbackQuery) -> None:
    if callback.message and callback.from_user:
        user_id = await _upsert_user(callback.from_user.id, callback.from_user.username, callback.from_user.first_name)
        await _edit_or_replace_watch_ui(
            callback,
            user_id,
            f"Открыт актуальный экран «Темы».\n\n{await _build_themes_panel_text(user_id)}",
            await _themes_panel_markup_for_user(user_id),
        )
    await callback.answer()


@router.callback_query(F.data == "themes:list")
async def cb_themes_list(callback: CallbackQuery) -> None:
    if callback.message and callback.from_user:
        _, database, _, _ = _require_services()
        user_id = await _upsert_user(callback.from_user.id, callback.from_user.username, callback.from_user.first_name)
        themes = await database.list_themes(user_id)
        await _edit_or_replace_watch_ui(
            callback,
            user_id,
            _render_themes_compact_list(themes),
            _themes_back_keyboard(),
        )
    await callback.answer()


@router.callback_query(F.data == "themes:current")
async def cb_themes_current(callback: CallbackQuery) -> None:
    if callback.message and callback.from_user:
        user_id = await _upsert_user(callback.from_user.id, callback.from_user.username, callback.from_user.first_name)
        try:
            theme = await _resolve_theme_for_user(user_id, None)
            status_map = await _build_theme_status_map([theme], user_id)
            status_icon, status_text = status_map.get(theme.id, ("•", "без статуса"))
            await _edit_or_replace_watch_ui(
                callback,
                user_id,
                _format_theme_card(theme, status_icon=status_icon, status_text=status_text),
                _theme_detail_keyboard(theme.id),
            )
        except Exception as e:
            await _edit_or_replace_watch_ui(callback, user_id, str(e), _themes_back_keyboard())
    await callback.answer()


@router.callback_query(F.data == "themes:search_chats")
async def cb_themes_search_chats(callback: CallbackQuery) -> None:
    if callback.message and callback.from_user:
        user_id = await _upsert_user(callback.from_user.id, callback.from_user.username, callback.from_user.first_name)
        text = await _build_search_chats_text(user_id)
        await _edit_or_replace_watch_ui(callback, user_id, text, _themes_back_keyboard())
    await callback.answer()


@router.callback_query(F.data == "themes:new")
async def cb_themes_new(callback: CallbackQuery, state: FSMContext) -> None:
    if callback.message and callback.from_user:
        user_id = await _upsert_user(callback.from_user.id, callback.from_user.username, callback.from_user.first_name)
        await state.set_state(ThemeUiStates.waiting_create_theme_name)
        await _edit_or_replace_watch_ui(
            callback,
            user_id,
            "Введите имя новой темы следующим сообщением.\n"
            "Пример: news",
            _theme_wizard_keyboard(),
        )
    await callback.answer()


async def _add_all_account_chats_to_theme_via_callback(
    callback: CallbackQuery,
    user_id: int,
    theme: ThemeDTO,
    *,
    back_callback: Optional[str] = None,
) -> None:
    _, database, auth, search = _require_services()
    next_markup = _theme_add_chat_keyboard(theme.id, back_callback=back_callback)
    if not await auth.is_authorized(user_id):
        await _edit_or_replace_watch_ui(
            callback,
            user_id,
            "Сначала авторизуйте аккаунт через /auth.",
            next_markup,
        )
        return

    await _edit_or_replace_watch_ui(
        callback,
        user_id,
        f"Тема: {theme.name}\nСобираю все чаты аккаунта. Это может занять до минуты...",
        next_markup,
    )

    try:
        account_chat_refs = await search.list_account_chat_refs(user_id)
    except Exception as e:
        await _edit_or_replace_watch_ui(
            callback,
            user_id,
            f"Не удалось получить чаты аккаунта: {e}",
            next_markup,
        )
        return

    if not account_chat_refs:
        await _edit_or_replace_watch_ui(
            callback,
            user_id,
            "В аккаунте не найдено доступных чатов для добавления.",
            next_markup,
        )
        return

    try:
        added, total_unique = await database.add_chats_bulk(user_id, theme.name, account_chat_refs)
    except Exception as e:
        await _edit_or_replace_watch_ui(
            callback,
            user_id,
            f"Не удалось добавить чаты в тему: {e}",
            next_markup,
        )
        return

    active_theme_by_user[user_id] = theme.name
    theme_after = await database.get_theme(user_id, theme.name)
    total_in_theme = len(theme_after.chats) if theme_after else "?"
    await _edit_or_replace_watch_ui(
        callback,
        user_id,
        (
            f"Готово. Тема '{theme.name}'.\n"
            f"Найдено чатов в аккаунте: {len(account_chat_refs)}\n"
            f"Уникальных к обработке: {total_unique}\n"
            f"Добавлено новых в тему: {added}\n"
            f"Всего чатов в теме: {total_in_theme}"
        ),
        next_markup,
    )


@router.callback_query(F.data == "themes:add_all_chats")
async def cb_themes_add_all_chats(callback: CallbackQuery) -> None:
    if not callback.message or not callback.from_user:
        await callback.answer()
        return

    _, database, auth, _ = _require_services()
    user_id = await _upsert_user(callback.from_user.id, callback.from_user.username, callback.from_user.first_name)
    if not await auth.is_authorized(user_id):
        await _edit_or_replace_watch_ui(
            callback,
            user_id,
            "Сначала авторизуйте аккаунт через /auth.",
            await _themes_panel_markup_for_user(user_id),
        )
        await callback.answer()
        return

    themes = await database.list_themes(user_id)
    if not themes:
        await _edit_or_replace_watch_ui(
            callback,
            user_id,
            "Тем пока нет. Сначала создайте тему.",
            await _themes_panel_markup_for_user(user_id),
        )
        await callback.answer()
        return

    active_theme = await _get_active_theme_for_user(user_id)
    if active_theme is not None:
        await callback.answer("Запускаю импорт чатов...", show_alert=False)
        await _add_all_account_chats_to_theme_via_callback(
            callback,
            user_id,
            active_theme,
            back_callback=f"themes:chats:{active_theme.id}",
        )
        return

    if len(themes) == 1:
        await callback.answer("Запускаю импорт чатов...", show_alert=False)
        await _add_all_account_chats_to_theme_via_callback(
            callback,
            user_id,
            themes[0],
            back_callback=f"themes:chats:{themes[0].id}",
        )
        return

    await _edit_or_replace_watch_ui(
        callback,
        user_id,
        (
            "Старая форма выбора больше не используется.\n\n"
            "Откройте нужную тему и нажмите «💬 Изменить чаты»."
        ),
        await _themes_panel_markup_for_user(user_id),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("themes:add_all_chats:th:"))
async def cb_themes_add_all_chats_theme(callback: CallbackQuery) -> None:
    if not callback.message or not callback.from_user:
        await callback.answer()
        return

    parts = (callback.data or "").split(":")
    if len(parts) != 4:
        await callback.answer("Некорректные данные", show_alert=False)
        return

    try:
        theme_id = int(parts[3])
    except ValueError:
        await callback.answer("Некорректная тема", show_alert=False)
        return

    _, database, _, _ = _require_services()
    user_id = await _upsert_user(callback.from_user.id, callback.from_user.username, callback.from_user.first_name)
    theme = await database.get_theme_by_id(user_id, theme_id)
    if theme is None:
        await _edit_or_replace_watch_ui(
            callback,
            user_id,
            "Тема не найдена или уже удалена.",
            await _themes_panel_markup_for_user(user_id),
        )
        await callback.answer()
        return

    await callback.answer("Запускаю импорт чатов...", show_alert=False)
    await _add_all_account_chats_to_theme_via_callback(
        callback,
        user_id,
        theme,
        back_callback=f"themes:chats:{theme.id}",
    )


@router.callback_query(F.data.startswith("themes:quick:"))
async def cb_themes_quick(callback: CallbackQuery, state: FSMContext) -> None:
    if not callback.message or not callback.from_user:
        await callback.answer()
        return

    parts = (callback.data or "").split(":")
    if len(parts) != 3:
        await callback.answer("Некорректный callback", show_alert=False)
        return

    action_code = parts[2]
    if action_code not in {"ac", "ak"}:
        await callback.answer("Неизвестное действие", show_alert=False)
        return
    action = "add_chat" if action_code == "ac" else "add_kw"

    _, database, _, _ = _require_services()
    user_id = await _upsert_user(callback.from_user.id, callback.from_user.username, callback.from_user.first_name)
    themes = await database.list_themes(user_id)
    if not themes:
        await _edit_or_replace_watch_ui(
            callback,
            user_id,
            "Сначала создайте тему: нажмите 'Создать тему'.",
            await _themes_panel_markup_for_user(user_id),
        )
        await callback.answer()
        return

    active_theme = await _get_active_theme_for_user(user_id)
    if active_theme is not None:
        active_theme_by_user[user_id] = active_theme.name
        await state.set_state(ThemeUiStates.waiting_bulk_payload)
        await state.update_data(theme_ui_action=action, theme_name=active_theme.name)
        next_markup = _theme_add_chat_keyboard(active_theme.id, back_callback=f"themes:chats:{active_theme.id}") if action == "add_chat" else _theme_keywords_keyboard(active_theme.id)
        await _edit_or_replace_watch_ui(callback, user_id, _theme_action_prompt(action, active_theme.name), next_markup)
        await callback.answer()
        return

    if len(themes) == 1:
        theme = themes[0]
        active_theme_by_user[user_id] = theme.name
        await state.set_state(ThemeUiStates.waiting_bulk_payload)
        await state.update_data(theme_ui_action=action, theme_name=theme.name)
        next_markup = _theme_add_chat_keyboard(theme.id, back_callback=f"themes:chats:{theme.id}") if action == "add_chat" else _theme_keywords_keyboard(theme.id)
        await _edit_or_replace_watch_ui(callback, user_id, _theme_action_prompt(action, theme.name), next_markup)
        await callback.answer()
        return

    await _edit_or_replace_watch_ui(
        callback,
        user_id,
        (
            "Старая форма быстрого выбора темы больше не используется.\n\n"
            "Выберите тему в списке и откройте нужное действие внутри неё."
        ),
        await _themes_panel_markup_for_user(user_id),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("themes:pick:"))
async def cb_themes_pick(callback: CallbackQuery) -> None:
    if not callback.message or not callback.from_user:
        await callback.answer()
        return

    parts = (callback.data or "").split(":")
    if len(parts) != 3:
        await callback.answer("Некорректный callback", show_alert=False)
        return

    action_code = parts[2]
    action = THEME_UI_ACTION_MAP.get(action_code)
    if action is None:
        await callback.answer("Неизвестное действие", show_alert=False)
        return

    _, database, _, _ = _require_services()
    user_id = await _upsert_user(callback.from_user.id, callback.from_user.username, callback.from_user.first_name)
    themes = await database.list_themes(user_id)
    if not themes:
        await _edit_or_replace_watch_ui(callback, user_id, "Тем пока нет. Сначала создайте тему.", await _themes_panel_markup_for_user(user_id))
        await callback.answer()
        return

    await _edit_or_replace_watch_ui(
        callback,
        user_id,
        (
            "Старая форма выбора темы больше не используется.\n\n"
            "Выберите тему в текущем списке и выполните действие внутри карточки темы."
        ),
        await _themes_panel_markup_for_user(user_id),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("themes:do:"))
async def cb_themes_do(callback: CallbackQuery, state: FSMContext) -> None:
    if not callback.message or not callback.from_user:
        await callback.answer()
        return

    parts = (callback.data or "").split(":")
    if len(parts) != 4:
        await callback.answer("Некорректный callback", show_alert=False)
        return

    action_code = parts[2]
    action = THEME_UI_ACTION_MAP.get(action_code)
    if action is None:
        await callback.answer("Неизвестное действие", show_alert=False)
        return

    try:
        theme_id = int(parts[3])
    except ValueError:
        await callback.answer("Некорректная тема", show_alert=False)
        return

    _, database, _, _ = _require_services()
    user_id = await _upsert_user(callback.from_user.id, callback.from_user.username, callback.from_user.first_name)
    theme = await database.get_theme_by_id(user_id, theme_id)
    if theme is None:
        await _edit_or_replace_watch_ui(callback, user_id, "Тема не найдена или уже удалена.", await _themes_panel_markup_for_user(user_id))
        await callback.answer()
        return

    if action == "set_active":
        active_theme_by_user[user_id] = theme.name
        status_map = await _build_theme_status_map([theme], user_id)
        status_icon, status_text = status_map.get(theme.id, ("•", "без статуса"))
        await _edit_or_replace_watch_ui(
            callback,
            user_id,
            _format_theme_card(theme, status_icon=status_icon, status_text=status_text),
            _theme_detail_keyboard(theme.id),
        )
        await callback.answer()
        return

    if action == "delete_theme":
        await _edit_or_replace_watch_ui(
            callback,
            user_id,
            (
                f"🗑️ Удаление темы '{theme.name}'\n\n"
                "Будут удалены ключи, чаты и связанные подписки.\n"
                "Подтвердите удаление."
            ),
            _theme_delete_confirm_keyboard(theme.id),
        )
        await callback.answer()
        return

    active_theme_by_user[user_id] = theme.name
    await state.set_state(ThemeUiStates.waiting_bulk_payload)
    await state.update_data(theme_ui_action=action, theme_name=theme.name)
    if action in {"add_chat", "del_chat"}:
        next_markup = _theme_chats_keyboard(theme.id)
    else:
        next_markup = _theme_keywords_keyboard(theme.id)
    await _edit_or_replace_watch_ui(callback, user_id, _theme_action_prompt(action, theme.name), next_markup)
    await callback.answer()


@router.message(ThemeUiStates.waiting_create_theme_name, F.text, ~F.text.startswith("/"))
async def st_theme_create_name(message: Message, state: FSMContext) -> None:
    user_id = await _ensure_user(message)
    _, database, _, _ = _require_services()
    name = (message.text or "").strip()
    if not name:
        await _send_watch_ui_message(message, user_id, "Имя темы пустое. Введите корректное имя.", _theme_wizard_keyboard())
        return

    try:
        theme = await database.create_theme(user_id, name)
    except Exception as e:
        await _send_watch_ui_message(message, user_id, f"Не удалось создать тему: {e}", _theme_wizard_keyboard())
        return

    active_theme_by_user[user_id] = theme.name
    await state.set_state(ThemeUiStates.waiting_new_theme_keywords)
    await state.update_data(theme_name=theme.name, wizard_kw_added=0)
    await _send_watch_ui_message(
        message,
        user_id,
        _theme_wizard_keywords_prompt(theme.name),
        _theme_wizard_keyboard(),
    )


@router.message(ThemeUiStates.waiting_new_theme_keywords, F.text.startswith("/skip"))
async def st_theme_wizard_keywords_skip(message: Message, state: FSMContext) -> None:
    user_id = await _ensure_user(message)
    data = await state.get_data()
    theme_name = data.get("theme_name")
    if not theme_name:
        await state.clear()
        await _send_watch_ui_message(
            message,
            user_id,
            "Сессия создания темы устарела. Откройте раздел 'Темы' и начните заново.",
            _theme_wizard_keyboard(),
        )
        return
    await state.set_state(ThemeUiStates.waiting_new_theme_chats)
    await state.update_data(theme_name=theme_name, wizard_kw_added=0)
    await _send_watch_ui_message(message, user_id, _theme_wizard_chats_prompt(theme_name), _theme_wizard_keyboard())


@router.message(ThemeUiStates.waiting_new_theme_keywords, F.text, ~F.text.startswith("/"))
async def st_theme_wizard_keywords_input(message: Message, state: FSMContext) -> None:
    user_id = await _ensure_user(message)
    _, database, _, _ = _require_services()
    text = (message.text or "").strip()
    if not text:
        await _send_watch_ui_message(message, user_id, "Пустой ввод. Отправьте ключевые слова списком.", _theme_wizard_keyboard())
        return

    data = await state.get_data()
    theme_name = data.get("theme_name")
    if not theme_name:
        await state.clear()
        await _send_watch_ui_message(
            message,
            user_id,
            "Сессия создания темы устарела. Откройте раздел 'Темы' и начните заново.",
            _theme_wizard_keyboard(),
        )
        return

    theme_before = await database.get_theme(user_id, theme_name)
    if theme_before is None:
        await state.clear()
        await _send_watch_ui_message(message, user_id, f"Тема '{theme_name}' не найдена.", _theme_wizard_keyboard())
        return

    items = _split_bulk_items(text, allow_comma=True)
    if not items:
        await _send_watch_ui_message(message, user_id, "Не удалось распознать ключевые слова. Проверьте формат.", _theme_wizard_keyboard())
        return

    try:
        for item in items:
            await database.add_keyword(user_id, theme_before.name, item)
    except Exception as e:
        await _send_watch_ui_message(message, user_id, f"Не удалось добавить ключевые слова: {e}", _theme_wizard_keyboard())
        return

    theme_after = await database.get_theme(user_id, theme_before.name)
    kw_added = 0
    if theme_after is not None:
        kw_added = max(0, len(theme_after.keywords) - len(theme_before.keywords))

    await state.set_state(ThemeUiStates.waiting_new_theme_chats)
    await state.update_data(theme_name=theme_before.name, wizard_kw_added=kw_added)
    await _send_watch_ui_message(
        message,
        user_id,
        f"Ключевые слова сохранены: +{kw_added} (введено: {len(items)}).\n\n{_theme_wizard_chats_prompt(theme_before.name)}",
        _theme_wizard_keyboard(),
    )


@router.message(ThemeUiStates.waiting_new_theme_chats, F.text.startswith("/skip"))
async def st_theme_wizard_chats_skip(message: Message, state: FSMContext) -> None:
    user_id = await _ensure_user(message)
    _, database, _, _ = _require_services()
    data = await state.get_data()
    theme_name = data.get("theme_name")
    kw_added = int(data.get("wizard_kw_added") or 0)
    await state.clear()
    summary = await _build_themes_panel_text(user_id)
    themes = await database.list_themes(user_id)
    await _send_watch_ui_message(
        message,
        user_id,
        (
            f"Готово. Тема '{theme_name or '(неизвестно)'}' настроена.\n"
            f"Ключевых слов добавлено: {kw_added}\n"
            "Чатов добавлено: 0\n\n"
            f"{summary}"
        ),
        await _themes_panel_markup_for_user(user_id),
    )


@router.message(ThemeUiStates.waiting_new_theme_chats, F.text, ~F.text.startswith("/"))
async def st_theme_wizard_chats_input(message: Message, state: FSMContext) -> None:
    user_id = await _ensure_user(message)
    _, database, _, _ = _require_services()
    text = (message.text or "").strip()
    if not text:
        await _send_watch_ui_message(message, user_id, "Пустой ввод. Отправьте чаты списком.", _theme_wizard_keyboard())
        return

    data = await state.get_data()
    theme_name = data.get("theme_name")
    kw_added = int(data.get("wizard_kw_added") or 0)
    if not theme_name:
        await state.clear()
        await _send_watch_ui_message(
            message,
            user_id,
            "Сессия создания темы устарела. Откройте раздел 'Темы' и начните заново.",
            _theme_wizard_keyboard(),
        )
        return

    theme_before = await database.get_theme(user_id, theme_name)
    if theme_before is None:
        await state.clear()
        await _send_watch_ui_message(message, user_id, f"Тема '{theme_name}' не найдена.", _theme_wizard_keyboard())
        return

    items = _split_bulk_items(text, allow_comma=False)
    if not items:
        await _send_watch_ui_message(message, user_id, "Не удалось распознать чаты. Проверьте формат.", _theme_wizard_keyboard())
        return

    try:
        for item in items:
            await database.add_chat(user_id, theme_before.name, item)
    except Exception as e:
        await _send_watch_ui_message(message, user_id, f"Не удалось добавить чаты: {e}", _theme_wizard_keyboard())
        return

    theme_after = await database.get_theme(user_id, theme_before.name)
    chat_added = 0
    if theme_after is not None:
        chat_added = max(0, len(theme_after.chats) - len(theme_before.chats))

    await state.clear()
    summary = await _build_themes_panel_text(user_id)
    themes = await database.list_themes(user_id)
    await _send_watch_ui_message(
        message,
        user_id,
        (
            f"Готово. Тема '{theme_before.name}' настроена.\n"
            f"Ключевых слов добавлено: {kw_added}\n"
            f"Чатов добавлено: {chat_added} (введено: {len(items)})\n\n"
            f"{summary}"
        ),
        await _themes_panel_markup_for_user(user_id),
    )


@router.message(ThemeUiStates.waiting_bulk_payload, F.text, ~F.text.startswith("/"))
async def st_theme_bulk_payload(message: Message, state: FSMContext) -> None:
    user_id = await _ensure_user(message)
    _, database, _, _ = _require_services()
    text = (message.text or "").strip()
    if not text:
        await _send_watch_ui_message(message, user_id, "Пустой ввод. Отправьте значения списком.", _themes_menu_keyboard())
        return

    data = await state.get_data()
    action = data.get("theme_ui_action")
    theme_name = data.get("theme_name")
    if action not in {"add_kw", "del_kw", "add_chat", "del_chat"} or not theme_name:
        await state.clear()
        await _send_watch_ui_message(
            message,
            user_id,
            "Сессия редактирования устарела. Откройте меню тем заново.",
            _themes_menu_keyboard(),
        )
        return

    theme = await database.get_theme(user_id, theme_name)
    if theme is None:
        await state.clear()
        await _send_watch_ui_message(message, user_id, f"Тема '{theme_name}' не найдена.", _themes_menu_keyboard())
        return

    allow_comma = action in {"add_kw", "del_kw"}
    items = _split_bulk_items(text, allow_comma=allow_comma)
    if not items:
        await _send_watch_ui_message(message, user_id, "Не удалось распознать значения. Проверьте формат.", _themes_menu_keyboard())
        return

    try:
        if action == "add_kw":
            for item in items:
                await database.add_keyword(user_id, theme.name, item)
            result_text = f"Добавлено ключевых слов: {len(items)} в тему '{theme.name}'."
        elif action == "del_kw":
            removed = 0
            for item in items:
                ok = await database.remove_keyword(user_id, theme.name, item)
                if ok:
                    removed += 1
            result_text = f"Удалено ключевых слов: {removed}/{len(items)} из темы '{theme.name}'."
        elif action == "add_chat":
            for item in items:
                await database.add_chat(user_id, theme.name, item)
            result_text = f"Добавлено чатов: {len(items)} в тему '{theme.name}'."
        else:
            removed = 0
            for item in items:
                ok = await database.remove_chat(user_id, theme.name, item)
                if ok:
                    removed += 1
            result_text = f"Удалено чатов: {removed}/{len(items)} из темы '{theme.name}'."
    except Exception as e:
        await _send_watch_ui_message(message, user_id, f"Ошибка обработки: {e}", _themes_menu_keyboard())
        return

    active_theme_by_user[user_id] = theme.name
    await state.clear()
    updated_theme = await database.get_theme(user_id, theme.name)
    if updated_theme is None:
        await _send_watch_ui_message(message, user_id, result_text, await _themes_panel_markup_for_user(user_id))
        return
    if action in {"add_kw", "del_kw"}:
        next_text = f"{result_text}\n\n{_format_theme_keywords_screen(updated_theme)}"
        next_markup = _theme_keywords_keyboard(updated_theme.id)
    else:
        next_text = f"{result_text}\n\n{_format_theme_chats_screen(updated_theme)}"
        next_markup = _theme_chats_keyboard(updated_theme.id)
    await _send_watch_ui_message(
        message,
        user_id,
        next_text,
        next_markup,
    )


@router.message(Command("theme_new"))
async def cmd_theme_new(message: Message, state: FSMContext) -> None:
    user_id = await _ensure_user(message)
    _, database, _, _ = _require_services()
    name = _get_command_body(message.text or "")
    if not name:
        await _send_watch_ui_message(message, user_id, "Использование: /theme_new <name>", _themes_menu_keyboard())
        return
    try:
        theme = await database.create_theme(user_id, name)
        active_theme_by_user[user_id] = theme.name
        await state.set_state(ThemeUiStates.waiting_new_theme_keywords)
        await state.update_data(theme_name=theme.name, wizard_kw_added=0)
        await _send_watch_ui_message(message, user_id, _theme_wizard_keywords_prompt(theme.name), _themes_menu_keyboard())
    except Exception as e:
        await _send_watch_ui_message(message, user_id, f"Не удалось создать тему: {e}", _themes_menu_keyboard())


@router.message(Command("theme_delete"))
async def cmd_theme_delete(message: Message) -> None:
    user_id = await _ensure_user(message)
    _, database, _, _ = _require_services()
    name = _get_command_body(message.text or "")
    if not name:
        await message.answer("Использование: /theme_delete <name>")
        return
    ok = await database.delete_theme(user_id, name)
    if ok:
        current = active_theme_by_user.get(user_id)
        if current and current.casefold() == name.casefold():
            active_theme_by_user.pop(user_id, None)
    await message.answer("Тема удалена." if ok else "Тема не найдена.")


@router.message(Command("theme_use"))
async def cmd_theme_use(message: Message) -> None:
    user_id = await _ensure_user(message)
    name = _get_command_body(message.text or "")
    if not name:
        await message.answer("Использование: /theme_use <name>")
        return
    try:
        theme = await _resolve_theme_for_user(user_id, name)
        await message.answer(f"Активная тема: {theme.name}")
    except Exception as e:
        await message.answer(str(e))


@router.message(Command("theme_current"))
async def cmd_theme_current(message: Message) -> None:
    user_id = await _ensure_user(message)
    try:
        theme = await _resolve_theme_for_user(user_id, None)
        await message.answer(f"Активная тема: {theme.name}")
    except Exception as e:
        await message.answer(str(e))


@router.message(Command("theme_show"))
async def cmd_theme_show(message: Message) -> None:
    user_id = await _ensure_user(message)
    provided = _get_command_body(message.text or "") or None
    try:
        theme = await _resolve_theme_for_user(user_id, provided)
        await message.answer(_format_theme(theme))
    except Exception as e:
        await message.answer(str(e))


@router.message(Command("search_chats"))
async def cmd_search_chats(message: Message) -> None:
    user_id = await _ensure_user(message)
    text = await _build_search_chats_text(user_id)
    await _send_watch_ui_message(message, user_id, text, _themes_menu_keyboard())


@router.message(Command("theme_add_chat"))
async def cmd_theme_add_chat(message: Message) -> None:
    user_id = await _ensure_user(message)
    _, database, _, _ = _require_services()
    try:
        theme, chat_refs = await _resolve_theme_and_value_from_body(
            user_id,
            _get_command_body(message.text or ""),
            allow_multiple=True,
            allow_comma=False,
        )
    except Exception:
        await message.answer(
            "Использование: /theme_add_chat [theme] <chat_ref>\n"
            "Массово: /theme_add_chat [theme] ref1; ref2; ref3"
        )
        return
    try:
        for chat_ref in chat_refs:
            await database.add_chat(user_id, theme.name, chat_ref)
        active_theme_by_user[user_id] = theme.name
        await message.answer(f"Добавлено чатов: {len(chat_refs)} в тему '{theme.name}'.")
    except Exception as e:
        await message.answer(f"Не удалось добавить чат: {e}")


@router.message(Command("theme_del_chat"))
async def cmd_theme_del_chat(message: Message) -> None:
    user_id = await _ensure_user(message)
    _, database, _, _ = _require_services()
    try:
        theme, chat_refs = await _resolve_theme_and_value_from_body(
            user_id,
            _get_command_body(message.text or ""),
            allow_multiple=True,
            allow_comma=False,
        )
    except Exception:
        await message.answer(
            "Использование: /theme_del_chat [theme] <chat_ref>\n"
            "Массово: /theme_del_chat [theme] ref1; ref2; ref3"
        )
        return
    try:
        removed = 0
        for chat_ref in chat_refs:
            ok = await database.remove_chat(user_id, theme.name, chat_ref)
            if ok:
                removed += 1
        active_theme_by_user[user_id] = theme.name
        await message.answer(f"Удалено чатов: {removed}/{len(chat_refs)} из темы '{theme.name}'.")
    except Exception as e:
        await message.answer(f"Не удалось удалить чат: {e}")


@router.message(Command("theme_add_kw"))
async def cmd_theme_add_kw(message: Message) -> None:
    user_id = await _ensure_user(message)
    _, database, _, _ = _require_services()
    try:
        theme, keywords = await _resolve_theme_and_value_from_body(
            user_id,
            _get_command_body(message.text or ""),
            allow_multiple=True,
            allow_comma=True,
        )
    except Exception:
        await message.answer(
            "Использование: /theme_add_kw [theme] <keyword>\n"
            "Массово: /theme_add_kw [theme] kw1; kw2; kw3"
        )
        return
    try:
        for keyword in keywords:
            await database.add_keyword(user_id, theme.name, keyword)
        active_theme_by_user[user_id] = theme.name
        await message.answer(f"Добавлено ключевых слов: {len(keywords)} в тему '{theme.name}'.")
    except Exception as e:
        await message.answer(f"Не удалось добавить ключевое слово: {e}")


@router.message(Command("theme_del_kw"))
async def cmd_theme_del_kw(message: Message) -> None:
    user_id = await _ensure_user(message)
    _, database, _, _ = _require_services()
    try:
        theme, keywords = await _resolve_theme_and_value_from_body(
            user_id,
            _get_command_body(message.text or ""),
            allow_multiple=True,
            allow_comma=True,
        )
    except Exception:
        await message.answer(
            "Использование: /theme_del_kw [theme] <keyword>\n"
            "Массово: /theme_del_kw [theme] kw1; kw2; kw3"
        )
        return
    try:
        removed = 0
        for keyword in keywords:
            ok = await database.remove_keyword(user_id, theme.name, keyword)
            if ok:
                removed += 1
        active_theme_by_user[user_id] = theme.name
        await message.answer(f"Удалено ключевых слов: {removed}/{len(keywords)} из темы '{theme.name}'.")
    except Exception as e:
        await message.answer(f"Не удалось удалить ключевое слово: {e}")


@router.message(Command("watch_set"))
async def cmd_watch_set(message: Message, state: FSMContext) -> None:
    user_id = await _ensure_user(message)
    current_state = await state.get_state()
    if current_state and current_state.startswith(f"{WatchUiStates.__name__}:"):
        await state.clear()
    _, database, auth, _ = _require_services()
    if not await auth.is_authorized(user_id):
        await _send_watch_ui_message(message, user_id, "Сначала авторизуйте аккаунт через /auth.", _watch_menu_keyboard())
        return

    try:
        theme_name, interval_minutes = _parse_watch_set_command(_get_command_body(message.text or ""))
    except Exception as e:
        await _send_watch_ui_message(message, user_id, f"{e}\n\n{_watch_usage_text()}", _watch_menu_keyboard())
        return

    try:
        theme = await _resolve_theme_for_user(user_id, theme_name)
        watch = await database.set_theme_watch(
            user_id=user_id,
            theme_name=theme.name,
            chat_id=message.chat.id,
            interval_minutes=interval_minutes,
        )
        active_theme_by_user[user_id] = theme.name
        panel_text = await _build_watch_panel_text(user_id)
        await _send_watch_ui_message(
            message,
            user_id,
            f"Автопроверка включена для темы '{watch.theme_name}'.\n"
            f"Интервал: {watch.interval_minutes} мин.\n"
            f"Следующая проверка: {_format_dt_utc(watch.next_check_at)}\n\n"
            f"{panel_text}",
            _watch_menu_keyboard(),
        )
    except Exception as e:
        await _send_watch_ui_message(message, user_id, f"Не удалось включить автопроверку: {e}", _watch_menu_keyboard())


@router.message(Command("watch_off"))
async def cmd_watch_off(message: Message) -> None:
    user_id = await _ensure_user(message)
    _, database, _, _ = _require_services()
    provided_theme = _get_command_body(message.text or "") or None
    try:
        theme = await _resolve_theme_for_user(user_id, provided_theme)
    except Exception as e:
        await _send_watch_ui_message(message, user_id, f"{e}\n\n{_watch_usage_text()}", _watch_menu_keyboard())
        return

    ok = await database.delete_theme_watch(user_id, theme.name)
    panel_text = await _build_watch_panel_text(user_id)
    if ok:
        await _send_watch_ui_message(
            message,
            user_id,
            f"Автопроверка для темы '{theme.name}' отключена.\n\n{panel_text}",
            _watch_menu_keyboard(),
        )
    else:
        await _send_watch_ui_message(
            message,
            user_id,
            f"Для темы '{theme.name}' нет активной автопроверки.\n\n{panel_text}",
            _watch_menu_keyboard(),
        )


@router.message(Command("watch_list"))
async def cmd_watch_list(message: Message) -> None:
    user_id = await _ensure_user(message)
    await _send_watch_panel(message, user_id)


@router.callback_query(F.data == "menu_search")
async def cb_search(callback: CallbackQuery) -> None:
    if callback.message and callback.from_user:
        _, _, auth, _ = _require_services()
        user_id = await _upsert_user(callback.from_user.id, callback.from_user.username, callback.from_user.first_name)
        is_auth = await auth.is_authorized(user_id)
        if not is_auth:
            await _edit_or_replace_watch_ui(
                callback,
                user_id,
                "Сначала подключите аккаунт в разделе «👤 Авторизация».",
                _main_menu(),
            )
            await callback.answer()
            return

        user_settings = await _get_user_settings(user_id)
        await _edit_or_replace_watch_ui(
            callback,
            user_id,
            "🔍 Поиск\n\n"
            f"📄 Формат по умолчанию: {_format_output_format_value(user_settings.search_output_format)}\n\n"
            "Выберите, где искать. Дальше бот последовательно запросит лимит, период и режим комментариев.",
            _search_menu_keyboard(),
        )
    await callback.answer()


@router.callback_query(F.data == "searchui:pick_theme")
async def cb_search_pick_theme(callback: CallbackQuery) -> None:
    if not callback.message or not callback.from_user:
        await callback.answer()
        return

    _, database, _, _ = _require_services()
    user_id = await _upsert_user(callback.from_user.id, callback.from_user.username, callback.from_user.first_name)
    themes = await database.list_themes(user_id)
    if not themes:
        await _edit_or_replace_watch_ui(
            callback,
            user_id,
            "Тем пока нет.\n\nСначала создайте тему в разделе «☰ Темы».",
            _search_menu_keyboard(),
        )
        await callback.answer()
        return

    await _edit_or_replace_watch_ui(callback, user_id, "Выберите тему для поиска:", _search_theme_picker_keyboard(themes))
    await callback.answer()


@router.callback_query(F.data == "searchui:all")
async def cb_search_all(callback: CallbackQuery) -> None:
    if callback.message and callback.from_user:
        app_settings, _, _, _ = _require_services()
        user_id = await _upsert_user(callback.from_user.id, callback.from_user.username, callback.from_user.first_name)
        user_settings = await _get_user_settings(user_id)
        await _edit_or_replace_watch_ui(
            callback,
            user_id,
            "🌐 Поиск по всем чатам из всех тем\n\n"
            f"📄 Формат: {_format_output_format_value(user_settings.search_output_format)}\n\n"
            "Выберите лимит найденных сообщений:",
            _search_limit_keyboard(user_settings.search_output_format, scope="all", theme_id=0, default_limit=app_settings.default_limit),
        )
    await callback.answer()


@router.callback_query(F.data.startswith("searchui:th:"))
async def cb_search_theme_selected(callback: CallbackQuery) -> None:
    if not callback.message or not callback.from_user:
        await callback.answer()
        return

    parts = (callback.data or "").split(":")
    if len(parts) != 3:
        await callback.answer("Некорректный callback", show_alert=False)
        return
    try:
        theme_id = int(parts[2])
    except ValueError:
        await callback.answer("Некорректная тема", show_alert=False)
        return

    _, database, _, _ = _require_services()
    user_id = await _upsert_user(callback.from_user.id, callback.from_user.username, callback.from_user.first_name)
    theme = await database.get_theme_by_id(user_id, theme_id)
    if theme is None:
        await _edit_or_replace_watch_ui(callback, user_id, "Тема не найдена.", _search_menu_keyboard())
        await callback.answer()
        return

    active_theme_by_user[user_id] = theme.name
    app_settings, _, _, _ = _require_services()
    user_settings = await _get_user_settings(user_id)
    await _edit_or_replace_watch_ui(
        callback,
        user_id,
        f"🎯 Поиск по теме «{theme.name}»\n\n"
        f"📄 Формат: {_format_output_format_value(user_settings.search_output_format)}\n\n"
        "Выберите лимит найденных сообщений:",
        _search_limit_keyboard(user_settings.search_output_format, scope="th", theme_id=theme.id, default_limit=app_settings.default_limit),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("searchui:run:"))
async def cb_search_run(callback: CallbackQuery) -> None:
    if not callback.message or not callback.from_user:
        await callback.answer()
        return

    parts = (callback.data or "").split(":")
    if len(parts) != 5:
        await callback.answer("Некорректный callback", show_alert=False)
        return

    app_settings, _, _, _ = _require_services()
    output_format = parts[2]
    scope = parts[3]
    try:
        payload_id = int(parts[4])
    except ValueError:
        await callback.answer("Некорректные данные", show_alert=False)
        return

    if output_format not in SEARCH_FORMAT_LABELS:
        await callback.answer("Некорректный формат", show_alert=False)
        return
    if scope not in {"th", "all"}:
        await callback.answer("Некорректный режим поиска", show_alert=False)
        return

    user_id = await _upsert_user(callback.from_user.id, callback.from_user.username, callback.from_user.first_name)
    await _edit_or_replace_watch_ui(
        callback,
        user_id,
        f"Формат: {SEARCH_FORMAT_LABELS[output_format]}\nВыберите лимит найденных сообщений:",
        _search_limit_keyboard(output_format, scope, payload_id, app_settings.default_limit),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("searchui:limit:"))
async def cb_search_limit(callback: CallbackQuery, state: FSMContext) -> None:
    if not callback.message or not callback.from_user:
        await callback.answer()
        return

    parts = (callback.data or "").split(":")
    if len(parts) != 6:
        await callback.answer("Некорректный callback", show_alert=False)
        return

    _, _, output_format, scope, payload_raw, limit_raw = parts
    if output_format not in SEARCH_FORMAT_LABELS:
        await callback.answer("Некорректный формат", show_alert=False)
        return
    if scope not in {"th", "all"}:
        await callback.answer("Некорректный режим поиска", show_alert=False)
        return
    try:
        payload_id = int(payload_raw)
    except ValueError:
        await callback.answer("Некорректные данные", show_alert=False)
        return

    user_id = await _upsert_user(callback.from_user.id, callback.from_user.username, callback.from_user.first_name)
    _, _, auth, _ = _require_services()
    if not await auth.is_authorized(user_id):
        await _edit_or_replace_watch_ui(callback, user_id, "Сначала подключите аккаунт в разделе «👤 Авторизация».", _main_menu())
        await callback.answer()
        return

    current_task = active_search_tasks.get(user_id)
    if current_task and not current_task.done():
        await _edit_or_replace_watch_ui(
            callback,
            user_id,
            "Поиск уже выполняется. Дождитесь завершения текущего поиска.",
            _search_menu_keyboard(),
        )
        await callback.answer()
        return

    if limit_raw == "custom":
        await state.set_state(SearchUiStates.waiting_custom_limit)
        await state.update_data(
            search_output_format=output_format,
            search_scope=scope,
            search_payload_id=payload_id,
        )
        await _edit_or_replace_watch_ui(
            callback,
            user_id,
            "Введите лимит сообщений числом.\n"
            "Примеры: 50, 200\n"
            "Для режима без лимита отправьте 0.\n"
            "Чтобы выйти, нажмите «🏠 Главное меню».",
            _search_menu_keyboard(),
        )
        await callback.answer()
        return

    try:
        limit = _parse_limit_token(limit_raw)
    except ValueError:
        await callback.answer("Некорректный лимит", show_alert=False)
        return

    await state.clear()
    await _edit_or_replace_watch_ui(
        callback,
        user_id,
        f"Лимит: {_format_limit_value(limit)}\nВыберите период поиска:",
        _search_date_keyboard(output_format, scope, payload_id, _limit_to_token(limit)),
    )
    await callback.answer()


@router.message(SearchUiStates.waiting_custom_limit, ~F.text.startswith("/"))
async def st_search_custom_limit(message: Message, state: FSMContext) -> None:
    user_id = await _ensure_user(message)
    value = (message.text or "").strip()
    if not value:
        await _send_watch_ui_message(message, user_id, "Введите число лимита или 0 для без лимита.", _search_menu_keyboard())
        return

    if value.casefold() in {"0", "none", "no", "nolimit", "no-limit", "unlimited", "безлимит", "без-лимита", "без_лимита"}:
        limit: Optional[int] = None
    else:
        try:
            limit = int(value)
        except ValueError:
            await _send_watch_ui_message(message, user_id, "Лимит должен быть числом. Пример: 50", _search_menu_keyboard())
            return
        if limit <= 0:
            await _send_watch_ui_message(
                message,
                user_id,
                "Лимит должен быть > 0. Для безлимитного поиска отправьте 0.",
                _search_menu_keyboard(),
            )
            return

    data = await state.get_data()
    output_format = data.get("search_output_format")
    scope = data.get("search_scope")
    payload_id = data.get("search_payload_id")
    if output_format not in SEARCH_FORMAT_LABELS or scope not in {"th", "all"} or not isinstance(payload_id, int):
        await state.clear()
        await _send_watch_ui_message(message, user_id, "Параметры поиска потеряны. Начните заново через раздел 'Поиск'.", _search_menu_keyboard())
        return

    await state.clear()
    await _send_watch_ui_message(
        message,
        user_id,
        f"Лимит: {_format_limit_value(limit)}\nВыберите период поиска:",
        _search_date_keyboard(output_format, scope, payload_id, _limit_to_token(limit)),
    )


@router.callback_query(F.data.startswith("searchui:date:"))
async def cb_search_date(callback: CallbackQuery, state: FSMContext) -> None:
    if not callback.message or not callback.from_user:
        await callback.answer()
        return

    parts = (callback.data or "").split(":")
    if len(parts) != 7:
        await callback.answer("Некорректный callback", show_alert=False)
        return

    _, _, output_format, scope, payload_raw, limit_token, preset = parts
    if output_format not in SEARCH_FORMAT_LABELS or scope not in {"th", "all"}:
        await callback.answer("Некорректные параметры", show_alert=False)
        return
    try:
        payload_id = int(payload_raw)
        limit = _parse_limit_token(limit_token)
    except Exception:
        await callback.answer("Некорректные параметры", show_alert=False)
        return

    user_id = await _upsert_user(callback.from_user.id, callback.from_user.username, callback.from_user.first_name)
    _, _, auth, _ = _require_services()
    if not await auth.is_authorized(user_id):
        await _edit_or_replace_watch_ui(callback, user_id, "Сначала подключите аккаунт в разделе «👤 Авторизация».", _main_menu())
        await callback.answer()
        return

    current_task = active_search_tasks.get(user_id)
    if current_task and not current_task.done():
        await _edit_or_replace_watch_ui(
            callback,
            user_id,
            "Поиск уже выполняется. Дождитесь завершения текущего поиска.",
            _search_menu_keyboard(),
        )
        await callback.answer()
        return

    if preset == "custom":
        await state.set_state(SearchUiStates.waiting_custom_dates)
        await state.update_data(
            search_output_format=output_format,
            search_scope=scope,
            search_payload_id=payload_id,
            search_limit_token=_limit_to_token(limit),
        )
        await _edit_or_replace_watch_ui(
            callback,
            user_id,
            "Введите диапазон дат.\n"
            "Формат: YYYY-MM-DD YYYY-MM-DD\n"
            "Пример: 2026-03-01 2026-03-12\n"
            "Можно ввести одну дату: 2026-03-12 (поиск только за день).\n"
            "Для поиска за всё время: all\n"
            "Чтобы выйти, нажмите «🏠 Главное меню».",
            _search_menu_keyboard(),
        )
        await callback.answer()
        return

    try:
        date_from, date_to = _preset_date_range(preset)
    except Exception as e:
        await _edit_or_replace_watch_ui(callback, user_id, f"Ошибка запуска поиска: {e}", _search_menu_keyboard())
        await callback.answer()
        return

    await state.clear()
    await _edit_or_replace_watch_ui(
        callback,
        user_id,
        _search_comments_mode_prompt(limit, date_from, date_to),
        _search_comments_mode_keyboard(
            output_format,
            scope,
            payload_id,
            _limit_to_token(limit),
            _date_range_to_token(date_from, date_to),
        ),
    )
    await callback.answer()


@router.message(SearchUiStates.waiting_custom_dates, ~F.text.startswith("/"))
async def st_search_custom_dates(message: Message, state: FSMContext) -> None:
    user_id = await _ensure_user(message)
    data = await state.get_data()
    output_format = data.get("search_output_format")
    scope = data.get("search_scope")
    payload_id = data.get("search_payload_id")
    limit_token = data.get("search_limit_token")
    if output_format not in SEARCH_FORMAT_LABELS or scope not in {"th", "all"} or not isinstance(payload_id, int):
        await state.clear()
        await _send_watch_ui_message(message, user_id, "Параметры поиска потеряны. Начните заново через раздел 'Поиск'.", _search_menu_keyboard())
        return

    try:
        limit = _parse_limit_token(str(limit_token))
    except Exception:
        await state.clear()
        await _send_watch_ui_message(message, user_id, "Некорректный лимит. Начните поиск заново.", _search_menu_keyboard())
        return

    try:
        date_from, date_to = _parse_custom_dates_input(message.text or "")
    except Exception as e:
        await _send_watch_ui_message(message, user_id, f"{e}\nПопробуйте снова или нажмите «🏠 Главное меню».", _search_menu_keyboard())
        return

    _, _, auth, _ = _require_services()
    if not await auth.is_authorized(user_id):
        await state.clear()
        await _send_watch_ui_message(message, user_id, "Сначала подключите аккаунт в разделе «👤 Авторизация».", _main_menu())
        return

    current_task = active_search_tasks.get(user_id)
    if current_task and not current_task.done():
        await state.clear()
        await _send_watch_ui_message(message, user_id, "Поиск уже выполняется. Дождитесь завершения текущего поиска.", _search_menu_keyboard())
        return

    await state.clear()
    await _send_watch_ui_message(
        message,
        user_id,
        _search_comments_mode_prompt(limit, date_from, date_to),
        _search_comments_mode_keyboard(
            output_format,
            scope,
            payload_id,
            _limit_to_token(limit),
            _date_range_to_token(date_from, date_to),
        ),
    )


@router.callback_query(F.data.startswith("searchui:cm:"))
async def cb_search_comments_mode(callback: CallbackQuery) -> None:
    if not callback.message or not callback.from_user:
        await callback.answer()
        return

    parts = (callback.data or "").split(":")
    if len(parts) != 8:
        await callback.answer("Некорректный callback", show_alert=False)
        return

    _, _, output_format, scope, payload_raw, limit_token, date_token, comments_mode = parts
    if output_format not in SEARCH_FORMAT_LABELS or scope not in {"th", "all"}:
        await callback.answer("Некорректные параметры", show_alert=False)
        return
    if comments_mode not in SEARCH_COMMENTS_MODE_LABELS:
        await callback.answer("Некорректный режим", show_alert=False)
        return

    try:
        payload_id = int(payload_raw)
        limit = _parse_limit_token(limit_token)
        date_from, date_to = _parse_date_range_token(date_token)
    except Exception:
        await callback.answer("Некорректные параметры", show_alert=False)
        return

    user_id = await _upsert_user(callback.from_user.id, callback.from_user.username, callback.from_user.first_name)
    _, _, auth, _ = _require_services()
    if not await auth.is_authorized(user_id):
        await _edit_or_replace_watch_ui(callback, user_id, "Сначала подключите аккаунт в разделе «👤 Авторизация».", _main_menu())
        await callback.answer()
        return

    current_task = active_search_tasks.get(user_id)
    if current_task and not current_task.done():
        await _edit_or_replace_watch_ui(
            callback,
            user_id,
            "Поиск уже выполняется. Дождитесь завершения текущего поиска.",
            _search_menu_keyboard(),
        )
        await callback.answer()
        return

    try:
        start_text, task = await _create_ui_search_task(
            bot=callback.message.bot,
            user_id=user_id,
            chat_id=callback.message.chat.id,
            scope=scope,
            payload_id=payload_id,
            output_format=output_format,
            limit=limit,
            date_from=date_from,
            date_to=date_to,
            deep_comments=(comments_mode == "deep"),
        )
    except Exception as e:
        await _edit_or_replace_watch_ui(callback, user_id, f"Ошибка запуска поиска: {e}", _search_menu_keyboard())
        await callback.answer()
        return

    active_search_tasks[user_id] = task
    await _edit_or_replace_watch_ui(callback, user_id, start_text, _search_menu_keyboard())
    await callback.answer()


@router.message(Command("search"))
async def cmd_search(message: Message) -> None:
    user_id = await _ensure_user(message)
    app_settings, _, auth, _ = _require_services()
    await _cleanup_previous_watch_ui_message(message.bot, user_id, message.chat.id)

    is_auth = await auth.is_authorized(user_id)
    if not is_auth:
        await message.answer("Сначала авторизуйте аккаунт через /auth.")
        return

    user_settings = await _get_user_settings(user_id)
    try:
        parsed = _parse_search_command(
            message.text or "",
            default_limit=app_settings.default_limit,
            default_output_format=user_settings.search_output_format,
        )
    except Exception as e:
        await message.answer(str(e))
        return

    try:
        theme = await _resolve_theme_for_user(user_id, parsed.theme_name)
    except Exception as e:
        await message.answer(str(e))
        return
    active_theme_by_user[user_id] = theme.name

    current_task = active_search_tasks.get(user_id)
    if current_task and not current_task.done():
        await message.answer("Поиск уже выполняется. Остановите его через /cancel.")
        return

    _set_active_search_themes(user_id, [theme.name])
    await message.answer(
        _format_search_start_text(
            f"Тема '{theme.name}'",
            parsed.output_format,
            parsed.limit,
            parsed.date_from,
            parsed.date_to,
            parsed.deep_comments,
        )
    )
    task = asyncio.create_task(
        _run_search_task(
            bot=message.bot,
            user_id=user_id,
            chat_id=message.chat.id,
            theme_name=theme.name,
            date_from=parsed.date_from,
            date_to=parsed.date_to,
            limit=parsed.limit,
            output_format=parsed.output_format,
            deep_comments=parsed.deep_comments,
        )
    )
    active_search_tasks[user_id] = task


async def _cancel_user_task(chat_id: int, user_id: int, bot: Bot) -> None:
    task = active_search_tasks.get(user_id)
    if not task or task.done():
        await bot.send_message(chat_id, "Активного поиска нет.")
        return
    task.cancel()
    await bot.send_message(chat_id, "Отменяю текущий поиск...")


@router.message(Command("cancel"))
async def cmd_cancel(message: Message, state: FSMContext) -> None:
    user_id = await _ensure_user(message)
    current_state = await state.get_state()
    if current_state and current_state.startswith(f"{ThemeUiStates.__name__}:"):
        await state.clear()
        await _send_watch_ui_message(
            message,
            user_id,
            "Редактирование темы отменено.",
            await _themes_panel_markup_for_user(user_id),
        )
        return
    if current_state and current_state.startswith(f"{WatchUiStates.__name__}:"):
        await state.clear()
        await _send_watch_ui_message(message, user_id, "Настройка периода автопроверки отменена.", _watch_menu_keyboard())
        return
    if current_state and current_state.startswith(f"{AuthStates.__name__}:"):
        await state.clear()
        await _send_watch_ui_message(message, user_id, "Ввод 2FA отменен.", await _main_menu_for_user(user_id))
        return
    if current_state and current_state.startswith(f"{SettingsUiStates.__name__}:"):
        await state.clear()
        user_settings = await _get_user_settings(user_id)
        await _send_watch_ui_message(message, user_id, _settings_watch_text(user_settings), _settings_watch_keyboard())
        return
    if current_state and current_state.startswith(f"{SearchUiStates.__name__}:"):
        await state.clear()
        await _send_watch_ui_message(message, user_id, "Настройка поиска отменена.", _search_menu_keyboard())
        return
    await _cancel_user_task(message.chat.id, user_id, message.bot)


@router.callback_query(F.data == "menu_cancel")
async def cb_cancel(callback: CallbackQuery, state: FSMContext) -> None:
    if callback.message and callback.from_user:
        user_id = await _upsert_user(callback.from_user.id, callback.from_user.username, callback.from_user.first_name)
        current_state = await state.get_state()
        if current_state and current_state.startswith(f"{ThemeUiStates.__name__}:"):
            await state.clear()
            await _edit_or_replace_watch_ui(
                callback,
                user_id,
                "Редактирование темы отменено.",
                await _themes_panel_markup_for_user(user_id),
            )
            await callback.answer()
            return
        if current_state and current_state.startswith(f"{WatchUiStates.__name__}:"):
            await state.clear()
            await _edit_or_replace_watch_ui(
                callback,
                user_id,
                "Настройка периода автопроверки отменена.",
                _watch_menu_keyboard(),
            )
            await callback.answer()
            return
        if current_state and current_state.startswith(f"{AuthStates.__name__}:"):
            await state.clear()
            await _edit_or_replace_watch_ui(callback, user_id, "Ввод 2FA отменен.", await _main_menu_for_user(user_id))
            await callback.answer()
            return
        if current_state and current_state.startswith(f"{SettingsUiStates.__name__}:"):
            await state.clear()
            user_settings = await _get_user_settings(user_id)
            await _edit_or_replace_watch_ui(callback, user_id, _settings_watch_text(user_settings), _settings_watch_keyboard())
            await callback.answer()
            return
        if current_state and current_state.startswith(f"{SearchUiStates.__name__}:"):
            await state.clear()
            await _edit_or_replace_watch_ui(callback, user_id, "Настройка поиска отменена.", _search_menu_keyboard())
            await callback.answer()
            return
        await _cancel_user_task(callback.message.chat.id, user_id, callback.message.bot)
    await callback.answer()


@router.errors()
async def on_error(event) -> bool:
    logger.error("Unhandled bot error: %s", event.exception)
    return True


async def on_startup(bot: Bot) -> None:
    global watch_scheduler_task
    app_settings, database, _, search = _require_services()
    await database.init()
    await database.cleanup_old_runs(retention_days=app_settings.retention_days)
    await search.cleanup_old_files()

    try:
        me = await bot.get_me()
    except (TelegramNotFound, TelegramUnauthorizedError) as e:
        raise RuntimeError("BOT_TOKEN is invalid. Check .env and use exact token from @BotFather.") from e

    try:
        await bot.delete_webhook(drop_pending_updates=True)
    except TelegramBadRequest:
        pass
    if watch_scheduler_task is None or watch_scheduler_task.done():
        watch_scheduler_task = asyncio.create_task(_watch_scheduler_loop(bot))
    logger.info("Bot started as @%s", me.username)


async def on_shutdown(bot: Bot) -> None:
    global watch_scheduler_task
    _, database, auth, _ = _require_services()
    if watch_scheduler_task and not watch_scheduler_task.done():
        watch_scheduler_task.cancel()
        with suppress(asyncio.CancelledError):
            await watch_scheduler_task
    watch_scheduler_task = None
    for task in list(active_search_tasks.values()):
        if not task.done():
            task.cancel()
    active_search_tasks.clear()
    watch_ui_message_by_user.clear()
    nav_keyboard_mode_by_user.clear()
    for user_id in list(auth.pending.keys()):
        await auth._close_pending(user_id)
    await database.close()
    await bot.session.close()
    logger.info("Bot stopped")


async def main() -> None:
    global settings, db, auth_manager, search_service

    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    settings = load_settings()

    sessions_dir = settings.data_dir / "sessions"
    results_dir = settings.data_dir / "results"
    db = Database(settings.database_url)
    auth_manager = AuthManager(settings.tg_api_id, settings.tg_api_hash, sessions_dir, settings.qr_timeout_seconds)
    search_service = SearchService(
        settings.tg_api_id,
        settings.tg_api_hash,
        sessions_dir=sessions_dir,
        results_dir=results_dir,
        retention_days=settings.retention_days,
    )

    bot = Bot(token=settings.bot_token)
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)

    try:
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    finally:
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())

