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
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
)

from bot_backend.auth import AuthManager
from bot_backend.config import Settings, load_settings
from bot_backend.db import Database, ThemeDTO, ThemeWatchDTO
from bot_backend.search import SearchError, SearchParams, SearchService, parse_date_from, parse_date_to

router = Router()
logger = logging.getLogger("tg_bot")

settings: Optional[Settings] = None
db: Optional[Database] = None
auth_manager: Optional[AuthManager] = None
search_service: Optional[SearchService] = None
active_search_tasks: dict[int, asyncio.Task] = {}
active_theme_by_user: dict[int, str] = {}
watch_scheduler_task: Optional[asyncio.Task] = None
watch_ui_message_by_user: dict[int, tuple[int, int]] = {}
nav_keyboard_mode_by_user: dict[int, bool] = {}

MIN_WATCH_INTERVAL_MINUTES = 1
MAX_WATCH_INTERVAL_MINUTES = 60 * 24 * 7
WATCH_SCHEDULER_SLEEP_SECONDS = 20
WATCH_INTERVAL_OPTIONS = (15, 30, 60, 180, 360, 720, 1440)
SEARCH_LIMIT_OPTIONS = (20, 50, 100, 200, 500)


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


@dataclass
class ParsedSearchCommand:
    theme_name: Optional[str]
    date_from: Optional[datetime]
    date_to: Optional[datetime]
    limit: Optional[int]
    output_format: str


def _main_menu(*, show_auth_actions: bool = True) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    if show_auth_actions:
        rows.append(
            [
                InlineKeyboardButton(text="Авторизация", callback_data="menu_auth"),
                InlineKeyboardButton(text="Проверить QR", callback_data="menu_auth_check"),
                InlineKeyboardButton(text="Обновить QR", callback_data="menu_auth_refresh"),
            ]
        )
    rows.extend(
        [
            [
                InlineKeyboardButton(text="Темы", callback_data="menu_themes"),
                InlineKeyboardButton(text="Поиск", callback_data="menu_search"),
            ],
            [
                InlineKeyboardButton(text="Подписки", callback_data="menu_watch"),
                InlineKeyboardButton(text="Отменить поиск", callback_data="menu_cancel"),
            ],
            [InlineKeyboardButton(text="Помощь", callback_data="menu_help")],
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def _main_menu_for_user(user_id: int) -> InlineKeyboardMarkup:
    _, _, auth, _ = _require_services()
    is_auth = await auth.is_authorized(user_id)
    return _main_menu(show_auth_actions=not is_auth)


def _nav_reply_keyboard(*, show_auth_actions: bool) -> ReplyKeyboardMarkup:
    rows: list[list[KeyboardButton]] = [
        [KeyboardButton(text="Главное меню"), KeyboardButton(text="Помощь")],
        [KeyboardButton(text="Темы"), KeyboardButton(text="Поиск"), KeyboardButton(text="Подписки")],
        [KeyboardButton(text="Отменить поиск")],
    ]
    if show_auth_actions:
        rows.append(
            [
                KeyboardButton(text="Авторизация"),
                KeyboardButton(text="Проверить QR"),
                KeyboardButton(text="Обновить QR"),
            ]
        )
    return ReplyKeyboardMarkup(
        keyboard=rows,
        resize_keyboard=True,
        is_persistent=True,
        input_field_placeholder="Выберите раздел",
    )


async def _ensure_nav_keyboard(
    message: Message,
    user_id: int,
    *,
    show_auth_actions: Optional[bool] = None,
    force: bool = False,
) -> None:
    if show_auth_actions is None:
        cached = nav_keyboard_mode_by_user.get(user_id)
        if cached is not None and not force:
            return
        _, _, auth, _ = _require_services()
        show_auth_actions = not await auth.is_authorized(user_id)
    if not force and nav_keyboard_mode_by_user.get(user_id) == show_auth_actions:
        return
    nav_keyboard_mode_by_user[user_id] = show_auth_actions
    await message.answer("Быстрая навигация закреплена ниже.", reply_markup=_nav_reply_keyboard(show_auth_actions=show_auth_actions))


def _auth_actions_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Проверить QR", callback_data="menu_auth_check"),
                InlineKeyboardButton(text="Обновить QR", callback_data="menu_auth_refresh"),
            ],
            [InlineKeyboardButton(text="Ввести 2FA", callback_data="menu_auth_2fa")],
            [InlineKeyboardButton(text="Главное меню", callback_data="menu_home")],
        ]
    )


def _themes_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="1) Создать и добавить", callback_data="themes:section:add")],
            [InlineKeyboardButton(text="2) Выбрать и удалить", callback_data="themes:section:manage")],
            [InlineKeyboardButton(text="3) Информация", callback_data="themes:section:info")],
            [InlineKeyboardButton(text="Главное меню", callback_data="menu_home")],
        ]
    )


def _themes_add_section_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Создать тему", callback_data="themes:new")],
            [
                InlineKeyboardButton(text="Добавить чаты", callback_data="themes:quick:ac"),
                InlineKeyboardButton(text="Добавить ключи", callback_data="themes:quick:ak"),
            ],
            [InlineKeyboardButton(text="Назад", callback_data="menu_themes")],
            [InlineKeyboardButton(text="Главное меню", callback_data="menu_home")],
        ]
    )


def _themes_manage_section_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Выбрать тему", callback_data="themes:pick:sa")],
            [
                InlineKeyboardButton(text="Удалить ключи", callback_data="themes:pick:dk"),
                InlineKeyboardButton(text="Удалить чаты", callback_data="themes:pick:dc"),
            ],
            [InlineKeyboardButton(text="Удалить тему", callback_data="themes:pick:dt")],
            [InlineKeyboardButton(text="Назад", callback_data="menu_themes")],
            [InlineKeyboardButton(text="Главное меню", callback_data="menu_home")],
        ]
    )


def _themes_info_section_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Список тем", callback_data="themes:list")],
            [InlineKeyboardButton(text="Чаты поиска", callback_data="themes:search_chats")],
            [InlineKeyboardButton(text="Назад", callback_data="menu_themes")],
            [InlineKeyboardButton(text="Главное меню", callback_data="menu_home")],
        ]
    )


def _theme_add_chat_keyboard(theme_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="2) Добавить ВСЕ чаты аккаунта", callback_data=f"themes:add_all_chats:th:{theme_id}")],
            [InlineKeyboardButton(text="Назад", callback_data="themes:section:add")],
            [InlineKeyboardButton(text="Главное меню", callback_data="menu_home")],
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
                InlineKeyboardButton(text="Выбрать тему", callback_data="searchui:pick_theme"),
                InlineKeyboardButton(text="Все чаты", callback_data="searchui:all"),
            ],
            [InlineKeyboardButton(text="Главное меню", callback_data="menu_home")],
        ]
    )


def _search_theme_picker_keyboard(themes: list[ThemeDTO]) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for theme in themes:
        rows.append([InlineKeyboardButton(text=theme.name, callback_data=f"searchui:th:{theme.id}")])
    rows.append([InlineKeyboardButton(text="Назад", callback_data="menu_search")])
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


def _parse_search_command(message_text: str, default_limit: int) -> ParsedSearchCommand:
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
    output_format = "both"
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
    )


def _format_limit_value(limit: Optional[int]) -> str:
    return "без лимита" if limit is None else str(limit)


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
        "Управление автопроверкой тем:\n"
        "Кнопки: Подписки -> Включить/Отключить (включая кнопку 'Свой период').\n"
        "/watch_set [theme] <minutes> - включить или обновить подписку\n"
        "/watch_off [theme] - отключить подписку\n"
        "/watch_list - показать активные подписки\n\n"
        f"Интервал: {MIN_WATCH_INTERVAL_MINUTES}..{MAX_WATCH_INTERVAL_MINUTES} минут.\n"
        "Если тема не указана, используется активная тема."
    )


def _render_watch_list_text(watches: list[ThemeWatchDTO]) -> str:
    if not watches:
        return (
            "У вас нет активных подписок на автопроверку.\n"
            "Пример: /watch_set news 60"
        )

    lines = ["Подписки на автопроверку:"]
    for idx, watch in enumerate(watches, start=1):
        lines.append(f"{idx}. Тема '{watch.theme_name}' - каждые {watch.interval_minutes} мин")
        lines.append(f"   Следующая проверка: {_format_dt_utc(watch.next_check_at)}")
        if watch.last_checked_at:
            lines.append(f"   Последняя проверка: {_format_dt_utc(watch.last_checked_at)}")
        if watch.last_match_at:
            lines.append(f"   Последнее совпадение: {_format_dt_utc(watch.last_match_at)}")
        if watch.last_error:
            lines.append(f"   Последняя ошибка: {_short_text(watch.last_error, max_len=150)}")
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
            "Или нажмите кнопку '2) Добавить ВСЕ чаты аккаунта'."
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
        f"Тема '{theme_name}' создана и выбрана активной.\n\n"
        "Шаг 2/3: отправьте ключевые слова для поиска.\n"
        "Разделители: ';', ',' или перенос строки.\n"
        "Пример: Москва; погода; дрон\n\n"
        "Если пропустить шаг: /skip"
    )


def _theme_wizard_chats_prompt(theme_name: str) -> str:
    return (
        f"Тема: {theme_name}\n\n"
        "Шаг 3/3: отправьте чаты/каналы для поиска.\n"
        "Разделители: ';' или перенос строки.\n"
        "Поддерживается: @username, id, точное имя, t.me-ссылка.\n\n"
        "Если пропустить шаг: /skip"
    )


async def _send_themes_panel(message: Message, user_id: int) -> None:
    summary = await _build_themes_panel_text(user_id)
    await _send_watch_ui_message(message, user_id, summary, _themes_menu_keyboard())


async def _build_themes_panel_text(user_id: int) -> str:
    _, database, _, _ = _require_services()
    themes = await database.list_themes(user_id)
    current = active_theme_by_user.get(user_id)
    if current:
        current_theme = await database.get_theme(user_id, current)
        if current_theme is None:
            active_theme_by_user.pop(user_id, None)
            current = None
    if not themes:
        return (
            "Тем пока нет.\n\n"
            "Быстрый старт:\n"
            "1) Откройте '1) Создать и добавить'\n"
            "2) Создайте тему\n"
            "3) В действии 'Добавить чаты' можно добавить чаты вручную или кнопкой '2) Добавить ВСЕ чаты аккаунта'\n"
            "4) Добавьте ключевые слова"
        )
    return (
        f"Темы: {len(themes)}\n"
        f"Активная: {current or '(не выбрана)'}\n\n"
        "Быстрый сценарий:\n"
        "1) Откройте '1) Создать и добавить'\n"
        "2) Добавьте чаты (вручную или кнопкой '2) Добавить ВСЕ чаты аккаунта')\n"
        "3) Добавьте ключевые слова"
    )


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


def _render_chat_list(items: list[str], *, max_items: int = 200) -> str:
    if not items:
        return "(нет)"
    lines: list[str] = []
    for idx, value in enumerate(items[:max_items], start=1):
        lines.append(f"{idx}. {_short_text(value, max_len=140)}")
    if len(items) > max_items:
        lines.append(f"... и еще {len(items) - max_items}")
    return "\n".join(lines)


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
                        [InlineKeyboardButton(text="Добавить чаты в тему", callback_data=f"themes:do:ac:{theme.id}")],
                        [InlineKeyboardButton(text="Открыть Темы", callback_data="menu_themes")],
                    ]
                ),
            )
        return (
            "Поиск пока невозможен: в выбранном наборе нет чатов.\n\n"
            "Добавьте чаты в темы и запустите поиск снова.",
            InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="Добавить чаты", callback_data="themes:quick:ac")],
                    [InlineKeyboardButton(text="Открыть Темы", callback_data="menu_themes")],
                ]
            ),
        )

    if "во всех темах пуст список чатов" in lowered:
        return (
            "Поиск по всем чатам пока невозможен: во всех темах нет чатов.\n\n"
            "Добавьте чаты в темы и запустите поиск снова.",
            InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="Добавить чаты", callback_data="themes:quick:ac")],
                    [InlineKeyboardButton(text="Открыть Темы", callback_data="menu_themes")],
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
                        [InlineKeyboardButton(text="Добавить ключевые слова", callback_data=f"themes:do:ak:{theme.id}")],
                        [InlineKeyboardButton(text="Открыть Темы", callback_data="menu_themes")],
                    ]
                ),
            )
        return (
            "Поиск пока невозможен: не заданы ключевые слова.",
            InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="Добавить ключевые слова", callback_data="themes:quick:ak")],
                    [InlineKeyboardButton(text="Открыть Темы", callback_data="menu_themes")],
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
) -> tuple[str, asyncio.Task]:
    _, database, _, _ = _require_services()
    if scope == "th":
        theme = await database.get_theme_by_id(user_id, payload_id)
        if theme is None:
            raise ValueError("Тема не найдена.")
        active_theme_by_user[user_id] = theme.name
        text = (
            f"Запускаю поиск по теме '{theme.name}' "
            f"({SEARCH_FORMAT_LABELS[output_format]}, limit={_format_limit_value(limit)}, "
            f"даты: {_format_search_date_range(date_from, date_to)})."
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
            )
        )
        return text, task

    text = (
        "Запускаю поиск по всем чатам "
        f"({SEARCH_FORMAT_LABELS[output_format]}, limit={_format_limit_value(limit)}, "
        f"даты: {_format_search_date_range(date_from, date_to)})."
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
            params=SearchParams(limit=limit, date_from=date_from, date_to=date_to),
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
) -> None:
    range_from = _format_dt_utc(date_from) if date_from else "начало истории"
    header = (
        f"Автопроверка темы '{theme_name}'\n"
        f"Период: {range_from} - {_format_dt_utc(date_to)}\n"
        f"Найдено новых сообщений: {len(items)}"
    )

    try:
        await bot.send_message(watch.chat_id, header, disable_web_page_preview=True)
        for chunk in _render_results_messages(theme_name, items):
            await bot.send_message(watch.chat_id, chunk, disable_web_page_preview=True)
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
            "1) Создайте тему и добавьте чаты/ключевые слова.\n"
            "2) Запустите /search <theme>.\n"
            "3) Для регулярной проверки: /watch_set <theme> <minutes>."
        )
    else:
        steps = (
            "1) Нажмите Авторизация и пройдите QR.\n"
            "2) Создайте тему и добавьте чаты/ключевые слова.\n"
            "3) Запустите /search <theme>.\n"
            "4) Для регулярной проверки: /watch_set <theme> <minutes>."
        )

    await _send_watch_ui_message(
        message,
        user_id,
        "Привет. Это бот для поиска по Telegram-аккаунту пользователя.\n"
        f"Статус аккаунта: {status}\n\n"
        f"{steps}\n\n"
        "Команда помощи: /help",
        _main_menu(show_auth_actions=not is_auth),
        nav_show_auth_actions=not is_auth,
        nav_force=True,
    )


def _help_text() -> str:
    return (
        "Как пользоваться ботом:\n"
        "1) Сначала пройдите авторизацию через QR-код.\n"
        "2) Откройте раздел 'Темы' и создайте тему.\n"
        "3) Добавьте ключевые слова и чаты для поиска.\n"
        "4) Перейдите в раздел 'Поиск': выберите тему (или все чаты), формат, лимит и период дат.\n"
        "5) Бот пришлет найденные сообщения и файл CSV.\n\n"
        "В разделе 'Темы' -> '1) Создать и добавить' в действии 'Добавить чаты' есть кнопка '2) Добавить ВСЕ чаты аккаунта'.\n"
        "В разделе 'Подписки' можно включить регулярную автопроверку: бот будет сам присылать новые совпадения.\n\n"
        "Подсказки:\n"
        "- Чаты можно задавать как @username, id, точное название или ссылку t.me.\n"
        "- Если результатов слишком много, уменьшайте период дат или ставьте лимит.\n"
        "- Если ничего не найдено, проверьте ключевые слова и список чатов."
    )


@router.message(Command("help"))
async def cmd_help(message: Message) -> None:
    user_id = await _ensure_user(message)
    await _send_watch_ui_message(message, user_id, _help_text(), await _main_menu_for_user(user_id))


@router.callback_query(F.data == "menu_home")
async def cb_home(callback: CallbackQuery) -> None:
    if callback.message and callback.from_user:
        user_id = await _upsert_user(callback.from_user.id, callback.from_user.username, callback.from_user.first_name)
        await _edit_or_replace_watch_ui(callback, user_id, "Главное меню:", await _main_menu_for_user(user_id))
    await callback.answer()


@router.callback_query(F.data == "menu_help")
async def cb_help(callback: CallbackQuery) -> None:
    if callback.message and callback.from_user:
        user_id = await _upsert_user(callback.from_user.id, callback.from_user.username, callback.from_user.first_name)
        await _edit_or_replace_watch_ui(callback, user_id, _help_text(), await _main_menu_for_user(user_id))
    await callback.answer()


@router.message(StateFilter(None), F.text == "Главное меню")
async def kb_home(message: Message) -> None:
    user_id = await _ensure_user(message)
    await _send_watch_ui_message(message, user_id, "Главное меню:", await _main_menu_for_user(user_id))


@router.message(StateFilter(None), F.text == "Помощь")
async def kb_help(message: Message) -> None:
    user_id = await _ensure_user(message)
    await _send_watch_ui_message(message, user_id, _help_text(), await _main_menu_for_user(user_id))


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
    await _send_watch_ui_message(
        message,
        user_id,
        "Быстрый поиск кнопками:\n"
        "1) Выберите тему или 'Все чаты'\n"
        "2) Выберите формат результата\n"
        "3) Выберите лимит\n"
        "4) Выберите период поиска.",
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

    await _edit_or_replace_watch_ui(
        callback,
        user_id,
        "Выберите тему для автопроверки:",
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

    active_theme_by_user[user_id] = theme.name
    await _edit_or_replace_watch_ui(
        callback,
        user_id,
        (
            f"Тема: {theme.name}\n"
            "Выберите период проверки кнопкой ниже.\n"
            "Если нужен нестандартный интервал, нажмите кнопку 'Свой период'."
        ),
        _watch_interval_keyboard(theme.id),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("watch:set:custom:"))
async def cb_watch_set_custom_interval(callback: CallbackQuery, state: FSMContext) -> None:
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

    _, database, auth, _ = _require_services()
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

    active_theme_by_user[user_id] = theme.name
    await state.set_state(WatchUiStates.waiting_custom_interval)
    await state.update_data(watch_theme_id=theme.id)
    await _edit_or_replace_watch_ui(
        callback,
        user_id,
        (
            f"Тема: {theme.name}\n"
            f"Введите период проверки в минутах (от {MIN_WATCH_INTERVAL_MINUTES} до {MAX_WATCH_INTERVAL_MINUTES}).\n"
            "Пример: 45\n"
            "Для отмены: /cancel"
        ),
        _watch_menu_keyboard(),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("watch:set:int:"))
async def cb_watch_set_interval(callback: CallbackQuery, state: FSMContext) -> None:
    if not callback.message or not callback.from_user:
        await callback.answer()
        return

    parts = (callback.data or "").split(":")
    if len(parts) != 5:
        await callback.answer("Некорректные данные", show_alert=False)
        return
    try:
        theme_id = int(parts[3])
        interval_minutes = int(parts[4])
        _validate_watch_interval(interval_minutes)
    except ValueError:
        await callback.answer("Некорректный интервал", show_alert=False)
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

    watch = await database.set_theme_watch(
        user_id=user_id,
        theme_name=theme.name,
        chat_id=callback.message.chat.id,
        interval_minutes=interval_minutes,
    )
    active_theme_by_user[user_id] = theme.name
    panel_text = await _build_watch_panel_text(user_id)
    await _edit_or_replace_watch_ui(
        callback,
        user_id,
        (
            f"Автопроверка включена для темы '{watch.theme_name}'.\n"
            f"Интервал: {watch.interval_minutes} мин.\n"
            f"Следующая проверка: {_format_dt_utc(watch.next_check_at)}\n\n"
            f"{panel_text}"
        ),
        _watch_menu_keyboard(),
    )
    await callback.answer("Сохранено")


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
            status.message,
            _main_menu(show_auth_actions=False),
            nav_show_auth_actions=False,
        )
        return
    if not status.qr_png:
        await _send_watch_ui_message(
            message,
            user_id,
            status.message,
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
            "После сканирования нажмите 'Проверить QR'.\n"
            "Если включена 2FA, бот попросит пароль."
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
            "Введите пароль 2FA следующим сообщением.",
            _main_menu(show_auth_actions=True),
            nav_show_auth_actions=True,
        )
        return
    await _send_watch_ui_message(
        message,
        user_id,
        result.message,
        _main_menu(show_auth_actions=result.status != "authorized"),
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
            "Нет активной QR-авторизации. Сначала запустите /auth.",
            _main_menu(show_auth_actions=True),
            nav_show_auth_actions=True,
        )
        return

    await state.set_state(AuthStates.waiting_2fa)
    await _send_watch_ui_message(
        message,
        user_id,
        "Отправьте пароль 2FA следующим сообщением. Для отмены: /cancel",
        _main_menu(show_auth_actions=True),
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


@router.callback_query(F.data == "menu_auth_2fa")
async def cb_auth_2fa(callback: CallbackQuery, state: FSMContext) -> None:
    if callback.message and callback.from_user:
        await _open_2fa_prompt(
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
            _main_menu(show_auth_actions=True),
            nav_show_auth_actions=True,
        )
        return
    result = await auth.submit_2fa(user_id, password)
    if result.status == "authorized":
        await state.clear()
    await _send_watch_ui_message(
        message,
        user_id,
        result.message,
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
        await _edit_or_replace_watch_ui(callback, user_id, text, _themes_menu_keyboard())
    await callback.answer()


@router.callback_query(F.data == "themes:section:add")
async def cb_themes_section_add(callback: CallbackQuery) -> None:
    if callback.message and callback.from_user:
        user_id = await _upsert_user(callback.from_user.id, callback.from_user.username, callback.from_user.first_name)
        await _edit_or_replace_watch_ui(
            callback,
            user_id,
            "Раздел 1/3: создание темы и добавление данных.\n"
            "Шаги: создайте тему, добавьте чаты, добавьте ключевые слова.",
            _themes_add_section_keyboard(),
        )
    await callback.answer()


@router.callback_query(F.data == "themes:section:manage")
async def cb_themes_section_manage(callback: CallbackQuery) -> None:
    if callback.message and callback.from_user:
        user_id = await _upsert_user(callback.from_user.id, callback.from_user.username, callback.from_user.first_name)
        await _edit_or_replace_watch_ui(
            callback,
            user_id,
            "Раздел 2/3: выбор темы и удаление данных.\n"
            "Здесь можно выбрать активную тему и удалять ключи/чаты/тему.",
            _themes_manage_section_keyboard(),
        )
    await callback.answer()


@router.callback_query(F.data == "themes:section:info")
async def cb_themes_section_info(callback: CallbackQuery) -> None:
    if callback.message and callback.from_user:
        user_id = await _upsert_user(callback.from_user.id, callback.from_user.username, callback.from_user.first_name)
        await _edit_or_replace_watch_ui(
            callback,
            user_id,
            "Раздел 3/3: информация по темам.\n"
            "Здесь можно посмотреть список тем и чаты, по которым идет поиск.",
            _themes_info_section_keyboard(),
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
            _themes_info_section_keyboard(),
        )
    await callback.answer()


@router.callback_query(F.data == "themes:current")
async def cb_themes_current(callback: CallbackQuery) -> None:
    if callback.message and callback.from_user:
        user_id = await _upsert_user(callback.from_user.id, callback.from_user.username, callback.from_user.first_name)
        try:
            theme = await _resolve_theme_for_user(user_id, None)
            await _edit_or_replace_watch_ui(callback, user_id, _format_theme(theme), _themes_manage_section_keyboard())
        except Exception as e:
            await _edit_or_replace_watch_ui(callback, user_id, str(e), _themes_manage_section_keyboard())
    await callback.answer()


@router.callback_query(F.data == "themes:search_chats")
async def cb_themes_search_chats(callback: CallbackQuery) -> None:
    if callback.message and callback.from_user:
        user_id = await _upsert_user(callback.from_user.id, callback.from_user.username, callback.from_user.first_name)
        text = await _build_search_chats_text(user_id)
        await _edit_or_replace_watch_ui(callback, user_id, text, _themes_info_section_keyboard())
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
            _themes_add_section_keyboard(),
        )
    await callback.answer()


async def _add_all_account_chats_to_theme_via_callback(
    callback: CallbackQuery,
    user_id: int,
    theme: ThemeDTO,
) -> None:
    _, database, auth, search = _require_services()
    if not await auth.is_authorized(user_id):
        await _edit_or_replace_watch_ui(
            callback,
            user_id,
            "Сначала авторизуйте аккаунт через /auth.",
            _theme_add_chat_keyboard(theme.id),
        )
        return

    await _edit_or_replace_watch_ui(
        callback,
        user_id,
        f"Тема: {theme.name}\nСобираю все чаты аккаунта. Это может занять до минуты...",
        _theme_add_chat_keyboard(theme.id),
    )

    try:
        account_chat_refs = await search.list_account_chat_refs(user_id)
    except Exception as e:
        await _edit_or_replace_watch_ui(
            callback,
            user_id,
            f"Не удалось получить чаты аккаунта: {e}",
            _theme_add_chat_keyboard(theme.id),
        )
        return

    if not account_chat_refs:
        await _edit_or_replace_watch_ui(
            callback,
            user_id,
            "В аккаунте не найдено доступных чатов для добавления.",
            _theme_add_chat_keyboard(theme.id),
        )
        return

    try:
        added, total_unique = await database.add_chats_bulk(user_id, theme.name, account_chat_refs)
    except Exception as e:
        await _edit_or_replace_watch_ui(
            callback,
            user_id,
            f"Не удалось добавить чаты в тему: {e}",
            _theme_add_chat_keyboard(theme.id),
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
        _theme_add_chat_keyboard(theme.id),
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
            _themes_add_section_keyboard(),
        )
        await callback.answer()
        return

    themes = await database.list_themes(user_id)
    if not themes:
        await _edit_or_replace_watch_ui(
            callback,
            user_id,
            "Тем пока нет. Сначала создайте тему.",
            _themes_add_section_keyboard(),
        )
        await callback.answer()
        return

    if len(themes) == 1:
        await callback.answer("Запускаю импорт чатов...", show_alert=False)
        await _add_all_account_chats_to_theme_via_callback(callback, user_id, themes[0])
        return

    await _edit_or_replace_watch_ui(
        callback,
        user_id,
        "Выберите тему, в которую добавить все чаты аккаунта:",
        _theme_picker_for_all_chats_keyboard(themes, back_callback="themes:section:add"),
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
            _themes_add_section_keyboard(),
        )
        await callback.answer()
        return

    await callback.answer("Запускаю импорт чатов...", show_alert=False)
    await _add_all_account_chats_to_theme_via_callback(callback, user_id, theme)


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
            _themes_add_section_keyboard(),
        )
        await callback.answer()
        return

    if len(themes) == 1:
        theme = themes[0]
        active_theme_by_user[user_id] = theme.name
        await state.set_state(ThemeUiStates.waiting_bulk_payload)
        await state.update_data(theme_ui_action=action, theme_name=theme.name)
        next_markup = _theme_add_chat_keyboard(theme.id) if action == "add_chat" else _themes_add_section_keyboard()
        await _edit_or_replace_watch_ui(callback, user_id, _theme_action_prompt(action, theme.name), next_markup)
        await callback.answer()
        return

    action_title = "Выберите тему для добавления чатов" if action_code == "ac" else "Выберите тему для добавления ключевых слов"
    await _edit_or_replace_watch_ui(
        callback,
        user_id,
        action_title,
        _theme_picker_keyboard(themes, action_code, back_callback="themes:section:add"),
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
        await _edit_or_replace_watch_ui(callback, user_id, "Тем пока нет. Сначала создайте тему.", _themes_manage_section_keyboard())
        await callback.answer()
        return

    action_title = {
        "set_active": "Выберите тему для активации",
        "add_kw": "Выберите тему для добавления ключевых слов",
        "del_kw": "Выберите тему для удаления ключевых слов",
        "add_chat": "Выберите тему для добавления чатов",
        "del_chat": "Выберите тему для удаления чатов",
        "delete_theme": "Выберите тему для удаления",
    }[action]

    await _edit_or_replace_watch_ui(
        callback,
        user_id,
        action_title,
        _theme_picker_keyboard(themes, action_code, back_callback="themes:section:manage"),
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
        fallback_markup = _themes_add_section_keyboard() if action in {"add_chat", "add_kw"} else _themes_manage_section_keyboard()
        await _edit_or_replace_watch_ui(callback, user_id, "Тема не найдена или уже удалена.", fallback_markup)
        await callback.answer()
        return

    if action == "set_active":
        active_theme_by_user[user_id] = theme.name
        await _edit_or_replace_watch_ui(callback, user_id, f"Активная тема: {theme.name}", _themes_manage_section_keyboard())
        await callback.answer()
        return

    if action == "delete_theme":
        ok = await database.delete_theme(user_id, theme.name)
        if ok:
            current = active_theme_by_user.get(user_id)
            if current and current.casefold() == theme.name.casefold():
                active_theme_by_user.pop(user_id, None)
            await _edit_or_replace_watch_ui(callback, user_id, f"Тема '{theme.name}' удалена.", _themes_manage_section_keyboard())
        else:
            await _edit_or_replace_watch_ui(callback, user_id, "Не удалось удалить тему.", _themes_manage_section_keyboard())
        await callback.answer()
        return

    active_theme_by_user[user_id] = theme.name
    await state.set_state(ThemeUiStates.waiting_bulk_payload)
    await state.update_data(theme_ui_action=action, theme_name=theme.name)
    if action == "add_chat":
        next_markup = _theme_add_chat_keyboard(theme.id)
    elif action == "add_kw":
        next_markup = _themes_add_section_keyboard()
    else:
        next_markup = _themes_manage_section_keyboard()
    await _edit_or_replace_watch_ui(callback, user_id, _theme_action_prompt(action, theme.name), next_markup)
    await callback.answer()


@router.message(ThemeUiStates.waiting_create_theme_name, F.text, ~F.text.startswith("/"))
async def st_theme_create_name(message: Message, state: FSMContext) -> None:
    user_id = await _ensure_user(message)
    _, database, _, _ = _require_services()
    name = (message.text or "").strip()
    if not name:
        await _send_watch_ui_message(message, user_id, "Имя темы пустое. Введите корректное имя.", _themes_menu_keyboard())
        return

    try:
        theme = await database.create_theme(user_id, name)
    except Exception as e:
        await _send_watch_ui_message(message, user_id, f"Не удалось создать тему: {e}", _themes_menu_keyboard())
        return

    active_theme_by_user[user_id] = theme.name
    await state.set_state(ThemeUiStates.waiting_new_theme_keywords)
    await state.update_data(theme_name=theme.name, wizard_kw_added=0)
    await _send_watch_ui_message(
        message,
        user_id,
        _theme_wizard_keywords_prompt(theme.name),
        _themes_menu_keyboard(),
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
            _themes_menu_keyboard(),
        )
        return
    await state.set_state(ThemeUiStates.waiting_new_theme_chats)
    await state.update_data(theme_name=theme_name, wizard_kw_added=0)
    await _send_watch_ui_message(message, user_id, _theme_wizard_chats_prompt(theme_name), _themes_menu_keyboard())


@router.message(ThemeUiStates.waiting_new_theme_keywords, F.text, ~F.text.startswith("/"))
async def st_theme_wizard_keywords_input(message: Message, state: FSMContext) -> None:
    user_id = await _ensure_user(message)
    _, database, _, _ = _require_services()
    text = (message.text or "").strip()
    if not text:
        await _send_watch_ui_message(message, user_id, "Пустой ввод. Отправьте ключевые слова списком.", _themes_menu_keyboard())
        return

    data = await state.get_data()
    theme_name = data.get("theme_name")
    if not theme_name:
        await state.clear()
        await _send_watch_ui_message(
            message,
            user_id,
            "Сессия создания темы устарела. Откройте раздел 'Темы' и начните заново.",
            _themes_menu_keyboard(),
        )
        return

    theme_before = await database.get_theme(user_id, theme_name)
    if theme_before is None:
        await state.clear()
        await _send_watch_ui_message(message, user_id, f"Тема '{theme_name}' не найдена.", _themes_menu_keyboard())
        return

    items = _split_bulk_items(text, allow_comma=True)
    if not items:
        await _send_watch_ui_message(message, user_id, "Не удалось распознать ключевые слова. Проверьте формат.", _themes_menu_keyboard())
        return

    try:
        for item in items:
            await database.add_keyword(user_id, theme_before.name, item)
    except Exception as e:
        await _send_watch_ui_message(message, user_id, f"Не удалось добавить ключевые слова: {e}", _themes_menu_keyboard())
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
        _themes_menu_keyboard(),
    )


@router.message(ThemeUiStates.waiting_new_theme_chats, F.text.startswith("/skip"))
async def st_theme_wizard_chats_skip(message: Message, state: FSMContext) -> None:
    user_id = await _ensure_user(message)
    data = await state.get_data()
    theme_name = data.get("theme_name")
    kw_added = int(data.get("wizard_kw_added") or 0)
    await state.clear()
    summary = await _build_themes_panel_text(user_id)
    await _send_watch_ui_message(
        message,
        user_id,
        (
            f"Готово. Тема '{theme_name or '(неизвестно)'}' настроена.\n"
            f"Ключевых слов добавлено: {kw_added}\n"
            "Чатов добавлено: 0\n\n"
            f"{summary}"
        ),
        _themes_menu_keyboard(),
    )


@router.message(ThemeUiStates.waiting_new_theme_chats, F.text, ~F.text.startswith("/"))
async def st_theme_wizard_chats_input(message: Message, state: FSMContext) -> None:
    user_id = await _ensure_user(message)
    _, database, _, _ = _require_services()
    text = (message.text or "").strip()
    if not text:
        await _send_watch_ui_message(message, user_id, "Пустой ввод. Отправьте чаты списком.", _themes_menu_keyboard())
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
            _themes_menu_keyboard(),
        )
        return

    theme_before = await database.get_theme(user_id, theme_name)
    if theme_before is None:
        await state.clear()
        await _send_watch_ui_message(message, user_id, f"Тема '{theme_name}' не найдена.", _themes_menu_keyboard())
        return

    items = _split_bulk_items(text, allow_comma=False)
    if not items:
        await _send_watch_ui_message(message, user_id, "Не удалось распознать чаты. Проверьте формат.", _themes_menu_keyboard())
        return

    try:
        for item in items:
            await database.add_chat(user_id, theme_before.name, item)
    except Exception as e:
        await _send_watch_ui_message(message, user_id, f"Не удалось добавить чаты: {e}", _themes_menu_keyboard())
        return

    theme_after = await database.get_theme(user_id, theme_before.name)
    chat_added = 0
    if theme_after is not None:
        chat_added = max(0, len(theme_after.chats) - len(theme_before.chats))

    await state.clear()
    summary = await _build_themes_panel_text(user_id)
    await _send_watch_ui_message(
        message,
        user_id,
        (
            f"Готово. Тема '{theme_before.name}' настроена.\n"
            f"Ключевых слов добавлено: {kw_added}\n"
            f"Чатов добавлено: {chat_added} (введено: {len(items)})\n\n"
            f"{summary}"
        ),
        _themes_menu_keyboard(),
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
    summary = await _build_themes_panel_text(user_id)
    await _send_watch_ui_message(message, user_id, f"{result_text}\n\n{summary}", _themes_menu_keyboard())


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
            await _edit_or_replace_watch_ui(callback, user_id, "Сначала авторизуйте аккаунт через /auth.", _main_menu())
            await callback.answer()
            return

        await _edit_or_replace_watch_ui(
            callback,
            user_id,
            "Быстрый поиск кнопками:\n"
            "1) Выберите тему или 'Все чаты'\n"
            "2) Выберите формат результата\n"
            "3) Выберите лимит\n"
            "4) Выберите период поиска.",
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
            "Темы отсутствуют. Создайте тему в разделе 'Темы'.",
            _search_menu_keyboard(),
        )
        await callback.answer()
        return

    await _edit_or_replace_watch_ui(callback, user_id, "Выберите тему для поиска:", _search_theme_picker_keyboard(themes))
    await callback.answer()


@router.callback_query(F.data == "searchui:all")
async def cb_search_all(callback: CallbackQuery) -> None:
    if callback.message and callback.from_user:
        user_id = await _upsert_user(callback.from_user.id, callback.from_user.username, callback.from_user.first_name)
        await _edit_or_replace_watch_ui(
            callback,
            user_id,
            "Режим: По всем чатам из всех ваших тем.\n"
            "Выберите формат результата:",
            _search_format_keyboard(scope="all", theme_id=0),
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
    await _edit_or_replace_watch_ui(
        callback,
        user_id,
        f"Режим: Тема '{theme.name}'.\nВыберите формат результата:",
        _search_format_keyboard(scope="th", theme_id=theme.id),
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
        await _edit_or_replace_watch_ui(callback, user_id, "Сначала авторизуйте аккаунт через /auth.", _main_menu())
        await callback.answer()
        return

    current_task = active_search_tasks.get(user_id)
    if current_task and not current_task.done():
        await _edit_or_replace_watch_ui(
            callback,
            user_id,
            "Поиск уже выполняется. Остановите его через /cancel.",
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
            "Отмена: /cancel",
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
        await _edit_or_replace_watch_ui(callback, user_id, "Сначала авторизуйте аккаунт через /auth.", _main_menu())
        await callback.answer()
        return

    current_task = active_search_tasks.get(user_id)
    if current_task and not current_task.done():
        await _edit_or_replace_watch_ui(
            callback,
            user_id,
            "Поиск уже выполняется. Остановите его через /cancel.",
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
            "Отмена: /cancel",
            _search_menu_keyboard(),
        )
        await callback.answer()
        return

    try:
        date_from, date_to = _preset_date_range(preset)
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
        )
    except Exception as e:
        await _edit_or_replace_watch_ui(callback, user_id, f"Ошибка запуска поиска: {e}", _search_menu_keyboard())
        await callback.answer()
        return

    await state.clear()
    active_search_tasks[user_id] = task
    await _edit_or_replace_watch_ui(callback, user_id, start_text, _search_menu_keyboard())
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
        await _send_watch_ui_message(message, user_id, f"{e}\nПопробуйте снова или введите /cancel.", _search_menu_keyboard())
        return

    _, _, auth, _ = _require_services()
    if not await auth.is_authorized(user_id):
        await state.clear()
        await _send_watch_ui_message(message, user_id, "Сначала авторизуйте аккаунт через /auth.", _main_menu())
        return

    current_task = active_search_tasks.get(user_id)
    if current_task and not current_task.done():
        await state.clear()
        await _send_watch_ui_message(message, user_id, "Поиск уже выполняется. Остановите его через /cancel.", _search_menu_keyboard())
        return

    try:
        start_text, task = await _create_ui_search_task(
            bot=message.bot,
            user_id=user_id,
            chat_id=message.chat.id,
            scope=scope,
            payload_id=payload_id,
            output_format=output_format,
            limit=limit,
            date_from=date_from,
            date_to=date_to,
        )
    except Exception as e:
        await state.clear()
        await _send_watch_ui_message(message, user_id, f"Ошибка запуска поиска: {e}", _search_menu_keyboard())
        return

    await state.clear()
    active_search_tasks[user_id] = task
    await _send_watch_ui_message(message, user_id, start_text, _search_menu_keyboard())


@router.message(Command("search"))
async def cmd_search(message: Message) -> None:
    user_id = await _ensure_user(message)
    app_settings, _, auth, _ = _require_services()
    await _cleanup_previous_watch_ui_message(message.bot, user_id, message.chat.id)

    is_auth = await auth.is_authorized(user_id)
    if not is_auth:
        await message.answer("Сначала авторизуйте аккаунт через /auth.")
        return

    try:
        parsed = _parse_search_command(message.text or "", default_limit=app_settings.default_limit)
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

    await message.answer(
        f"Запускаю поиск по теме '{theme.name}' "
        f"(limit={_format_limit_value(parsed.limit)}, format={parsed.output_format})."
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
        await _send_watch_ui_message(message, user_id, "Редактирование темы отменено.", _themes_menu_keyboard())
        return
    if current_state and current_state.startswith(f"{WatchUiStates.__name__}:"):
        await state.clear()
        await _send_watch_ui_message(message, user_id, "Настройка периода автопроверки отменена.", _watch_menu_keyboard())
        return
    if current_state and current_state.startswith(f"{AuthStates.__name__}:"):
        await state.clear()
        await _send_watch_ui_message(message, user_id, "Ввод 2FA отменен.", await _main_menu_for_user(user_id))
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
            await _edit_or_replace_watch_ui(callback, user_id, "Редактирование темы отменено.", _themes_menu_keyboard())
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

