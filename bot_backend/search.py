from __future__ import annotations

import asyncio
import csv
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Awaitable, Callable, Optional

from telethon import TelegramClient
from telethon.errors import FloodWaitError

from bot_backend.db import ThemeDTO

WORD_RE = re.compile(r"[a-zа-я0-9]+")
DATE_ONLY_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


class SearchError(RuntimeError):
    pass


@dataclass
class SearchParams:
    limit: Optional[int]
    date_from: Optional[datetime]
    date_to: Optional[datetime]


@dataclass
class SearchItem:
    date: datetime
    chat: str
    msg_id: int
    matched_keywords: list[str]
    text: str
    link: str


def ensure_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def parse_date_bound(value: str, end_of_day: bool) -> datetime:
    raw = value.strip()
    if DATE_ONLY_RE.fullmatch(raw):
        dt = datetime.strptime(raw, "%Y-%m-%d")
        if end_of_day:
            dt = dt + timedelta(days=1) - timedelta(microseconds=1)
        return ensure_utc(dt)
    dt = datetime.fromisoformat(raw.replace("Z", "+00:00").replace("z", "+00:00"))
    return ensure_utc(dt)


def parse_date_from(value: str) -> datetime:
    return parse_date_bound(value, end_of_day=False)


def parse_date_to(value: str) -> datetime:
    return parse_date_bound(value, end_of_day=True)


def validate_date_range(date_from: Optional[datetime], date_to: Optional[datetime]) -> None:
    if date_from and date_to and date_from > date_to:
        raise SearchError("Некорректный диапазон: date-from позже date-to.")


def in_date_range(msg_date: datetime, date_from: Optional[datetime], date_to: Optional[datetime]) -> bool:
    msg_dt = ensure_utc(msg_date)
    if date_from and msg_dt < date_from:
        return False
    if date_to and msg_dt > date_to:
        return False
    return True


def should_stop_by_lower_bound(msg_date: datetime, date_from: Optional[datetime]) -> bool:
    return bool(date_from and ensure_utc(msg_date) < date_from)


def iter_offset_date(date_to: Optional[datetime]) -> Optional[datetime]:
    if not date_to:
        return None
    return date_to + timedelta(microseconds=1)


def normalize_text(value: str) -> str:
    return (value or "").lower().replace("ё", "е")


def normalize_theme_for_filename(theme: str) -> str:
    normalized = re.sub(r"[^\w-]+", "_", (theme or "").strip().casefold(), flags=re.UNICODE)
    normalized = normalized.strip("_")
    return normalized or "theme"


def format_date_label(date_from: Optional[datetime], date_to: Optional[datetime]) -> str:
    if not date_from and not date_to:
        return datetime.now(timezone.utc).strftime("%Y%m%d")
    from_str = date_from.strftime("%Y%m%d") if date_from else "start"
    to_str = date_to.strftime("%Y%m%d") if date_to else "now"
    if from_str == to_str:
        return from_str
    return f"{from_str}-{to_str}"


def prepare_keywords(keywords: list[str]) -> list[tuple[str, str, list[str]]]:
    dedup: list[str] = []
    seen: set[str] = set()
    for kw in keywords:
        value = kw.strip()
        if not value:
            continue
        key = value.casefold()
        if key in seen:
            continue
        seen.add(key)
        dedup.append(value)

    prepared: list[tuple[str, str, list[str]]] = []
    for kw in dedup:
        kw_norm = normalize_text(kw).strip()
        if not kw_norm:
            continue
        parts = WORD_RE.findall(kw_norm)
        stems = []
        for part in parts:
            if len(part) >= 5:
                stems.append(part[:5])
            elif len(part) >= 4:
                stems.append(part[:4])
            else:
                stems.append(part)
        prepared.append((kw, kw_norm, stems))
    return prepared


def detect_matched_keywords(text: str, prepared_keywords: list[tuple[str, str, list[str]]]) -> list[str]:
    text_norm = normalize_text(text)
    words = WORD_RE.findall(text_norm)
    matched = []
    for original, kw_norm, stems in prepared_keywords:
        if kw_norm in text_norm:
            matched.append(original)
            continue
        if not stems:
            continue
        if all(any(w.startswith(stem) for w in words) for stem in stems):
            matched.append(original)
    return matched


def extract_text(msg) -> str:
    return (msg.message or "").strip()


def entity_display_name(entity, fallback: str) -> str:
    return getattr(entity, "title", None) or getattr(entity, "first_name", None) or getattr(entity, "username", None) or fallback


def normalize_tme_internal_id(entity_id: int) -> int:
    if entity_id > 10**12 and str(entity_id).startswith("100"):
        return int(str(entity_id)[3:])
    return entity_id


def build_message_link(entity, msg) -> str:
    direct_link = getattr(msg, "link", None)
    if isinstance(direct_link, str) and direct_link.strip():
        return direct_link.strip()

    message_id = getattr(msg, "id", None)
    if not isinstance(message_id, int):
        return ""

    username = getattr(entity, "username", None)
    if username:
        return f"https://t.me/{username}/{message_id}"

    entity_id = getattr(entity, "id", None)
    is_channel_like = hasattr(entity, "broadcast") or hasattr(entity, "megagroup")
    if isinstance(entity_id, int) and entity_id > 0 and is_channel_like:
        internal_id = normalize_tme_internal_id(entity_id)
        return f"https://t.me/c/{internal_id}/{message_id}"
    return ""


class SearchService:
    def __init__(
        self,
        api_id: int,
        api_hash: str,
        sessions_dir: Path,
        results_dir: Path,
        retention_days: int,
    ) -> None:
        self.api_id = api_id
        self.api_hash = api_hash
        self.sessions_dir = sessions_dir
        self.results_dir = results_dir
        self.retention_days = retention_days
        self.sessions_dir.mkdir(parents=True, exist_ok=True)
        self.results_dir.mkdir(parents=True, exist_ok=True)

    def _session_path(self, user_id: int) -> Path:
        return self.sessions_dir / f"user_{user_id}"

    def _new_client(self, user_id: int) -> TelegramClient:
        return TelegramClient(str(self._session_path(user_id)), self.api_id, self.api_hash)

    @staticmethod
    def _dialog_to_chat_ref(dialog) -> str:
        entity = getattr(dialog, "entity", None)
        username = getattr(entity, "username", None) if entity is not None else None
        if isinstance(username, str) and username.strip():
            return f"@{username.strip()}"

        dialog_id = getattr(dialog, "id", None)
        if isinstance(dialog_id, int):
            return str(dialog_id)

        entity_id = getattr(entity, "id", None) if entity is not None else None
        if isinstance(entity_id, int):
            return str(entity_id)

        name = getattr(dialog, "name", None)
        if isinstance(name, str) and name.strip():
            return name.strip()
        return ""

    async def list_account_chat_refs(self, user_id: int) -> list[str]:
        client = self._new_client(user_id)
        try:
            await client.connect()
            if not await client.is_user_authorized():
                raise SearchError("Аккаунт не авторизован. Сначала выполните /auth.")

            refs: list[str] = []
            seen: set[str] = set()
            dialogs = await self._list_dialogs_with_retry(client)
            for dialog in dialogs:
                ref = self._dialog_to_chat_ref(dialog).strip()
                if not ref:
                    continue
                key = ref.casefold()
                if key in seen:
                    continue
                seen.add(key)
                refs.append(ref)
            return refs
        finally:
            await client.disconnect()

    async def cleanup_old_files(self) -> None:
        border = datetime.now(timezone.utc) - timedelta(days=self.retention_days)
        if self.results_dir.exists():
            for path in self.results_dir.rglob("*.csv"):
                mtime = datetime.fromtimestamp(path.stat().st_mtime, timezone.utc)
                if mtime < border:
                    path.unlink(missing_ok=True)
        if self.sessions_dir.exists():
            for suffix in ("*.session", "*.session-journal"):
                for path in self.sessions_dir.rglob(suffix):
                    mtime = datetime.fromtimestamp(path.stat().st_mtime, timezone.utc)
                    if mtime < border:
                        path.unlink(missing_ok=True)

    async def run_theme_search(
        self,
        user_id: int,
        theme: ThemeDTO,
        params: SearchParams,
        progress_cb: Optional[Callable[[str], Awaitable[None]]] = None,
    ) -> tuple[list[SearchItem], Path]:
        validate_date_range(params.date_from, params.date_to)
        if params.limit is not None and params.limit <= 0:
            raise SearchError("Лимит должен быть > 0, либо None для поиска без лимита.")
        if not theme.chats:
            raise SearchError("В теме нет чатов.")
        if not theme.keywords:
            raise SearchError("В теме нет ключевых слов.")

        prepared_keywords = prepare_keywords(theme.keywords)
        if not prepared_keywords:
            raise SearchError("Список ключевых слов пуст после нормализации.")
        search_keywords = [kw for kw, _, _ in prepared_keywords]

        client = self._new_client(user_id)
        try:
            await client.connect()
            if not await client.is_user_authorized():
                raise SearchError("Аккаунт не авторизован. Сначала выполните /auth.")

            entities = await self._resolve_entities(client, theme.chats)
            if not entities:
                raise SearchError("Не удалось получить ни один чат из темы.")

            collected: dict[tuple[int, int], dict] = {}
            match_cache: dict[tuple[int, int], list[str]] = {}
            search_limit: Optional[int] = params.limit if params.limit is not None and not params.date_from and not params.date_to else None
            offset_date = iter_offset_date(params.date_to)

            scanned = 0
            for chat_ref, entity in entities.items():
                for kw in search_keywords:
                    per_keyword_found = 0
                    async for msg in self._iter_messages_with_retry(client, entity, kw, search_limit, offset_date):
                        if should_stop_by_lower_bound(msg.date, params.date_from):
                            break
                        if not in_date_range(msg.date, params.date_from, params.date_to):
                            continue

                        key = (entity.id, msg.id)
                        local_matches = match_cache.get(key)
                        if local_matches is None:
                            local_matches = detect_matched_keywords(extract_text(msg), prepared_keywords)
                            match_cache[key] = local_matches
                        if not local_matches:
                            continue

                        if key not in collected:
                            collected[key] = {
                                "date": msg.date,
                                "entity": entity,
                                "chat_ref": chat_ref,
                                "msg": msg,
                                "matched_keywords": set(),
                            }
                        collected[key]["matched_keywords"].update(local_matches)

                        per_keyword_found += 1
                        scanned += 1
                        if progress_cb and scanned % 20 == 0:
                            await progress_cb(f"Обработано совпадений: {scanned}")
                        if params.limit is not None and per_keyword_found >= params.limit:
                            break

            raw_items = list(collected.values())
            raw_items.sort(key=lambda x: x["date"], reverse=True)
            if params.limit is not None:
                raw_items = raw_items[: params.limit]

            items: list[SearchItem] = []
            for item in raw_items:
                entity = item["entity"]
                msg = item["msg"]
                chat_ref = item["chat_ref"]
                items.append(
                    SearchItem(
                        date=msg.date,
                        chat=entity_display_name(entity, chat_ref),
                        msg_id=msg.id,
                        matched_keywords=sorted(item["matched_keywords"]),
                        text=extract_text(msg),
                        link=build_message_link(entity, msg),
                    )
                )

            csv_path = self._save_results_csv(user_id, theme.name, params.date_from, params.date_to, items)
            return items, csv_path
        finally:
            await client.disconnect()

    async def _resolve_entities(self, client: TelegramClient, chats: list[str]) -> dict[str, object]:
        entities: dict[str, object] = {}
        dialogs = await self._list_dialogs_with_retry(client)
        by_id, by_name, by_username = self._build_dialog_index(dialogs)
        for chat_ref in chats:
            ref = chat_ref.strip()
            if not ref:
                continue
            try:
                entity = self._resolve_from_dialog_index(ref, by_id, by_name, by_username)
                if entity is None:
                    entity = await self._resolve_chat_with_retry(client, ref)
                if entity is not None:
                    entities[chat_ref] = entity
            except Exception:
                continue
        return entities

    @staticmethod
    def _normalize_chat_ref(chat_ref: str) -> str:
        value = (chat_ref or "").strip()
        if not value:
            return ""
        lower = value.casefold()
        for prefix in ("https://t.me/", "http://t.me/", "t.me/"):
            if lower.startswith(prefix):
                value = value[len(prefix) :]
                break
        value = value.strip().strip("/")
        if value.startswith("@"):
            value = value[1:]
        return value.casefold().strip()

    @staticmethod
    def _build_dialog_index(dialogs: list[object]) -> tuple[dict[int, object], dict[str, object], dict[str, object]]:
        by_id: dict[int, object] = {}
        by_name: dict[str, object] = {}
        by_username: dict[str, object] = {}
        for dialog in dialogs:
            entity = getattr(dialog, "entity", None)
            if entity is None:
                continue

            dialog_id = getattr(dialog, "id", None)
            if isinstance(dialog_id, int):
                by_id.setdefault(dialog_id, entity)
                by_id.setdefault(abs(dialog_id), entity)

            entity_id = getattr(entity, "id", None)
            if isinstance(entity_id, int):
                by_id.setdefault(entity_id, entity)
                by_id.setdefault(abs(entity_id), entity)

            name = getattr(dialog, "name", None)
            if isinstance(name, str) and name.strip():
                by_name.setdefault(name.strip().casefold(), entity)

            username = getattr(entity, "username", None)
            if isinstance(username, str) and username.strip():
                by_username.setdefault(username.strip().lstrip("@").casefold(), entity)
        return by_id, by_name, by_username

    def _resolve_from_dialog_index(
        self,
        chat_ref: str,
        by_id: dict[int, object],
        by_name: dict[str, object],
        by_username: dict[str, object],
    ) -> Optional[object]:
        if chat_ref.lstrip("-").isdigit():
            target_id = int(chat_ref)
            return by_id.get(target_id) or by_id.get(abs(target_id))

        normalized = self._normalize_chat_ref(chat_ref)
        if normalized:
            username_key = normalized.split("/", maxsplit=1)[0]
            if username_key:
                from_username = by_username.get(username_key)
                if from_username is not None:
                    return from_username

            if "/" not in normalized:
                from_name = by_name.get(normalized)
                if from_name is not None:
                    return from_name

        return by_name.get(chat_ref.strip().casefold())

    async def _resolve_chat_with_retry(self, client: TelegramClient, chat_ref: str):
        while True:
            try:
                return await self._resolve_chat_direct(client, chat_ref)
            except FloodWaitError as e:
                await asyncio.sleep(max(1, int(e.seconds)) + 1)

    async def _resolve_chat_direct(self, client: TelegramClient, chat_ref: str):
        if chat_ref.startswith("@"):
            return await client.get_entity(chat_ref)
        if chat_ref.startswith("https://t.me/") or chat_ref.startswith("http://t.me/") or chat_ref.startswith("t.me/"):
            return await client.get_entity(chat_ref)
        if chat_ref.lstrip("-").isdigit():
            return await client.get_entity(int(chat_ref))
        return await client.get_entity(chat_ref)

    async def _list_dialogs_with_retry(self, client: TelegramClient) -> list[object]:
        while True:
            try:
                return [dialog async for dialog in client.iter_dialogs()]
            except FloodWaitError as e:
                await asyncio.sleep(max(1, int(e.seconds)) + 1)

    async def _iter_messages_with_retry(
        self,
        client: TelegramClient,
        entity,
        query: str,
        limit: Optional[int],
        offset_date: Optional[datetime],
    ):
        while True:
            try:
                async for msg in client.iter_messages(entity, search=query, limit=limit, offset_date=offset_date):
                    yield msg
                return
            except FloodWaitError as e:
                await asyncio.sleep(max(1, int(e.seconds)) + 1)

    def _save_results_csv(
        self,
        user_id: int,
        theme_name: str,
        date_from: Optional[datetime],
        date_to: Optional[datetime],
        items: list[SearchItem],
    ) -> Path:
        user_dir = self.results_dir / str(user_id)
        user_dir.mkdir(parents=True, exist_ok=True)

        filename = f"results_{normalize_theme_for_filename(theme_name)}_{format_date_label(date_from, date_to)}.csv"
        target = user_dir / filename
        if target.exists():
            stem = target.stem
            ext = target.suffix
            index = 2
            while True:
                candidate = user_dir / f"{stem}_{index}{ext}"
                if not candidate.exists():
                    target = candidate
                    break
                index += 1

        with target.open("w", encoding="utf-8-sig", newline="") as file:
            writer = csv.DictWriter(
                file,
                fieldnames=["date", "chat", "msg_id", "matched_keywords", "text", "link"],
            )
            writer.writeheader()
            for item in items:
                writer.writerow(
                    {
                        "date": item.date.isoformat(),
                        "chat": item.chat,
                        "msg_id": item.msg_id,
                        "matched_keywords": ", ".join(item.matched_keywords),
                        "text": item.text,
                        "link": item.link,
                    }
                )
        return target
