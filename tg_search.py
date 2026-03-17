import os
import argparse
import asyncio
import re
from getpass import getpass
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Tuple, Dict, Set
import csv

from themes import THEMES
from dotenv import load_dotenv

from telethon import TelegramClient
from telethon.errors import (
    SessionPasswordNeededError,
    PasswordHashInvalidError,
    PhoneCodeInvalidError,
    PhoneCodeExpiredError,
    PhoneNumberInvalidError,
    FloodWaitError,
)
import qrcode

WORD_RE = re.compile(r"[a-zа-я0-9]+")
DATE_ONLY_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")



def must_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise SystemExit(f"Нет переменной окружения {name}. Проверь .env")
    return v


def must_env_int(name: str) -> int:
    raw = must_env(name)
    try:
        return int(raw)
    except ValueError as e:
        raise SystemExit(f"Переменная {name} должна быть целым числом, сейчас: {raw!r}") from e


def positive_int(value: str) -> int:
    try:
        ivalue = int(value)
    except ValueError as e:
        raise argparse.ArgumentTypeError("Ожидалось целое число") from e
    if ivalue <= 0:
        raise argparse.ArgumentTypeError("Значение должно быть > 0")
    return ivalue


def ensure_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def parse_date_bound(value: str, end_of_day: bool) -> datetime:
    raw = value.strip()
    try:
        if DATE_ONLY_RE.fullmatch(raw):
            dt = datetime.strptime(raw, "%Y-%m-%d")
            if end_of_day:
                dt = dt + timedelta(days=1) - timedelta(microseconds=1)
            return ensure_utc(dt)

        dt = datetime.fromisoformat(raw.replace("Z", "+00:00").replace("z", "+00:00"))
        return ensure_utc(dt)
    except ValueError as e:
        raise argparse.ArgumentTypeError(
            f"Неверная дата '{value}'. Используй YYYY-MM-DD или ISO, например 2026-02-27T15:30:00+03:00"
        ) from e


def parse_date_from(value: str) -> datetime:
    return parse_date_bound(value, end_of_day=False)


def parse_date_to(value: str) -> datetime:
    return parse_date_bound(value, end_of_day=True)


def validate_date_range(date_from: Optional[datetime], date_to: Optional[datetime]) -> None:
    if date_from and date_to and date_from > date_to:
        raise SystemExit("Некорректный диапазон: --date-from позже, чем --date-to.")


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


def print_date_range(date_from: Optional[datetime], date_to: Optional[datetime]) -> None:
    if not date_from and not date_to:
        return
    from_str = date_from.isoformat() if date_from else "-inf"
    to_str = date_to.isoformat() if date_to else "+inf"
    print(f"Диапазон дат (UTC): {from_str} .. {to_str}")


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


def build_auto_out_path(theme: str, date_from: Optional[datetime], date_to: Optional[datetime], out_dir: Optional[str]) -> str:
    folder = out_dir.strip() if out_dir else os.path.dirname(os.path.abspath(__file__))
    os.makedirs(folder, exist_ok=True)

    filename = f"results_{normalize_theme_for_filename(theme)}_{format_date_label(date_from, date_to)}.csv"
    target = os.path.join(folder, filename)
    if not os.path.exists(target):
        return target

    stem, ext = os.path.splitext(target)
    i = 2
    while True:
        candidate = f"{stem}_{i}{ext}"
        if not os.path.exists(candidate):
            return candidate
        i += 1


def add_date_args(cmd_parser: argparse.ArgumentParser) -> None:
    cmd_parser.add_argument(
        "--date-from",
        type=parse_date_from,
        default=None,
        help="Начальная дата включительно (YYYY-MM-DD или ISO datetime)",
    )
    cmd_parser.add_argument(
        "--date-to",
        type=parse_date_to,
        default=None,
        help="Конечная дата включительно (YYYY-MM-DD или ISO datetime)",
    )


def normalize_text(value: str) -> str:
    return (value or "").lower().replace("ё", "е")


def dedupe_preserve_order(values: List[str]) -> List[str]:
    seen: Set[str] = set()
    result: List[str] = []
    for raw in values:
        value = raw.strip()
        if not value:
            continue
        key = value.casefold()
        if key in seen:
            continue
        seen.add(key)
        result.append(value)
    return result


def prepare_keywords(keywords: List[str]) -> List[Tuple[str, str, List[str]]]:
    prepared: List[Tuple[str, str, List[str]]] = []
    for kw in dedupe_preserve_order(keywords):
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


def print_qr_ascii(url: str) -> None:
    qr = qrcode.QRCode(border=1)
    qr.add_data(url)
    qr.make(fit=True)
    matrix = qr.get_matrix()
    for row in matrix:
        print("".join("██" if cell else "  " for cell in row))


def normalize_phone(phone: str) -> str:
    phone = phone.strip().replace(" ", "").replace("-", "").replace("(", "").replace(")", "")
    if not phone.startswith("+"):
        raise SystemExit("Телефон должен быть в международном формате, например: +79191234567")
    return phone


def short_text(text: str, max_len: int = 180) -> str:
    text = (text or "").replace("\n", " ").strip()
    if len(text) <= max_len:
        return text
    return text[: max_len - 3] + "..."


def entity_display_name(entity, fallback: str) -> str:
    return (
        getattr(entity, "title", None)
        or getattr(entity, "first_name", None)
        or getattr(entity, "username", None)
        or fallback
    )


def normalize_tme_internal_id(entity_id: int) -> int:
    # Для некоторых источников id может приходить с префиксом 100...
    if entity_id > 10**12 and str(entity_id).startswith("100"):
        return int(str(entity_id)[3:])
    return entity_id


def build_message_link(entity, msg) -> Optional[str]:
    direct_link = getattr(msg, "link", None)
    if isinstance(direct_link, str) and direct_link.strip():
        return direct_link.strip()

    message_id = getattr(msg, "id", None)
    if not isinstance(message_id, int):
        return None

    username = getattr(entity, "username", None)
    if username:
        return f"https://t.me/{username}/{message_id}"

    entity_id = getattr(entity, "id", None)
    is_channel_like = hasattr(entity, "broadcast") or hasattr(entity, "megagroup")
    if isinstance(entity_id, int) and entity_id > 0 and is_channel_like:
        internal_id = normalize_tme_internal_id(entity_id)
        return f"https://t.me/c/{internal_id}/{message_id}"

    return None


def save_results_to_csv(items, out_path: str) -> None:
    with open(out_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "date",
                "chat",
                "msg_id",
                "matched_keywords",
                "text",
                "link",
            ],
        )
        writer.writeheader()

        for item in items:
            entity = item["entity"]
            msg = item["msg"]
            chat_ref = item["chat_ref"]
            matched_keywords = sorted(item["matched_keywords"])

            writer.writerow(
                {
                    "date": msg.date.isoformat(),
                    "chat": entity_display_name(entity, chat_ref),
                    "msg_id": msg.id,
                    "matched_keywords": ", ".join(matched_keywords),
                    "text": extract_text(msg),
                    "link": build_message_link(entity, msg) or "",
                }
            )


def extract_text(msg) -> str:
    return (msg.message or "").strip()

def detect_matched_keywords(text: str, prepared_keywords: List[Tuple[str, str, List[str]]]) -> List[str]:
    text_norm = normalize_text(text)
    words = WORD_RE.findall(text_norm)

    matched = []

    for kw, kw_norm, stems in prepared_keywords:
        # 1) Сначала пробуем точное вхождение
        if kw_norm in text_norm:
            matched.append(kw)
            continue

        # 2) Если не нашли, пробуем мягкое сравнение по основе слова
        #    Москва -> москв, дрон -> дрон, погода -> погод
        if not stems:
            continue

        ok = True
        for stem in stems:
            if not any(w.startswith(stem) for w in words):
                ok = False
                break

        if ok:
            matched.append(kw)

    return matched


async def iter_messages_with_retry(
    client: TelegramClient,
    entity,
    query: str,
    limit: Optional[int],
    offset_date: Optional[datetime] = None,
):
    while True:
        try:
            async for msg in client.iter_messages(entity, search=query, limit=limit, offset_date=offset_date):
                yield msg
            return
        except FloodWaitError as e:
            wait_for = max(1, int(e.seconds)) + 1
            print(f"FloodWait ({wait_for} сек) для запроса '{query}'. Жду и продолжаю...")
            await asyncio.sleep(wait_for)


async def resolve_chat(client: TelegramClient, chat_ref: str):
    chat_ref = chat_ref.strip()

    if chat_ref.startswith("@"):
        return await client.get_entity(chat_ref)

    if chat_ref.lstrip("-").isdigit():
        target_id = int(chat_ref)
        async for d in client.iter_dialogs():
            if d.entity.id == abs(target_id) or d.id == target_id:
                return d.entity
        raise SystemExit(f"Чат с id={chat_ref} не найден среди доступных диалогов.")

    async for d in client.iter_dialogs():
        if d.name == chat_ref:
            return d.entity

    raise SystemExit(
        f"Чат '{chat_ref}' не найден. Используй @username, id из dialogs или точное имя чата."
    )


async def cmd_login_phone(client: TelegramClient, phone: Optional[str], force_sms: bool) -> None:
    await client.connect()

    if await client.is_user_authorized():
        print("Уже авторизован ✅")
        return

    if not phone:
        phone = input("Введите телефон в формате +79191234567: ")
    phone = normalize_phone(phone)

    try:
        sent = await client.send_code_request(phone, force_sms=force_sms)
    except PhoneNumberInvalidError:
        raise SystemExit("Неверный номер телефона (формат/цифры).")
    except FloodWaitError as e:
        raise SystemExit(f"Слишком много попыток. Подожди {e.seconds} секунд и попробуй снова.")

    try:
        code_type = type(sent.type).__name__
    except Exception:
        code_type = "unknown"

    print(f"Код отправлен. Тип доставки: {code_type}")
    print("Если SMS не приходит — проверь Telegram на другом устройстве: сообщение от 'Telegram' с кодом входа.")

    code = input("Введите код: ").strip().replace(" ", "")

    try:
        await client.sign_in(phone=phone, code=code)
    except PhoneCodeInvalidError:
        raise SystemExit("Неверный код. Запусти login_phone ещё раз и введи новый код.")
    except PhoneCodeExpiredError:
        raise SystemExit("Код истёк. Запусти login_phone ещё раз — придёт новый код.")
    except SessionPasswordNeededError:
        pw = os.getenv("TG_2FA_PASSWORD") or getpass("Включена 2FA. Введи пароль (не код): ")
        try:
            await client.sign_in(password=pw)
        except PasswordHashInvalidError:
            raise SystemExit("Неверный пароль 2FA. Запусти снова и введи правильный.")
    except FloodWaitError as e:
        raise SystemExit(f"Слишком много попыток. Подожди {e.seconds} секунд и попробуй снова.")

    print("Вход выполнен ✅ Сессия сохранена.")


async def cmd_login_qr(client: TelegramClient) -> None:
    await client.connect()

    if await client.is_user_authorized():
        print("Уже авторизован ✅")
        return

    print("Открой Telegram на телефоне → Настройки → Устройства → Подключить устройство (сканер QR)")
    print("Сканируй QR и подтверди вход. Если не успеешь — QR обновится автоматически.\n")

    while not await client.is_user_authorized():
        qr_login = await client.qr_login()
        print("Новый QR:\n")
        print_qr_ascii(qr_login.url)

        try:
            await qr_login.wait(timeout=180)
        except asyncio.TimeoutError:
            print("\n⏳ Время вышло — генерирую новый QR...\n")
            continue
        except SessionPasswordNeededError:
            pw = os.getenv("TG_2FA_PASSWORD") or getpass("\nВключена 2FA. Введи пароль: ")
            try:
                await client.sign_in(password=pw)
            except PasswordHashInvalidError:
                print("❌ Пароль 2FA неверный. Попробуем снова.\n")
                continue

    print("\nВход выполнен ✅ Сессия сохранена.")


async def cmd_dialogs(client: TelegramClient, limit: int) -> None:
    await client.connect()

    if not await client.is_user_authorized():
        raise SystemExit("Сессия не авторизована. Сначала выполни: python tg_search.py login_phone")

    i = 0
    async for d in client.iter_dialogs():
        i += 1
        entity = d.entity
        username = getattr(entity, "username", None)
        kind = type(entity).__name__
        print(f"{i:02d}. [{kind}] {d.name} | id={entity.id}" + (f" | @{username}" if username else ""))
        if i >= limit:
            break


async def cmd_search(
    client: TelegramClient,
    chat: str,
    query: str,
    limit: int,
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None,
) -> None:
    await client.connect()

    if not await client.is_user_authorized():
        raise SystemExit("Сессия не авторизована. Сначала выполни вход.")

    validate_date_range(date_from, date_to)
    entity = await resolve_chat(client, chat)

    print(f"Ищу в: {chat}")
    print(f"Запрос: {query}")
    print_date_range(date_from, date_to)
    print("-" * 80)

    found = 0
    search_limit: Optional[int] = limit if not date_from and not date_to else None
    offset_date = iter_offset_date(date_to)

    async for msg in iter_messages_with_retry(client, entity, query, search_limit, offset_date):
        if should_stop_by_lower_bound(msg.date, date_from):
            break
        if not in_date_range(msg.date, date_from, date_to):
            continue

        found += 1
        chat_name = entity_display_name(entity, chat)
        text = extract_text(msg)
        link = build_message_link(entity, msg)

        print(f"[{found}] {msg.date} | chat={chat_name} | msg_id={msg.id}")
        print(short_text(text))
        if link:
            print(f"link: {link}")
        print("-" * 80)
        if found >= limit:
            break

    if found == 0:
        print("Ничего не найдено.")


async def cmd_search_many(
    client: TelegramClient,
    chats: List[str],
    query: str,
    limit: int,
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None,
) -> None:
    await client.connect()

    if not await client.is_user_authorized():
        raise SystemExit("Сессия не авторизована. Сначала выполни вход.")

    validate_date_range(date_from, date_to)
    chats = dedupe_preserve_order(chats)

    if not chats:
        raise SystemExit("Нужно указать хотя бы один --chat")

    print("Ищу в нескольких чатах:")
    for chat in chats:
        print(f" - {chat}")
    print(f"Запрос: {query}")
    print_date_range(date_from, date_to)
    print("-" * 80)

    collected: List[Tuple] = []
    search_limit: Optional[int] = limit if not date_from and not date_to else None
    offset_date = iter_offset_date(date_to)

    for chat in chats:
        try:
            entity = await resolve_chat(client, chat)
        except Exception as e:
            print(f"Пропускаю {chat}: {e}")
            continue

        per_chat_found = 0
        async for msg in iter_messages_with_retry(client, entity, query, search_limit, offset_date):
            if should_stop_by_lower_bound(msg.date, date_from):
                break
            if not in_date_range(msg.date, date_from, date_to):
                continue

            collected.append((msg.date, entity, chat, msg))
            per_chat_found += 1
            if per_chat_found >= limit:
                break

    collected.sort(key=lambda x: x[0], reverse=True)
    collected = collected[:limit]

    if not collected:
        print("Ничего не найдено.")
        return

    for idx, (_, entity, chat_ref, msg) in enumerate(collected, start=1):
        chat_name = entity_display_name(entity, chat_ref)
        text = extract_text(msg)
        link = build_message_link(entity, msg)

        print(f"[{idx}] {msg.date} | chat={chat_name} | msg_id={msg.id}")
        print(short_text(text))
        if link:
            print(f"link: {link}")
        print("-" * 80)


async def cmd_search_set(
    client: TelegramClient,
    theme: str,
    chats: List[str],
    keywords: List[str],
    limit: int,
    out_path: Optional[str] = None,
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None,
    out_dir: Optional[str] = None,
) -> None:
    await client.connect()

    if not await client.is_user_authorized():
        raise SystemExit("Сессия не авторизована. Сначала выполни вход.")

    validate_date_range(date_from, date_to)
    chats = dedupe_preserve_order(chats)
    prepared_keywords = prepare_keywords(keywords)
    search_keywords = [kw for kw, _, _ in prepared_keywords]

    if not chats:
        raise SystemExit("Нужно указать хотя бы один --chat")
    if not search_keywords:
        raise SystemExit("Нужно указать хотя бы один --kw")

    print(f"Тема: {theme}")
    print("Чаты:")
    for chat in chats:
        print(f" - {chat}")
    print("Ключевые слова:")
    for kw in search_keywords:
        print(f" - {kw}")
    print_date_range(date_from, date_to)
    print("-" * 80)

    entities: Dict[str, object] = {}
    for chat in chats:
        try:
            entities[chat] = await resolve_chat(client, chat)
        except Exception as e:
            print(f"Пропускаю {chat}: {e}")

    if not entities:
        print("Не удалось получить ни один чат.")
        return

    collected: Dict[Tuple[int, int], Dict] = {}
    match_cache: Dict[Tuple[int, int], List[str]] = {}
    search_limit: Optional[int] = limit if not date_from and not date_to else None
    offset_date = iter_offset_date(date_to)

    for chat_ref, entity in entities.items():
        for kw in search_keywords:
            per_keyword_found = 0
            async for msg in iter_messages_with_retry(client, entity, kw, search_limit, offset_date):
                if should_stop_by_lower_bound(msg.date, date_from):
                    break
                if not in_date_range(msg.date, date_from, date_to):
                    continue

                key = (entity.id, msg.id)
                local_matches = match_cache.get(key)
                if local_matches is None:
                    text = extract_text(msg)
                    local_matches = detect_matched_keywords(text, prepared_keywords)
                    match_cache[key] = local_matches

                # Показываем только те сообщения, где ключ реально есть в видимом тексте
                if not local_matches:
                    continue

                if key not in collected:
                    collected[key] = {
                        "date": msg.date,
                        "entity": entity,
                        "chat_ref": chat_ref,
                        "msg": msg,
                        "matched_keywords": set(),  # type: Set[str]
                    }

                collected[key]["matched_keywords"].update(local_matches)
                per_keyword_found += 1
                if per_keyword_found >= limit:
                    break

    items = list(collected.values())
    items.sort(key=lambda x: x["date"], reverse=True)
    items = items[:limit]

    if not items:
        print("Ничего не найдено.")
        return

    for idx, item in enumerate(items, start=1):
        entity = item["entity"]
        msg = item["msg"]
        chat_ref = item["chat_ref"]
        matched_keywords = sorted(item["matched_keywords"])

        chat_name = entity_display_name(entity, chat_ref)
        text = extract_text(msg)
        link = build_message_link(entity, msg)

        print(f"[{idx}] {msg.date} | chat={chat_name} | msg_id={msg.id}")
        print(f"matched: {', '.join(matched_keywords)}")
        print(short_text(text))
        if link:
            print(f"link: {link}")
        print("-" * 80)

    final_out_path = out_path or build_auto_out_path(theme, date_from, date_to, out_dir)
    save_results_to_csv(items, final_out_path)
    if out_path:
        print(f"CSV сохранён: {final_out_path}")
    else:
        print(f"CSV авто-сохранён: {final_out_path}")


async def cmd_run_theme(
    client: TelegramClient,
    theme: str,
    limit: int,
    out_path: Optional[str] = None,
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None,
    out_dir: Optional[str] = None,
) -> None:
    cfg = THEMES.get(theme)

    if not cfg:
        available = ", ".join(sorted(THEMES.keys()))
        raise SystemExit(f"Неизвестная тема '{theme}'. Доступные темы: {available}")

    chats = dedupe_preserve_order(cfg.get("chats", []))
    keywords = dedupe_preserve_order(cfg.get("keywords", []))

    if not chats:
        raise SystemExit(f"У темы '{theme}' пустой список chats")
    if not keywords:
        raise SystemExit(f"У темы '{theme}' пустой список keywords")

    await cmd_search_set(
        client,
        theme,
        chats,
        keywords,
        limit,
        out_path,
        date_from,
        date_to,
        out_dir,
    )


async def cmd_run_all_themes(
    client: TelegramClient,
    limit: int,
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None,
    out_dir: Optional[str] = None,
) -> None:
    themes = sorted(THEMES.keys())
    if not themes:
        raise SystemExit("В themes.py нет ни одной темы.")

    validate_date_range(date_from, date_to)
    print("Запуск по всем темам:")
    for theme in themes:
        print(f" - {theme}")
    print_date_range(date_from, date_to)
    print("-" * 80)

    for idx, theme in enumerate(themes, start=1):
        print(f"\n=== [{idx}/{len(themes)}] {theme} ===")
        try:
            await cmd_run_theme(client, theme, limit, None, date_from, date_to, out_dir)
        except SystemExit as e:
            print(f"Тема '{theme}' пропущена: {e}")


async def main() -> None:
    load_dotenv()

    parser = argparse.ArgumentParser(description="Telegram parser: login + dialogs + search")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_phone = sub.add_parser("login_phone", help="Войти по телефону (код)")
    p_phone.add_argument("--phone", type=str, default=None, help="Напр: +79191234567")
    p_phone.add_argument("--force-sms", action="store_true", help="Попросить SMS (если возможно)")

    sub.add_parser("login_qr", help="Войти через QR (без SMS/кодов)")

    p_dialogs = sub.add_parser("dialogs", help="Показать список чатов/каналов (первые N)")
    p_dialogs.add_argument("--limit", type=positive_int, default=20)

    p_search = sub.add_parser("search", help="Поиск сообщений в одном чате/канале")
    p_search.add_argument("--chat", required=True, help="@username, id или точное имя чата")
    p_search.add_argument("--query", required=True, help="Строка поиска")
    p_search.add_argument("--limit", type=positive_int, default=10, help="Сколько сообщений вывести")
    add_date_args(p_search)

    p_search_many = sub.add_parser("search_many", help="Поиск сообщений сразу в нескольких чатах/каналах")
    p_search_many.add_argument("--chat", action="append", required=True, help="Можно указывать несколько раз")
    p_search_many.add_argument("--query", required=True, help="Строка поиска")
    p_search_many.add_argument("--limit", type=positive_int, default=10, help="Сколько сообщений вывести всего")
    add_date_args(p_search_many)

    p_search_set = sub.add_parser("search_set", help="Поиск по теме и набору ключевых слов")
    p_run_theme = sub.add_parser("run_theme", help="Запуск готовой темы из themes.py")
    p_run_all = sub.add_parser("run_all_themes", help="Запуск поиска сразу по всем темам из themes.py")
    p_run_theme.add_argument("--theme", required=True, help="Название темы из themes.py")
    p_run_theme.add_argument("--limit", type=positive_int, default=10, help="Сколько сообщений вывести всего")
    p_run_theme.add_argument("--out", type=str, default=None, help="Путь к CSV-файлу (иначе имя создастся автоматически)")
    p_run_theme.add_argument("--out-dir", type=str, default=None, help="Папка для авто-сохранения CSV")
    add_date_args(p_run_theme)
    p_run_all.add_argument("--limit", type=positive_int, default=10, help="Сколько сообщений вывести на тему")
    p_run_all.add_argument("--out-dir", type=str, default=None, help="Папка для авто-сохранения CSV")
    add_date_args(p_run_all)
    p_search_set.add_argument("--theme", required=True, help="Название темы, например news / fertilizers / vpn")
    p_search_set.add_argument("--chat", action="append", required=True, help="Можно указывать несколько раз")
    p_search_set.add_argument("--kw", action="append", required=True, help="Ключевое слово или фраза; можно указывать несколько раз")
    p_search_set.add_argument("--limit", type=positive_int, default=10, help="Сколько сообщений вывести всего")
    p_search_set.add_argument("--out", type=str, default=None, help="Путь к CSV-файлу (иначе имя создастся автоматически)")
    p_search_set.add_argument("--out-dir", type=str, default=None, help="Папка для авто-сохранения CSV")
    add_date_args(p_search_set)

    args = parser.parse_args()

    api_id = must_env_int("TG_API_ID")
    api_hash = must_env("TG_API_HASH")
    session = os.getenv("TG_SESSION", "tg_session")

    client = TelegramClient(session, api_id, api_hash)
    try:
        if args.cmd == "login_phone":
            await cmd_login_phone(client, args.phone, args.force_sms)
        elif args.cmd == "login_qr":
            await cmd_login_qr(client)
        elif args.cmd == "dialogs":
            await cmd_dialogs(client, args.limit)
        elif args.cmd == "search":
            await cmd_search(client, args.chat, args.query, args.limit, args.date_from, args.date_to)
        elif args.cmd == "search_many":
            await cmd_search_many(client, args.chat, args.query, args.limit, args.date_from, args.date_to)
        elif args.cmd == "search_set":
            await cmd_search_set(
                client,
                args.theme,
                args.chat,
                args.kw,
                args.limit,
                args.out,
                args.date_from,
                args.date_to,
                args.out_dir,
            )
        elif args.cmd == "run_theme":
            await cmd_run_theme(client, args.theme, args.limit, args.out, args.date_from, args.date_to, args.out_dir)
        elif args.cmd == "run_all_themes":
            await cmd_run_all_themes(client, args.limit, args.date_from, args.date_to, args.out_dir)
    finally:
        await client.disconnect()


if __name__ == "__main__":
    if os.name == "nt":
        try:
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        except Exception:
            pass
    asyncio.run(main())
