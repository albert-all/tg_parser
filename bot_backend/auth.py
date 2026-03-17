from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from io import BytesIO
from pathlib import Path
from typing import Optional

import qrcode
from telethon import TelegramClient
from telethon.errors import PasswordHashInvalidError, SessionPasswordNeededError


@dataclass
class AuthStatus:
    status: str
    message: str
    qr_png: Optional[bytes] = None


@dataclass
class PendingAuth:
    client: TelegramClient
    qr_login: object
    created_at: datetime


class AuthManager:
    def __init__(self, api_id: int, api_hash: str, sessions_dir: Path, qr_timeout_seconds: int = 180) -> None:
        self.api_id = api_id
        self.api_hash = api_hash
        self.sessions_dir = sessions_dir
        self.qr_timeout_seconds = qr_timeout_seconds
        self.sessions_dir.mkdir(parents=True, exist_ok=True)
        self.pending: dict[int, PendingAuth] = {}

    def has_pending(self, user_id: int) -> bool:
        return user_id in self.pending

    def session_path(self, user_id: int) -> Path:
        return self.sessions_dir / f"user_{user_id}"

    def _new_client(self, user_id: int) -> TelegramClient:
        return TelegramClient(str(self.session_path(user_id)), self.api_id, self.api_hash)

    async def _close_pending(self, user_id: int) -> None:
        ctx = self.pending.pop(user_id, None)
        if ctx:
            try:
                await ctx.client.disconnect()
            except Exception:
                pass

    async def is_authorized(self, user_id: int) -> bool:
        client = self._new_client(user_id)
        try:
            await client.connect()
            return await client.is_user_authorized()
        finally:
            await client.disconnect()

    async def start_qr_auth(self, user_id: int) -> AuthStatus:
        await self._close_pending(user_id)
        client = self._new_client(user_id)
        await client.connect()

        if await client.is_user_authorized():
            await client.disconnect()
            return AuthStatus(status="authorized", message="Вы уже авторизованы.")

        qr_login = await client.qr_login()
        self.pending[user_id] = PendingAuth(
            client=client,
            qr_login=qr_login,
            created_at=datetime.now(timezone.utc),
        )
        return AuthStatus(
            status="pending",
            message="Сканируйте QR в Telegram: Настройки -> Устройства -> Подключить устройство.",
            qr_png=self._qr_to_png(qr_login.url),
        )

    async def refresh_qr(self, user_id: int) -> AuthStatus:
        return await self.start_qr_auth(user_id)

    async def check_qr(self, user_id: int) -> AuthStatus:
        ctx = self.pending.get(user_id)
        if not ctx:
            if await self.is_authorized(user_id):
                return AuthStatus(status="authorized", message="Авторизация уже выполнена.")
            return AuthStatus(status="missing", message="Нет активной авторизации. Запустите /auth.")

        # Иногда wait() не успевает отдать событие, хотя сессия уже авторизована.
        if await ctx.client.is_user_authorized():
            await self._close_pending(user_id)
            return AuthStatus(status="authorized", message="Авторизация завершена успешно.")

        try:
            await ctx.qr_login.wait(timeout=2)
        except asyncio.TimeoutError:
            if await ctx.client.is_user_authorized():
                await self._close_pending(user_id)
                return AuthStatus(status="authorized", message="Авторизация завершена успешно.")

            # Доп. проверка через новый клиент и сессию на диске:
            # иногда текущий pending-клиент не обновляет auth-state мгновенно.
            if await self.is_authorized(user_id):
                await self._close_pending(user_id)
                return AuthStatus(status="authorized", message="Авторизация завершена успешно.")

            ttl = self.qr_timeout_seconds - int((datetime.now(timezone.utc) - ctx.created_at).total_seconds())
            if ttl <= 0:
                return AuthStatus(status="expired", message="QR истек. Обновите через /auth_refresh.")
            return AuthStatus(
                status="pending",
                message=(
                    "Пока не подтверждено. Сканируйте QR и повторите /auth_check.\n"
                    "Если у аккаунта включена 2FA и QR уже подтвержден, нажмите /auth_2fa и введите пароль."
                ),
            )
        except SessionPasswordNeededError:
            return AuthStatus(status="need_2fa", message="Требуется пароль 2FA. Отправьте его следующим сообщением.")
        except Exception as e:
            await self._close_pending(user_id)
            return AuthStatus(status="error", message=f"Ошибка авторизации: {e}")

        is_auth = await ctx.client.is_user_authorized()
        await self._close_pending(user_id)
        if is_auth:
            return AuthStatus(status="authorized", message="Авторизация завершена успешно.")
        return AuthStatus(status="error", message="Не удалось завершить авторизацию.")

    async def submit_2fa(self, user_id: int, password: str) -> AuthStatus:
        ctx = self.pending.get(user_id)
        if not ctx:
            return AuthStatus(status="missing", message="Нет активной авторизации. Запустите /auth.")
        try:
            await ctx.client.sign_in(password=password)
        except PasswordHashInvalidError:
            return AuthStatus(status="need_2fa", message="Неверный пароль 2FA. Попробуйте снова.")
        except Exception as e:
            await self._close_pending(user_id)
            return AuthStatus(status="error", message=f"Ошибка при вводе 2FA: {e}")

        is_auth = await ctx.client.is_user_authorized()
        await self._close_pending(user_id)
        if is_auth:
            return AuthStatus(status="authorized", message="Авторизация завершена успешно.")
        return AuthStatus(status="error", message="Не удалось завершить авторизацию.")

    async def logout(self, user_id: int) -> bool:
        await self._close_pending(user_id)
        client = self._new_client(user_id)
        try:
            await client.connect()
            if await client.is_user_authorized():
                await client.log_out()
            await client.disconnect()
        except Exception:
            try:
                await client.disconnect()
            except Exception:
                pass
            return False

        removed = False
        for suffix in (".session", ".session-journal"):
            path = Path(f"{self.session_path(user_id)}{suffix}")
            if path.exists():
                path.unlink(missing_ok=True)
                removed = True
        return removed

    @staticmethod
    def _qr_to_png(url: str) -> bytes:
        qr = qrcode.QRCode(border=2, box_size=8)
        qr.add_data(url)
        qr.make(fit=True)
        image = qr.make_image(fill_color="black", back_color="white")
        buffer = BytesIO()
        image.save(buffer, format="PNG")
        return buffer.getvalue()
