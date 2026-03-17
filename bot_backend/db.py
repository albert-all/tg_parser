from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import AsyncIterator, Optional

from sqlalchemy import BigInteger, DateTime, ForeignKey, Integer, String, Text, UniqueConstraint, delete, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"

    tg_user_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    username: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    first_name: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc)
    )

    themes: Mapped[list["Theme"]] = relationship(back_populates="user", cascade="all, delete-orphan")
    theme_watches: Mapped[list["ThemeWatch"]] = relationship(back_populates="user", cascade="all, delete-orphan")


class Theme(Base):
    __tablename__ = "themes"
    __table_args__ = (UniqueConstraint("user_id", "name_norm", name="uq_theme_user_name_norm"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.tg_user_id", ondelete="CASCADE"), index=True)
    name: Mapped[str] = mapped_column(String(120))
    name_norm: Mapped[str] = mapped_column(String(120))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc)
    )

    user: Mapped["User"] = relationship(back_populates="themes")
    chats: Mapped[list["ThemeChat"]] = relationship(back_populates="theme", cascade="all, delete-orphan")
    keywords: Mapped[list["ThemeKeyword"]] = relationship(back_populates="theme", cascade="all, delete-orphan")
    watches: Mapped[list["ThemeWatch"]] = relationship(back_populates="theme", cascade="all, delete-orphan")


class ThemeChat(Base):
    __tablename__ = "theme_chats"
    __table_args__ = (UniqueConstraint("theme_id", "chat_ref_norm", name="uq_theme_chat_ref"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    theme_id: Mapped[int] = mapped_column(ForeignKey("themes.id", ondelete="CASCADE"), index=True)
    chat_ref: Mapped[str] = mapped_column(Text)
    chat_ref_norm: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

    theme: Mapped["Theme"] = relationship(back_populates="chats")


class ThemeKeyword(Base):
    __tablename__ = "theme_keywords"
    __table_args__ = (UniqueConstraint("theme_id", "keyword_norm", name="uq_theme_keyword"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    theme_id: Mapped[int] = mapped_column(ForeignKey("themes.id", ondelete="CASCADE"), index=True)
    keyword: Mapped[str] = mapped_column(Text)
    keyword_norm: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

    theme: Mapped["Theme"] = relationship(back_populates="keywords")


class SearchRun(Base):
    __tablename__ = "search_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(BigInteger, index=True)
    theme_name: Mapped[str] = mapped_column(String(120))
    status: Mapped[str] = mapped_column(String(32))
    result_count: Mapped[int] = mapped_column(Integer, default=0)
    error_text: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    finished_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)


class ThemeWatch(Base):
    __tablename__ = "theme_watches"
    __table_args__ = (UniqueConstraint("user_id", "theme_id", name="uq_theme_watch_user_theme"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.tg_user_id", ondelete="CASCADE"), index=True)
    theme_id: Mapped[int] = mapped_column(ForeignKey("themes.id", ondelete="CASCADE"), index=True)
    chat_id: Mapped[int] = mapped_column(BigInteger)
    interval_minutes: Mapped[int] = mapped_column(Integer)
    last_checked_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    next_check_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    last_match_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc)
    )

    user: Mapped["User"] = relationship(back_populates="theme_watches")
    theme: Mapped["Theme"] = relationship(back_populates="watches")


def normalize_key(value: str) -> str:
    return value.strip().casefold()


@dataclass
class ThemeDTO:
    id: int
    name: str
    chats: list[str]
    keywords: list[str]


@dataclass
class ThemeWatchDTO:
    id: int
    user_id: int
    theme_id: int
    theme_name: str
    chat_id: int
    interval_minutes: int
    last_checked_at: Optional[datetime]
    next_check_at: datetime
    last_match_at: Optional[datetime]
    last_error: Optional[str]


class Database:
    def __init__(self, database_url: str) -> None:
        self.engine = create_async_engine(database_url, pool_pre_ping=True)
        self.session_factory = async_sessionmaker(self.engine, expire_on_commit=False)

    async def init(self) -> None:
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async def close(self) -> None:
        await self.engine.dispose()

    @asynccontextmanager
    async def session(self) -> AsyncIterator[AsyncSession]:
        async with self.session_factory() as session:
            yield session

    async def upsert_user(self, tg_user_id: int, username: Optional[str], first_name: Optional[str]) -> None:
        async with self.session() as session:
            user = await session.get(User, tg_user_id)
            if user is None:
                user = User(tg_user_id=tg_user_id, username=username, first_name=first_name)
                session.add(user)
            else:
                user.username = username
                user.first_name = first_name
                user.updated_at = datetime.now(timezone.utc)
            await session.commit()

    async def create_theme(self, user_id: int, name: str) -> ThemeDTO:
        name = name.strip()
        name_norm = normalize_key(name)
        if not name:
            raise ValueError("Theme name is required")
        async with self.session() as session:
            exists = await session.scalar(select(Theme).where(Theme.user_id == user_id, Theme.name_norm == name_norm))
            if exists:
                raise ValueError("Theme already exists")
            theme = Theme(user_id=user_id, name=name, name_norm=name_norm)
            session.add(theme)
            await session.commit()
            await session.refresh(theme)
            return ThemeDTO(id=theme.id, name=theme.name, chats=[], keywords=[])

    async def delete_theme(self, user_id: int, name: str) -> bool:
        theme = await self.get_theme(user_id, name)
        if theme is None:
            return False
        async with self.session() as session:
            await session.execute(delete(Theme).where(Theme.id == theme.id))
            await session.commit()
            return True

    async def get_theme(self, user_id: int, name: str) -> Optional[ThemeDTO]:
        name_norm = normalize_key(name)
        async with self.session() as session:
            theme = await session.scalar(select(Theme).where(Theme.user_id == user_id, Theme.name_norm == name_norm))
            if theme is None:
                return None
            chats = list((await session.scalars(select(ThemeChat).where(ThemeChat.theme_id == theme.id))).all())
            keywords = list((await session.scalars(select(ThemeKeyword).where(ThemeKeyword.theme_id == theme.id))).all())
            return ThemeDTO(
                id=theme.id,
                name=theme.name,
                chats=[item.chat_ref for item in chats],
                keywords=[item.keyword for item in keywords],
            )

    async def get_theme_by_id(self, user_id: int, theme_id: int) -> Optional[ThemeDTO]:
        async with self.session() as session:
            theme = await session.scalar(select(Theme).where(Theme.user_id == user_id, Theme.id == theme_id))
            if theme is None:
                return None
            chats = list((await session.scalars(select(ThemeChat).where(ThemeChat.theme_id == theme.id))).all())
            keywords = list((await session.scalars(select(ThemeKeyword).where(ThemeKeyword.theme_id == theme.id))).all())
            return ThemeDTO(
                id=theme.id,
                name=theme.name,
                chats=[item.chat_ref for item in chats],
                keywords=[item.keyword for item in keywords],
            )

    async def list_themes(self, user_id: int) -> list[ThemeDTO]:
        async with self.session() as session:
            themes = list((await session.scalars(select(Theme).where(Theme.user_id == user_id).order_by(Theme.name))).all())
            result: list[ThemeDTO] = []
            for theme in themes:
                chats = list((await session.scalars(select(ThemeChat).where(ThemeChat.theme_id == theme.id))).all())
                keywords = list((await session.scalars(select(ThemeKeyword).where(ThemeKeyword.theme_id == theme.id))).all())
                result.append(
                    ThemeDTO(
                        id=theme.id,
                        name=theme.name,
                        chats=[item.chat_ref for item in chats],
                        keywords=[item.keyword for item in keywords],
                    )
                )
            return result

    async def add_chat(self, user_id: int, theme_name: str, chat_ref: str) -> None:
        theme = await self.get_theme(user_id, theme_name)
        if theme is None:
            raise ValueError("Theme not found")
        chat_ref = chat_ref.strip()
        if not chat_ref:
            raise ValueError("Chat reference is required")
        async with self.session() as session:
            exists = await session.scalar(
                select(ThemeChat).where(ThemeChat.theme_id == theme.id, ThemeChat.chat_ref_norm == normalize_key(chat_ref))
            )
            if exists:
                return
            session.add(ThemeChat(theme_id=theme.id, chat_ref=chat_ref, chat_ref_norm=normalize_key(chat_ref)))
            await session.commit()

    async def add_chats_bulk(self, user_id: int, theme_name: str, chat_refs: list[str]) -> tuple[int, int]:
        theme = await self.get_theme(user_id, theme_name)
        if theme is None:
            raise ValueError("Theme not found")

        cleaned: list[tuple[str, str]] = []
        seen_norms: set[str] = set()
        for raw in chat_refs:
            value = (raw or "").strip()
            if not value:
                continue
            norm = normalize_key(value)
            if not norm or norm in seen_norms:
                continue
            seen_norms.add(norm)
            cleaned.append((value, norm))

        if not cleaned:
            return 0, 0

        async with self.session() as session:
            existing_norms = set(
                (
                    await session.scalars(
                        select(ThemeChat.chat_ref_norm).where(ThemeChat.theme_id == theme.id)
                    )
                ).all()
            )
            added = 0
            for value, norm in cleaned:
                if norm in existing_norms:
                    continue
                session.add(ThemeChat(theme_id=theme.id, chat_ref=value, chat_ref_norm=norm))
                existing_norms.add(norm)
                added += 1
            await session.commit()
            return added, len(cleaned)

    async def remove_chat(self, user_id: int, theme_name: str, chat_ref: str) -> bool:
        theme = await self.get_theme(user_id, theme_name)
        if theme is None:
            raise ValueError("Theme not found")
        async with self.session() as session:
            result = await session.execute(
                delete(ThemeChat).where(
                    ThemeChat.theme_id == theme.id,
                    ThemeChat.chat_ref_norm == normalize_key(chat_ref),
                )
            )
            await session.commit()
            return result.rowcount > 0

    async def add_keyword(self, user_id: int, theme_name: str, keyword: str) -> None:
        theme = await self.get_theme(user_id, theme_name)
        if theme is None:
            raise ValueError("Theme not found")
        keyword = keyword.strip()
        if not keyword:
            raise ValueError("Keyword is required")
        async with self.session() as session:
            exists = await session.scalar(
                select(ThemeKeyword).where(
                    ThemeKeyword.theme_id == theme.id,
                    ThemeKeyword.keyword_norm == normalize_key(keyword),
                )
            )
            if exists:
                return
            session.add(ThemeKeyword(theme_id=theme.id, keyword=keyword, keyword_norm=normalize_key(keyword)))
            await session.commit()

    async def remove_keyword(self, user_id: int, theme_name: str, keyword: str) -> bool:
        theme = await self.get_theme(user_id, theme_name)
        if theme is None:
            raise ValueError("Theme not found")
        async with self.session() as session:
            result = await session.execute(
                delete(ThemeKeyword).where(
                    ThemeKeyword.theme_id == theme.id,
                    ThemeKeyword.keyword_norm == normalize_key(keyword),
                )
            )
            await session.commit()
            return result.rowcount > 0

    async def create_search_run(self, user_id: int, theme_name: str) -> int:
        async with self.session() as session:
            run = SearchRun(user_id=user_id, theme_name=theme_name, status="running")
            session.add(run)
            await session.commit()
            await session.refresh(run)
            return run.id

    async def finish_search_run(
        self,
        run_id: int,
        status: str,
        result_count: int = 0,
        error_text: Optional[str] = None,
    ) -> None:
        async with self.session() as session:
            run = await session.get(SearchRun, run_id)
            if run is None:
                return
            run.status = status
            run.result_count = result_count
            run.error_text = error_text
            run.finished_at = datetime.now(timezone.utc)
            await session.commit()

    async def cleanup_old_runs(self, retention_days: int) -> int:
        border = datetime.now(timezone.utc) - timedelta(days=retention_days)
        async with self.session() as session:
            result = await session.execute(delete(SearchRun).where(SearchRun.started_at < border))
            await session.commit()
            return result.rowcount or 0

    @staticmethod
    def _watch_to_dto(watch: ThemeWatch, theme_name: str) -> ThemeWatchDTO:
        return ThemeWatchDTO(
            id=watch.id,
            user_id=watch.user_id,
            theme_id=watch.theme_id,
            theme_name=theme_name,
            chat_id=watch.chat_id,
            interval_minutes=watch.interval_minutes,
            last_checked_at=watch.last_checked_at,
            next_check_at=watch.next_check_at,
            last_match_at=watch.last_match_at,
            last_error=watch.last_error,
        )

    async def set_theme_watch(self, user_id: int, theme_name: str, chat_id: int, interval_minutes: int) -> ThemeWatchDTO:
        if interval_minutes <= 0:
            raise ValueError("Interval must be > 0")
        theme = await self.get_theme(user_id, theme_name)
        if theme is None:
            raise ValueError("Theme not found")

        now = datetime.now(timezone.utc)
        next_check_at = now + timedelta(minutes=interval_minutes)
        async with self.session() as session:
            watch = await session.scalar(
                select(ThemeWatch).where(ThemeWatch.user_id == user_id, ThemeWatch.theme_id == theme.id)
            )
            if watch is None:
                watch = ThemeWatch(
                    user_id=user_id,
                    theme_id=theme.id,
                    chat_id=chat_id,
                    interval_minutes=interval_minutes,
                    last_checked_at=now,
                    next_check_at=next_check_at,
                    last_error=None,
                )
                session.add(watch)
            else:
                watch.chat_id = chat_id
                watch.interval_minutes = interval_minutes
                watch.last_checked_at = now
                watch.next_check_at = next_check_at
                watch.last_error = None
                watch.updated_at = now
            await session.commit()
            await session.refresh(watch)
            return self._watch_to_dto(watch, theme.name)

    async def delete_theme_watch(self, user_id: int, theme_name: str) -> bool:
        theme = await self.get_theme(user_id, theme_name)
        if theme is None:
            return False
        async with self.session() as session:
            result = await session.execute(
                delete(ThemeWatch).where(ThemeWatch.user_id == user_id, ThemeWatch.theme_id == theme.id)
            )
            await session.commit()
            return result.rowcount > 0

    async def list_theme_watches(self, user_id: int) -> list[ThemeWatchDTO]:
        async with self.session() as session:
            rows = (
                await session.execute(
                    select(ThemeWatch, Theme.name)
                    .join(Theme, Theme.id == ThemeWatch.theme_id)
                    .where(ThemeWatch.user_id == user_id)
                    .order_by(ThemeWatch.next_check_at, Theme.name)
                )
            ).all()
            return [self._watch_to_dto(watch, theme_name) for watch, theme_name in rows]

    async def list_due_theme_watches(self, now: Optional[datetime] = None, limit: int = 50) -> list[ThemeWatchDTO]:
        due_at = now or datetime.now(timezone.utc)
        async with self.session() as session:
            rows = (
                await session.execute(
                    select(ThemeWatch, Theme.name)
                    .join(Theme, Theme.id == ThemeWatch.theme_id)
                    .where(ThemeWatch.next_check_at <= due_at)
                    .order_by(ThemeWatch.next_check_at)
                    .limit(limit)
                )
            ).all()
            return [self._watch_to_dto(watch, theme_name) for watch, theme_name in rows]

    async def mark_theme_watch_checked(
        self,
        watch_id: int,
        checked_at: datetime,
        *,
        had_matches: bool,
        error_text: Optional[str] = None,
    ) -> None:
        async with self.session() as session:
            watch = await session.get(ThemeWatch, watch_id)
            if watch is None:
                return

            ts = checked_at if checked_at.tzinfo else checked_at.replace(tzinfo=timezone.utc)
            ts = ts.astimezone(timezone.utc)
            watch.last_checked_at = ts
            watch.next_check_at = ts + timedelta(minutes=watch.interval_minutes)
            watch.last_error = (error_text or "").strip() or None
            if watch.last_error and len(watch.last_error) > 2000:
                watch.last_error = watch.last_error[:2000]
            if had_matches:
                watch.last_match_at = ts
            watch.updated_at = datetime.now(timezone.utc)
            await session.commit()
