import os
import re
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


@dataclass(frozen=True)
class Settings:
    bot_token: str
    tg_api_id: int
    tg_api_hash: str
    database_url: str
    data_dir: Path
    retention_days: int
    default_limit: int
    qr_timeout_seconds: int


BOT_TOKEN_RE = re.compile(r"^\d+:[A-Za-z0-9_-]{20,}$")


def _must_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Environment variable {name} is required")
    return value.strip()


def _validate_bot_token(token: str) -> str:
    if token.count(":") != 1 or not BOT_TOKEN_RE.match(token):
        raise RuntimeError(
            "BOT_TOKEN looks invalid. "
            "Use token from @BotFather in format '<digits>:<token>', without extra suffixes."
        )
    return token


def load_settings() -> Settings:
    load_dotenv()

    data_dir = Path(os.getenv("BOT_DATA_DIR", "data")).resolve()
    data_dir.mkdir(parents=True, exist_ok=True)

    bot_token = _validate_bot_token(_must_env("BOT_TOKEN"))

    return Settings(
        bot_token=bot_token,
        tg_api_id=int(_must_env("TG_API_ID")),
        tg_api_hash=_must_env("TG_API_HASH"),
        database_url=_must_env("DATABASE_URL"),
        data_dir=data_dir,
        retention_days=int(os.getenv("RETENTION_DAYS", "30")),
        default_limit=int(os.getenv("DEFAULT_LIMIT", "50")),
        qr_timeout_seconds=int(os.getenv("QR_TIMEOUT_SECONDS", "180")),
    )
