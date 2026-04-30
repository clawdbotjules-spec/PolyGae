"""Centralised configuration for the NewsTrader bot.

All values are loaded from environment variables (with a .env file via
python-dotenv). Required secrets are validated on import; if any are missing
the process logs the offenders and exits immediately.
"""
from __future__ import annotations

import os
import sys
from typing import Final

from dotenv import load_dotenv

load_dotenv()


def _get_str(name: str, default: str | None = None, required: bool = False) -> str:
    val = os.getenv(name, default)
    if required and (val is None or val == ""):
        return ""
    return val if val is not None else ""


def _get_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on", "y"}


def _get_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        return float(raw)
    except ValueError:
        print(f"[config] Invalid float for {name}={raw!r}, using default {default}", file=sys.stderr)
        return default


def _get_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError:
        print(f"[config] Invalid int for {name}={raw!r}, using default {default}", file=sys.stderr)
        return default


# --- Anthropic ---
ANTHROPIC_API_KEY: Final[str] = _get_str("ANTHROPIC_API_KEY", required=True)
CLAUDE_MODEL: Final[str] = _get_str("CLAUDE_MODEL", "claude-opus-4-5")

# --- Polygon / Polymarket wallet & API ---
POLYGON_PRIVATE_KEY: Final[str] = _get_str("POLYGON_PRIVATE_KEY", required=True)
POLYMARKET_API_KEY: Final[str] = _get_str("POLYMARKET_API_KEY", required=True)
POLYMARKET_API_SECRET: Final[str] = _get_str("POLYMARKET_API_SECRET", required=True)
POLYMARKET_API_PASSPHRASE: Final[str] = _get_str("POLYMARKET_API_PASSPHRASE", required=True)
POLYMARKET_FUNDER_ADDRESS: Final[str] = _get_str("POLYMARKET_FUNDER_ADDRESS", required=True)
POLYMARKET_CLOB_HOST: Final[str] = _get_str("POLYMARKET_CLOB_HOST", "https://clob.polymarket.com")
POLYMARKET_GAMMA_HOST: Final[str] = _get_str("POLYMARKET_GAMMA_HOST", "https://gamma-api.polymarket.com")

# --- News providers ---
NEWSFILTER_API_KEY: Final[str] = _get_str("NEWSFILTER_API_KEY", required=True)
NEWSAPI_KEY: Final[str] = _get_str("NEWSAPI_KEY", required=True)

# --- Telegram ---
TELEGRAM_BOT_TOKEN: Final[str] = _get_str("TELEGRAM_BOT_TOKEN", required=True)
TELEGRAM_CHAT_ID: Final[str] = _get_str("TELEGRAM_CHAT_ID", required=True)

# --- Mode ---
DRY_RUN: Final[bool] = _get_bool("DRY_RUN", True)

# --- Capital / risk limits ---
TOTAL_CAPITAL_USDC: Final[float] = _get_float("TOTAL_CAPITAL_USDC", 1000.0)
MAX_SINGLE_BET_USDC: Final[float] = _get_float("MAX_SINGLE_BET_USDC", 150.0)
MAX_TOTAL_OPEN_USDC: Final[float] = _get_float("MAX_TOTAL_OPEN_USDC", 600.0)
MAX_DAILY_LOSS_USDC: Final[float] = _get_float("MAX_DAILY_LOSS_USDC", 300.0)
MAX_TRADES_PER_HOUR: Final[int] = _get_int("MAX_TRADES_PER_HOUR", 6)
SAFETY_MODE_CONSECUTIVE_LOSSES: Final[int] = _get_int("SAFETY_MODE_CONSECUTIVE_LOSSES", 3)
SAFETY_MODE_PAUSE_HOURS: Final[int] = _get_int("SAFETY_MODE_PAUSE_HOURS", 4)

# --- Trade quality thresholds ---
MIN_MARKET_VOLUME_24H: Final[float] = _get_float("MIN_MARKET_VOLUME_24H", 10000.0)
MIN_CONFIDENCE: Final[float] = _get_float("MIN_CONFIDENCE", 0.72)
MIN_EDGE_AFTER_FEES: Final[float] = _get_float("MIN_EDGE_AFTER_FEES", 0.07)
MAX_SPREAD_CENTS: Final[float] = _get_float("MAX_SPREAD_CENTS", 0.08)
NEWS_STALENESS_MAX_MINUTES: Final[int] = _get_int("NEWS_STALENESS_MAX_MINUTES", 4)

# --- Cadences ---
MARKET_SCANNER_REFRESH_SECONDS: Final[int] = _get_int("MARKET_SCANNER_REFRESH_SECONDS", 300)
POSITION_MONITOR_INTERVAL_SECONDS: Final[int] = _get_int("POSITION_MONITOR_INTERVAL_SECONDS", 600)
HEARTBEAT_INTERVAL_SECONDS: Final[int] = _get_int("HEARTBEAT_INTERVAL_SECONDS", 5)
ORDER_FILL_TIMEOUT_SECONDS: Final[int] = _get_int("ORDER_FILL_TIMEOUT_SECONDS", 90)

# --- Misc ---
ARTICLE_BODY_MAX_WORDS: Final[int] = _get_int("ARTICLE_BODY_MAX_WORDS", 800)
LOG_DB_PATH: Final[str] = _get_str("LOG_DB_PATH", "newstrader.db")


_REQUIRED_VARS: tuple[tuple[str, str], ...] = (
    ("ANTHROPIC_API_KEY", ANTHROPIC_API_KEY),
    ("POLYGON_PRIVATE_KEY", POLYGON_PRIVATE_KEY),
    ("POLYMARKET_API_KEY", POLYMARKET_API_KEY),
    ("POLYMARKET_API_SECRET", POLYMARKET_API_SECRET),
    ("POLYMARKET_API_PASSPHRASE", POLYMARKET_API_PASSPHRASE),
    ("POLYMARKET_FUNDER_ADDRESS", POLYMARKET_FUNDER_ADDRESS),
    ("NEWSFILTER_API_KEY", NEWSFILTER_API_KEY),
    ("NEWSAPI_KEY", NEWSAPI_KEY),
    ("TELEGRAM_BOT_TOKEN", TELEGRAM_BOT_TOKEN),
    ("TELEGRAM_CHAT_ID", TELEGRAM_CHAT_ID),
)


def validate_required_env() -> None:
    """Validate every required env var on startup. Exits with code 2 on failure.

    Tests and tooling can import this module without triggering the exit by
    setting the variables in the environment first; calling code (main.py)
    is what actually invokes this function.
    """
    missing = [name for name, value in _REQUIRED_VARS if not value]
    if missing:
        print("[config] Missing required environment variables:", file=sys.stderr)
        for name in missing:
            print(f"  - {name}", file=sys.stderr)
        print("Populate them in .env (see .env.example) and try again.", file=sys.stderr)
        sys.exit(2)


def safe_summary() -> dict[str, object]:
    """Return a non-secret summary of config for diagnostic logging."""
    return {
        "dry_run": DRY_RUN,
        "total_capital_usdc": TOTAL_CAPITAL_USDC,
        "max_single_bet_usdc": MAX_SINGLE_BET_USDC,
        "max_total_open_usdc": MAX_TOTAL_OPEN_USDC,
        "max_daily_loss_usdc": MAX_DAILY_LOSS_USDC,
        "max_trades_per_hour": MAX_TRADES_PER_HOUR,
        "min_confidence": MIN_CONFIDENCE,
        "min_edge_after_fees": MIN_EDGE_AFTER_FEES,
        "max_spread_cents": MAX_SPREAD_CENTS,
        "news_staleness_max_minutes": NEWS_STALENESS_MAX_MINUTES,
        "claude_model": CLAUDE_MODEL,
        "clob_host": POLYMARKET_CLOB_HOST,
        "gamma_host": POLYMARKET_GAMMA_HOST,
    }
