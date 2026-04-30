"""Structured logging, SQLite trade journal, and Telegram alerts.

The TradeLogger is the bot's audit trail. Every news article seen, every
Claude decision, and every trade open/close is persisted asynchronously to
SQLite. Telegram is fire-and-forget — failures are logged and swallowed so
they cannot interfere with trading.
"""
from __future__ import annotations

import asyncio
import json
import logging
from datetime import date, datetime, timezone
from typing import Any

import aiosqlite
import structlog

from config import LOG_DB_PATH, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, DRY_RUN
from models import (
    ClaudeDecision,
    MarketSnapshot,
    NewsArticle,
    OpenPosition,
    TradeResult,
)


def configure_structlog() -> None:
    """Configure structlog once at process start."""
    logging.basicConfig(
        format="%(message)s",
        level=logging.INFO,
    )
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.dev.ConsoleRenderer(colors=False),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
        cache_logger_on_first_use=True,
    )


log = structlog.get_logger("newstrader")


_SCHEMA = """
CREATE TABLE IF NOT EXISTS news_articles (
    id TEXT PRIMARY KEY,
    headline TEXT NOT NULL,
    source TEXT NOT NULL,
    published_at TEXT NOT NULL,
    url TEXT,
    received_at TEXT NOT NULL,
    action TEXT NOT NULL,
    claude_decision_id INTEGER
);

CREATE TABLE IF NOT EXISTS claude_decisions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    article_id TEXT NOT NULL,
    decision TEXT NOT NULL,
    reasoning TEXT,
    news_quality TEXT,
    confidence_max REAL,
    markets_count INTEGER,
    raw_json TEXT,
    created_at TEXT NOT NULL,
    latency_ms INTEGER,
    input_tokens INTEGER,
    output_tokens INTEGER
);

CREATE TABLE IF NOT EXISTS trades_opened (
    position_id TEXT PRIMARY KEY,
    condition_id TEXT NOT NULL,
    question TEXT NOT NULL,
    direction TEXT NOT NULL,
    entry_price REAL NOT NULL,
    size_usdc REAL NOT NULL,
    shares REAL NOT NULL,
    exit_target REAL,
    max_hold_until TEXT,
    opened_at TEXT NOT NULL,
    dry_run INTEGER NOT NULL,
    news_article_id TEXT
);

CREATE TABLE IF NOT EXISTS trades_closed (
    position_id TEXT PRIMARY KEY,
    condition_id TEXT NOT NULL,
    direction TEXT NOT NULL,
    entry_price REAL NOT NULL,
    exit_price REAL NOT NULL,
    size_usdc REAL NOT NULL,
    pnl_usdc REAL NOT NULL,
    pnl_pct REAL NOT NULL,
    hold_hours REAL NOT NULL,
    exit_reason TEXT NOT NULL,
    opened_at TEXT NOT NULL,
    closed_at TEXT NOT NULL,
    dry_run INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS daily_summary (
    date TEXT PRIMARY KEY,
    total_trades INTEGER NOT NULL DEFAULT 0,
    winning_trades INTEGER NOT NULL DEFAULT 0,
    losing_trades INTEGER NOT NULL DEFAULT 0,
    total_pnl REAL NOT NULL DEFAULT 0.0,
    max_drawdown REAL NOT NULL DEFAULT 0.0,
    avg_confidence REAL,
    avg_hold_hours REAL
);

CREATE TABLE IF NOT EXISTS seen_articles (
    id TEXT PRIMARY KEY,
    seen_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_seen_articles_seen_at ON seen_articles(seen_at);
CREATE INDEX IF NOT EXISTS idx_trades_closed_opened_at ON trades_closed(opened_at);
CREATE INDEX IF NOT EXISTS idx_news_received_at ON news_articles(received_at);
"""


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class TradeLogger:
    """Async SQLite journal + Telegram alerts."""

    def __init__(self, db_path: str = LOG_DB_PATH) -> None:
        self.db_path = db_path
        self._db: aiosqlite.Connection | None = None
        self._lock = asyncio.Lock()
        self._tg_app: Any = None  # python-telegram-bot Application
        self._tg_ready = False

    async def init_db(self) -> None:
        self._db = await aiosqlite.connect(self.db_path)
        # Iterate the multi-statement schema so the driver runs each
        # CREATE in its own execute() call.
        for stmt in [s.strip() for s in _SCHEMA.split(";") if s.strip()]:
            await self._db.execute(stmt)
        await self._db.commit()

        # Lazy-init Telegram (do not fail startup if unavailable)
        try:
            from telegram import Bot  # type: ignore

            if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
                self._tg_app = Bot(token=TELEGRAM_BOT_TOKEN)
                self._tg_ready = True
        except Exception as e:  # pragma: no cover - best effort
            log.warning("telegram_init_failed", error=str(e))
            self._tg_ready = False

    async def close(self) -> None:
        if self._db is not None:
            await self._db.close()
            self._db = None

    # ----- Dedup helpers (used by news_engine) ---------------------------

    async def has_seen_article(self, article_id: str) -> bool:
        assert self._db is not None
        async with self._lock:
            cur = await self._db.execute(
                "SELECT 1 FROM seen_articles WHERE id = ? LIMIT 1", (article_id,)
            )
            row = await cur.fetchone()
            await cur.close()
            return row is not None

    async def mark_article_seen(self, article_id: str) -> None:
        assert self._db is not None
        async with self._lock:
            await self._db.execute(
                "INSERT OR IGNORE INTO seen_articles (id, seen_at) VALUES (?, ?)",
                (article_id, _utcnow_iso()),
            )
            await self._db.commit()

    async def prune_seen_articles(self, older_than_hours: int = 24) -> None:
        assert self._db is not None
        async with self._lock:
            await self._db.execute(
                "DELETE FROM seen_articles WHERE seen_at < datetime('now', ?)",
                (f"-{older_than_hours} hours",),
            )
            await self._db.commit()

    # ----- Article / decision / trade logging ----------------------------

    async def log_article(
        self,
        article: NewsArticle,
        action: str,
        decision_id: int | None = None,
    ) -> None:
        assert self._db is not None
        async with self._lock:
            await self._db.execute(
                """
                INSERT OR REPLACE INTO news_articles
                  (id, headline, source, published_at, url, received_at, action, claude_decision_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    article.id,
                    article.headline,
                    article.source,
                    article.published_at.isoformat(),
                    article.url,
                    _utcnow_iso(),
                    action,
                    decision_id,
                ),
            )
            await self._db.commit()

    async def log_decision(
        self,
        decision: ClaudeDecision,
        article_id: str,
        latency_ms: int,
        tokens: dict[str, int] | None = None,
    ) -> int:
        assert self._db is not None
        tokens = tokens or {}
        confidence_max = max(
            (m.confidence for m in decision.affected_markets), default=None
        )
        async with self._lock:
            cur = await self._db.execute(
                """
                INSERT INTO claude_decisions
                  (article_id, decision, reasoning, news_quality, confidence_max,
                   markets_count, raw_json, created_at, latency_ms, input_tokens, output_tokens)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    article_id,
                    decision.decision,
                    decision.reasoning,
                    decision.news_quality,
                    confidence_max,
                    len(decision.affected_markets),
                    decision.raw_response or json.dumps(decision.model_dump(mode="json")),
                    _utcnow_iso(),
                    latency_ms,
                    tokens.get("input_tokens"),
                    tokens.get("output_tokens"),
                ),
            )
            await self._db.commit()
            decision_id = cur.lastrowid or 0
            await cur.close()
            return decision_id

    async def log_trade_opened(self, position: OpenPosition) -> None:
        assert self._db is not None
        is_dry = 1 if position.position_id.startswith("DRY-") else (1 if DRY_RUN else 0)
        async with self._lock:
            await self._db.execute(
                """
                INSERT OR REPLACE INTO trades_opened
                  (position_id, condition_id, question, direction, entry_price,
                   size_usdc, shares, exit_target, max_hold_until, opened_at,
                   dry_run, news_article_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    position.position_id,
                    position.condition_id,
                    position.question,
                    position.direction,
                    position.entry_price,
                    position.size_usdc,
                    position.shares,
                    position.exit_target_price,
                    position.max_hold_until.isoformat(),
                    position.opened_at.isoformat(),
                    is_dry,
                    position.news_article_id,
                ),
            )
            await self._db.commit()

    async def log_trade_closed(self, result: TradeResult) -> None:
        assert self._db is not None
        is_dry = 1 if result.position_id.startswith("DRY-") else (1 if DRY_RUN else 0)
        async with self._lock:
            await self._db.execute(
                """
                INSERT OR REPLACE INTO trades_closed
                  (position_id, condition_id, direction, entry_price, exit_price,
                   size_usdc, pnl_usdc, pnl_pct, hold_hours, exit_reason,
                   opened_at, closed_at, dry_run)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    result.position_id,
                    result.condition_id,
                    result.direction,
                    result.entry_price,
                    result.exit_price,
                    result.size_usdc,
                    result.pnl_usdc,
                    result.pnl_pct,
                    result.hold_hours,
                    result.exit_reason,
                    result.opened_at.isoformat(),
                    result.closed_at.isoformat(),
                    is_dry,
                ),
            )
            await self._db.commit()

        await self._upsert_daily_summary(result)

    async def _upsert_daily_summary(self, result: TradeResult) -> None:
        assert self._db is not None
        d = result.closed_at.date().isoformat()
        win = 1 if result.pnl_usdc > 0 else 0
        loss = 1 if result.pnl_usdc < 0 else 0
        async with self._lock:
            await self._db.execute(
                """
                INSERT INTO daily_summary
                  (date, total_trades, winning_trades, losing_trades, total_pnl, avg_hold_hours)
                VALUES (?, 1, ?, ?, ?, ?)
                ON CONFLICT(date) DO UPDATE SET
                  total_trades = total_trades + 1,
                  winning_trades = winning_trades + ?,
                  losing_trades = losing_trades + ?,
                  total_pnl = total_pnl + ?,
                  avg_hold_hours = (
                    (avg_hold_hours * total_trades + ?) / (total_trades + 1)
                  )
                """,
                (
                    d,
                    win,
                    loss,
                    result.pnl_usdc,
                    result.hold_hours,
                    win,
                    loss,
                    result.pnl_usdc,
                    result.hold_hours,
                ),
            )
            await self._db.commit()

    async def get_daily_summary(self, day: date | None = None) -> dict[str, Any]:
        assert self._db is not None
        d = (day or datetime.now(timezone.utc).date()).isoformat()
        async with self._lock:
            cur = await self._db.execute(
                "SELECT * FROM daily_summary WHERE date = ?", (d,)
            )
            row = await cur.fetchone()
            cols = [c[0] for c in cur.description] if cur.description else []
            await cur.close()
        if not row:
            return {
                "date": d,
                "total_trades": 0,
                "winning_trades": 0,
                "losing_trades": 0,
                "total_pnl": 0.0,
            }
        return dict(zip(cols, row))

    # ----- Telegram ------------------------------------------------------

    async def send_telegram(self, message: str) -> None:
        """Fire-and-forget Telegram send. Never raises."""
        if not self._tg_ready or self._tg_app is None:
            log.info("telegram_skipped", reason="not_ready", preview=message[:80])
            return
        try:
            text = message if len(message) <= 4096 else message[:4090] + "..."
            await asyncio.wait_for(
                self._tg_app.send_message(chat_id=TELEGRAM_CHAT_ID, text=text),
                timeout=10.0,
            )
        except Exception as e:
            log.warning("telegram_send_failed", error=str(e), preview=message[:80])

    @staticmethod
    def format_trade_opened_message(
        position: OpenPosition, market: MarketSnapshot, edge: float = 0.0
    ) -> str:
        prefix = "[DRY RUN] " if (position.position_id.startswith("DRY-") or DRY_RUN) else ""
        max_hold_h = max(0, int((position.max_hold_until - position.opened_at).total_seconds() // 3600))
        question = market.question if market else position.question
        return (
            f"NEW TRADE {prefix}\n"
            f"Market: {question[:60]}{'...' if len(question) > 60 else ''}\n"
            f"Direction: {position.direction}\n"
            f"Entry: ${position.entry_price:.3f}\n"
            f"Target: ${position.exit_target_price:.3f}\n"
            f"Size: ${position.size_usdc:.2f} USDC\n"
            f"Max Hold: {max_hold_h}h\n"
            f"Edge: {edge:.1%}"
        )

    @staticmethod
    def format_trade_closed_message(result: TradeResult) -> str:
        prefix = "[DRY RUN] " if result.position_id.startswith("DRY-") else ""
        outcome = "WIN" if result.pnl_usdc > 0 else "LOSS"
        sign = "+$" if result.pnl_usdc > 0 else "-$"
        return (
            f"TRADE CLOSED {prefix}{outcome}\n"
            f"P&L: {sign}{abs(result.pnl_usdc):.2f} ({result.pnl_pct:+.1%})\n"
            f"Hold: {result.hold_hours:.1f}h\n"
            f"Reason: {result.exit_reason}"
        )
