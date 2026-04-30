"""Logger tests — SQLite journal, dedup persistence, telegram fail-soft."""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

# Required env
for k in (
    "ANTHROPIC_API_KEY POLYGON_PRIVATE_KEY POLYMARKET_API_KEY POLYMARKET_API_SECRET "
    "POLYMARKET_API_PASSPHRASE POLYMARKET_FUNDER_ADDRESS NEWSFILTER_API_KEY "
    "NEWSAPI_KEY"
).split():
    os.environ.setdefault(k, "test")
os.environ.setdefault("DRY_RUN", "true")
# Force telegram to be deliberately broken so we exercise the fail-soft path.
os.environ["TELEGRAM_BOT_TOKEN"] = ""
os.environ["TELEGRAM_CHAT_ID"] = ""

import pytest
import pytest_asyncio

from logger import TradeLogger
from models import (
    AffectedMarket,
    ClaudeDecision,
    NewsArticle,
    OpenPosition,
    SkippedMarket,
    TradeResult,
)


def _utc():
    return datetime.now(timezone.utc)


def _article() -> NewsArticle:
    return NewsArticle(
        id="article-1",
        headline="hello",
        body="b",
        source="src",
        published_at=_utc(),
        url="https://x",
        categories=[],
    )


def _decision(decision_type: str = "TRADE") -> ClaudeDecision:
    return ClaudeDecision(
        decision=decision_type,  # type: ignore[arg-type]
        reasoning="r",
        news_staleness_ok=True,
        news_quality="primary_confirmed",
        news_category="political",
        affected_markets=[
            AffectedMarket(
                condition_id="0xPOL1",
                question="q",
                direction="YES",
                current_price=0.5,
                prior_probability=0.5,
                likelihood_ratio="favours",
                fair_value_estimate=0.8,
                edge_after_fees=0.25,
                confidence=0.9,
                reasoning="r",
                recommended_position_usdc=100,
                exit_target_price=0.78,
                max_hold_hours=12,
                resolution_dependency="x",
            )
        ],
        red_flags=[],
        markets_considered_but_skipped=[
            SkippedMarket(condition_id="0xPOL2", question="q2", reason_skipped="why")
        ],
        raw_response='{"decision": "TRADE"}',
    )


def _position(pid: str = "DRY-1") -> OpenPosition:
    return OpenPosition(
        position_id=pid,
        condition_id="0xPOL1",
        token_id="0xPOL1-Y",
        question="q",
        direction="YES",
        entry_price=0.55,
        size_usdc=100.0,
        shares=181.81,
        exit_target_price=0.78,
        max_hold_until=_utc() + timedelta(hours=12),
        opened_at=_utc(),
        news_article_id="article-1",
        order_id="ORD-1",
    )


def _result(pnl: float) -> TradeResult:
    now = _utc()
    return TradeResult(
        position_id="DRY-1",
        condition_id="0xPOL1",
        direction="YES",
        entry_price=0.55,
        exit_price=0.55 + (pnl / 100.0),
        size_usdc=100.0,
        pnl_usdc=pnl,
        pnl_pct=pnl / 100.0,
        hold_hours=2.5,
        exit_reason="target_hit",
        opened_at=now - timedelta(hours=3),
        closed_at=now,
    )


@pytest_asyncio.fixture
async def trade_logger(tmp_path):
    db = tmp_path / "test.db"
    tl = TradeLogger(db_path=str(db))
    await tl.init_db()
    yield tl
    await tl.close()


@pytest.mark.asyncio
async def test_init_creates_all_tables(trade_logger: TradeLogger):
    assert trade_logger._db is not None
    cur = await trade_logger._db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    )
    tables = {row[0] async for row in cur}
    await cur.close()
    expected = {
        "news_articles",
        "claude_decisions",
        "trades_opened",
        "trades_closed",
        "daily_summary",
        "seen_articles",
    }
    assert expected.issubset(tables)


@pytest.mark.asyncio
async def test_dedup_workflow(trade_logger: TradeLogger):
    aid = "test-id"
    assert await trade_logger.has_seen_article(aid) is False
    await trade_logger.mark_article_seen(aid)
    assert await trade_logger.has_seen_article(aid) is True
    # Idempotent
    await trade_logger.mark_article_seen(aid)
    assert await trade_logger.has_seen_article(aid) is True


@pytest.mark.asyncio
async def test_log_article_persists(trade_logger: TradeLogger):
    art = _article()
    await trade_logger.log_article(art, "trade", decision_id=42)
    cur = await trade_logger._db.execute(
        "SELECT id, action, claude_decision_id FROM news_articles WHERE id = ?",
        (art.id,),
    )
    row = await cur.fetchone()
    await cur.close()
    assert row is not None
    assert row[0] == art.id
    assert row[1] == "trade"
    assert row[2] == 42


@pytest.mark.asyncio
async def test_log_decision_returns_id_and_persists(trade_logger: TradeLogger):
    art = _article()
    await trade_logger.log_article(art, "pending")
    decision = _decision("TRADE")
    decision_id = await trade_logger.log_decision(
        decision, art.id, latency_ms=120, tokens={"input_tokens": 1000, "output_tokens": 200}
    )
    assert decision_id > 0
    cur = await trade_logger._db.execute(
        "SELECT decision, confidence_max, markets_count, latency_ms, input_tokens FROM claude_decisions WHERE id = ?",
        (decision_id,),
    )
    row = await cur.fetchone()
    await cur.close()
    assert row[0] == "TRADE"
    assert row[1] == 0.9
    assert row[2] == 1
    assert row[3] == 120
    assert row[4] == 1000


@pytest.mark.asyncio
async def test_log_trade_open_and_close(trade_logger: TradeLogger):
    pos = _position()
    await trade_logger.log_trade_opened(pos)
    cur = await trade_logger._db.execute(
        "SELECT direction, size_usdc, dry_run FROM trades_opened WHERE position_id = ?",
        (pos.position_id,),
    )
    row = await cur.fetchone()
    await cur.close()
    assert row[0] == "YES"
    assert row[1] == 100.0
    assert row[2] == 1  # DRY-prefixed pos id

    res = _result(20.0)
    await trade_logger.log_trade_closed(res)
    cur = await trade_logger._db.execute(
        "SELECT pnl_usdc, exit_reason FROM trades_closed WHERE position_id = ?",
        (res.position_id,),
    )
    row = await cur.fetchone()
    await cur.close()
    assert row[0] == 20.0
    assert row[1] == "target_hit"


@pytest.mark.asyncio
async def test_daily_summary_aggregates_correctly(trade_logger: TradeLogger):
    await trade_logger.log_trade_closed(_result(50.0))
    await trade_logger.log_trade_closed(_result(-30.0))
    await trade_logger.log_trade_closed(_result(20.0))
    summary = await trade_logger.get_daily_summary()
    assert summary["total_trades"] == 3
    assert summary["winning_trades"] == 2
    assert summary["losing_trades"] == 1
    assert abs(summary["total_pnl"] - 40.0) < 0.01


@pytest.mark.asyncio
async def test_send_telegram_does_not_raise_when_unconfigured(
    trade_logger: TradeLogger,
):
    # No bot token / chat id -> should be a silent no-op
    await trade_logger.send_telegram("hello")  # must not raise


def test_format_trade_opened_message_dry_run(monkeypatch):
    pos = _position()
    market = type(
        "M",
        (),
        {"question": "Will Z happen with very long question " * 5},
    )()
    msg = TradeLogger.format_trade_opened_message(pos, market, edge=0.21)
    assert "NEW TRADE" in msg
    assert "[DRY RUN]" in msg
    assert "21.0%" in msg or "21%" in msg
    assert "YES" in msg


def test_format_trade_closed_message_winner():
    res = _result(35.0)
    msg = TradeLogger.format_trade_closed_message(res)
    assert "WIN" in msg
    assert "+$35.00" in msg
    assert "target_hit" in msg


def test_format_trade_closed_message_loser():
    res = _result(-15.5)
    msg = TradeLogger.format_trade_closed_message(res)
    assert "LOSS" in msg
    assert "-$15.50" in msg


@pytest.mark.asyncio
async def test_prune_seen_articles_keeps_recent_drops_old(trade_logger: TradeLogger):
    # Insert two records: one fresh, one fake-old via direct SQL.
    await trade_logger.mark_article_seen("recent")
    await trade_logger._db.execute(
        "INSERT OR REPLACE INTO seen_articles (id, seen_at) VALUES (?, datetime('now', '-30 hours'))",
        ("ancient",),
    )
    await trade_logger._db.commit()
    await trade_logger.prune_seen_articles(older_than_hours=24)
    assert await trade_logger.has_seen_article("recent")
    assert not await trade_logger.has_seen_article("ancient")
