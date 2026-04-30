"""End-to-end integration test with mocked external services.

Wires the real RiskManager, Executor (DRY_RUN), MarketScanner, PositionMonitor,
and TradeLogger together; mocks the Anthropic client; pumps a single article
through the news queue; verifies a trade is opened, logged, and closes
correctly when the price crosses the target.
"""
from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

# Required env (DRY_RUN forced on)
for k in (
    "ANTHROPIC_API_KEY POLYGON_PRIVATE_KEY POLYMARKET_API_KEY POLYMARKET_API_SECRET "
    "POLYMARKET_API_PASSPHRASE POLYMARKET_FUNDER_ADDRESS NEWSFILTER_API_KEY "
    "NEWSAPI_KEY"
).split():
    os.environ.setdefault(k, "test")
os.environ["DRY_RUN"] = "true"
os.environ["TELEGRAM_BOT_TOKEN"] = ""
os.environ["TELEGRAM_CHAT_ID"] = ""

import pytest

from analyst import ClaudeAnalyst
from executor import Executor
from logger import TradeLogger
from market_scanner import MarketScanner
from models import MarketSnapshot, NewsArticle
from position_monitor import PositionMonitor
from risk_manager import RiskManager


def _utc():
    return datetime.now(timezone.utc)


def _market_at(yes_price: float = 0.55) -> MarketSnapshot:
    return MarketSnapshot(
        condition_id="0xPOL1",
        token_id_yes="YES-TOKEN",
        token_id_no="NO-TOKEN",
        question="Will Senate confirm Fed Chair?",
        category="politics",
        yes_price=yes_price,
        no_price=round(1.0 - yes_price, 4),
        volume_24h=200000.0,
        spread=0.02,
        resolution_date=_utc() + timedelta(days=30),
        fee_rate=0.01,
        last_updated=_utc(),
    )


def _trade_decision_json() -> str:
    return json.dumps(
        {
            "decision": "TRADE",
            "reasoning": "Senate confirmed; news directly resolves question.",
            "news_staleness_ok": True,
            "news_quality": "primary_confirmed",
            "news_category": "political",
            "affected_markets": [
                {
                    "condition_id": "0xPOL1",
                    "question": "Will Senate confirm Fed Chair?",
                    "direction": "YES",
                    "current_price": 0.55,
                    "prior_probability": 0.55,
                    "likelihood_ratio": "strongly favours YES",
                    "fair_value_estimate": 0.92,
                    "edge_after_fees": 0.36,
                    "confidence": 0.92,
                    "reasoning": "Senate already voted; nothing left.",
                    "recommended_position_usdc": 150,
                    "exit_target_price": 0.85,
                    "max_hold_hours": 24,
                    "resolution_dependency": "Confirmation per official record.",
                }
            ],
            "red_flags": [],
            "markets_considered_but_skipped": [],
        }
    )


def _skip_decision_json() -> str:
    return json.dumps(
        {
            "decision": "SKIP",
            "reasoning": "No causal link.",
            "news_staleness_ok": True,
            "news_quality": "secondary",
            "news_category": "other",
            "affected_markets": [],
            "red_flags": [],
            "markets_considered_but_skipped": [],
        }
    )


def _mock_claude_response(text: str):
    block = SimpleNamespace(text=text, type="text")
    usage = SimpleNamespace(input_tokens=42, output_tokens=84)
    return SimpleNamespace(content=[block], usage=usage)


@pytest.mark.asyncio
async def test_full_flow_news_to_trade_to_close(tmp_path):
    """Drive a TRADE decision end-to-end and verify the position lifecycle."""
    db_path = tmp_path / "integration.db"

    trade_logger = TradeLogger(db_path=str(db_path))
    await trade_logger.init_db()

    market_scanner = MarketScanner()
    market_scanner._cache = {"0xPOL1": _market_at(0.55)}
    market_scanner.refresh_single_market = AsyncMock(return_value=_market_at(0.55))

    risk_manager = RiskManager()
    executor = Executor(market_scanner=market_scanner)
    position_monitor = PositionMonitor(
        risk_manager=risk_manager,
        executor=executor,
        market_scanner=market_scanner,
        logger=trade_logger,
    )
    analyst = ClaudeAnalyst()

    article = NewsArticle(
        id="news-1",
        headline="Senate confirms Fed Chair",
        body="The Senate voted today...",
        source="Reuters",
        published_at=_utc() - timedelta(seconds=30),
        url="https://reuters.com/x",
        categories=["politics"],
        minutes_ago=0.5,
    )

    fake_create = AsyncMock(return_value=_mock_claude_response(_trade_decision_json()))

    with patch.object(analyst.client.messages, "create", fake_create):
        decision = await analyst.analyze(
            article,
            await market_scanner.get_markets_for_prompt(),
            {
                "session_start": _utc().isoformat(),
                "trades_this_session": 0,
                "already_traded": [],
                "open_positions": [],
                "daily_pnl": 0.0,
            },
        )

    assert decision.decision == "TRADE"
    assert len(decision.affected_markets) == 1

    dm = decision.affected_markets[0]
    market = await market_scanner.get_market(dm.condition_id)
    assert market is not None
    risk_result = risk_manager.check_trade(market, dm)
    assert risk_result.approved is True
    assert risk_result.adjusted_size_usdc > 0

    position = await executor.place_order(
        market=market,
        direction=dm.direction,
        size_usdc=risk_result.adjusted_size_usdc,
        article=article,
        decision_market=dm,
    )
    assert position is not None
    risk_manager.record_trade_opened(position)
    await trade_logger.log_trade_opened(position)
    assert position.position_id in risk_manager.open_positions

    # Bump price past target -> position monitor should close on next sweep
    market_scanner._cache["0xPOL1"] = _market_at(0.90)
    await position_monitor._check_positions()
    assert position.position_id not in risk_manager.open_positions

    # Verify the closed trade landed in the database
    cur = await trade_logger._db.execute(
        "SELECT exit_reason, pnl_usdc FROM trades_closed WHERE position_id = ?",
        (position.position_id,),
    )
    row = await cur.fetchone()
    await cur.close()
    assert row is not None
    assert row[0] == "target_hit"
    assert row[1] > 0  # profitable

    await trade_logger.close()


@pytest.mark.asyncio
async def test_full_flow_skip_decision_does_not_open_trade(tmp_path):
    """A SKIP decision must not open any trade or write trade rows."""
    db_path = tmp_path / "skip.db"
    trade_logger = TradeLogger(db_path=str(db_path))
    await trade_logger.init_db()

    market_scanner = MarketScanner()
    market_scanner._cache = {"0xPOL1": _market_at(0.55)}
    risk_manager = RiskManager()
    analyst = ClaudeAnalyst()

    article = NewsArticle(
        id="news-skip",
        headline="boring",
        body="b",
        source="src",
        published_at=_utc() - timedelta(seconds=30),
        url="https://x",
        categories=[],
        minutes_ago=0.5,
    )

    fake_create = AsyncMock(return_value=_mock_claude_response(_skip_decision_json()))
    with patch.object(analyst.client.messages, "create", fake_create):
        decision = await analyst.analyze(article, "", {})

    assert decision.decision == "SKIP"
    assert risk_manager.open_positions == {}
    cur = await trade_logger._db.execute("SELECT COUNT(*) FROM trades_opened")
    row = await cur.fetchone()
    await cur.close()
    assert row[0] == 0
    await trade_logger.close()


@pytest.mark.asyncio
async def test_kill_switch_short_circuits_check_trade(tmp_path):
    """Confirm risk gate stops trades after kill switch."""
    market_scanner = MarketScanner()
    market_scanner._cache = {"0xPOL1": _market_at(0.55)}
    risk_manager = RiskManager()

    risk_manager.activate_kill_switch("manual integration test")
    market = await market_scanner.get_market("0xPOL1")

    # Build a fake AffectedMarket
    from models import AffectedMarket

    dm = AffectedMarket(
        condition_id="0xPOL1",
        question="q",
        direction="YES",
        current_price=0.55,
        prior_probability=0.55,
        likelihood_ratio="favours",
        fair_value_estimate=0.85,
        edge_after_fees=0.30,
        confidence=0.90,
        reasoning="r",
        recommended_position_usdc=100,
        exit_target_price=0.80,
        max_hold_hours=12,
        resolution_dependency="x",
    )
    res = risk_manager.check_trade(market, dm)
    assert res.approved is False
    assert "Kill switch" in res.reason


@pytest.mark.asyncio
async def test_full_flow_safety_mode_after_three_losses(tmp_path):
    """Three consecutive losses activate safety mode and reject the next trade."""
    market_scanner = MarketScanner()
    market_scanner._cache = {"0xPOL1": _market_at(0.55)}
    risk_manager = RiskManager()

    from models import AffectedMarket, TradeResult

    for i in range(3):
        loss = TradeResult(
            position_id=f"DRY-{i}",
            condition_id=f"0x{i}",
            direction="YES",
            entry_price=0.5,
            exit_price=0.4,
            size_usdc=50.0,
            pnl_usdc=-50.0,
            pnl_pct=-1.0,
            hold_hours=1.0,
            exit_reason="time_expired",
            opened_at=_utc() - timedelta(hours=2),
            closed_at=_utc(),
        )
        risk_manager.record_trade_closed(loss)

    assert risk_manager.safety_mode_until is not None

    market = await market_scanner.get_market("0xPOL1")
    dm = AffectedMarket(
        condition_id="0xPOL1",
        question="q",
        direction="YES",
        current_price=0.55,
        prior_probability=0.55,
        likelihood_ratio="favours",
        fair_value_estimate=0.85,
        edge_after_fees=0.30,
        confidence=0.90,
        reasoning="r",
        recommended_position_usdc=100,
        exit_target_price=0.80,
        max_hold_hours=12,
        resolution_dependency="x",
    )
    res = risk_manager.check_trade(market, dm)
    assert res.approved is False
    assert "Safety mode" in res.reason
