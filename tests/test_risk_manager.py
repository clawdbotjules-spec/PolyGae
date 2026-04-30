"""Risk manager tests."""
from __future__ import annotations

import os

# Provide required env vars before importing the package's config module so
# `validate_required_env` is satisfied even though it's not invoked by tests.
os.environ.setdefault("ANTHROPIC_API_KEY", "test")
os.environ.setdefault("POLYGON_PRIVATE_KEY", "test")
os.environ.setdefault("POLYMARKET_API_KEY", "test")
os.environ.setdefault("POLYMARKET_API_SECRET", "test")
os.environ.setdefault("POLYMARKET_API_PASSPHRASE", "test")
os.environ.setdefault("POLYMARKET_FUNDER_ADDRESS", "test")
os.environ.setdefault("NEWSFILTER_API_KEY", "test")
os.environ.setdefault("NEWSAPI_KEY", "test")
os.environ.setdefault("TELEGRAM_BOT_TOKEN", "test")
os.environ.setdefault("TELEGRAM_CHAT_ID", "test")
os.environ.setdefault("DRY_RUN", "true")

from datetime import datetime, timedelta, timezone

import pytest

import config
from models import AffectedMarket, MarketSnapshot, OpenPosition, TradeResult
from risk_manager import RiskManager
from tests.mock_data import MARKETS


def _good_dm(cid: str = "0xPOL1") -> AffectedMarket:
    return AffectedMarket(
        condition_id=cid,
        question="Test",
        direction="YES",
        current_price=0.55,
        prior_probability=0.55,
        likelihood_ratio="favours YES",
        fair_value_estimate=0.80,
        edge_after_fees=0.20,
        confidence=0.85,
        reasoning="strong evidence",
        recommended_position_usdc=120,
        exit_target_price=0.78,
        max_hold_hours=12,
        resolution_dependency="event",
    )


def _market(cid: str = "0xPOL1", spread: float = 0.02, volume: float = 100000.0) -> MarketSnapshot:
    return MarketSnapshot(
        condition_id=cid,
        token_id_yes=f"{cid}-Y",
        token_id_no=f"{cid}-N",
        question="Test",
        category="politics",
        yes_price=0.55,
        no_price=0.45,
        volume_24h=volume,
        spread=spread,
        resolution_date=datetime.now(timezone.utc) + timedelta(days=30),
        fee_rate=0.01,
        last_updated=datetime.now(timezone.utc),
    )


def _trade_result(pnl: float, pos_id: str = "DRY-1", cid: str = "0xPOL1") -> TradeResult:
    now = datetime.now(timezone.utc)
    size = 100.0
    return TradeResult(
        position_id=pos_id,
        condition_id=cid,
        direction="YES",
        entry_price=0.5,
        exit_price=0.55,
        size_usdc=size,
        pnl_usdc=pnl,
        pnl_pct=pnl / size,
        hold_hours=2.0,
        exit_reason="target_hit" if pnl > 0 else "time_expired",
        opened_at=now - timedelta(hours=2),
        closed_at=now,
    )


# ---- Tests -----------------------------------------------------------------

def test_trade_approved_when_all_conditions_met():
    rm = RiskManager()
    res = rm.check_trade(_market(), _good_dm())
    assert res.approved is True
    assert res.adjusted_size_usdc >= 10.0


def test_trade_rejected_when_daily_loss_exceeds_limit():
    rm = RiskManager()
    rm.daily_loss_usdc = config.MAX_DAILY_LOSS_USDC + 1.0
    res = rm.check_trade(_market(), _good_dm())
    assert res.approved is False
    assert rm.is_killed is True


def test_trade_rejected_when_confidence_below_threshold():
    rm = RiskManager()
    dm = _good_dm()
    dm.confidence = 0.50
    res = rm.check_trade(_market(), dm)
    assert res.approved is False
    assert "Confidence" in res.reason


def test_trade_rejected_when_edge_below_threshold():
    rm = RiskManager()
    dm = _good_dm()
    dm.edge_after_fees = 0.02
    res = rm.check_trade(_market(), dm)
    assert res.approved is False
    assert "Edge" in res.reason


def test_trade_rejected_when_already_holding_position_in_market():
    rm = RiskManager()
    pos = OpenPosition(
        position_id="DRY-x",
        condition_id="0xPOL1",
        token_id="0xPOL1-Y",
        question="Test",
        direction="YES",
        entry_price=0.5,
        size_usdc=50,
        shares=100,
        exit_target_price=0.7,
        max_hold_until=datetime.now(timezone.utc) + timedelta(hours=10),
        opened_at=datetime.now(timezone.utc),
        news_article_id="x",
        order_id="o",
    )
    rm.open_positions[pos.position_id] = pos
    res = rm.check_trade(_market(), _good_dm())
    assert res.approved is False
    assert "open position" in res.reason.lower()


def test_trade_rejected_when_volume_below_minimum():
    rm = RiskManager()
    res = rm.check_trade(_market(volume=500.0), _good_dm())
    assert res.approved is False
    assert "Volume" in res.reason


def test_trade_rejected_when_spread_too_wide():
    rm = RiskManager()
    res = rm.check_trade(_market(spread=0.20), _good_dm())
    assert res.approved is False
    assert "Spread" in res.reason


def test_trade_size_capped_at_single_bet_max():
    rm = RiskManager()
    dm = _good_dm()
    dm.recommended_position_usdc = 10_000  # absurd ask
    res = rm.check_trade(_market(), dm)
    assert res.approved is True
    assert res.adjusted_size_usdc <= config.MAX_SINGLE_BET_USDC


def test_kelly_criterion_sizing_is_applied():
    rm = RiskManager()
    # Tiny edge -> Kelly will scale way below claude's recommendation
    dm = _good_dm()
    dm.current_price = 0.55
    dm.fair_value_estimate = 0.62  # small edge
    dm.edge_after_fees = 0.07
    dm.confidence = 0.75
    dm.recommended_position_usdc = 300
    res = rm.check_trade(_market(), dm)
    assert res.approved is True
    # Quarter-Kelly on a small edge with $1000 capital is small.
    assert res.adjusted_size_usdc < dm.recommended_position_usdc


def test_safety_mode_activates_after_consecutive_losses():
    rm = RiskManager()
    for i in range(config.SAFETY_MODE_CONSECUTIVE_LOSSES):
        rm.record_trade_closed(_trade_result(-50.0, pos_id=f"DRY-{i}", cid=f"0x{i}"))
    assert rm.safety_mode_until is not None
    res = rm.check_trade(_market(), _good_dm())
    assert res.approved is False
    assert "Safety mode" in res.reason


def test_safety_mode_clears_after_pause_window():
    rm = RiskManager()
    for i in range(config.SAFETY_MODE_CONSECUTIVE_LOSSES):
        rm.record_trade_closed(_trade_result(-50.0, pos_id=f"DRY-{i}", cid=f"0x{i}"))
    # Simulate pause expired
    rm.safety_mode_until = datetime.now(timezone.utc) - timedelta(seconds=1)
    res = rm.check_trade(_market(), _good_dm())
    assert res.approved is True


def test_kill_switch_blocks_all_trades():
    rm = RiskManager()
    rm.activate_kill_switch("manual test")
    res = rm.check_trade(_market(), _good_dm())
    assert res.approved is False
    assert "Kill switch" in res.reason


def test_hourly_rate_limit_works():
    rm = RiskManager()
    now = datetime.now(timezone.utc)
    for _ in range(config.MAX_TRADES_PER_HOUR):
        rm.trades_this_hour.append(now)
    res = rm.check_trade(_market(), _good_dm())
    assert res.approved is False
    assert "Hourly" in res.reason


def test_consecutive_losses_reset_after_win():
    rm = RiskManager()
    rm.record_trade_closed(_trade_result(-25.0, pos_id="DRY-1", cid="0x1"))
    rm.record_trade_closed(_trade_result(-25.0, pos_id="DRY-2", cid="0x2"))
    assert rm.consecutive_losses == 2
    rm.record_trade_closed(_trade_result(50.0, pos_id="DRY-3", cid="0x3"))
    assert rm.consecutive_losses == 0
