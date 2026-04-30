"""Executor tests — DRY_RUN order placement, sell calculation, P&L."""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

# Required env (DRY_RUN forced on so the executor never touches the CLOB)
for k in (
    "ANTHROPIC_API_KEY POLYGON_PRIVATE_KEY POLYMARKET_API_KEY POLYMARKET_API_SECRET "
    "POLYMARKET_API_PASSPHRASE POLYMARKET_FUNDER_ADDRESS NEWSFILTER_API_KEY "
    "NEWSAPI_KEY TELEGRAM_BOT_TOKEN TELEGRAM_CHAT_ID"
).split():
    os.environ.setdefault(k, "test")
os.environ["DRY_RUN"] = "true"

import pytest

# Important: import config first so DRY_RUN is captured by all modules.
import config  # noqa: F401
from executor import Executor, _new_position_id
from models import AffectedMarket, MarketSnapshot, NewsArticle


def _utc():
    return datetime.now(timezone.utc)


def _market(yes_price: float = 0.55, no_price: float = 0.45) -> MarketSnapshot:
    return MarketSnapshot(
        condition_id="0xABC",
        token_id_yes="YES",
        token_id_no="NO",
        question="Will Y happen?",
        category="politics",
        yes_price=yes_price,
        no_price=no_price,
        volume_24h=100000.0,
        spread=0.02,
        resolution_date=_utc() + timedelta(days=30),
        fee_rate=0.01,
        last_updated=_utc(),
    )


def _article() -> NewsArticle:
    return NewsArticle(
        id="abc",
        headline="h",
        body="b",
        source="s",
        published_at=_utc(),
        url="https://x",
        categories=[],
    )


def _dm(direction: str = "YES", target: float = 0.78, hold: int = 12) -> AffectedMarket:
    return AffectedMarket(
        condition_id="0xABC",
        question="Will Y happen?",
        direction=direction,  # type: ignore[arg-type]
        current_price=0.55 if direction == "YES" else 0.45,
        prior_probability=0.5,
        likelihood_ratio="favours",
        fair_value_estimate=0.80 if direction == "YES" else 0.20,
        edge_after_fees=0.20,
        confidence=0.85,
        reasoning="strong",
        recommended_position_usdc=120,
        exit_target_price=target,
        max_hold_hours=hold,
        resolution_dependency="event",
    )


# ---- Helpers ---------------------------------------------------------------

def test_new_position_id_dry_run_prefix():
    pid = _new_position_id(dry_run=True)
    assert pid.startswith("DRY-")
    pid2 = _new_position_id(dry_run=False)
    assert pid2.startswith("POS-")


# ---- Order placement (DRY_RUN) --------------------------------------------

@pytest.mark.asyncio
async def test_place_order_dry_run_yes_creates_position():
    scanner = AsyncMock()
    ex = Executor(market_scanner=scanner)
    pos = await ex.place_order(
        market=_market(),
        direction="YES",
        size_usdc=120.0,
        article=_article(),
        decision_market=_dm(direction="YES"),
    )
    assert pos is not None
    assert pos.position_id.startswith("DRY-")
    assert pos.direction == "YES"
    assert pos.token_id == "YES"
    assert pos.entry_price == 0.55
    assert pos.size_usdc == 120.0
    assert abs(pos.shares - (120.0 / 0.55)) < 0.05
    assert pos.exit_target_price == 0.78
    # max_hold_until is opened_at + max_hold_hours
    diff_h = (pos.max_hold_until - pos.opened_at).total_seconds() / 3600
    assert 11.5 < diff_h <= 12.0


@pytest.mark.asyncio
async def test_place_order_dry_run_no_uses_no_token():
    scanner = AsyncMock()
    ex = Executor(market_scanner=scanner)
    pos = await ex.place_order(
        market=_market(),
        direction="NO",
        size_usdc=80.0,
        article=_article(),
        decision_market=_dm(direction="NO", target=0.55),
    )
    assert pos is not None
    assert pos.direction == "NO"
    assert pos.token_id == "NO"
    assert pos.entry_price == 0.45


@pytest.mark.asyncio
async def test_place_order_clamps_extreme_prices():
    """Even if the market price is silly, we clamp entry to [0.01, 0.97]."""
    scanner = AsyncMock()
    ex = Executor(market_scanner=scanner)
    m = _market(yes_price=0.99, no_price=0.01)
    pos = await ex.place_order(
        market=m,
        direction="YES",
        size_usdc=50.0,
        article=_article(),
        decision_market=_dm(),
    )
    assert pos is not None
    assert pos.entry_price <= 0.97


# ---- Exit P&L --------------------------------------------------------------

@pytest.mark.asyncio
async def test_sell_position_yes_profit():
    scanner = AsyncMock()
    scanner.get_market.return_value = _market(yes_price=0.80)
    ex = Executor(market_scanner=scanner)
    pos = await ex.place_order(
        market=_market(yes_price=0.50, no_price=0.50),
        direction="YES",
        size_usdc=100.0,
        article=_article(),
        decision_market=_dm(),
    )
    result = await ex.sell_position(pos, "target_hit")
    assert result is not None
    assert result.direction == "YES"
    # Bought 200 shares @ 0.50 = $100. Sold @ 0.80 = $160. PnL = $60.
    assert abs(result.pnl_usdc - 60.0) < 0.5
    assert result.pnl_pct > 0
    assert result.exit_reason == "target_hit"


@pytest.mark.asyncio
async def test_sell_position_yes_loss():
    scanner = AsyncMock()
    scanner.get_market.return_value = _market(yes_price=0.30)
    ex = Executor(market_scanner=scanner)
    pos = await ex.place_order(
        market=_market(yes_price=0.50, no_price=0.50),
        direction="YES",
        size_usdc=100.0,
        article=_article(),
        decision_market=_dm(),
    )
    result = await ex.sell_position(pos, "time_expired")
    assert result is not None
    # Bought 200 @ 0.50 = $100. Sold @ 0.30 = $60. PnL = -$40.
    assert -41 < result.pnl_usdc < -39
    assert result.pnl_pct < 0
    assert result.exit_reason == "time_expired"


@pytest.mark.asyncio
async def test_sell_position_at_resolution_full_pay():
    scanner = AsyncMock()
    # Market resolved YES (yes price ~ 1.0)
    scanner.get_market.return_value = _market(yes_price=0.999, no_price=0.001)
    ex = Executor(market_scanner=scanner)
    pos = await ex.place_order(
        market=_market(yes_price=0.50, no_price=0.50),
        direction="YES",
        size_usdc=100.0,
        article=_article(),
        decision_market=_dm(),
    )
    result = await ex.sell_position(pos, "manual")
    assert result is not None
    # Resolved markets clamp to 1.0; reason switches to "resolved".
    assert result.exit_reason == "resolved"
    assert result.exit_price == 1.0
    # 200 shares paid out at $1 = $200; net PnL = $100.
    assert abs(result.pnl_usdc - 100.0) < 0.5


@pytest.mark.asyncio
async def test_sell_position_at_resolution_no_pay():
    scanner = AsyncMock()
    scanner.get_market.return_value = _market(yes_price=0.001, no_price=0.999)
    ex = Executor(market_scanner=scanner)
    pos = await ex.place_order(
        market=_market(yes_price=0.50, no_price=0.50),
        direction="YES",
        size_usdc=100.0,
        article=_article(),
        decision_market=_dm(),
    )
    result = await ex.sell_position(pos, "manual")
    assert result is not None
    assert result.exit_reason == "resolved"
    assert result.exit_price == 0.0
    assert abs(result.pnl_usdc + 100.0) < 0.5  # full loss


@pytest.mark.asyncio
async def test_sell_position_handles_missing_market_gracefully():
    scanner = AsyncMock()
    scanner.get_market.return_value = None
    scanner.refresh_single_market.return_value = None
    ex = Executor(market_scanner=scanner)
    pos = await ex.place_order(
        market=_market(yes_price=0.50, no_price=0.50),
        direction="YES",
        size_usdc=100.0,
        article=_article(),
        decision_market=_dm(),
    )
    # Should fall back to entry price (zero PnL) rather than crash.
    result = await ex.sell_position(pos, "time_expired")
    assert result is not None
    assert abs(result.pnl_usdc) < 0.5
