"""Position monitor tests — close on target / time / resolved."""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

# Required env
for k in (
    "ANTHROPIC_API_KEY POLYGON_PRIVATE_KEY POLYMARKET_API_KEY POLYMARKET_API_SECRET "
    "POLYMARKET_API_PASSPHRASE POLYMARKET_FUNDER_ADDRESS NEWSFILTER_API_KEY "
    "NEWSAPI_KEY TELEGRAM_BOT_TOKEN TELEGRAM_CHAT_ID"
).split():
    os.environ.setdefault(k, "test")
os.environ.setdefault("DRY_RUN", "true")

import pytest
import pytest_asyncio

from logger import TradeLogger
from market_scanner import MarketScanner
from models import (
    AffectedMarket,
    MarketSnapshot,
    NewsArticle,
    OpenPosition,
    TradeResult,
)
from position_monitor import PositionMonitor
from risk_manager import RiskManager


def _utc():
    return datetime.now(timezone.utc)


def _market(yes: float = 0.55, no: float = 0.45) -> MarketSnapshot:
    return MarketSnapshot(
        condition_id="0xABC",
        token_id_yes="Y",
        token_id_no="N",
        question="q",
        category="politics",
        yes_price=yes,
        no_price=no,
        volume_24h=100000,
        spread=0.02,
        resolution_date=_utc() + timedelta(days=30),
        fee_rate=0.01,
        last_updated=_utc(),
    )


def _position(
    direction: str = "YES",
    target: float = 0.78,
    hold_until: datetime | None = None,
) -> OpenPosition:
    return OpenPosition(
        position_id="DRY-1",
        condition_id="0xABC",
        token_id="Y" if direction == "YES" else "N",
        question="q",
        direction=direction,  # type: ignore[arg-type]
        entry_price=0.55 if direction == "YES" else 0.45,
        size_usdc=100.0,
        shares=100 / (0.55 if direction == "YES" else 0.45),
        exit_target_price=target,
        max_hold_until=hold_until or (_utc() + timedelta(hours=12)),
        opened_at=_utc(),
        news_article_id="art",
        order_id="ORD",
    )


def _trade_result(reason: str = "target_hit", pnl: float = 10.0) -> TradeResult:
    now = _utc()
    return TradeResult(
        position_id="DRY-1",
        condition_id="0xABC",
        direction="YES",
        entry_price=0.55,
        exit_price=0.65,
        size_usdc=100.0,
        pnl_usdc=pnl,
        pnl_pct=pnl / 100.0,
        hold_hours=2.0,
        exit_reason=reason,  # type: ignore[arg-type]
        opened_at=now - timedelta(hours=2),
        closed_at=now,
    )


class _RecordingExecutor:
    def __init__(self):
        self.calls: list[tuple[str, str]] = []
        self.return_value: TradeResult | None = _trade_result()

    async def sell_position(self, pos: OpenPosition, reason: str) -> TradeResult | None:
        self.calls.append((pos.position_id, reason))
        if self.return_value is None:
            return None
        # Return a result tagged with the requested reason
        return _trade_result(reason=reason)


@pytest_asyncio.fixture
async def monitor_setup(tmp_path):
    rm = RiskManager()
    scanner = AsyncMock(spec=MarketScanner)
    scanner.get_market.return_value = _market()
    scanner.refresh_single_market.return_value = _market()
    executor = _RecordingExecutor()
    db = tmp_path / "monitor.db"
    tl = TradeLogger(db_path=str(db))
    await tl.init_db()
    pm = PositionMonitor(rm, executor, scanner, tl)  # type: ignore[arg-type]
    yield pm, rm, scanner, executor, tl
    await tl.close()


@pytest.mark.asyncio
async def test_target_hit_closes_position(monitor_setup):
    pm, rm, scanner, executor, _tl = monitor_setup
    pos = _position(direction="YES", target=0.70)
    rm.record_trade_opened(pos)
    # Market price now exceeds target
    scanner.get_market.return_value = _market(yes=0.75, no=0.25)
    await pm._check_positions()
    assert executor.calls
    assert executor.calls[0] == (pos.position_id, "target_hit")
    assert pos.position_id not in rm.open_positions


@pytest.mark.asyncio
async def test_time_expired_closes_position(monitor_setup):
    pm, rm, scanner, executor, _tl = monitor_setup
    expired = _utc() - timedelta(seconds=1)
    pos = _position(direction="YES", target=0.99, hold_until=expired)
    rm.record_trade_opened(pos)
    scanner.get_market.return_value = _market(yes=0.55)
    await pm._check_positions()
    assert executor.calls
    assert executor.calls[0][1] == "time_expired"


@pytest.mark.asyncio
async def test_resolved_market_closes_position(monitor_setup):
    pm, rm, scanner, executor, _tl = monitor_setup
    pos = _position(direction="YES", target=0.99)
    rm.record_trade_opened(pos)
    # YES price ~ 1.0 -> resolved
    scanner.get_market.return_value = _market(yes=0.999, no=0.001)
    await pm._check_positions()
    assert executor.calls
    assert executor.calls[0][1] == "resolved"


@pytest.mark.asyncio
async def test_no_action_on_open_position_below_target(monitor_setup):
    pm, rm, scanner, executor, _tl = monitor_setup
    pos = _position(direction="YES", target=0.99)
    rm.record_trade_opened(pos)
    scanner.get_market.return_value = _market(yes=0.60)
    await pm._check_positions()
    assert not executor.calls
    assert pos.position_id in rm.open_positions


@pytest.mark.asyncio
async def test_no_position_target_hit_uses_no_price(monitor_setup):
    pm, rm, scanner, executor, _tl = monitor_setup
    pos = _position(direction="NO", target=0.70)
    rm.record_trade_opened(pos)
    # NO price 0.75 >= target 0.70 -> close
    scanner.get_market.return_value = _market(yes=0.25, no=0.75)
    await pm._check_positions()
    assert executor.calls
    assert executor.calls[0][1] == "target_hit"


@pytest.mark.asyncio
async def test_contradicting_news_closes_position(monitor_setup):
    pm, rm, scanner, executor, _tl = monitor_setup
    pos = _position(direction="YES", target=0.99)
    rm.record_trade_opened(pos)
    article = NewsArticle(
        id="a1",
        headline="contradiction",
        body="b",
        source="s",
        published_at=_utc(),
        url="https://x",
        categories=[],
    )
    result = await pm.notify_contradicting_news(pos, article)
    assert result is not None
    assert result.exit_reason == "contradicting_news"
    assert pos.position_id not in rm.open_positions


@pytest.mark.asyncio
async def test_check_positions_continues_on_individual_failure(monitor_setup):
    pm, rm, scanner, executor, _tl = monitor_setup
    pos1 = _position(direction="YES", target=0.70)
    pos2 = _position(direction="YES", target=0.70)
    pos2 = pos2.model_copy(update={"position_id": "DRY-2"})
    rm.record_trade_opened(pos1)
    rm.record_trade_opened(pos2)

    # Make get_market raise on first call, succeed on second
    call_count = {"n": 0}

    async def flaky_get_market(cid):
        call_count["n"] += 1
        if call_count["n"] == 1:
            raise RuntimeError("transient")
        return _market(yes=0.75, no=0.25)

    scanner.get_market = flaky_get_market
    scanner.refresh_single_market.return_value = None

    await pm._check_positions()
    # The second position should still close even though the first crashed.
    assert any(c[0] == "DRY-2" for c in executor.calls)
