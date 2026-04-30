"""Background position monitor.

Polls every POSITION_MONITOR_INTERVAL_SECONDS, checking each open position
for: target hit, time expiry, or market resolution. Closes positions that
match these criteria via the executor.

Also exposes notify_contradicting_news(), which the main loop calls when
Claude flags incoming news that contradicts an open trade.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import structlog

from config import POSITION_MONITOR_INTERVAL_SECONDS
from models import NewsArticle, OpenPosition, TradeResult

if TYPE_CHECKING:
    from executor import Executor
    from logger import TradeLogger
    from market_scanner import MarketScanner
    from risk_manager import RiskManager


log = structlog.get_logger("position_monitor")


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class PositionMonitor:
    def __init__(
        self,
        risk_manager: "RiskManager",
        executor: "Executor",
        market_scanner: "MarketScanner",
        logger: "TradeLogger",
    ) -> None:
        self.risk_manager = risk_manager
        self.executor = executor
        self.market_scanner = market_scanner
        self.logger = logger
        self._stop = asyncio.Event()

    async def start(self) -> None:
        log.info("position_monitor_started", interval_s=POSITION_MONITOR_INTERVAL_SECONDS)
        while not self._stop.is_set():
            try:
                await asyncio.wait_for(
                    self._stop.wait(), timeout=POSITION_MONITOR_INTERVAL_SECONDS
                )
            except asyncio.TimeoutError:
                pass
            if self._stop.is_set():
                break
            try:
                await self._check_positions()
            except Exception as e:
                log.error("position_monitor_iteration_failed", error=str(e), exc_info=True)

    async def stop(self) -> None:
        self._stop.set()

    async def _check_positions(self) -> None:
        positions = list(self.risk_manager.open_positions.values())
        if not positions:
            return
        for pos in positions:
            try:
                await self._check_one(pos)
            except Exception as e:
                log.error(
                    "position_check_failed",
                    position_id=pos.position_id,
                    error=str(e),
                    exc_info=True,
                )

    async def _check_one(self, pos: OpenPosition) -> None:
        market = await self.market_scanner.get_market(pos.condition_id)
        if market is None:
            market = await self.market_scanner.refresh_single_market(pos.condition_id)
        if market is None:
            log.warning("position_market_unknown", position_id=pos.position_id)
            return

        current_price = market.yes_price if pos.direction == "YES" else market.no_price
        now = _utcnow()

        # Resolved? (mid clamped to 0 or 1)
        if current_price <= 0.001 or current_price >= 0.999:
            await self._close(pos, "resolved")
            return
        # Target hit?
        if pos.direction == "YES" and current_price >= pos.exit_target_price:
            await self._close(pos, "target_hit")
            return
        if pos.direction == "NO" and current_price >= pos.exit_target_price:
            # NO contracts pay $1 when the YES side resolves false; we use the
            # NO price directly so the same comparison works.
            await self._close(pos, "target_hit")
            return
        # Time expired?
        if now >= pos.max_hold_until:
            await self._close(pos, "time_expired")
            return

    async def _close(self, pos: OpenPosition, reason: str) -> None:
        result = await self.executor.sell_position(pos, reason)
        if result is None:
            log.warning("close_returned_none", position_id=pos.position_id, reason=reason)
            return
        self.risk_manager.record_trade_closed(result)
        try:
            await self.logger.log_trade_closed(result)
        except Exception as e:
            log.error("log_trade_closed_failed", error=str(e), exc_info=True)
        try:
            await self.logger.send_telegram(
                self.logger.format_trade_closed_message(result)
            )
        except Exception:
            pass
        log.info(
            "position_closed",
            position_id=pos.position_id,
            reason=reason,
            pnl=result.pnl_usdc,
        )

    async def notify_contradicting_news(
        self, position: OpenPosition, article: NewsArticle
    ) -> TradeResult | None:
        log.info(
            "contradicting_news_close",
            position_id=position.position_id,
            article_id=article.id,
        )
        result = await self.executor.sell_position(position, "contradicting_news")
        if result is None:
            return None
        self.risk_manager.record_trade_closed(result)
        try:
            await self.logger.log_trade_closed(result)
            await self.logger.send_telegram(
                self.logger.format_trade_closed_message(result)
            )
        except Exception:
            pass
        return result
