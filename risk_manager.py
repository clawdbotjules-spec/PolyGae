"""Risk management — the last gate before any order is placed.

Enforces hard caps (single bet, total open, daily loss), confidence/edge
thresholds, hourly rate limit, duplicate-position guard, kill switch and
safety mode (consecutive losses). Applies quarter-Kelly sizing on top of
Claude's recommendation.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Awaitable, Callable

import structlog

from config import (
    MAX_DAILY_LOSS_USDC,
    MAX_SINGLE_BET_USDC,
    MAX_SPREAD_CENTS,
    MAX_TOTAL_OPEN_USDC,
    MAX_TRADES_PER_HOUR,
    MIN_CONFIDENCE,
    MIN_EDGE_AFTER_FEES,
    MIN_MARKET_VOLUME_24H,
    SAFETY_MODE_CONSECUTIVE_LOSSES,
    SAFETY_MODE_PAUSE_HOURS,
    TOTAL_CAPITAL_USDC,
)
from models import (
    AffectedMarket,
    MarketSnapshot,
    OpenPosition,
    RiskCheckResult,
    TradeResult,
)


log = structlog.get_logger("risk_manager")


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


# Optional async hook for sending Telegram alerts on critical events
TelegramHook = Callable[[str], Awaitable[None]]


class RiskManager:
    def __init__(self, telegram_hook: TelegramHook | None = None) -> None:
        self.trades_this_hour: list[datetime] = []
        self.consecutive_losses: int = 0
        self.daily_loss_usdc: float = 0.0
        self.daily_pnl_usdc: float = 0.0
        self.daily_trades: list[TradeResult] = []
        self.open_positions: dict[str, OpenPosition] = {}
        self.safety_mode_until: datetime | None = None
        self.is_killed: bool = False
        self._daily_anchor: datetime = _utcnow().date()  # type: ignore[assignment]
        self._telegram_hook = telegram_hook

    # ---- daily rollover ---------------------------------------------------

    def _maybe_rollover(self) -> None:
        today = _utcnow().date()
        if today != self._daily_anchor:
            log.info(
                "daily_rollover",
                previous_pnl=self.daily_pnl_usdc,
                previous_loss=self.daily_loss_usdc,
                trades=len(self.daily_trades),
            )
            self.daily_loss_usdc = 0.0
            self.daily_pnl_usdc = 0.0
            self.daily_trades = []
            self._daily_anchor = today  # type: ignore[assignment]

    # ---- hourly rate limit ------------------------------------------------

    def _prune_hourly(self) -> None:
        cutoff = _utcnow() - timedelta(hours=1)
        self.trades_this_hour = [t for t in self.trades_this_hour if t >= cutoff]

    # ---- core check -------------------------------------------------------

    def check_trade(
        self, market: MarketSnapshot, decision_market: AffectedMarket
    ) -> RiskCheckResult:
        self._maybe_rollover()
        self._prune_hourly()

        # 1. Kill switch
        if self.is_killed:
            return RiskCheckResult(approved=False, reason="Kill switch active", adjusted_size_usdc=0.0)

        # 2. Safety mode
        if self.safety_mode_until and _utcnow() < self.safety_mode_until:
            return RiskCheckResult(
                approved=False,
                reason=f"Safety mode active until {self.safety_mode_until.isoformat()}",
                adjusted_size_usdc=0.0,
            )

        # 3. Daily loss
        if self.daily_loss_usdc >= MAX_DAILY_LOSS_USDC:
            self.activate_kill_switch(
                f"Daily loss {self.daily_loss_usdc:.2f} >= cap {MAX_DAILY_LOSS_USDC}"
            )
            return RiskCheckResult(
                approved=False,
                reason="Daily loss limit reached",
                adjusted_size_usdc=0.0,
            )

        # 4. Hourly rate
        if len(self.trades_this_hour) >= MAX_TRADES_PER_HOUR:
            return RiskCheckResult(
                approved=False,
                reason=f"Hourly rate limit ({MAX_TRADES_PER_HOUR}) hit",
                adjusted_size_usdc=0.0,
            )

        # 5. Confidence threshold
        if decision_market.confidence < MIN_CONFIDENCE:
            return RiskCheckResult(
                approved=False,
                reason=f"Confidence {decision_market.confidence:.2f} below {MIN_CONFIDENCE}",
                adjusted_size_usdc=0.0,
            )

        # 6. Edge threshold
        if decision_market.edge_after_fees < MIN_EDGE_AFTER_FEES:
            return RiskCheckResult(
                approved=False,
                reason=f"Edge {decision_market.edge_after_fees:.3f} below {MIN_EDGE_AFTER_FEES}",
                adjusted_size_usdc=0.0,
            )

        # 7. Market volume
        if market.volume_24h < MIN_MARKET_VOLUME_24H:
            return RiskCheckResult(
                approved=False,
                reason=f"Volume {market.volume_24h:.0f} below {MIN_MARKET_VOLUME_24H}",
                adjusted_size_usdc=0.0,
            )

        # 8. Market spread
        if market.spread > MAX_SPREAD_CENTS:
            return RiskCheckResult(
                approved=False,
                reason=f"Spread {market.spread:.3f} above {MAX_SPREAD_CENTS}",
                adjusted_size_usdc=0.0,
            )

        # 9. Duplicate position
        for p in self.open_positions.values():
            if p.condition_id == market.condition_id:
                return RiskCheckResult(
                    approved=False,
                    reason="Already have an open position in this market",
                    adjusted_size_usdc=0.0,
                )

        # ---- sizing ------------------------------------------------------
        size = self._compute_size(decision_market)

        # 10. Open capital cap (clamp size if necessary, reject if cannot fit minimum)
        current_open = sum(p.size_usdc for p in self.open_positions.values())
        remaining_capacity = MAX_TOTAL_OPEN_USDC - current_open
        if remaining_capacity < 10.0:
            return RiskCheckResult(
                approved=False,
                reason=f"Open capital ${current_open:.2f} leaves no headroom (cap ${MAX_TOTAL_OPEN_USDC:.0f})",
                adjusted_size_usdc=0.0,
            )
        if size > remaining_capacity:
            size = round(remaining_capacity, 2)

        # 11. Single bet cap (already applied in _compute_size, but enforce again)
        if size > MAX_SINGLE_BET_USDC:
            size = MAX_SINGLE_BET_USDC

        if size < 10.0:
            return RiskCheckResult(
                approved=False,
                reason=f"Computed size ${size:.2f} below $10 minimum",
                adjusted_size_usdc=0.0,
            )

        return RiskCheckResult(
            approved=True,
            reason="All checks passed",
            adjusted_size_usdc=round(size, 2),
        )

    # ---- Kelly sizing -----------------------------------------------------

    @staticmethod
    def _compute_size(dm: AffectedMarket) -> float:
        price = max(min(dm.current_price, 0.999), 0.001)
        b = (1.0 - price) / price  # net odds
        p = max(min(dm.fair_value_estimate, 0.999), 0.001)
        q = 1.0 - p
        full_kelly = (b * p - q) / b if b > 0 else 0.0
        if full_kelly <= 0:
            kelly_size = 0.0
        else:
            quarter_kelly = full_kelly * 0.25
            kelly_size = quarter_kelly * TOTAL_CAPITAL_USDC

        recommended = float(dm.recommended_position_usdc or 0)
        # The smallest of: kelly, claude's rec, single-bet cap. Then floor at $10.
        candidates = [c for c in (kelly_size, recommended, MAX_SINGLE_BET_USDC) if c > 0]
        if not candidates:
            return 0.0
        size = min(candidates)
        size = min(size, MAX_SINGLE_BET_USDC)
        size = max(size, 10.0) if size > 0 else 0.0
        return round(size, 2)

    # ---- lifecycle hooks --------------------------------------------------

    def record_trade_opened(self, position: OpenPosition) -> None:
        self.open_positions[position.position_id] = position
        self.trades_this_hour.append(_utcnow())
        log.info(
            "trade_opened_recorded",
            position_id=position.position_id,
            condition_id=position.condition_id,
            size_usdc=position.size_usdc,
        )

    def record_trade_closed(self, result: TradeResult) -> None:
        self._maybe_rollover()
        self.open_positions.pop(result.position_id, None)
        self.daily_trades.append(result)
        self.daily_pnl_usdc += result.pnl_usdc

        if result.pnl_usdc < 0:
            self.consecutive_losses += 1
            self.daily_loss_usdc += abs(result.pnl_usdc)
            if self.consecutive_losses >= SAFETY_MODE_CONSECUTIVE_LOSSES:
                self.safety_mode_until = _utcnow() + timedelta(hours=SAFETY_MODE_PAUSE_HOURS)
                log.warning(
                    "safety_mode_activated",
                    until=self.safety_mode_until.isoformat(),
                    consecutive_losses=self.consecutive_losses,
                )
                if self._telegram_hook is not None:
                    import asyncio
                    try:
                        asyncio.create_task(
                            self._telegram_hook(
                                f"SAFETY MODE: {self.consecutive_losses} consecutive losses. "
                                f"Trading paused until {self.safety_mode_until.isoformat()}."
                            )
                        )
                    except RuntimeError:
                        pass
            if self.daily_loss_usdc >= MAX_DAILY_LOSS_USDC:
                self.activate_kill_switch(
                    f"Daily loss reached ${self.daily_loss_usdc:.2f}"
                )
        else:
            self.consecutive_losses = 0

        log.info(
            "trade_closed_recorded",
            position_id=result.position_id,
            pnl=result.pnl_usdc,
            daily_loss=self.daily_loss_usdc,
            daily_pnl=self.daily_pnl_usdc,
        )

    def activate_kill_switch(self, reason: str) -> None:
        if self.is_killed:
            return
        self.is_killed = True
        log.error("kill_switch_activated", reason=reason)
        if self._telegram_hook is not None:
            import asyncio
            try:
                asyncio.create_task(
                    self._telegram_hook(f"KILL SWITCH ACTIVATED: {reason}")
                )
            except RuntimeError:
                pass

    def reset_safety_mode(self) -> None:
        self.safety_mode_until = None
        self.consecutive_losses = 0
        log.info("safety_mode_reset")
