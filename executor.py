"""Order execution against Polymarket CLOB.

In DRY_RUN mode the executor never calls the live CLOB; it returns mock
positions and simulates exits at the current mid-price from the scanner.
A heartbeat task keeps the connection alive.
"""
from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

import structlog

from config import (
    DRY_RUN,
    HEARTBEAT_INTERVAL_SECONDS,
    ORDER_FILL_TIMEOUT_SECONDS,
    POLYGON_PRIVATE_KEY,
    POLYMARKET_API_KEY,
    POLYMARKET_API_PASSPHRASE,
    POLYMARKET_API_SECRET,
    POLYMARKET_CLOB_HOST,
    POLYMARKET_FUNDER_ADDRESS,
)
from market_scanner import MarketScanner
from models import AffectedMarket, MarketSnapshot, NewsArticle, OpenPosition, TradeResult


log = structlog.get_logger("executor")


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _new_position_id(dry_run: bool) -> str:
    return ("DRY-" if dry_run else "POS-") + uuid.uuid4().hex[:16]


class Executor:
    """Wraps py-clob-client-v2 for order placement, cancellation, and exits.

    Heartbeat runs in its own task. DRY_RUN short-circuits all CLOB calls.
    """

    def __init__(self, market_scanner: MarketScanner) -> None:
        self.market_scanner = market_scanner
        self._client: Any | None = None
        self._heartbeat_task: asyncio.Task[Any] | None = None
        self._last_heartbeat_id: str | None = None
        self._heartbeat_failures: int = 0
        self._stop = asyncio.Event()
        self._lock = asyncio.Lock()
        self._init_client()

    # --- client setup -----------------------------------------------------

    def _init_client(self) -> None:
        if DRY_RUN:
            log.info("executor_init_dry_run")
            return
        try:
            # py-clob-client-v2 exposes ClobClient; the exact module path can
            # differ slightly between releases, so we fall back gracefully.
            try:
                from py_clob_client.client import ClobClient  # type: ignore
                from py_clob_client.constants import POLYGON  # type: ignore
                from py_clob_client.clob_types import ApiCreds  # type: ignore
            except ImportError:  # pragma: no cover
                from py_clob_client_v2.client import ClobClient  # type: ignore
                from py_clob_client_v2.constants import POLYGON  # type: ignore
                from py_clob_client_v2.clob_types import ApiCreds  # type: ignore

            creds = ApiCreds(
                api_key=POLYMARKET_API_KEY,
                api_secret=POLYMARKET_API_SECRET,
                api_passphrase=POLYMARKET_API_PASSPHRASE,
            )
            self._client = ClobClient(
                host=POLYMARKET_CLOB_HOST,
                key=POLYGON_PRIVATE_KEY,
                chain_id=POLYGON,
                creds=creds,
                funder=POLYMARKET_FUNDER_ADDRESS,
            )
            log.info("executor_clob_client_ready", host=POLYMARKET_CLOB_HOST)
        except Exception as e:
            log.error("executor_client_init_failed", error=str(e), exc_info=True)
            self._client = None

    # --- heartbeat --------------------------------------------------------

    async def heartbeat_loop(self) -> None:
        """Periodically pings the CLOB to keep the session warm."""
        while not self._stop.is_set():
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=HEARTBEAT_INTERVAL_SECONDS)
            except asyncio.TimeoutError:
                pass
            if self._stop.is_set():
                break
            if DRY_RUN or self._client is None:
                continue
            try:
                # py-clob-client exposes either post_heartbeat() or get_ok().
                fn = getattr(self._client, "post_heartbeat", None) or getattr(
                    self._client, "get_ok", None
                )
                if fn is None:
                    # nothing to do
                    continue
                kwargs: dict[str, Any] = {}
                if self._last_heartbeat_id and "post_heartbeat" in (fn.__name__ or ""):
                    kwargs["last_heartbeat_id"] = self._last_heartbeat_id
                result = await asyncio.get_running_loop().run_in_executor(
                    None, lambda: fn(**kwargs) if kwargs else fn()
                )
                if isinstance(result, dict):
                    self._last_heartbeat_id = result.get("heartbeatId") or self._last_heartbeat_id
                self._heartbeat_failures = 0
            except Exception as e:
                self._heartbeat_failures += 1
                log.warning(
                    "heartbeat_failed",
                    failures=self._heartbeat_failures,
                    error=str(e),
                )
                if self._heartbeat_failures >= 3:
                    log.error("heartbeat_failed_repeatedly", failures=self._heartbeat_failures)

    async def stop(self) -> None:
        self._stop.set()
        if self._heartbeat_task is not None:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except (asyncio.CancelledError, Exception):
                pass

    # --- order placement --------------------------------------------------

    async def place_order(
        self,
        market: MarketSnapshot,
        direction: str,
        size_usdc: float,
        article: NewsArticle,
        decision_market: AffectedMarket,
    ) -> OpenPosition | None:
        async with self._lock:
            return await self._place_order_inner(
                market, direction, size_usdc, article, decision_market
            )

    async def _place_order_inner(
        self,
        market: MarketSnapshot,
        direction: str,
        size_usdc: float,
        article: NewsArticle,
        decision_market: AffectedMarket,
    ) -> OpenPosition | None:
        token_id = market.token_id_yes if direction == "YES" else market.token_id_no
        max_hold_until = _utcnow() + timedelta(hours=max(1, decision_market.max_hold_hours))

        if DRY_RUN:
            entry_price = (
                market.yes_price if direction == "YES" else market.no_price
            )
            entry_price = round(min(max(entry_price, 0.01), 0.97), 4)
            shares = round(size_usdc / entry_price, 2) if entry_price > 0 else 0.0
            position = OpenPosition(
                position_id=_new_position_id(dry_run=True),
                condition_id=market.condition_id,
                token_id=token_id,
                question=market.question,
                direction=direction,  # type: ignore[arg-type]
                entry_price=entry_price,
                size_usdc=round(size_usdc, 2),
                shares=shares,
                exit_target_price=round(decision_market.exit_target_price, 4),
                max_hold_until=max_hold_until,
                opened_at=_utcnow(),
                news_article_id=article.id,
                order_id="DRY-" + uuid.uuid4().hex[:8],
            )
            log.info(
                "dry_run_order_placed",
                position_id=position.position_id,
                question=market.question[:60],
                direction=direction,
                size_usdc=size_usdc,
                entry_price=entry_price,
            )
            return position

        # --- live path --------------------------------------------------
        if self._client is None:
            log.error("clob_client_unavailable")
            return None

        # Re-fetch market for fresh price
        fresh = await self.market_scanner.refresh_single_market(market.condition_id)
        if fresh is None:
            log.warning("could_not_refresh_market", condition_id=market.condition_id)
            return None
        live_yes = fresh.yes_price
        live_no = fresh.no_price
        old_price = market.yes_price if direction == "YES" else market.no_price
        new_price = live_yes if direction == "YES" else live_no
        if abs(new_price - old_price) > 0.03:
            log.warning(
                "price_moved_too_much",
                condition_id=market.condition_id,
                old=old_price,
                new=new_price,
            )
            return None

        limit_price = round(min(new_price + 0.01, 0.97), 4)
        if limit_price <= 0:
            log.warning("invalid_limit_price", limit_price=limit_price)
            return None
        shares = round(size_usdc / limit_price, 4)

        order_id = await self._submit_limit_order(token_id, "BUY", limit_price, shares)
        if order_id is None:
            return None

        filled_shares = await self._wait_for_fill(order_id, ORDER_FILL_TIMEOUT_SECONDS)
        if filled_shares <= 0:
            await self.cancel_order(order_id)
            log.warning("retrying_at_market", token_id=token_id)
            order_id = await self._submit_limit_order(
                token_id, "BUY", round(min(new_price + 0.03, 0.99), 4), shares, fok=True
            )
            if order_id is None:
                return None
            filled_shares = await self._wait_for_fill(order_id, 30)
            if filled_shares <= 0:
                log.error("order_unfilled_after_retry", token_id=token_id)
                return None

        position = OpenPosition(
            position_id=_new_position_id(dry_run=False),
            condition_id=market.condition_id,
            token_id=token_id,
            question=market.question,
            direction=direction,  # type: ignore[arg-type]
            entry_price=limit_price,
            size_usdc=round(size_usdc, 2),
            shares=round(filled_shares, 4),
            exit_target_price=round(decision_market.exit_target_price, 4),
            max_hold_until=max_hold_until,
            opened_at=_utcnow(),
            news_article_id=article.id,
            order_id=str(order_id),
        )
        log.info(
            "order_filled",
            position_id=position.position_id,
            order_id=order_id,
            shares=filled_shares,
            price=limit_price,
        )
        return position

    async def _submit_limit_order(
        self,
        token_id: str,
        side: str,
        price: float,
        size: float,
        fok: bool = False,
    ) -> str | None:
        if DRY_RUN or self._client is None:
            return None
        try:
            from py_clob_client.clob_types import OrderArgs, OrderType  # type: ignore
        except ImportError:  # pragma: no cover
            try:
                from py_clob_client_v2.clob_types import OrderArgs, OrderType  # type: ignore
            except ImportError:
                log.error("clob_types_not_importable")
                return None

        order_type = OrderType.FOK if fok else OrderType.GTC
        args = OrderArgs(
            token_id=token_id,
            price=price,
            size=size,
            side=side,
        )
        loop = asyncio.get_running_loop()
        try:
            resp = await loop.run_in_executor(
                None,
                lambda: self._client.create_and_post_order(args, order_type=order_type),
            )
        except Exception as e:
            log.error("submit_order_failed", error=str(e), exc_info=True)
            return None
        if not isinstance(resp, dict):
            log.warning("unexpected_order_response", response=str(resp)[:200])
            return None
        if resp.get("success") is False:
            log.warning("order_rejected", error=resp.get("errorMsg"))
            return None
        return str(resp.get("orderID") or resp.get("orderId") or resp.get("id") or "")

    async def _wait_for_fill(self, order_id: str, timeout: int) -> float:
        if DRY_RUN or self._client is None:
            return 0.0
        deadline = _utcnow() + timedelta(seconds=timeout)
        loop = asyncio.get_running_loop()
        while _utcnow() < deadline:
            try:
                status = await loop.run_in_executor(
                    None, lambda: self._client.get_order(order_id)
                )
            except Exception as e:
                log.warning("order_status_check_failed", order_id=order_id, error=str(e))
                await asyncio.sleep(5)
                continue
            if isinstance(status, dict):
                state = (status.get("status") or "").lower()
                size_filled = float(status.get("size_matched") or status.get("filled") or 0.0)
                if state in {"filled", "matched", "fully_filled"}:
                    return size_filled
                if state in {"cancelled", "canceled", "expired", "rejected"}:
                    return size_filled
            await asyncio.sleep(5)
        # Timeout: report partial fills if we can read them
        try:
            status = await loop.run_in_executor(
                None, lambda: self._client.get_order(order_id)
            )
            if isinstance(status, dict):
                return float(status.get("size_matched") or status.get("filled") or 0.0)
        except Exception:
            pass
        return 0.0

    async def cancel_order(self, order_id: str) -> bool:
        if DRY_RUN or self._client is None:
            return True
        loop = asyncio.get_running_loop()
        for attempt in range(2):
            try:
                await loop.run_in_executor(None, lambda: self._client.cancel(order_id))
                log.info("order_cancelled", order_id=order_id, attempt=attempt + 1)
                return True
            except Exception as e:
                log.warning("cancel_failed", order_id=order_id, error=str(e), attempt=attempt + 1)
                await asyncio.sleep(2)
        return False

    async def cancel_all_open_orders(self) -> None:
        if DRY_RUN or self._client is None:
            return
        loop = asyncio.get_running_loop()
        try:
            await loop.run_in_executor(None, lambda: self._client.cancel_all())
            log.info("all_orders_cancelled")
        except Exception as e:
            log.warning("cancel_all_failed", error=str(e))

    # --- exits ------------------------------------------------------------

    async def sell_position(
        self, position: OpenPosition, reason: str
    ) -> TradeResult | None:
        async with self._lock:
            return await self._sell_position_inner(position, reason)

    async def _sell_position_inner(
        self, position: OpenPosition, reason: str
    ) -> TradeResult | None:
        market = await self.market_scanner.get_market(position.condition_id)
        # Allow a refresh if the market dropped out of our active cache.
        if market is None:
            market = await self.market_scanner.refresh_single_market(position.condition_id)

        if market is None:
            # Use entry price as a worst-case fallback so we never blow up
            mid = position.entry_price
            log.warning(
                "exit_no_market_data",
                position_id=position.position_id,
                fallback_price=mid,
            )
        else:
            mid = market.yes_price if position.direction == "YES" else market.no_price

        # Resolved markets clamp to 0/1
        resolved = mid <= 0.001 or mid >= 0.999
        if resolved:
            mid = 1.0 if mid >= 0.999 else 0.0

        if DRY_RUN:
            exit_price = round(mid, 4)
        else:
            exit_price = await self._market_sell(position, mid)

        return self._build_trade_result(position, exit_price, reason if not resolved else "resolved")

    async def _market_sell(self, position: OpenPosition, mid_hint: float) -> float:
        if self._client is None:
            return position.entry_price
        try:
            from py_clob_client.clob_types import OrderArgs, OrderType  # type: ignore
        except ImportError:  # pragma: no cover
            try:
                from py_clob_client_v2.clob_types import OrderArgs, OrderType  # type: ignore
            except ImportError:
                return position.entry_price

        # Aggressive sell: price 2 cents below mid (clamped) using FOK.
        sell_price = round(max(mid_hint - 0.02, 0.01), 4)
        args = OrderArgs(
            token_id=position.token_id,
            price=sell_price,
            size=position.shares,
            side="SELL",
        )
        loop = asyncio.get_running_loop()
        try:
            resp = await loop.run_in_executor(
                None,
                lambda: self._client.create_and_post_order(args, order_type=OrderType.FOK),
            )
            order_id = (resp or {}).get("orderID") or (resp or {}).get("orderId")
            if not order_id:
                return mid_hint
            filled_size = await self._wait_for_fill(str(order_id), 30)
            if filled_size > 0:
                return sell_price
        except Exception as e:
            log.error("market_sell_failed", error=str(e), exc_info=True)
        return mid_hint

    @staticmethod
    def _build_trade_result(
        position: OpenPosition, exit_price: float, reason: str
    ) -> TradeResult:
        closed_at = _utcnow()
        proceeds = position.shares * exit_price
        cost = position.shares * position.entry_price
        pnl_usdc = round(proceeds - cost, 4)
        pnl_pct = (pnl_usdc / cost) if cost > 0 else 0.0
        hold_hours = max(0.0, (closed_at - position.opened_at).total_seconds() / 3600.0)
        return TradeResult(
            position_id=position.position_id,
            condition_id=position.condition_id,
            direction=position.direction,
            entry_price=round(position.entry_price, 4),
            exit_price=round(exit_price, 4),
            size_usdc=round(position.size_usdc, 2),
            pnl_usdc=round(pnl_usdc, 2),
            pnl_pct=round(pnl_pct, 4),
            hold_hours=round(hold_hours, 2),
            exit_reason=reason,  # type: ignore[arg-type]
            opened_at=position.opened_at,
            closed_at=closed_at,
        )
