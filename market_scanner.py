"""Polymarket Gamma API scanner.

Maintains a thread-safe cache of all currently tradeable markets that meet
liquidity, freshness, and spread criteria. Refreshes every
MARKET_SCANNER_REFRESH_SECONDS. Provides a formatted snapshot string for
inclusion in Claude prompts.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone, timedelta
from typing import Any

import aiohttp
import structlog

from config import (
    MARKET_SCANNER_REFRESH_SECONDS,
    MAX_SPREAD_CENTS,
    MIN_MARKET_VOLUME_24H,
    POLYMARKET_GAMMA_HOST,
)
from models import MarketSnapshot


log = structlog.get_logger("market_scanner")


_FEE_RATES: dict[str, float] = {
    "crypto": 0.018,
    "sports": 0.0075,
    "finance": 0.010,
    "politics": 0.010,
    "economics": 0.015,
    "culture": 0.0125,
    "weather": 0.0125,
    "tech": 0.010,
    "geopolitics": 0.0,
    "other": 0.0125,
}
_DEFAULT_FEE_RATE = 0.0125


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _parse_iso(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    s = str(value).strip()
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _normalise_category(raw: Any) -> str:
    if not raw:
        return "other"
    s = str(raw).strip().lower()
    if not s:
        return "other"
    aliases = {
        "election": "politics",
        "elections": "politics",
        "us politics": "politics",
        "world": "geopolitics",
        "geo-politics": "geopolitics",
        "macro": "economics",
        "stocks": "finance",
        "ai": "tech",
        "technology": "tech",
        "esports": "sports",
        "weather & climate": "weather",
        "climate": "weather",
        "pop culture": "culture",
    }
    s = aliases.get(s, s)
    return s if s in _FEE_RATES else "other"


class MarketScanner:
    def __init__(self) -> None:
        self._cache: dict[str, MarketSnapshot] = {}
        self._lock = asyncio.Lock()
        self._session: aiohttp.ClientSession | None = None
        self._stop = asyncio.Event()
        self._task: asyncio.Task[Any] | None = None

    @staticmethod
    def fee_rate_for(category: str) -> float:
        return _FEE_RATES.get(_normalise_category(category), _DEFAULT_FEE_RATE)

    async def start(self) -> None:
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30),
            headers={"User-Agent": "NewsTraderBot/1.0"},
        )
        # initial scan
        try:
            await self._scan_once()
        except Exception as e:
            log.error("initial_market_scan_failed", error=str(e), exc_info=True)
        self._task = asyncio.create_task(self._run_loop(), name="market_scanner_loop")
        try:
            await self._task
        except asyncio.CancelledError:
            raise

    async def stop(self) -> None:
        self._stop.set()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
        if self._session is not None:
            await self._session.close()
            self._session = None

    async def _run_loop(self) -> None:
        while not self._stop.is_set():
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=MARKET_SCANNER_REFRESH_SECONDS)
            except asyncio.TimeoutError:
                pass
            if self._stop.is_set():
                break
            try:
                await self._scan_once()
            except Exception as e:
                log.error("market_scan_failed", error=str(e), exc_info=True)

    async def _scan_once(self) -> None:
        markets = await self._fetch_all_markets()
        kept: dict[str, MarketSnapshot] = {}
        filtered_volume = filtered_resolution = filtered_spread = filtered_paused = 0
        now = _utcnow()
        for raw in markets:
            try:
                ms = self._raw_to_snapshot(raw)
            except Exception as e:
                log.debug("market_parse_failed", error=str(e))
                continue
            if ms is None:
                continue
            if self._is_paused(raw):
                filtered_paused += 1
                continue
            if ms.volume_24h < MIN_MARKET_VOLUME_24H:
                filtered_volume += 1
                continue
            if ms.resolution_date <= now + timedelta(hours=24):
                filtered_resolution += 1
                continue
            if ms.spread > MAX_SPREAD_CENTS:
                filtered_spread += 1
                continue
            kept[ms.condition_id] = ms

        async with self._lock:
            self._cache = kept

        log.info(
            "market_scan_complete",
            total_fetched=len(markets),
            kept=len(kept),
            filtered_volume=filtered_volume,
            filtered_resolution=filtered_resolution,
            filtered_spread=filtered_spread,
            filtered_paused=filtered_paused,
        )

    async def _fetch_all_markets(self) -> list[dict[str, Any]]:
        assert self._session is not None
        out: list[dict[str, Any]] = []
        offset = 0
        page_size = 500
        url = f"{POLYMARKET_GAMMA_HOST}/markets"
        while True:
            params = {
                "active": "true",
                "closed": "false",
                "limit": page_size,
                "offset": offset,
            }
            async with self._session.get(url, params=params) as resp:
                if resp.status != 200:
                    txt = await resp.text()
                    log.warning("gamma_non_200", status=resp.status, body=txt[:200])
                    break
                data = await resp.json()
            if not isinstance(data, list):
                # Some gamma versions wrap in {"data": [...]}
                data = data.get("data", []) if isinstance(data, dict) else []
            if not data:
                break
            out.extend(data)
            if len(data) < page_size:
                break
            offset += page_size
            if offset >= 5000:  # hard safety cap
                break
        return out

    @staticmethod
    def _is_paused(raw: dict[str, Any]) -> bool:
        for key in ("paused", "isPaused", "disputed", "isDisputed", "archived"):
            if bool(raw.get(key)):
                return True
        return False

    @staticmethod
    def _extract_token_ids(raw: dict[str, Any]) -> tuple[str | None, str | None]:
        # The Gamma API exposes clobTokenIds as a JSON-encoded list ordered
        # [YES_token_id, NO_token_id] in standard binary markets.
        tokens = raw.get("clobTokenIds") or raw.get("clob_token_ids")
        if isinstance(tokens, str):
            import json as _json
            try:
                tokens = _json.loads(tokens)
            except Exception:
                tokens = None
        if isinstance(tokens, list) and len(tokens) >= 2:
            return str(tokens[0]), str(tokens[1])
        # Older shape: outcomes list with token ids
        outcomes = raw.get("outcomes")
        if isinstance(outcomes, list) and len(outcomes) >= 2:
            yes_tok = outcomes[0].get("tokenId") if isinstance(outcomes[0], dict) else None
            no_tok = outcomes[1].get("tokenId") if isinstance(outcomes[1], dict) else None
            if yes_tok and no_tok:
                return str(yes_tok), str(no_tok)
        return None, None

    @staticmethod
    def _extract_prices(raw: dict[str, Any]) -> tuple[float, float, float]:
        """Return (yes_price, no_price, spread)."""
        prices = raw.get("outcomePrices") or raw.get("outcome_prices")
        if isinstance(prices, str):
            import json as _json
            try:
                prices = _json.loads(prices)
            except Exception:
                prices = None
        yes_price = no_price = 0.0
        if isinstance(prices, list) and len(prices) >= 2:
            yes_price = _safe_float(prices[0])
            no_price = _safe_float(prices[1])
        else:
            yes_price = _safe_float(raw.get("lastTradePrice"))
            no_price = max(0.0, 1.0 - yes_price) if yes_price else 0.0

        best_bid = _safe_float(raw.get("bestBid"))
        best_ask = _safe_float(raw.get("bestAsk"))
        if best_ask > 0 and best_bid > 0 and best_ask > best_bid:
            spread = round(best_ask - best_bid, 4)
        else:
            # fall back to deviation-from-1 mismatch
            spread = round(abs(1.0 - (yes_price + no_price)), 4)
        return yes_price, no_price, spread

    def _raw_to_snapshot(self, raw: dict[str, Any]) -> MarketSnapshot | None:
        condition_id = raw.get("conditionId") or raw.get("condition_id")
        question = (raw.get("question") or "").strip()
        if not condition_id or not question:
            return None
        token_yes, token_no = self._extract_token_ids(raw)
        if not token_yes or not token_no:
            return None

        yes_price, no_price, spread = self._extract_prices(raw)
        volume_24h = _safe_float(
            raw.get("volume24hr") or raw.get("volumeNum") or raw.get("volume_24h") or raw.get("volume")
        )
        resolution_date = (
            _parse_iso(raw.get("endDate"))
            or _parse_iso(raw.get("end_date_iso"))
            or _parse_iso(raw.get("gameStartTime"))
            or (_utcnow() + timedelta(days=365))
        )
        category = _normalise_category(
            raw.get("category") or raw.get("subCategory") or raw.get("topic")
        )
        description = (raw.get("description") or "").strip()

        return MarketSnapshot(
            condition_id=str(condition_id),
            token_id_yes=token_yes,
            token_id_no=token_no,
            question=question,
            description=description,
            category=category,
            yes_price=round(yes_price, 4),
            no_price=round(no_price, 4),
            volume_24h=round(volume_24h, 2),
            spread=spread,
            resolution_date=resolution_date,
            fee_rate=self.fee_rate_for(category),
            last_updated=_utcnow(),
        )

    # ---- Read API ---------------------------------------------------------

    async def get_market(self, condition_id: str) -> MarketSnapshot | None:
        async with self._lock:
            return self._cache.get(condition_id)

    async def list_markets(self) -> list[MarketSnapshot]:
        async with self._lock:
            return list(self._cache.values())

    async def refresh_single_market(self, condition_id: str) -> MarketSnapshot | None:
        assert self._session is not None
        url = f"{POLYMARKET_GAMMA_HOST}/markets"
        params = {"condition_ids": condition_id}
        try:
            async with self._session.get(url, params=params) as resp:
                if resp.status != 200:
                    return await self.get_market(condition_id)
                data = await resp.json()
        except Exception as e:
            log.warning("refresh_single_failed", condition_id=condition_id, error=str(e))
            return await self.get_market(condition_id)

        if isinstance(data, dict):
            data = data.get("data", [data])
        if not data:
            return await self.get_market(condition_id)
        ms = self._raw_to_snapshot(data[0])
        if ms is None:
            return await self.get_market(condition_id)
        async with self._lock:
            self._cache[ms.condition_id] = ms
        return ms

    async def get_markets_for_prompt(self, max_markets: int = 200) -> str:
        markets = await self.list_markets()
        markets.sort(key=lambda m: m.volume_24h, reverse=True)
        markets = markets[:max_markets]
        if not markets:
            return "(no markets currently cached — scanner may still be initialising)"

        lines: list[str] = []
        lines.append("CONDITION_ID | YES | $24h VOL | RESOLVES | CAT | QUESTION")
        for m in markets:
            vol = m.volume_24h
            if vol >= 1_000_000:
                vol_s = f"${vol / 1_000_000:.1f}M"
            elif vol >= 1_000:
                vol_s = f"${vol / 1_000:.0f}k"
            else:
                vol_s = f"${vol:.0f}"
            resolve = m.resolution_date.strftime("%Y-%m-%d")
            q = m.question if len(m.question) <= 100 else m.question[:97] + "..."
            lines.append(
                f"{m.condition_id} | {m.yes_price:.3f} | {vol_s} | {resolve} | {m.category} | {q}"
            )
        return "\n".join(lines)
