"""Market scanner tests — fee table, parsing, filtering, prompt formatting."""
from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta, timezone

# Required env
for k in (
    "ANTHROPIC_API_KEY POLYGON_PRIVATE_KEY POLYMARKET_API_KEY POLYMARKET_API_SECRET "
    "POLYMARKET_API_PASSPHRASE POLYMARKET_FUNDER_ADDRESS NEWSFILTER_API_KEY "
    "NEWSAPI_KEY TELEGRAM_BOT_TOKEN TELEGRAM_CHAT_ID"
).split():
    os.environ.setdefault(k, "test")
os.environ.setdefault("DRY_RUN", "true")

import pytest

from market_scanner import (
    MarketScanner,
    _normalise_category,
    _parse_iso,
    _safe_float,
)
from models import MarketSnapshot
from tests.mock_data import MARKETS


def test_fee_rate_known_categories():
    assert MarketScanner.fee_rate_for("crypto") == 0.018
    assert MarketScanner.fee_rate_for("politics") == 0.010
    assert MarketScanner.fee_rate_for("geopolitics") == 0.0
    assert MarketScanner.fee_rate_for("sports") == 0.0075


def test_fee_rate_unknown_falls_back_to_default():
    assert MarketScanner.fee_rate_for("zzzz") == 0.0125
    assert MarketScanner.fee_rate_for("") == 0.0125


def test_normalise_category_handles_aliases():
    assert _normalise_category("Election") == "politics"
    assert _normalise_category("World") == "geopolitics"
    assert _normalise_category("AI") == "tech"
    assert _normalise_category("Climate") == "weather"
    assert _normalise_category(None) == "other"


def test_safe_float_handles_strings_and_none():
    assert _safe_float("3.14") == 3.14
    assert _safe_float(None) == 0.0
    assert _safe_float("garbage") == 0.0
    assert _safe_float("garbage", default=42.0) == 42.0


def test_parse_iso_handles_various_formats():
    assert _parse_iso("2026-04-30T12:00:00Z") is not None
    assert _parse_iso("2026-04-30T12:00:00+00:00") is not None
    assert _parse_iso("garbage") is None
    assert _parse_iso(None) is None


def _raw_market(**overrides):
    base = {
        "conditionId": "0xabc",
        "question": "Will X happen?",
        "clobTokenIds": '["YES_TOKEN_ID", "NO_TOKEN_ID"]',
        "outcomePrices": '["0.55", "0.45"]',
        "volume24hr": 250000,
        "endDate": (datetime.now(timezone.utc) + timedelta(days=30)).isoformat(),
        "category": "politics",
        "description": "desc",
        "bestBid": 0.54,
        "bestAsk": 0.56,
    }
    base.update(overrides)
    return base


def test_raw_to_snapshot_extracts_fields():
    sc = MarketScanner()
    ms = sc._raw_to_snapshot(_raw_market())
    assert ms is not None
    assert ms.condition_id == "0xabc"
    assert ms.token_id_yes == "YES_TOKEN_ID"
    assert ms.token_id_no == "NO_TOKEN_ID"
    assert ms.yes_price == 0.55
    assert ms.no_price == 0.45
    assert ms.volume_24h == 250000.0
    assert ms.spread == 0.02  # 0.56 - 0.54
    assert ms.category == "politics"
    assert ms.fee_rate == 0.010


def test_raw_to_snapshot_returns_none_on_missing_fields():
    sc = MarketScanner()
    assert sc._raw_to_snapshot({"question": "no condition_id"}) is None
    assert sc._raw_to_snapshot({"conditionId": "x"}) is None  # no question
    assert sc._raw_to_snapshot(_raw_market(clobTokenIds="[]")) is None  # no tokens


def test_raw_to_snapshot_handles_list_tokens_natively():
    sc = MarketScanner()
    ms = sc._raw_to_snapshot(_raw_market(clobTokenIds=["A", "B"]))
    assert ms is not None
    assert ms.token_id_yes == "A"


def test_raw_to_snapshot_falls_back_to_last_trade_price():
    sc = MarketScanner()
    raw = _raw_market(outcomePrices=None)
    raw["lastTradePrice"] = 0.42
    raw["bestBid"] = 0
    raw["bestAsk"] = 0
    ms = sc._raw_to_snapshot(raw)
    assert ms is not None
    assert ms.yes_price == 0.42
    assert abs(ms.no_price - 0.58) < 1e-6


def test_is_paused_detects_pause_flags():
    assert MarketScanner._is_paused({"paused": True}) is True
    assert MarketScanner._is_paused({"isPaused": True}) is True
    assert MarketScanner._is_paused({"disputed": True}) is True
    assert MarketScanner._is_paused({"isDisputed": True}) is True
    assert MarketScanner._is_paused({"archived": True}) is True
    assert MarketScanner._is_paused({"paused": False}) is False
    assert MarketScanner._is_paused({}) is False


def test_extract_token_ids_handles_outcomes_list():
    raw = {"outcomes": [{"tokenId": "Y"}, {"tokenId": "N"}]}
    yes, no = MarketScanner._extract_token_ids(raw)
    assert yes == "Y" and no == "N"


def test_extract_token_ids_handles_invalid_json():
    raw = {"clobTokenIds": "not-valid-json"}
    yes, no = MarketScanner._extract_token_ids(raw)
    assert yes is None and no is None


@pytest.mark.asyncio
async def test_get_market_returns_cached_value():
    sc = MarketScanner()
    sc._cache = {m.condition_id: m for m in MARKETS}
    got = await sc.get_market("0xPOL1")
    assert got is not None
    assert got.condition_id == "0xPOL1"
    assert await sc.get_market("0xMISSING") is None


@pytest.mark.asyncio
async def test_get_markets_for_prompt_sorted_by_volume_desc():
    sc = MarketScanner()
    sc._cache = {m.condition_id: m for m in MARKETS}
    out = await sc.get_markets_for_prompt()
    lines = out.split("\n")
    # Header + at least one market line
    assert lines[0].startswith("CONDITION_ID")
    # Subsequent lines should be sorted descending by volume; the first
    # market in the body should be the highest-volume one (0xECO2 @ $800k)
    assert "0xECO2" in lines[1]


@pytest.mark.asyncio
async def test_get_markets_for_prompt_handles_empty_cache():
    sc = MarketScanner()
    out = await sc.get_markets_for_prompt()
    assert "no markets" in out.lower()


@pytest.mark.asyncio
async def test_get_markets_for_prompt_truncates_long_questions():
    sc = MarketScanner()
    long_market = MarketSnapshot(
        condition_id="0xLONG",
        token_id_yes="Y",
        token_id_no="N",
        question="Q" * 200,
        category="politics",
        yes_price=0.5,
        no_price=0.5,
        volume_24h=50000,
        spread=0.02,
        resolution_date=datetime.now(timezone.utc) + timedelta(days=30),
        fee_rate=0.01,
        last_updated=datetime.now(timezone.utc),
    )
    sc._cache = {long_market.condition_id: long_market}
    out = await sc.get_markets_for_prompt()
    assert "..." in out
    # No line should exceed a reasonable length (header + truncated question)
    assert all(len(line) < 250 for line in out.split("\n"))


@pytest.mark.asyncio
async def test_get_markets_for_prompt_respects_max_markets():
    sc = MarketScanner()
    cache = {}
    for i in range(10):
        m = MARKETS[0].model_copy(update={"condition_id": f"0xM{i}"})
        cache[m.condition_id] = m
    sc._cache = cache
    out = await sc.get_markets_for_prompt(max_markets=3)
    # Header + 3 markets = 4 lines
    assert len(out.split("\n")) == 4
