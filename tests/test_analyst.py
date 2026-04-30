"""Analyst (Claude) tests using a mocked Anthropic client."""
from __future__ import annotations

import json
import os
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

# Ensure required env is in place before importing config-bound modules.
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

import pytest

from analyst import ClaudeAnalyst, _extract_json_object
from tests.mock_data import NEWS_POLITICAL


def _mk_response(text: str):
    block = SimpleNamespace(text=text, type="text")
    usage = SimpleNamespace(input_tokens=42, output_tokens=84)
    return SimpleNamespace(content=[block], usage=usage)


_VALID_JSON = json.dumps(
    {
        "decision": "TRADE",
        "reasoning": "ok",
        "news_staleness_ok": True,
        "news_quality": "primary_confirmed",
        "news_category": "political",
        "affected_markets": [
            {
                "condition_id": "0xPOL1",
                "question": "Will X happen?",
                "direction": "YES",
                "current_price": 0.50,
                "prior_probability": 0.50,
                "likelihood_ratio": "favours YES",
                "fair_value_estimate": 0.80,
                "edge_after_fees": 0.25,
                "confidence": 0.85,
                "reasoning": "Causal chain holds.",
                "recommended_position_usdc": 150,
                "exit_target_price": 0.78,
                "max_hold_hours": 24,
                "resolution_dependency": "official source",
            }
        ],
        "red_flags": [],
        "markets_considered_but_skipped": [],
    }
)


@pytest.mark.asyncio
async def test_valid_json_parsed_into_decision():
    analyst = ClaudeAnalyst()
    fake_create = AsyncMock(return_value=_mk_response(_VALID_JSON))
    with patch.object(analyst.client.messages, "create", fake_create):
        decision = await analyst.analyze(NEWS_POLITICAL, "(no markets)", {})
    assert decision.decision == "TRADE"
    assert decision.affected_markets[0].confidence == 0.85
    assert decision.raw_response.startswith("{")


@pytest.mark.asyncio
async def test_markdown_code_fences_are_cleaned():
    analyst = ClaudeAnalyst()
    fenced = "```json\n" + _VALID_JSON + "\n```"
    fake_create = AsyncMock(return_value=_mk_response(fenced))
    with patch.object(analyst.client.messages, "create", fake_create):
        decision = await analyst.analyze(NEWS_POLITICAL, "(no markets)", {})
    assert decision.decision == "TRADE"


@pytest.mark.asyncio
async def test_invalid_json_raises_runtime_error():
    analyst = ClaudeAnalyst()
    fake_create = AsyncMock(return_value=_mk_response("not actually json"))
    with patch.object(analyst.client.messages, "create", fake_create):
        with pytest.raises(RuntimeError):
            await analyst.analyze(NEWS_POLITICAL, "(no markets)", {})


@pytest.mark.asyncio
async def test_pydantic_validation_catches_missing_fields():
    bad = json.dumps({"decision": "TRADE"})
    analyst = ClaudeAnalyst()
    fake_create = AsyncMock(return_value=_mk_response(bad))
    with patch.object(analyst.client.messages, "create", fake_create):
        with pytest.raises(Exception):
            await analyst.analyze(NEWS_POLITICAL, "(no markets)", {})


@pytest.mark.asyncio
async def test_low_confidence_decision_is_visible_in_output():
    body = json.loads(_VALID_JSON)
    body["affected_markets"][0]["confidence"] = 0.55
    body["decision"] = "MONITOR"
    text = json.dumps(body)
    analyst = ClaudeAnalyst()
    fake_create = AsyncMock(return_value=_mk_response(text))
    with patch.object(analyst.client.messages, "create", fake_create):
        decision = await analyst.analyze(NEWS_POLITICAL, "(no markets)", {})
    assert decision.decision == "MONITOR"
    assert decision.affected_markets[0].confidence < 0.72


def test_extract_json_object_handles_prefix_text():
    raw = 'Here is your JSON:\n{"a": 1}'
    assert _extract_json_object(raw) == '{"a": 1}'


def test_extract_json_object_handles_code_fence():
    raw = "```json\n{\"a\": 1}\n```"
    assert _extract_json_object(raw).strip() == '{"a": 1}'
