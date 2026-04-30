"""Tests for RelevanceFilter — keyword extraction + sliding-window rate limit."""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

# Required env (filter doesn't need any of it but config.py is imported transitively)
for k in (
    "ANTHROPIC_API_KEY POLYGON_PRIVATE_KEY POLYMARKET_API_KEY POLYMARKET_API_SECRET "
    "POLYMARKET_API_PASSPHRASE POLYMARKET_FUNDER_ADDRESS NEWSAPI_KEY "
    "TELEGRAM_BOT_TOKEN TELEGRAM_CHAT_ID"
).split():
    os.environ.setdefault(k, "test")
os.environ.setdefault("DRY_RUN", "true")

from relevance_filter import RelevanceFilter, _STOPWORDS, _WORD_RE


# ---- Keyword extraction ----------------------------------------------------

def test_update_keywords_extracts_meaningful_words():
    rf = RelevanceFilter()
    rf.update_keywords([
        "Will Senate confirm Fed Chair by May 2026?",
        "Will Bitcoin hit $250k by December?",
    ])
    kws = rf._keywords
    assert "senate" in kws
    assert "confirm" in kws
    assert "chair" in kws
    assert "bitcoin" in kws
    assert "december" in kws
    # Stopwords must be excluded
    assert "the" not in kws
    assert "by" not in kws
    assert "will" not in kws
    # Short numerals filtered by min_keyword_length
    assert "may" in kws or "may" not in kws  # 3 chars → filtered with default min=4
    # min_keyword_length=4 means 3-letter words are excluded
    assert "may" not in kws
    assert "fed" not in kws


def test_stopword_set_includes_common_words():
    for w in ("the", "a", "is", "are", "and", "or", "will", "would"):
        assert w in _STOPWORDS


def test_word_re_extracts_apostrophes_and_hyphens():
    matches = _WORD_RE.findall("It's a covid-19 pandemic-related question")
    assert "It's" in matches or "It" in matches
    assert "covid-19" in matches
    assert "pandemic-related" in matches


def test_keyword_count_property():
    rf = RelevanceFilter()
    assert rf.keyword_count == 0
    rf.update_keywords(["Will Trump win election?"])
    assert rf.keyword_count > 0


# ---- Relevance check -------------------------------------------------------

def test_is_relevant_matches_when_keyword_present():
    rf = RelevanceFilter()
    rf.update_keywords(["Will Senate confirm Fed Chair by May?"])
    assert rf.is_relevant(
        headline="Senate votes to confirm new Chair",
        body="The U.S. Senate held a confirmation vote today.",
    )


def test_is_relevant_returns_false_when_no_overlap():
    rf = RelevanceFilter()
    rf.update_keywords(["Will Bitcoin hit $250k?"])
    assert (
        rf.is_relevant(
            headline="Local farmers markets blossom in spring",
            body="Tulips and daisies are everywhere.",
        )
        is False
    )


def test_is_relevant_fails_open_when_no_keywords_loaded():
    rf = RelevanceFilter()
    # Empty keyword set → allow all (scanner not yet initialised)
    assert rf.is_relevant("anything") is True


def test_is_relevant_can_be_disabled():
    rf = RelevanceFilter(enable_relevance=False)
    rf.update_keywords(["Will Senate confirm Fed Chair?"])
    # Disabled → always relevant, even when no overlap
    assert rf.is_relevant("Tulips bloom in spring") is True


def test_matched_keywords_returns_overlap():
    rf = RelevanceFilter()
    rf.update_keywords(["Will Senate confirm Fed Chair by May 2026?"])
    matches = rf.matched_keywords(
        "Senate confirmation hearing for Fed Chair concludes"
    )
    assert "senate" in matches
    assert "chair" in matches


def test_matched_keywords_respects_limit():
    rf = RelevanceFilter()
    rf.update_keywords([
        "Will A win? Will B win? Will C win? Will D win? Will E win?"
        " Senate Senate Senate Trump Biden Bitcoin Ethereum Solana Cardano "
        "Polkadot election president governor mayor"
    ])
    matches = rf.matched_keywords(
        "Senate Trump Biden Bitcoin Ethereum Solana Cardano Polkadot election",
        limit=3,
    )
    assert len(matches) <= 3


# ---- Rate limiter ----------------------------------------------------------

def test_rate_limiter_allows_under_limit():
    rf = RelevanceFilter(max_calls_per_minute=3)
    assert rf.can_call_now() is True
    rf.record_call()
    rf.record_call()
    assert rf.can_call_now() is True
    rf.record_call()
    assert rf.can_call_now() is False


def test_rate_limiter_clears_after_60_seconds():
    rf = RelevanceFilter(max_calls_per_minute=2)
    # Inject ancient calls
    rf._call_history.append(datetime.now(timezone.utc) - timedelta(seconds=120))
    rf._call_history.append(datetime.now(timezone.utc) - timedelta(seconds=90))
    # These should both be pruned, leaving room for new calls
    assert rf.can_call_now() is True
    assert rf.calls_in_last_minute == 0


def test_rate_limiter_partial_window():
    rf = RelevanceFilter(max_calls_per_minute=3)
    now = datetime.now(timezone.utc)
    rf._call_history.append(now - timedelta(seconds=70))  # too old
    rf._call_history.append(now - timedelta(seconds=30))  # in window
    rf._call_history.append(now - timedelta(seconds=10))  # in window
    assert rf.calls_in_last_minute == 2
    assert rf.can_call_now() is True
    rf.record_call()
    assert rf.can_call_now() is False  # 3 in window, at limit


def test_rate_limiter_minimum_one_call_per_minute():
    """Even max=0 input gets clamped to 1 to avoid total deadlock."""
    rf = RelevanceFilter(max_calls_per_minute=0)
    assert rf.max_calls_per_minute == 1
