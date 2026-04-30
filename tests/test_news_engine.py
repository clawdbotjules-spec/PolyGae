"""News engine tests — parsers, dedup, staleness, queue emission."""
from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta, timezone

# Required env before importing config-bound modules
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
os.environ.setdefault("NEWS_STALENESS_MAX_MINUTES", "4")

import pytest

from news_engine import (
    NewsEngine,
    _SeenCache,
    _make_id,
    _parse_dt,
    _truncate_words,
)


def _utcnow():
    return datetime.now(timezone.utc)


def test_make_id_is_stable_and_distinguishes_inputs():
    a = _make_id("https://x/1", "h1")
    b = _make_id("https://x/1", "h1")
    c = _make_id("https://x/1", "h2")
    d = _make_id("https://x/2", "h1")
    assert a == b
    assert a != c
    assert a != d
    assert len(a) == 64  # sha256 hex


def test_truncate_words_preserves_short_text():
    assert _truncate_words("a b c", 10) == "a b c"
    assert _truncate_words("", 5) == ""


def test_truncate_words_truncates_long_text():
    txt = " ".join(str(i) for i in range(1000))
    out = _truncate_words(txt, 10)
    assert out.endswith("...")
    assert len(out.split()) == 11  # 10 words + "..."


def test_parse_dt_handles_iso_z():
    dt = _parse_dt("2026-01-02T03:04:05Z")
    assert dt is not None
    assert dt.tzinfo is not None
    assert dt.year == 2026


def test_parse_dt_handles_iso_offset():
    dt = _parse_dt("2026-01-02T03:04:05+00:00")
    assert dt is not None
    assert dt.year == 2026


def test_parse_dt_handles_rfc2822():
    dt = _parse_dt("Wed, 02 Jan 2026 03:04:05 GMT")
    assert dt is not None
    assert dt.year == 2026


def test_parse_dt_handles_naive_iso():
    dt = _parse_dt("2026-01-02T03:04:05")
    assert dt is not None
    assert dt.tzinfo is not None  # auto-localised to UTC


def test_parse_dt_returns_none_for_garbage():
    assert _parse_dt("") is None
    assert _parse_dt(None) is None
    assert _parse_dt("nonsense") is None


def test_seen_cache_basic_add_and_check():
    cache = _SeenCache()
    cache.__post_init__()
    cache.add("a")
    assert cache.has("a")
    assert not cache.has("b")


def test_seen_cache_prune_removes_old_entries():
    cache = _SeenCache(horizon_hours=12)
    cache.__post_init__()
    cache.add("recent")
    # Inject an old entry directly
    cache._items["old"] = _utcnow() - timedelta(hours=24)  # type: ignore[index]
    cache.prune()
    assert cache.has("recent")
    assert not cache.has("old")


def _make_engine() -> NewsEngine:
    q: asyncio.Queue = asyncio.Queue()
    return NewsEngine(queue=q, trade_logger=None)


def test_parse_newsfilter_msg_envelope():
    eng = _make_engine()
    msg = {
        "data": {
            "title": "Test Headline",
            "url": "https://x/y",
            "publishedAt": "2026-04-30T12:00:00Z",
            "description": "Body text",
            "source": {"name": "Reuters"},
        }
    }
    art = eng._parse_newsfilter_msg(msg)
    assert art is not None
    assert art.headline == "Test Headline"
    assert art.url == "https://x/y"
    assert art.source == "Reuters"


def test_parse_newsfilter_msg_bare_payload():
    eng = _make_engine()
    msg = {
        "title": "T",
        "url": "https://u",
        "publishedAt": "2026-04-30T12:00:00Z",
        "summary": "s",
        "source": "ESPN",
    }
    art = eng._parse_newsfilter_msg(msg)
    assert art is not None
    assert art.source == "ESPN"


def test_parse_newsfilter_rejects_missing_url_or_title():
    eng = _make_engine()
    assert eng._parse_newsfilter_msg({"title": "x"}) is None
    assert eng._parse_newsfilter_msg({"url": "https://x"}) is None
    assert eng._parse_newsfilter_msg({}) is None


def test_parse_newsapi_article_basic():
    eng = _make_engine()
    art = eng._parse_newsapi_article(
        {
            "title": "Breaking",
            "url": "https://newsapi/a",
            "publishedAt": "2026-04-30T11:30:00Z",
            "description": "d",
            "content": "c",
            "source": {"name": "AP"},
        }
    )
    assert art is not None
    assert "d" in art.body and "c" in art.body
    assert art.source == "AP"


def test_parse_newsapi_article_rejects_invalid():
    eng = _make_engine()
    assert eng._parse_newsapi_article({"title": "", "url": "x"}) is None
    assert eng._parse_newsapi_article({"title": "x"}) is None


@pytest.mark.asyncio
async def test_emit_drops_stale_articles():
    """Articles older than NEWS_STALENESS_MAX_MINUTES should not reach queue."""
    from models import NewsArticle

    eng = _make_engine()
    stale = NewsArticle(
        id=_make_id("https://x/old", "old headline"),
        headline="old headline",
        body="old body",
        source="Test",
        published_at=_utcnow() - timedelta(minutes=30),
        url="https://x/old",
        categories=[],
    )
    await eng._emit(stale)
    assert eng.queue.empty(), "stale article should not be queued"


@pytest.mark.asyncio
async def test_emit_pushes_fresh_articles():
    from models import NewsArticle

    eng = _make_engine()
    fresh = NewsArticle(
        id=_make_id("https://x/fresh", "fresh"),
        headline="fresh",
        body="b",
        source="Test",
        published_at=_utcnow() - timedelta(seconds=30),
        url="https://x/fresh",
        categories=[],
    )
    await eng._emit(fresh)
    assert eng.queue.qsize() == 1
    art = await eng.queue.get()
    assert art.headline == "fresh"
    assert art.minutes_ago < 1.0


@pytest.mark.asyncio
async def test_emit_dedupes_repeat_articles():
    from models import NewsArticle

    eng = _make_engine()
    art = NewsArticle(
        id=_make_id("https://x/dup", "dup"),
        headline="dup",
        body="b",
        source="T",
        published_at=_utcnow(),
        url="https://x/dup",
        categories=[],
    )
    await eng._emit(art)
    await eng._emit(art)
    await eng._emit(art)
    assert eng.queue.qsize() == 1


# --- Guardian -----------------------------------------------------------

def test_parse_guardian_entry_basic():
    eng = _make_engine()
    entry = {
        "webTitle": "Senate confirms Fed Chair",
        "webUrl": "https://www.theguardian.com/world/article",
        "webPublicationDate": "2026-04-30T12:00:00Z",
        "sectionId": "world",
        "fields": {
            "standfirst": "stand",
            "trailText": "trail",
            "bodyText": "Long body text here...",
        },
    }
    art = eng._parse_guardian_entry(entry)
    assert art is not None
    assert art.headline == "Senate confirms Fed Chair"
    assert art.source == "The Guardian"
    assert "stand" in art.body and "trail" in art.body
    assert art.categories == ["world"]


def test_parse_guardian_entry_rejects_missing_fields():
    eng = _make_engine()
    assert eng._parse_guardian_entry({"webTitle": ""}) is None
    assert eng._parse_guardian_entry({"webUrl": "https://x"}) is None
    assert eng._parse_guardian_entry({}) is None


# --- GDELT --------------------------------------------------------------

def test_parse_gdelt_entry_basic():
    eng = _make_engine()
    entry = {
        "title": "Senate confirms Fed Chair",
        "url": "https://example.com/article",
        "seendate": "20260430T120000Z",
        "domain": "example.com",
    }
    art = eng._parse_gdelt_entry(entry)
    assert art is not None
    assert art.headline == "Senate confirms Fed Chair"
    assert art.source == "GDELT/example.com"
    assert art.published_at.year == 2026
    assert art.published_at.month == 4
    assert art.published_at.day == 30
    assert art.body == ""  # GDELT doesn't include bodies


def test_parse_gdelt_entry_handles_bad_seendate():
    eng = _make_engine()
    entry = {"title": "h", "url": "https://x", "seendate": "garbage"}
    art = eng._parse_gdelt_entry(entry)
    assert art is not None
    # Falls back to "now"; just verify we got a recent timestamp.
    assert (_utcnow() - art.published_at).total_seconds() < 5


def test_parse_gdelt_entry_rejects_missing():
    eng = _make_engine()
    assert eng._parse_gdelt_entry({}) is None
    assert eng._parse_gdelt_entry({"title": "x"}) is None


# --- Marketaux ----------------------------------------------------------

def test_parse_marketaux_entry_basic():
    eng = _make_engine()
    entry = {
        "title": "Apple beats earnings",
        "url": "https://example.com/aapl",
        "published_at": "2026-04-30T12:00:00.000000Z",
        "description": "desc",
        "snippet": "snip",
        "source": "Reuters",
        "entities": [
            {"symbol": "AAPL", "name": "Apple"},
            {"symbol": "MSFT", "name": "Microsoft"},
        ],
    }
    art = eng._parse_marketaux_entry(entry)
    assert art is not None
    assert art.headline == "Apple beats earnings"
    assert art.source == "Reuters"
    assert "AAPL" in art.categories and "MSFT" in art.categories
    assert "desc" in art.body and "snip" in art.body


def test_parse_marketaux_entry_rejects_missing():
    eng = _make_engine()
    assert eng._parse_marketaux_entry({"title": "h"}) is None
    assert eng._parse_marketaux_entry({"url": "https://x"}) is None


# --- Reddit -------------------------------------------------------------

def test_parse_reddit_entry_link_post():
    eng = _make_engine()
    entry = {
        "title": "Big news headline",
        "url": "https://reuters.com/world/article",
        "selftext": "",
        "created_utc": 1719777600.0,
        "is_self": False,
        "over_18": False,
    }
    art = eng._parse_reddit_entry(entry, "worldnews")
    assert art is not None
    assert art.headline == "Big news headline"
    assert art.source == "Reddit/r/worldnews"
    assert art.url.startswith("https://reuters.com")
    assert art.categories == ["worldnews"]


def test_parse_reddit_entry_skips_self_post():
    eng = _make_engine()
    entry = {
        "title": "ask r/worldnews question",
        "url": "https://reddit.com/r/worldnews/comments/x",
        "is_self": True,
        "created_utc": 1719777600.0,
    }
    assert eng._parse_reddit_entry(entry, "worldnews") is None


def test_parse_reddit_entry_skips_nsfw():
    eng = _make_engine()
    entry = {
        "title": "x",
        "url": "https://example.com/x",
        "is_self": False,
        "over_18": True,
        "created_utc": 1719777600.0,
    }
    assert eng._parse_reddit_entry(entry, "worldnews") is None


def test_parse_reddit_entry_skips_reddit_internal_urls():
    eng = _make_engine()
    entry = {
        "title": "x",
        "url": "https://www.reddit.com/r/worldnews/comments/abc/x",
        "is_self": False,
        "over_18": False,
        "created_utc": 1719777600.0,
    }
    assert eng._parse_reddit_entry(entry, "worldnews") is None


def test_parse_reddit_entry_handles_invalid_timestamp():
    eng = _make_engine()
    entry = {
        "title": "x",
        "url": "https://example.com/x",
        "is_self": False,
        "over_18": False,
        "created_utc": "not_a_number",
    }
    art = eng._parse_reddit_entry(entry, "worldnews")
    assert art is not None  # falls back to now
    assert (_utcnow() - art.published_at).total_seconds() < 5
