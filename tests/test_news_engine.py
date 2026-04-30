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
