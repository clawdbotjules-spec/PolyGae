"""Multi-source news ingestion engine.

Pushes normalised NewsArticle objects onto an asyncio.Queue. Sources:
  1. Newsfilter.io WebSocket (real-time, optional — only enabled when an
     API key is configured)
  2. NewsAPI.org polling (top-headlines + everything)
  3. The Guardian Open API polling (free, near-real-time)
  4. GDELT 2.0 DOC API polling (free, no key, ~15-min lag)
  5. Marketaux polling (free tier — financial news)
  6. Reddit JSON polling (free, no key — r/worldnews etc.)
  7. RSS feeds (Reuters, AP, BBC, Politico, WaPo)

Deduplication is keyed on SHA-256(url + headline). Articles older than
NEWS_STALENESS_MAX_MINUTES at receipt are dropped.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime
from typing import Any

import aiohttp
import structlog
import websockets

from config import (
    ARTICLE_BODY_MAX_WORDS,
    GDELT_QUERY,
    GUARDIAN_API_KEY,
    MARKETAUX_API_KEY,
    NEWSAPI_KEY,
    NEWSFILTER_API_KEY,
    NEWS_STALENESS_MAX_MINUTES,
    REDDIT_SUBREDDITS,
    REDDIT_USER_AGENT,
)
from logger import TradeLogger
from models import NewsArticle


log = structlog.get_logger("news_engine")


_RSS_FEEDS: tuple[tuple[str, str], ...] = (
    ("Reuters Top News", "https://feeds.reuters.com/reuters/topNews"),
    ("Reuters Business", "https://feeds.reuters.com/reuters/businessNews"),
    ("Reuters Politics", "https://feeds.reuters.com/reuters/politicsNews"),
    ("AP News", "https://apnews.com/rss"),
    ("BBC News", "https://feeds.bbci.co.uk/news/rss.xml"),
    ("Politico", "https://rss.politico.com/politics-news.xml"),
    ("Washington Post World", "https://feeds.washingtonpost.com/rss/world"),
)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _parse_dt(value: Any) -> datetime | None:
    """Best-effort parsing of dates from APIs / RSS into aware UTC datetimes."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, tz=timezone.utc)
    s = str(value).strip()
    if not s:
        return None
    # Try ISO-8601 first
    try:
        if s.endswith("Z"):
            s2 = s[:-1] + "+00:00"
        else:
            s2 = s
        dt = datetime.fromisoformat(s2)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except ValueError:
        pass
    # RFC 2822 (RSS)
    try:
        dt = parsedate_to_datetime(s)
        if dt is None:
            return None
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _truncate_words(text: str, max_words: int) -> str:
    if not text:
        return ""
    words = text.split()
    if len(words) <= max_words:
        return text
    return " ".join(words[:max_words]) + " ..."


def _make_id(url: str, headline: str) -> str:
    raw = f"{url}|{headline}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


@dataclass
class _SeenCache:
    """In-memory dedup cache, periodically pruned."""

    horizon_hours: int = 12
    _items: dict[str, datetime] | None = None

    def __post_init__(self) -> None:
        self._items = {}

    def has(self, key: str) -> bool:
        assert self._items is not None
        return key in self._items

    def add(self, key: str) -> None:
        assert self._items is not None
        self._items[key] = _utcnow()

    def prune(self) -> None:
        assert self._items is not None
        cutoff = _utcnow() - timedelta(hours=self.horizon_hours)
        for k in [k for k, t in self._items.items() if t < cutoff]:
            self._items.pop(k, None)


class NewsEngine:
    """Drives all news sources and emits NewsArticle to the shared queue."""

    def __init__(
        self,
        queue: asyncio.Queue[NewsArticle],
        trade_logger: TradeLogger | None = None,
    ) -> None:
        self.queue = queue
        self.trade_logger = trade_logger
        self._seen = _SeenCache()
        self._session: aiohttp.ClientSession | None = None
        self._tasks: list[asyncio.Task[Any]] = []
        self._stop = asyncio.Event()

    async def start(self) -> None:
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30),
            headers={"User-Agent": "NewsTraderBot/1.0"},
        )
        self._tasks = []
        # Newsfilter WS only if a key is configured (otherwise it just spams
        # SSL handshake errors).
        if NEWSFILTER_API_KEY:
            self._tasks.append(
                asyncio.create_task(self._run_newsfilter_ws(), name="newsfilter_ws")
            )
        self._tasks.append(asyncio.create_task(self._run_newsapi(), name="newsapi"))
        if GUARDIAN_API_KEY:
            self._tasks.append(
                asyncio.create_task(self._run_guardian(), name="guardian")
            )
        self._tasks.append(asyncio.create_task(self._run_gdelt(), name="gdelt"))
        if MARKETAUX_API_KEY:
            self._tasks.append(
                asyncio.create_task(self._run_marketaux(), name="marketaux")
            )
        self._tasks.append(asyncio.create_task(self._run_reddit(), name="reddit"))
        self._tasks.append(asyncio.create_task(self._run_rss(), name="rss"))
        self._tasks.append(asyncio.create_task(self._run_pruner(), name="dedup_pruner"))
        log.info(
            "news_engine_started",
            sources=[t.get_name() for t in self._tasks if not t.get_name().startswith("dedup")],
        )
        try:
            await asyncio.gather(*self._tasks)
        except asyncio.CancelledError:
            raise

    async def stop(self) -> None:
        self._stop.set()
        for t in self._tasks:
            t.cancel()
        for t in self._tasks:
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass
        if self._session is not None:
            await self._session.close()
            self._session = None
        log.info("news_engine_stopped")

    # --- shared emission path ------------------------------------------------

    async def _emit(self, article: NewsArticle) -> None:
        if self._seen.has(article.id):
            return
        if self.trade_logger is not None:
            try:
                if await self.trade_logger.has_seen_article(article.id):
                    self._seen.add(article.id)
                    return
            except Exception as e:  # pragma: no cover - defensive
                log.warning("seen_check_failed", error=str(e))

        # staleness gate
        age_minutes = (_utcnow() - article.published_at).total_seconds() / 60.0
        article.minutes_ago = max(0.0, age_minutes)
        if age_minutes > NEWS_STALENESS_MAX_MINUTES:
            log.info(
                "article_stale_dropped",
                source=article.source,
                age_min=round(age_minutes, 1),
                headline=article.headline[:80],
            )
            self._seen.add(article.id)
            if self.trade_logger is not None:
                try:
                    await self.trade_logger.mark_article_seen(article.id)
                except Exception:
                    pass
            return

        self._seen.add(article.id)
        if self.trade_logger is not None:
            try:
                await self.trade_logger.mark_article_seen(article.id)
            except Exception:
                pass
        await self.queue.put(article)
        log.info(
            "article_emitted",
            source=article.source,
            age_min=round(age_minutes, 1),
            headline=article.headline[:100],
        )

    # --- Newsfilter.io WebSocket --------------------------------------------

    async def _run_newsfilter_ws(self) -> None:
        url = "wss://websocket-v1.newsfilter.io/connection/websocket"
        backoff = 1.0
        while not self._stop.is_set():
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                    auth = {
                        "method": "auth",
                        "params": {"apiKey": NEWSFILTER_API_KEY},
                    }
                    await ws.send(json.dumps(auth))
                    sub = {"method": "subscribe", "params": {"channel": "articles"}}
                    await ws.send(json.dumps(sub))
                    log.info("newsfilter_connected")
                    backoff = 1.0  # reset on successful connect
                    async for raw in ws:
                        if self._stop.is_set():
                            break
                        try:
                            msg = json.loads(raw)
                        except Exception:
                            continue
                        article = self._parse_newsfilter_msg(msg)
                        if article is not None:
                            await self._emit(article)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning("newsfilter_ws_disconnect", error=str(e), backoff_s=backoff)
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=backoff)
                except asyncio.TimeoutError:
                    pass
                backoff = min(backoff * 2, 60.0)

    def _parse_newsfilter_msg(self, msg: dict[str, Any]) -> NewsArticle | None:
        # Accept both the raw envelope and bare article payloads.
        payload = msg
        if isinstance(msg, dict) and "data" in msg and isinstance(msg["data"], dict):
            payload = msg["data"]
        if not isinstance(payload, dict):
            return None
        title = (payload.get("title") or payload.get("headline") or "").strip()
        if not title:
            return None
        url = (payload.get("url") or payload.get("link") or "").strip()
        if not url:
            return None
        published = _parse_dt(
            payload.get("publishedAt")
            or payload.get("published_at")
            or payload.get("publishedDate")
            or payload.get("date")
        ) or _utcnow()
        body = payload.get("description") or payload.get("summary") or payload.get("body") or ""
        source = (
            (payload.get("source") or {}).get("name") if isinstance(payload.get("source"), dict)
            else payload.get("source")
        ) or "Newsfilter"
        categories = payload.get("categories") or payload.get("topics") or []
        if not isinstance(categories, list):
            categories = [str(categories)]
        return NewsArticle(
            id=_make_id(url, title),
            headline=title,
            body=_truncate_words(str(body), ARTICLE_BODY_MAX_WORDS),
            source=str(source),
            published_at=published,
            url=url,
            categories=[str(c) for c in categories],
        )

    # --- NewsAPI.org polling -------------------------------------------------

    async def _run_newsapi(self) -> None:
        if not NEWSAPI_KEY:
            log.warning("newsapi_disabled", reason="no_key")
            return
        endpoints = [
            (
                "https://newsapi.org/v2/top-headlines",
                {
                    "sources": "reuters,associated-press,bbc-news,the-wall-street-journal,bloomberg",
                    "language": "en",
                    "pageSize": 20,
                },
            ),
            (
                "https://newsapi.org/v2/everything",
                {
                    "q": "election OR fed OR GDP OR war OR ceasefire OR trade",
                    "language": "en",
                    "sortBy": "publishedAt",
                    "pageSize": 20,
                },
            ),
        ]
        while not self._stop.is_set():
            for url, params in endpoints:
                if self._stop.is_set():
                    break
                try:
                    await self._poll_newsapi(url, params)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    log.warning("newsapi_poll_failed", url=url, error=str(e))
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=60)
            except asyncio.TimeoutError:
                pass

    async def _poll_newsapi(self, url: str, params: dict[str, Any]) -> None:
        assert self._session is not None
        params = dict(params, apiKey=NEWSAPI_KEY)
        async with self._session.get(url, params=params) as resp:
            if resp.status != 200:
                txt = await resp.text()
                log.warning("newsapi_non_200", status=resp.status, body=txt[:200])
                return
            data = await resp.json()
        for art in data.get("articles", []) or []:
            article = self._parse_newsapi_article(art)
            if article is not None:
                await self._emit(article)

    def _parse_newsapi_article(self, art: dict[str, Any]) -> NewsArticle | None:
        title = (art.get("title") or "").strip()
        url = (art.get("url") or "").strip()
        if not title or not url:
            return None
        published = _parse_dt(art.get("publishedAt")) or _utcnow()
        body_parts = [art.get("description") or "", art.get("content") or ""]
        body = "\n".join([p for p in body_parts if p]).strip()
        source_obj = art.get("source") or {}
        source = (source_obj.get("name") if isinstance(source_obj, dict) else None) or "NewsAPI"
        return NewsArticle(
            id=_make_id(url, title),
            headline=title,
            body=_truncate_words(body, ARTICLE_BODY_MAX_WORDS),
            source=str(source),
            published_at=published,
            url=url,
            categories=[],
        )

    # --- The Guardian Open API ----------------------------------------------

    async def _run_guardian(self) -> None:
        if not GUARDIAN_API_KEY:
            return
        url = "https://content.guardianapis.com/search"
        params_base = {
            "api-key": GUARDIAN_API_KEY,
            "order-by": "newest",
            "page-size": 50,
            "show-fields": "trailText,bodyText,standfirst",
        }
        # Free tier is 12k/day. 30s polling = ~2.9k/day, comfortable margin.
        while not self._stop.is_set():
            try:
                await self._poll_guardian(url, params_base)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning("guardian_poll_failed", error=str(e))
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=30)
            except asyncio.TimeoutError:
                pass

    async def _poll_guardian(self, url: str, params: dict[str, Any]) -> None:
        assert self._session is not None
        async with self._session.get(url, params=params) as resp:
            if resp.status != 200:
                txt = await resp.text()
                log.warning("guardian_non_200", status=resp.status, body=txt[:200])
                return
            data = await resp.json()
        results = (data.get("response") or {}).get("results", []) or []
        for entry in results:
            article = self._parse_guardian_entry(entry)
            if article is not None:
                await self._emit(article)

    def _parse_guardian_entry(self, entry: dict[str, Any]) -> NewsArticle | None:
        title = (entry.get("webTitle") or "").strip()
        url = (entry.get("webUrl") or "").strip()
        if not title or not url:
            return None
        published = _parse_dt(entry.get("webPublicationDate")) or _utcnow()
        fields = entry.get("fields") or {}
        body_parts = [
            fields.get("standfirst") or "",
            fields.get("trailText") or "",
            fields.get("bodyText") or "",
        ]
        body = "\n".join(p for p in body_parts if p).strip()
        section = entry.get("sectionId") or entry.get("sectionName") or ""
        return NewsArticle(
            id=_make_id(url, title),
            headline=title,
            body=_truncate_words(body, ARTICLE_BODY_MAX_WORDS),
            source="The Guardian",
            published_at=published,
            url=url,
            categories=[str(section)] if section else [],
        )

    # --- GDELT 2.0 DOC API --------------------------------------------------

    async def _run_gdelt(self) -> None:
        url = "https://api.gdeltproject.org/api/v2/doc/doc"
        # GDELT 2.0 publishes new articles every 15 min; poll every 5 min so
        # we get fresh data shortly after each publish window.
        while not self._stop.is_set():
            try:
                await self._poll_gdelt(url)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning("gdelt_poll_failed", error=str(e))
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=300)
            except asyncio.TimeoutError:
                pass

    async def _poll_gdelt(self, url: str) -> None:
        assert self._session is not None
        params = {
            "query": GDELT_QUERY + " sourcelang:eng",
            "mode": "ArtList",
            "format": "json",
            "timespan": "30min",
            "maxrecords": 75,
            "sort": "DateDesc",
        }
        async with self._session.get(url, params=params) as resp:
            if resp.status != 200:
                txt = await resp.text()
                log.warning("gdelt_non_200", status=resp.status, body=txt[:200])
                return
            try:
                data = await resp.json(content_type=None)
            except Exception as e:
                log.warning("gdelt_invalid_json", error=str(e))
                return
        for entry in data.get("articles") or []:
            article = self._parse_gdelt_entry(entry)
            if article is not None:
                await self._emit(article)

    def _parse_gdelt_entry(self, entry: dict[str, Any]) -> NewsArticle | None:
        title = (entry.get("title") or "").strip()
        url = (entry.get("url") or "").strip()
        if not title or not url:
            return None
        # GDELT timestamps look like "20260430T200000Z"
        seen = entry.get("seendate") or ""
        published: datetime | None = None
        if isinstance(seen, str) and len(seen) >= 15:
            try:
                published = datetime.strptime(seen[:15], "%Y%m%dT%H%M%S").replace(
                    tzinfo=timezone.utc
                )
            except ValueError:
                published = None
        if published is None:
            published = _utcnow()
        domain = entry.get("domain") or ""
        return NewsArticle(
            id=_make_id(url, title),
            headline=title,
            # GDELT does not include article bodies — headline-only signal.
            body="",
            source=f"GDELT/{domain}" if domain else "GDELT",
            published_at=published,
            url=url,
            categories=[],
        )

    # --- Marketaux ----------------------------------------------------------

    async def _run_marketaux(self) -> None:
        if not MARKETAUX_API_KEY:
            return
        url = "https://api.marketaux.com/v1/news/all"
        # Free tier: 100 calls/day = one every ~14.4 min. Use 16 min to leave
        # a small margin; 90 calls/day.
        while not self._stop.is_set():
            try:
                await self._poll_marketaux(url)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning("marketaux_poll_failed", error=str(e))
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=960)
            except asyncio.TimeoutError:
                pass

    async def _poll_marketaux(self, url: str) -> None:
        assert self._session is not None
        params = {
            "api_token": MARKETAUX_API_KEY,
            "language": "en",
            "limit": 50,
            "filter_entities": "true",
        }
        async with self._session.get(url, params=params) as resp:
            if resp.status != 200:
                txt = await resp.text()
                log.warning("marketaux_non_200", status=resp.status, body=txt[:200])
                return
            data = await resp.json()
        for entry in data.get("data") or []:
            article = self._parse_marketaux_entry(entry)
            if article is not None:
                await self._emit(article)

    def _parse_marketaux_entry(self, entry: dict[str, Any]) -> NewsArticle | None:
        title = (entry.get("title") or "").strip()
        url = (entry.get("url") or "").strip()
        if not title or not url:
            return None
        published = _parse_dt(entry.get("published_at")) or _utcnow()
        body_parts = [entry.get("description") or "", entry.get("snippet") or ""]
        body = "\n".join(p for p in body_parts if p).strip()
        source = entry.get("source") or "Marketaux"
        entities = entry.get("entities") or []
        symbols = [
            e.get("symbol") for e in entities if isinstance(e, dict) and e.get("symbol")
        ]
        return NewsArticle(
            id=_make_id(url, title),
            headline=title,
            body=_truncate_words(body, ARTICLE_BODY_MAX_WORDS),
            source=str(source),
            published_at=published,
            url=url,
            categories=[str(s) for s in symbols][:5],
        )

    # --- Reddit JSON --------------------------------------------------------

    async def _run_reddit(self) -> None:
        subs = [s.strip() for s in REDDIT_SUBREDDITS.split(",") if s.strip()]
        if not subs:
            return
        # Reddit's unauthenticated rate limit is ~10 req/min. Per-sub interval
        # is 60s so total is len(subs) per minute.
        while not self._stop.is_set():
            for sub in subs:
                if self._stop.is_set():
                    break
                try:
                    await self._poll_reddit(sub)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    log.warning("reddit_poll_failed", subreddit=sub, error=str(e))
                # Stagger sub polls so we never hit the 10/min cap
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=12)
                except asyncio.TimeoutError:
                    pass

    async def _poll_reddit(self, subreddit: str) -> None:
        assert self._session is not None
        url = f"https://www.reddit.com/r/{subreddit}/new.json"
        params = {"limit": 25}
        headers = {"User-Agent": REDDIT_USER_AGENT}
        async with self._session.get(url, params=params, headers=headers) as resp:
            if resp.status == 429:
                log.warning("reddit_rate_limited", subreddit=subreddit)
                return
            if resp.status != 200:
                txt = await resp.text()
                log.warning(
                    "reddit_non_200",
                    subreddit=subreddit,
                    status=resp.status,
                    body=txt[:200],
                )
                return
            data = await resp.json()
        children = ((data.get("data") or {}).get("children")) or []
        for child in children:
            entry = child.get("data") if isinstance(child, dict) else None
            if not isinstance(entry, dict):
                continue
            article = self._parse_reddit_entry(entry, subreddit)
            if article is not None:
                await self._emit(article)

    def _parse_reddit_entry(
        self, entry: dict[str, Any], subreddit: str
    ) -> NewsArticle | None:
        title = (entry.get("title") or "").strip()
        if not title:
            return None
        # Prefer the linked article URL over the reddit thread URL when the
        # post is a link submission. is_self=True means a text post — those
        # rarely contain breaking news, so we skip them.
        if entry.get("is_self"):
            return None
        if entry.get("over_18"):
            return None
        url = (entry.get("url") or entry.get("url_overridden_by_dest") or "").strip()
        if not url:
            return None
        # Reject Reddit's own URLs — they're not real news outlets.
        lowered = url.lower()
        if "reddit.com" in lowered or "redd.it" in lowered:
            return None
        created = entry.get("created_utc")
        try:
            published = (
                datetime.fromtimestamp(float(created), tz=timezone.utc)
                if created is not None
                else _utcnow()
            )
        except (TypeError, ValueError):
            published = _utcnow()
        body = entry.get("selftext") or ""
        return NewsArticle(
            id=_make_id(url, title),
            headline=title,
            body=_truncate_words(str(body), ARTICLE_BODY_MAX_WORDS),
            source=f"Reddit/r/{subreddit}",
            published_at=published,
            url=url,
            categories=[subreddit],
        )

    # --- RSS polling ---------------------------------------------------------

    async def _run_rss(self) -> None:
        try:
            import feedparser  # type: ignore
        except ImportError as e:
            log.warning("rss_disabled", reason=f"feedparser unavailable: {e}")
            return
        loop = asyncio.get_running_loop()
        while not self._stop.is_set():
            for source_name, url in _RSS_FEEDS:
                if self._stop.is_set():
                    break
                try:
                    parsed = await loop.run_in_executor(None, feedparser.parse, url)
                    for entry in getattr(parsed, "entries", []) or []:
                        article = self._parse_rss_entry(entry, source_name)
                        if article is not None:
                            await self._emit(article)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    log.warning("rss_poll_failed", url=url, error=str(e))
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=90)
            except asyncio.TimeoutError:
                pass

    def _parse_rss_entry(self, entry: Any, source: str) -> NewsArticle | None:
        title = (getattr(entry, "title", None) or "").strip()
        link = (getattr(entry, "link", None) or "").strip()
        if not title or not link:
            return None
        published = (
            _parse_dt(getattr(entry, "published", None))
            or _parse_dt(getattr(entry, "updated", None))
            or _utcnow()
        )
        body = (
            getattr(entry, "summary", None)
            or getattr(entry, "description", None)
            or ""
        )
        return NewsArticle(
            id=_make_id(link, title),
            headline=title,
            body=_truncate_words(str(body), ARTICLE_BODY_MAX_WORDS),
            source=source,
            published_at=published,
            url=link,
            categories=[t.get("term", "") for t in getattr(entry, "tags", []) or [] if isinstance(t, dict)],
        )

    # --- Maintenance --------------------------------------------------------

    async def _run_pruner(self) -> None:
        while not self._stop.is_set():
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=3600)
            except asyncio.TimeoutError:
                pass
            self._seen.prune()
            if self.trade_logger is not None:
                try:
                    await self.trade_logger.prune_seen_articles(older_than_hours=24)
                except Exception as e:
                    log.warning("dedup_prune_failed", error=str(e))
