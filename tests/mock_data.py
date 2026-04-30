"""Mock data for tests."""
from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone

from models import (
    AffectedMarket,
    ClaudeDecision,
    MarketSnapshot,
    NewsArticle,
    OpenPosition,
    SkippedMarket,
)


def _utc(year=2026, month=4, day=30, hour=12, minute=0):
    return datetime(year, month, day, hour, minute, tzinfo=timezone.utc)


def _id(url: str, headline: str) -> str:
    return hashlib.sha256(f"{url}|{headline}".encode("utf-8")).hexdigest()


# ---- News articles ---------------------------------------------------------

NEWS_POLITICAL = NewsArticle(
    id=_id("https://reuters.com/p1", "Senate confirms new Fed Chair"),
    headline="Senate confirms new Fed Chair",
    body="The U.S. Senate voted 65-32 to confirm the new Federal Reserve chair...",
    source="Reuters",
    published_at=_utc(hour=11, minute=58),
    url="https://reuters.com/p1",
    categories=["politics"],
    minutes_ago=2.0,
)

NEWS_ECONOMIC = NewsArticle(
    id=_id("https://reuters.com/e1", "BLS reports CPI at 2.4%"),
    headline="BLS reports CPI at 2.4%",
    body="The Bureau of Labor Statistics reported a 2.4% YoY CPI...",
    source="Reuters",
    published_at=_utc(hour=11, minute=59),
    url="https://reuters.com/e1",
    categories=["economics"],
    minutes_ago=1.0,
)

NEWS_SPORTS = NewsArticle(
    id=_id("https://espn.com/s1", "Lakers eliminated in playoffs"),
    headline="Lakers eliminated in playoffs",
    body="The Lakers lost 4-1 in their first-round playoff series.",
    source="ESPN",
    published_at=_utc(hour=11, minute=59),
    url="https://espn.com/s1",
    categories=["sports"],
    minutes_ago=1.0,
)

NEWS_CRYPTO = NewsArticle(
    id=_id("https://coindesk.com/c1", "Bitcoin breaks $200k"),
    headline="Bitcoin breaks $200k",
    body="Bitcoin surged past $200,000 today on heavy spot ETF inflows.",
    source="CoinDesk",
    published_at=_utc(hour=11, minute=59),
    url="https://coindesk.com/c1",
    categories=["crypto"],
    minutes_ago=1.0,
)

NEWS_STALE = NewsArticle(
    id=_id("https://reuters.com/old", "Old news from 30 minutes ago"),
    headline="Old news from 30 minutes ago",
    body="This event happened a long time ago in market terms.",
    source="Reuters",
    published_at=_utc(hour=11, minute=30),
    url="https://reuters.com/old",
    categories=["other"],
    minutes_ago=30.0,
)

ARTICLES = [NEWS_POLITICAL, NEWS_ECONOMIC, NEWS_SPORTS, NEWS_CRYPTO, NEWS_STALE]


# ---- Markets ---------------------------------------------------------------

def _market(
    cid: str,
    question: str,
    yes_price: float,
    volume: float = 50000.0,
    spread: float = 0.02,
    category: str = "politics",
    days_to_resolve: int = 30,
) -> MarketSnapshot:
    return MarketSnapshot(
        condition_id=cid,
        token_id_yes=f"{cid}-YES",
        token_id_no=f"{cid}-NO",
        question=question,
        description="",
        category=category,
        yes_price=yes_price,
        no_price=round(1.0 - yes_price, 4),
        volume_24h=volume,
        spread=spread,
        resolution_date=_utc() + timedelta(days=days_to_resolve),
        fee_rate=0.01 if category in {"politics", "tech", "finance"} else 0.0125,
        last_updated=_utc(),
    )


MARKETS = [
    _market("0xPOL1", "Will Senate confirm Fed Chair by May?", 0.55, 200000, 0.03, "politics"),
    _market("0xPOL2", "Will GOP win House majority?", 0.42, 350000, 0.02, "politics"),
    _market("0xECO1", "Will CPI come in below 2.5%?", 0.30, 120000, 0.04, "economics"),
    _market("0xECO2", "Will Fed cut rates in June?", 0.38, 800000, 0.02, "economics"),
    _market("0xSPO1", "Will Lakers win NBA Finals?", 0.18, 90000, 0.03, "sports"),
    _market("0xSPO2", "Will Celtics win NBA Finals?", 0.22, 110000, 0.03, "sports"),
    _market("0xCRY1", "Will BTC hit $250k by Dec?", 0.35, 500000, 0.02, "crypto"),
    _market("0xCRY2", "Will ETH hit $10k by Dec?", 0.28, 300000, 0.03, "crypto"),
    _market("0xGEO1", "Will Ukraine ceasefire be reached?", 0.40, 250000, 0.04, "geopolitics"),
    _market("0xCOR1", "Will Apple market cap > $5T?", 0.25, 75000, 0.05, "finance"),
]


# ---- Decisions -------------------------------------------------------------

def _trade_decision() -> ClaudeDecision:
    return ClaudeDecision(
        decision="TRADE",
        reasoning="Senate confirmation directly resolves the question; primary source.",
        news_staleness_ok=True,
        news_quality="primary_confirmed",
        news_category="political",
        affected_markets=[
            AffectedMarket(
                condition_id="0xPOL1",
                question="Will Senate confirm Fed Chair by May?",
                direction="YES",
                current_price=0.55,
                prior_probability=0.55,
                likelihood_ratio="strongly favours YES",
                fair_value_estimate=0.96,
                edge_after_fees=0.40,
                confidence=0.92,
                reasoning="Senate already voted; nothing left but signing.",
                recommended_position_usdc=200,
                exit_target_price=0.95,
                max_hold_hours=24,
                resolution_dependency="Confirmation has occurred per official record.",
            )
        ],
        red_flags=[],
        markets_considered_but_skipped=[],
    )


def _skip_decision() -> ClaudeDecision:
    return ClaudeDecision(
        decision="SKIP",
        reasoning="News is correlated but not causal for any active market.",
        news_staleness_ok=True,
        news_quality="secondary",
        news_category="other",
        affected_markets=[],
        red_flags=["weak causal chain"],
        markets_considered_but_skipped=[
            SkippedMarket(
                condition_id="0xCRY1",
                question="Will BTC hit $250k by Dec?",
                reason_skipped="single price spike not predictive of EOY level",
            )
        ],
    )


def _monitor_decision() -> ClaudeDecision:
    return ClaudeDecision(
        decision="MONITOR",
        reasoning="Relevant news but confidence below threshold.",
        news_staleness_ok=True,
        news_quality="primary_unconfirmed",
        news_category="economic_data",
        affected_markets=[
            AffectedMarket(
                condition_id="0xECO1",
                question="Will CPI come in below 2.5%?",
                direction="YES",
                current_price=0.30,
                prior_probability=0.35,
                likelihood_ratio="weakly favours YES",
                fair_value_estimate=0.45,
                edge_after_fees=0.10,
                confidence=0.65,
                reasoning="Single data point, awaits revisions.",
                recommended_position_usdc=50,
                exit_target_price=0.45,
                max_hold_hours=48,
                resolution_dependency="Final CPI release",
            )
        ],
        red_flags=[],
        markets_considered_but_skipped=[],
    )


DECISIONS = [_trade_decision(), _skip_decision(), _monitor_decision()]


# ---- Open positions --------------------------------------------------------

def _position(pid: str, cid: str, direction: str = "YES", size: float = 100.0) -> OpenPosition:
    return OpenPosition(
        position_id=pid,
        condition_id=cid,
        token_id=f"{cid}-{direction}",
        question=f"Test market {cid}",
        direction=direction,  # type: ignore[arg-type]
        entry_price=0.40,
        size_usdc=size,
        shares=round(size / 0.40, 2),
        exit_target_price=0.60,
        max_hold_until=_utc() + timedelta(hours=24),
        opened_at=_utc(),
        news_article_id=_id("u", "h"),
        order_id="ORD-1",
    )


POSITIONS = [_position("DRY-1", "0xPOL1"), _position("DRY-2", "0xECO2", "NO", 75.0)]
