"""Pydantic v2 models shared across the NewsTrader bot.

These are the canonical data shapes for news articles, market snapshots,
Claude's structured output, risk-check results, and trade lifecycle records.
"""
from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class NewsArticle(BaseModel):
    """A normalised news article from any source."""

    model_config = ConfigDict(extra="ignore")

    id: str = Field(..., description="SHA-256 of url+headline")
    headline: str
    body: str
    source: str
    published_at: datetime
    url: str
    categories: list[str] = Field(default_factory=list)
    minutes_ago: float = 0.0


class MarketSnapshot(BaseModel):
    """A point-in-time view of a Polymarket binary market."""

    model_config = ConfigDict(extra="ignore")

    condition_id: str
    token_id_yes: str
    token_id_no: str
    question: str
    description: str = ""
    category: str = "other"
    yes_price: float
    no_price: float
    volume_24h: float
    spread: float
    resolution_date: datetime
    fee_rate: float
    last_updated: datetime


class AffectedMarket(BaseModel):
    """Claude's per-market trade recommendation."""

    model_config = ConfigDict(extra="ignore")

    condition_id: str
    question: str
    direction: Literal["YES", "NO"]
    current_price: float
    prior_probability: float
    likelihood_ratio: str
    fair_value_estimate: float
    edge_after_fees: float
    confidence: float
    reasoning: str
    recommended_position_usdc: int
    exit_target_price: float
    max_hold_hours: int
    resolution_dependency: str


class SkippedMarket(BaseModel):
    model_config = ConfigDict(extra="ignore")

    condition_id: str
    question: str
    reason_skipped: str


class ClaudeDecision(BaseModel):
    """The full structured response Claude returns for one news article."""

    model_config = ConfigDict(extra="ignore")

    decision: Literal["TRADE", "SKIP", "MONITOR"]
    reasoning: str
    news_staleness_ok: bool
    news_quality: Literal[
        "primary_confirmed", "primary_unconfirmed", "secondary", "rumor"
    ]
    news_category: str
    affected_markets: list[AffectedMarket] = Field(default_factory=list)
    red_flags: list[str] = Field(default_factory=list)
    markets_considered_but_skipped: list[SkippedMarket] = Field(default_factory=list)
    raw_response: str = ""


class OpenPosition(BaseModel):
    """An open trade we currently hold."""

    model_config = ConfigDict(extra="ignore")

    position_id: str
    condition_id: str
    token_id: str
    question: str
    direction: Literal["YES", "NO"]
    entry_price: float
    size_usdc: float
    shares: float
    exit_target_price: float
    max_hold_until: datetime
    opened_at: datetime
    news_article_id: str
    order_id: str


class TradeResult(BaseModel):
    """A closed trade with realised P&L."""

    model_config = ConfigDict(extra="ignore")

    position_id: str
    condition_id: str
    direction: Literal["YES", "NO"]
    entry_price: float
    exit_price: float
    size_usdc: float
    pnl_usdc: float
    pnl_pct: float
    hold_hours: float
    exit_reason: Literal[
        "target_hit",
        "time_expired",
        "contradicting_news",
        "manual",
        "kill_switch",
        "resolved",
    ]
    opened_at: datetime
    closed_at: datetime


class RiskCheckResult(BaseModel):
    """Output of RiskManager.check_trade."""

    model_config = ConfigDict(extra="ignore")

    approved: bool
    reason: str
    adjusted_size_usdc: float
