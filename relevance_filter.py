"""Pre-filter to decide which articles are worth sending to Claude.

Two gates run in series:

1. Per-minute Claude rate limiter (sliding window). We never make more
   than MAX_CLAUDE_CALLS_PER_MINUTE requests/minute, so we never hit the
   Anthropic 429 path.

2. Keyword relevance: we tokenise every active market question, drop
   stopwords, keep tokens >= 4 chars, and require the article's headline
   plus body to contain at least one of those tokens. This kills the
   "every Reddit post becomes a Claude call" problem cheaply.

Everything is best-effort and fail-open: if the keyword set is empty
(scanner hasn't initialised yet), every article is considered relevant.
"""
from __future__ import annotations

import re
from collections import deque
from datetime import datetime, timedelta, timezone

import structlog


log = structlog.get_logger("relevance_filter")


_STOPWORDS: frozenset[str] = frozenset(
    {
        # articles / determiners
        "the", "a", "an", "this", "that", "these", "those",
        # conjunctions
        "and", "or", "but", "if", "then", "while", "than",
        # prepositions
        "of", "in", "on", "at", "to", "for", "with", "by", "from",
        "into", "over", "under", "after", "before", "during",
        # auxiliaries / common verbs
        "is", "are", "was", "were", "be", "been", "being",
        "have", "has", "had", "do", "does", "did", "done",
        "will", "would", "could", "should", "may", "might", "can",
        # pronouns
        "it", "its", "they", "them", "their", "theirs", "his", "her",
        "him", "she", "he", "we", "us", "our", "ours", "i", "my", "me",
        "you", "your", "yours",
        # negation / quantifiers
        "not", "no", "yes", "all", "any", "some", "few", "many", "more",
        "most", "much", "such", "very", "just", "only", "also", "even",
        "ever", "never",
        # question words
        "what", "when", "where", "which", "who", "whom", "whose",
        "how", "why",
        # market-specific filler that adds nothing
        "polymarket", "market", "happen", "happens", "vs", "versus",
        "win", "wins", "won", "lose", "lost", "yes", "make", "makes",
        "made", "say", "says", "said", "tell", "told", "get", "gets",
        "got", "go", "goes", "went", "gone", "come", "comes", "came",
        # numerals as words
        "one", "two", "three", "four", "five", "six", "seven", "eight",
        "nine", "ten",
    }
)

# Capture word-ish tokens. We allow apostrophe and hyphen for things like
# "biden's" / "covid-19" but require at least one alpha char to start.
_WORD_RE = re.compile(r"[A-Za-z][A-Za-z0-9'\-]+")


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class RelevanceFilter:
    """Cheap keyword-based pre-filter + sliding-window rate limiter."""

    def __init__(
        self,
        max_calls_per_minute: int = 4,
        min_keyword_length: int = 4,
        enable_relevance: bool = True,
    ) -> None:
        self.max_calls_per_minute = max(1, max_calls_per_minute)
        self.min_keyword_length = max(2, min_keyword_length)
        self.enable_relevance = enable_relevance
        self._keywords: set[str] = set()
        self._call_history: deque[datetime] = deque()

    # --- keyword maintenance --------------------------------------------

    def update_keywords(self, market_questions: list[str]) -> None:
        kws: set[str] = set()
        for q in market_questions:
            for tok in _WORD_RE.findall((q or "").lower()):
                if len(tok) < self.min_keyword_length:
                    continue
                if tok in _STOPWORDS:
                    continue
                kws.add(tok)
        self._keywords = kws
        log.info("relevance_keywords_updated", count=len(kws))

    @property
    def keyword_count(self) -> int:
        return len(self._keywords)

    # --- relevance check -------------------------------------------------

    def is_relevant(self, headline: str, body: str = "") -> bool:
        if not self.enable_relevance:
            return True
        if not self._keywords:
            return True  # scanner not yet populated → fail open
        text = f"{headline} {body}".lower()
        for kw in self._keywords:
            if kw in text:
                return True
        return False

    def matched_keywords(self, headline: str, body: str = "", limit: int = 8) -> list[str]:
        """Return up to `limit` keywords that appear in the article (for logging)."""
        if not self._keywords:
            return []
        text = f"{headline} {body}".lower()
        out: list[str] = []
        for kw in self._keywords:
            if kw in text:
                out.append(kw)
                if len(out) >= limit:
                    break
        return out

    # --- rate limiter ----------------------------------------------------

    def _prune_history(self) -> None:
        cutoff = _utcnow() - timedelta(seconds=60)
        while self._call_history and self._call_history[0] < cutoff:
            self._call_history.popleft()

    def can_call_now(self) -> bool:
        self._prune_history()
        return len(self._call_history) < self.max_calls_per_minute

    def record_call(self) -> None:
        self._call_history.append(_utcnow())

    @property
    def calls_in_last_minute(self) -> int:
        self._prune_history()
        return len(self._call_history)
