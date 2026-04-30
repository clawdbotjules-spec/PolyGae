"""NewsTrader entry point.

Wires every component together, runs background tasks, processes news from
the queue, and shuts down cleanly on SIGTERM/SIGINT.
"""
from __future__ import annotations

import asyncio
import signal
import sys
import time
from datetime import datetime, timezone

import structlog

import config
from analyst import ClaudeAnalyst
from executor import Executor
from logger import TradeLogger, configure_structlog
from market_scanner import MarketScanner
from models import NewsArticle
from news_engine import NewsEngine
from position_monitor import PositionMonitor
from risk_manager import RiskManager


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


configure_structlog()
log = structlog.get_logger("main")


async def _health_checks(
    analyst: ClaudeAnalyst,
    market_scanner: MarketScanner,
    trade_logger: TradeLogger,
) -> bool:
    """Verify Anthropic API key, Polymarket connectivity, Telegram, and DB.

    Returns True if all checks pass. Logs each result so the operator can see
    which component failed.
    """
    ok = True

    # SQLite is writable (init_db already created tables)
    try:
        await trade_logger.has_seen_article("__healthcheck__")
        log.info("health_db_ok")
    except Exception as e:
        log.error("health_db_failed", error=str(e))
        ok = False

    # Anthropic minimal call (only checks the key works)
    try:
        await analyst.client.messages.create(
            model=config.CLAUDE_MODEL,
            max_tokens=8,
            messages=[{"role": "user", "content": "ping"}],
        )
        log.info("health_anthropic_ok")
    except Exception as e:
        log.error("health_anthropic_failed", error=str(e))
        ok = False

    # Gamma fetch (one market is enough)
    try:
        await market_scanner._scan_once()  # noqa: SLF001 - single-call check
        log.info("health_gamma_ok")
    except Exception as e:
        log.error("health_gamma_failed", error=str(e))
        ok = False

    # Telegram fire-and-forget; failure is logged but not fatal
    try:
        await trade_logger.send_telegram("NewsTrader health check ping")
        log.info("health_telegram_attempted")
    except Exception as e:
        log.warning("health_telegram_failed", error=str(e))

    return ok


async def _process_news(
    news_queue: asyncio.Queue[NewsArticle],
    analyst: ClaudeAnalyst,
    market_scanner: MarketScanner,
    risk_manager: RiskManager,
    executor: Executor,
    position_monitor: PositionMonitor,
    trade_logger: TradeLogger,
    session_state: dict,
) -> None:
    while True:
        article = await news_queue.get()
        try:
            # Short-circuit when bot is halted
            if risk_manager.is_killed:
                await trade_logger.log_article(article, "skipped_killed")
                continue
            if (
                risk_manager.safety_mode_until
                and _utcnow() < risk_manager.safety_mode_until
            ):
                await trade_logger.log_article(article, "skipped_safety")
                continue

            session_context = {
                "session_start": session_state["session_start"].isoformat(),
                "trades_this_session": session_state["trades_this_session"],
                "already_traded": session_state["already_traded_markets"],
                "open_positions": [
                    p.question for p in risk_manager.open_positions.values()
                ],
                "daily_pnl": risk_manager.daily_pnl_usdc,
            }

            markets_str = await market_scanner.get_markets_for_prompt()

            t0 = time.monotonic()
            try:
                decision = await analyst.analyze(article, markets_str, session_context)
            except Exception as e:
                log.error(
                    "analyze_failed",
                    article_id=article.id,
                    error=str(e),
                    exc_info=True,
                )
                await trade_logger.log_article(article, "error")
                continue
            latency_ms = int((time.monotonic() - t0) * 1000)

            decision_id = await trade_logger.log_decision(
                decision,
                article.id,
                latency_ms,
                {},
            )
            await trade_logger.log_article(
                article, decision.decision.lower(), decision_id
            )

            # Contradicting news handling: if Claude flags any open position
            # in red_flags or skipped reasons, close the position immediately.
            await _maybe_close_contradicted(
                decision, article, risk_manager, position_monitor
            )

            if decision.decision != "TRADE":
                continue

            for dm in decision.affected_markets:
                if (
                    dm.confidence < config.MIN_CONFIDENCE
                    or dm.edge_after_fees < config.MIN_EDGE_AFTER_FEES
                ):
                    continue
                market = await market_scanner.get_market(dm.condition_id)
                if market is None:
                    log.warning("market_not_in_cache", condition_id=dm.condition_id)
                    continue
                risk_result = risk_manager.check_trade(market, dm)
                if not risk_result.approved:
                    log.info(
                        "trade_rejected",
                        condition_id=dm.condition_id,
                        reason=risk_result.reason,
                    )
                    continue

                position = await executor.place_order(
                    market=market,
                    direction=dm.direction,
                    size_usdc=risk_result.adjusted_size_usdc,
                    article=article,
                    decision_market=dm,
                )
                if position is None:
                    log.warning(
                        "order_returned_none",
                        condition_id=dm.condition_id,
                    )
                    continue
                risk_manager.record_trade_opened(position)
                session_state["already_traded_markets"].append(dm.condition_id)
                session_state["trades_this_session"] += 1
                try:
                    await trade_logger.log_trade_opened(position)
                    msg = trade_logger.format_trade_opened_message(
                        position, market, dm.edge_after_fees
                    )
                    await trade_logger.send_telegram(msg)
                except Exception as e:
                    log.warning("trade_open_log_failed", error=str(e))
                log.info("trade_opened", position_id=position.position_id)
        except Exception as e:
            log.error(
                "news_processing_failed",
                article_id=article.id,
                error=str(e),
                exc_info=True,
            )
        finally:
            news_queue.task_done()


async def _maybe_close_contradicted(
    decision,
    article: NewsArticle,
    risk_manager: RiskManager,
    position_monitor: PositionMonitor,
) -> None:
    """If Claude calls out an open position as contradicted, close it.

    Heuristic: scan the decision's red_flags and skipped-market reasons for
    references to condition_ids we currently hold.
    """
    if not risk_manager.open_positions:
        return
    blob = " ".join(
        [decision.reasoning or ""]
        + (decision.red_flags or [])
        + [
            f"{s.question} {s.reason_skipped}"
            for s in (decision.markets_considered_but_skipped or [])
        ]
    ).lower()
    for pos in list(risk_manager.open_positions.values()):
        keywords = {
            pos.condition_id.lower(),
            pos.question.lower()[:60],
        }
        if any(k and k in blob for k in keywords):
            await position_monitor.notify_contradicting_news(pos, article)


async def _shutdown(
    tasks: list[asyncio.Task],
    news_engine: NewsEngine,
    market_scanner: MarketScanner,
    executor: Executor,
    position_monitor: PositionMonitor,
    trade_logger: TradeLogger,
) -> None:
    log.info("shutdown_starting")
    try:
        await trade_logger.send_telegram("NewsTrader shutting down…")
    except Exception:
        pass
    await position_monitor.stop()
    await news_engine.stop()
    await market_scanner.stop()
    await executor.stop()
    await executor.cancel_all_open_orders()

    if risk_open := list(getattr(_shutdown, "_risk_open", [])):
        log.info("open_positions_at_shutdown", count=len(risk_open))

    for t in tasks:
        t.cancel()
    for t in tasks:
        try:
            await t
        except (asyncio.CancelledError, Exception):
            pass

    await trade_logger.close()
    log.info("shutdown_complete")


async def amain() -> int:
    config.validate_required_env()

    trade_logger = TradeLogger()
    await trade_logger.init_db()

    news_queue: asyncio.Queue[NewsArticle] = asyncio.Queue()
    market_scanner = MarketScanner()
    analyst = ClaudeAnalyst()
    risk_manager = RiskManager(telegram_hook=trade_logger.send_telegram)
    executor = Executor(market_scanner=market_scanner)
    position_monitor = PositionMonitor(
        risk_manager=risk_manager,
        executor=executor,
        market_scanner=market_scanner,
        logger=trade_logger,
    )
    news_engine = NewsEngine(queue=news_queue, trade_logger=trade_logger)

    log.info("startup", config=config.safe_summary())

    if not await _health_checks(analyst, market_scanner, trade_logger):
        log.error("health_checks_failed")
        await trade_logger.close()
        return 3

    session_state = {
        "session_start": _utcnow(),
        "trades_this_session": 0,
        "already_traded_markets": [],
    }

    tasks = [
        asyncio.create_task(news_engine.start(), name="news_engine"),
        asyncio.create_task(market_scanner.start(), name="market_scanner"),
        asyncio.create_task(executor.heartbeat_loop(), name="heartbeat"),
        asyncio.create_task(position_monitor.start(), name="position_monitor"),
        asyncio.create_task(
            _process_news(
                news_queue,
                analyst,
                market_scanner,
                risk_manager,
                executor,
                position_monitor,
                trade_logger,
                session_state,
            ),
            name="news_processor",
        ),
    ]

    mode = "DRY RUN" if config.DRY_RUN else "LIVE"
    await trade_logger.send_telegram(
        f"NewsTrader started in {mode} mode. Capital: ${config.TOTAL_CAPITAL_USDC:.0f}"
    )

    stop_event = asyncio.Event()

    def _signal_handler(signame: str) -> None:
        log.info("signal_received", signal=signame)
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _signal_handler, sig.name)
        except NotImplementedError:
            # Windows fallback
            signal.signal(sig, lambda *_: stop_event.set())

    # Wait until either: stop signal or any task fails fatally
    done = asyncio.create_task(stop_event.wait())
    try:
        await asyncio.wait(
            [done, *tasks],
            return_when=asyncio.FIRST_COMPLETED,
        )
    finally:
        # Capture risk-manager open positions for shutdown summary
        _shutdown._risk_open = list(risk_manager.open_positions.values())  # type: ignore[attr-defined]
        await _shutdown(
            tasks=tasks,
            news_engine=news_engine,
            market_scanner=market_scanner,
            executor=executor,
            position_monitor=position_monitor,
            trade_logger=trade_logger,
        )
    return 0


def main() -> int:
    try:
        return asyncio.run(amain())
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
