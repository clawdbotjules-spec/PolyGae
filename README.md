# NewsTrader — Polymarket News-Driven Trading Bot

NewsTrader watches breaking news in real time, asks Claude Opus whether the
news creates mispricing on a Polymarket binary market, applies hard risk
controls, and (optionally) places live orders on the Polymarket CLOB.

This is a real-money system. **DRY_RUN defaults to `true`.** Always run in
dry-run mode for at least a week before trusting live orders.

## Architecture

```
+---------------+      +------------------+
|  Newsfilter.io|----->|                  |
|   WebSocket   |      |                  |
+---------------+      |   NewsEngine     |
                       |  (asyncio.Queue) |----+
+---------------+      |                  |    |
|  NewsAPI.org  |----->|                  |    |
+---------------+      +------------------+    |
                                               v
+---------------+                          +-----------+      +------------+
|   RSS feeds   |------------------------->|   main    |----->|  Analyst   |
+---------------+                          | event loop|      | (Claude 4) |
                                           +-----------+      +-----+------+
                                                 |  ^                |
                                                 v  |                v
                                         +------------------+   structured
                                         |  Risk Manager    |   ClaudeDecision
                                         |  (Kelly + caps)  |
                                         +--------+---------+
                                                  |
                                                  v
                                         +------------------+
                                         |   Executor       |
                                         |  (Polymarket CLOB|
                                         |   + heartbeat)   |
                                         +--------+---------+
                                                  |
                                                  v
+--------------------+     +------------------+   |
|  PositionMonitor   |---->|   TradeLogger    |<--+
|  (target/time/res) |     |  SQLite + Telegram|
+--------------------+     +------------------+
```

## Prerequisites

- Python 3.12 or newer
- A Polygon (POL) wallet funded with USDC.e
- A Polymarket account (the wallet must already be approved for the CLOB)
- API keys for Anthropic, Newsfilter.io, and NewsAPI
- A Telegram bot + a chat ID to alert

## Installation

```bash
git clone <this-repo>
cd polymarket-news-trader
python3.12 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
$EDITOR .env  # populate every variable
```

## Where to get each API key

| Provider                     | Required? | URL                                                                 |
|------------------------------|-----------|---------------------------------------------------------------------|
| Anthropic                    | yes       | https://console.anthropic.com/                                      |
| Polymarket (CLOB)            | yes       | https://docs.polymarket.com/  (CLOB authentication & API creation)  |
| NewsAPI.org                  | yes       | https://newsapi.org/register                                        |
| Telegram bot                 | yes       | DM `@BotFather` on Telegram                                         |
| Telegram chat ID             | yes       | DM `@userinfobot` to find your numeric chat ID                      |
| The Guardian Open API        | optional  | https://open-platform.theguardian.com/access/  (12,000/day, free)   |
| Marketaux                    | optional  | https://www.marketaux.com/account/dashboard    (100/day, free)      |
| Newsfilter.io                | optional  | https://newsfilter.io/api  (real-time WebSocket; paid)              |

Reddit and GDELT do not require keys.

### News sources at a glance

The bot polls multiple sources concurrently. None are individually required —
NewsAPI is the only mandatory source. Adding more keys widens coverage.

| Source       | Real-time? | Cadence | Cost                | Coverage                 |
|--------------|------------|---------|---------------------|--------------------------|
| NewsAPI      | ~1 min     | 60s     | Free 100/day        | General                  |
| Guardian     | seconds    | 30s     | Free 12k/day        | World, politics, finance |
| GDELT 2.0    | 15-min lag | 5 min   | Free, no key        | Global event monitoring  |
| Marketaux    | seconds    | 16 min  | Free 100/day        | Financial / markets      |
| Reddit       | seconds    | 12s/sub | Free, no key        | Breaking, social-driven  |
| RSS feeds    | minutes    | 90s     | Free, no key        | Reuters, AP, BBC, etc.   |
| Newsfilter   | seconds    | live WS | Paid                | Wire-style breaking news |

## Funding your Polymarket wallet

1. Bridge USDC to the Polygon mainnet (e.g. via Polymarket's UI, Across, or
   Stargate).
2. Deposit USDC into Polymarket from your wallet on the website.
3. Generate API credentials (key / secret / passphrase) in your Polymarket
   profile and copy them into `.env` together with `POLYGON_PRIVATE_KEY` and
   the wallet address (`POLYMARKET_FUNDER_ADDRESS`).

## Dry-run first

```bash
# .env: DRY_RUN=true
python main.py
```

You should see:

- A "NewsTrader started in DRY RUN mode" message in Telegram
- Structured logs scrolling on stdout for each article seen
- Mock trades opened and closed (`position_id` prefixed `DRY-`)

Let it run for at least a few days and read the SQLite journal:

```bash
sqlite3 newstrader.db
> select decision, count(*) from claude_decisions group by 1;
> select * from trades_closed order by closed_at desc limit 20;
> select * from daily_summary order by date desc limit 14;
```

## Going live

Only after you have audited the dry-run output and are comfortable with the
recommendations, set `DRY_RUN=false`. Start with a small `TOTAL_CAPITAL_USDC`
(say $100) and modest single-bet caps.

```bash
# .env: DRY_RUN=false
python main.py
```

## Telegram alerts

The bot sends:

- A startup message with the active mode (DRY RUN or LIVE)
- A "NEW TRADE" alert on every fill (or simulated fill in dry run)
- A "TRADE CLOSED" alert whenever a position closes
- "SAFETY MODE" alerts after consecutive losses
- "KILL SWITCH" alerts when the daily-loss cap is reached

## Querying the journal

```sql
-- Daily P&L
SELECT date, total_trades, winning_trades, losing_trades, total_pnl
FROM daily_summary ORDER BY date DESC;

-- Recent decisions
SELECT created_at, decision, news_quality, confidence_max, markets_count, latency_ms
FROM claude_decisions ORDER BY created_at DESC LIMIT 50;

-- Per-market P&L
SELECT condition_id, COUNT(*) AS n, SUM(pnl_usdc) AS pnl
FROM trades_closed GROUP BY condition_id ORDER BY pnl DESC;
```

## Stopping safely

`Ctrl-C` (or `systemctl stop newstrader`) triggers a graceful shutdown:

1. New news stops being processed
2. Open *limit* orders that have not yet filled are cancelled
3. Open *positions* are left as-is (the bot does not panic-sell on shutdown —
   restart it and let `PositionMonitor` close them on the normal cadence)
4. A "shutting down" Telegram message is sent
5. The SQLite database is closed

## Risk warnings

- **This is real money.** Past dry-run performance does not guarantee future
  live performance. Slippage, partial fills, and exchange issues are real.
- **Claude can be wrong.** The `MIN_CONFIDENCE` and `MIN_EDGE_AFTER_FEES`
  thresholds are floors, not ceilings — they do not guarantee profitable
  trades.
- **Audit your trades daily** for the first month and compare against the
  Telegram alerts. Tighten the caps if anything looks off.
- **Never commit your `.env`.** It contains your private key.

## Running as a systemd service

Edit `systemd/newstrader.service` to point at the actual install path and
user, then:

```bash
sudo cp systemd/newstrader.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now newstrader
sudo journalctl -u newstrader -f
```
