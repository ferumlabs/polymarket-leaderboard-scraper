# Polymarket Leaderboard Scraper

Exhaustive scraper for the [Polymarket leaderboard API](https://docs.polymarket.com/api-reference/core/get-trader-leaderboard-rankings). Pulls all traders across every category, time period, and ordering, deduplicates by wallet, and outputs a single CSV.

## Coverage

- **10 categories:** Overall, Politics, Sports, Crypto, Culture, Mentions, Weather, Economics, Tech, Finance
- **4 time periods:** Day, Week, Month, All
- **2 orderings:** PNL, VOL
- **80 total combinations**, up to 1,050 traders each

## Output

`polymarket_leaderboard.csv` with columns:

| Column | Description |
|---|---|
| `proxyWallet` | Unique wallet address (dedupe key) |
| `userName` | Display name |
| `xUsername` | X/Twitter handle |
| `verifiedBadge` | Whether the account is verified |
| `best_rank` | Best rank across all leaderboard appearances |
| `max_pnl` | Highest PNL seen |
| `max_vol` | Highest volume seen |
| `categories` | Which categories the trader appeared in |
| `time_periods` | Which time periods the trader appeared in |
| `appearances` | Total number of leaderboard appearances |
| `profileImage` | Profile image URL |

## Usage

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
.venv/bin/python scrape.py
```

Takes ~5 minutes. Output is written to `polymarket_leaderboard.csv`.
