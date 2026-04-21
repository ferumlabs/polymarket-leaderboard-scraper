#!/usr/bin/env python3
"""
Polymarket Leaderboard Scraper

Scrapes all category × timePeriod combinations from the Polymarket leaderboard API,
deduplicates by wallet address, and outputs a single CSV for BD use.
"""

import csv
import sys
import time
from datetime import datetime, timezone

import requests

BASE_URL = "https://data-api.polymarket.com/v1/leaderboard"

CATEGORIES = [
    "OVERALL",
    "POLITICS",
    "SPORTS",
    "CRYPTO",
    "CULTURE",
    "MENTIONS",
    "WEATHER",
    "ECONOMICS",
    "TECH",
    "FINANCE",
]

TIME_PERIODS = ["DAY", "WEEK", "MONTH", "ALL"]

LIMIT = 50  # max per request
MAX_OFFSET = 1000  # API cap

OUTPUT_FILE = "polymarket_leaderboard.csv"


ORDER_BYS = ["PNL", "VOL"]


def fetch_page(category: str, time_period: str, order_by: str, offset: int) -> list[dict]:
    """Fetch a single page from the leaderboard API."""
    params = {
        "category": category,
        "timePeriod": time_period,
        "orderBy": order_by,
        "limit": LIMIT,
        "offset": offset,
    }
    resp = requests.get(BASE_URL, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    return data if isinstance(data, list) else []


def scrape_combo(category: str, time_period: str, order_by: str) -> list[dict]:
    """Paginate through all results for a single category × timePeriod × orderBy."""
    all_records = []
    offset = 0

    while offset <= MAX_OFFSET:
        page = fetch_page(category, time_period, order_by, offset)
        if not page:
            break
        for record in page:
            record["_category"] = category
            record["_timePeriod"] = time_period
        all_records.extend(page)
        if len(page) < LIMIT:
            break  # last page
        offset += LIMIT
        time.sleep(0.15)  # be respectful

    return all_records


def merge_records(raw_records: list[dict]) -> list[dict]:
    """Deduplicate by proxyWallet, keeping the richest info per trader."""
    wallets: dict[str, dict] = {}

    for r in raw_records:
        wallet = r.get("proxyWallet", "").lower()
        if not wallet:
            continue

        if wallet not in wallets:
            wallets[wallet] = {
                "proxyWallet": wallet,
                "userName": "",
                "xUsername": "",
                "profileImage": "",
                "verifiedBadge": False,
                "best_rank": float("inf"),
                "max_pnl": float("-inf"),
                "max_vol": float("-inf"),
                "categories": set(),
                "time_periods": set(),
                "appearances": 0,
            }

        entry = wallets[wallet]

        # prefer non-empty strings
        if r.get("userName") and not entry["userName"]:
            entry["userName"] = r["userName"]
        if r.get("xUsername") and not entry["xUsername"]:
            entry["xUsername"] = r["xUsername"]
        if r.get("profileImage") and not entry["profileImage"]:
            entry["profileImage"] = r["profileImage"]
        if r.get("verifiedBadge"):
            entry["verifiedBadge"] = True

        rank = int(r.get("rank", 0) or 0)
        pnl = float(r.get("pnl", 0) or 0)
        vol = float(r.get("vol", 0) or 0)

        if rank > 0 and rank < entry["best_rank"]:
            entry["best_rank"] = rank
        if pnl > entry["max_pnl"]:
            entry["max_pnl"] = pnl
        if vol > entry["max_vol"]:
            entry["max_vol"] = vol

        entry["categories"].add(r["_category"])
        entry["time_periods"].add(r["_timePeriod"])
        entry["appearances"] += 1

    # flatten sets to sorted strings
    results = []
    for entry in wallets.values():
        entry["categories"] = ", ".join(sorted(entry["categories"]))
        entry["time_periods"] = ", ".join(sorted(entry["time_periods"]))
        if entry["best_rank"] == float("inf"):
            entry["best_rank"] = ""
        if entry["max_pnl"] == float("-inf"):
            entry["max_pnl"] = ""
        if entry["max_vol"] == float("-inf"):
            entry["max_vol"] = ""
        results.append(entry)

    # sort by best_rank (unranked at the end), then by max_pnl desc
    results.sort(key=lambda x: (
        x["best_rank"] if isinstance(x["best_rank"], int) else 999999,
        -(x["max_pnl"] if isinstance(x["max_pnl"], (int, float)) else 0),
    ))

    return results


def write_csv(records: list[dict], path: str) -> None:
    """Write merged records to CSV."""
    columns = [
        "proxyWallet",
        "userName",
        "xUsername",
        "verifiedBadge",
        "best_rank",
        "max_pnl",
        "max_vol",
        "categories",
        "time_periods",
        "appearances",
        "profileImage",
    ]
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(records)


def main():
    total_combos = len(CATEGORIES) * len(TIME_PERIODS) * len(ORDER_BYS)
    print(f"Scraping {len(CATEGORIES)} categories x {len(TIME_PERIODS)} time periods x {len(ORDER_BYS)} orderings = {total_combos} combinations")
    print(f"Max {((MAX_OFFSET // LIMIT) + 1) * LIMIT} entries per combination\n")

    all_raw = []
    combo_num = 0

    for category in CATEGORIES:
        for period in TIME_PERIODS:
            for order_by in ORDER_BYS:
                combo_num += 1
                sys.stdout.write(f"[{combo_num}/{total_combos}] {category} / {period} / {order_by} ... ")
                sys.stdout.flush()

                try:
                    records = scrape_combo(category, period, order_by)
                    all_raw.extend(records)
                    print(f"{len(records)} records")
                except requests.HTTPError as e:
                    print(f"ERROR {e}")
                except Exception as e:
                    print(f"ERROR {e}")

                time.sleep(0.25)

    print(f"\nTotal raw records: {len(all_raw)}")

    merged = merge_records(all_raw)
    print(f"Unique wallets after dedup: {len(merged)}")

    has_x = sum(1 for r in merged if r["xUsername"])
    has_name = sum(1 for r in merged if r["userName"])
    verified = sum(1 for r in merged if r["verifiedBadge"])
    print(f"  with X/Twitter username: {has_x}")
    print(f"  with display name: {has_name}")
    print(f"  verified: {verified}")

    write_csv(merged, OUTPUT_FILE)
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    print(f"\nWrote {len(merged)} rows to {OUTPUT_FILE} at {ts}")


if __name__ == "__main__":
    main()
