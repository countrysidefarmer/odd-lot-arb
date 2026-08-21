"""Step 1: pull EDGAR daily form indexes for a window of business days,
filter to Form 4 / SC 13D / SC 13D/A, print counts, write filings.json."""
import sys
import json
import argparse
import datetime as dt
from collections import Counter

from edgar_client import TokenBucket, EdgarClient
from parsers import parse_form_idx

IDX_URL = "https://www.sec.gov/Archives/edgar/daily-index/{yr}/QTR{q}/form.{ymd}.idx"


def business_days(end_date, back_calendar_days):
    """Yield business days within [end - back_calendar_days, end], newest first."""
    days = []
    for i in range(back_calendar_days + 1):
        d = end_date - dt.timedelta(days=i)
        if d.weekday() < 5:  # Mon-Fri
            days.append(d)
    return days  # already newest-first


def fetch_indexes(client, days):
    all_rows = []
    per_day = {}
    for d in days:
        ymd = d.strftime("%Y%m%d")
        q = (d.month - 1) // 3 + 1
        url = IDX_URL.format(yr=d.year, q=q, ymd=ymd)
        cache_path = f"cache/index_{ymd}.idx"
        text, cached = client.fetch(url, cache_path)
        if text is None:
            per_day[ymd] = None  # 404 - weekend/holiday, skip silently
            continue
        rows = parse_form_idx(text)
        per_day[ymd] = len(rows)
        for r in rows:
            r["index_date"] = d.strftime("%Y-%m-%d")
        all_rows.extend(rows)
    return all_rows, per_day


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--end", default="2026-08-04", help="end date YYYY-MM-DD")
    ap.add_argument("--calendar-days", type=int, default=30)
    ap.add_argument("--recent-bdays", type=int, default=0,
                    help="if >0, keep only the N most recent business days")
    ap.add_argument("--out", default="filings.json")
    args = ap.parse_args()

    end = dt.datetime.strptime(args.end, "%Y-%m-%d").date()
    days = business_days(end, args.calendar_days)
    if args.recent_bdays > 0:
        days = days[:args.recent_bdays]

    bucket = TokenBucket(rate=10, capacity=10)
    client = EdgarClient(bucket)

    print(f"Fetching {len(days)} business-day indexes "
          f"({days[-1]} .. {days[0]})", flush=True)
    rows, per_day = fetch_indexes(client, days)

    counts = Counter(r["form_class"] for r in rows)
    raw_counts = Counter(r["form_type"] for r in rows)
    found_days = sum(1 for v in per_day.values() if v is not None)
    missing = [k for k, v in per_day.items() if v is None]

    print("\n=== Index results ===")
    print(f"business days requested : {len(days)}")
    print(f"days with an index (200): {found_days}")
    print(f"days 404 (weekend/holiday/not-posted): {len(missing)}  {sorted(missing)}")
    print(f"net index fetches: {client.net_hits} | cache hits: {client.cache_hits}")
    print("\nCounts by form class (kept):")
    for ft in ("4", "SC 13D", "SC 13D/A"):
        print(f"  {ft:10s}: {counts.get(ft, 0)}")
    print(f"  {'TOTAL':10s}: {sum(counts.values())}")
    print("\nRaw label breakdown:")
    for ft, n in raw_counts.most_common():
        print(f"  {ft:16s}: {n}")

    with open(args.out, "w") as f:
        json.dump(rows, f)
    print(f"\nWrote {len(rows)} filing rows -> {args.out}")

    form4 = counts.get("4", 0)
    if form4 > 15000 and args.recent_bdays == 0:
        print("\n" + "!" * 60)
        print(f"STOP: Form 4 count = {form4} (> 15,000).")
        print("Per spec, re-run with --recent-bdays 5 before full extraction.")
        print("!" * 60)
        sys.exit(2)


if __name__ == "__main__":
    main()
