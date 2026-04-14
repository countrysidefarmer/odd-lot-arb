"""
Odd-Lot Tender Offer Historical Backtest
Searches SEC EDGAR for all SC TO-I filings with odd-lot provisions since 2016-01-01,
finds realized clearing prices from SC TO-I/A amendments, fetches T-1 historical prices,
and writes data/historical.json for the website chart.

Run once locally:
    python backtest.py

Takes 20-60 minutes depending on number of filings found.
"""

from __future__ import annotations

import json
import os
import pathlib
import re
import sys
import time
from collections import defaultdict
from datetime import date, datetime, timedelta

import requests

# ---------------------------------------------------------------------------
# Import shared utilities from scanner.py
# ---------------------------------------------------------------------------
from scanner import (
    EDGAR_ARCHIVES,
    EDGAR_SEARCH_URL,
    EXCHANGE_DISPLAY,
    VALID_EXCHANGES,
    YAHOO_CHART_URL,
    _YAHOO_HEADERS,
    _make_session,
    _parse_date,
    _pick_offer_document,
    _sec_get,
    _strip_html,
    _valid_price,
    extract_offer_details,
    fetch_primary_document,
    parse_hit,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BACKTEST_START = "2016-01-01"
JSON_PATH = pathlib.Path("data/historical.json")

# ---------------------------------------------------------------------------
# EDGAR search (for both SC TO-I and SC TO-I/A)
# ---------------------------------------------------------------------------

def _search_edgar_form(start, end, form):
    """Paginated EDGAR EFTS search for a given form type + 'odd lot'."""
    session = _make_session()
    hits = []
    offset = 0
    page_size = 100

    while True:
        params = {
            "q": '"odd lot"',
            "forms": form,
            "dateRange": "custom",
            "startdt": start,
            "enddt": end,
            "from": offset,
        }
        try:
            resp = _sec_get(session, EDGAR_SEARCH_URL, params=params)
        except Exception as e:
            print("[WARN] EDGAR search failed for {}: {}".format(form, e), file=sys.stderr)
            break

        data = resp.json()
        page_hits = data.get("hits", {}).get("hits", [])
        total = data.get("hits", {}).get("total", {}).get("value", 0)
        hits.extend(page_hits)

        print("[INFO] {} {}: fetched {}/{} hits".format(form, offset, len(hits), total),
              file=sys.stderr)

        if len(hits) >= total or not page_hits:
            break

        offset += page_size
        time.sleep(0.15)

    return hits


# ---------------------------------------------------------------------------
# Amendment index
# ---------------------------------------------------------------------------

def build_amendment_index(amendment_hits):
    """
    Group SC TO-I/A hits by company CIK.
    Returns {cik: [sorted list of amendment metadata dicts by filed_date]}.
    """
    index = defaultdict(list)
    for hit in amendment_hits:
        meta = parse_hit(hit)
        if not meta:
            continue
        entry = {
            "adsh": meta["adsh"],
            "clean_adsh": meta["clean_adsh"],
            "cik": meta["cik"],
            "agent_cik": meta.get("agent_cik"),
            "filed_date": meta["filed_date"],
            "direct_filename": meta.get("direct_filename"),
        }
        index[meta["cik"]].append(entry)

    # Sort each CIK's amendments by filed_date ascending
    for cik in index:
        index[cik].sort(key=lambda x: x["filed_date"] or date.min)

    return dict(index)


# ---------------------------------------------------------------------------
# Clearing price extraction from SC TO-I/A amendments
# ---------------------------------------------------------------------------

# Pattern 1: explicit "clearing price" mention
_CP_CLEARING = re.compile(
    r'clearing\s+price.{0,200}?\$\s*(\d{1,4}(?:\.\d{1,2})?)',
    re.IGNORECASE,
)
# Pattern 2: "accepted for payment/purchase at $X per share"
_CP_ACCEPTED = re.compile(
    r'accepted\s+for\s+(?:payment|purchase).{0,300}?'
    r'at\s+(?:a\s+(?:purchase\s+)?price\s+of\s+)?\$\s*(\d{1,4}(?:\.\d{1,2})?)\s+per\s+[Ss]hare',
    re.IGNORECASE,
)
# Pattern 3: "Purchase Price ... determined to be $X"
_CP_DETERMINED = re.compile(
    r'[Pp]urchase\s+[Pp]rice.{0,300}?determined.{0,200}?\$\s*(\d{1,4}(?:\.\d{1,2})?)',
    re.IGNORECASE,
)
# Pattern 4: "will pay $X per share"
_CP_WILL_PAY = re.compile(
    r'will\s+(?:pay|purchase)\s+\$\s*(\d{1,4}(?:\.\d{1,2})?)\s+per\s+[Ss]hare',
    re.IGNORECASE,
)
# Pattern 5: generic "$X per Share" — last resort
_CP_GENERIC = re.compile(
    r'\$\s*(\d{1,4}(?:\.\d{1,2})?)\s+per\s+[Ss]hare',
    re.IGNORECASE,
)


def extract_clearing_price(html):
    """
    Extract the final clearing price from an SC TO-I/A amendment HTML.
    Returns (price: float, source: str) or (None, None).
    """
    text = _strip_html(html)

    for pat, label in [
        (_CP_CLEARING, "amendment"),
        (_CP_ACCEPTED, "amendment"),
        (_CP_DETERMINED, "amendment"),
        (_CP_WILL_PAY, "amendment"),
        (_CP_GENERIC, "amendment_generic"),
    ]:
        m = pat.search(text)
        if m:
            p = float(m.group(1))
            if _valid_price(p):
                return p, label

    return None, None


def find_clearing_price_from_amendments(session, cik, expiry, amendment_index):
    """
    Find the first SC TO-I/A for this CIK filed on or after the offer expiry,
    fetch it, and extract the clearing price.
    Returns (clearing_price, source, amendment_link) or (None, None, None).
    """
    candidates = amendment_index.get(cik, [])
    for amend in candidates:
        # Only consider amendments filed on or after expiry
        if amend["filed_date"] and amend["filed_date"] < expiry:
            continue
        # Skip amendments filed more than 90 days after expiry (likely unrelated)
        if amend["filed_date"] and amend["filed_date"] > expiry + timedelta(days=90):
            continue

        html = fetch_primary_document(
            session,
            amend["cik"],
            amend["adsh"],
            amend["clean_adsh"],
            agent_cik=amend.get("agent_cik"),
            direct_filename=amend.get("direct_filename"),
        )
        if not html:
            continue

        price, source = extract_clearing_price(html)
        if price is not None:
            clean = amend["clean_adsh"]
            link = "{}/{}/{}/".format(EDGAR_ARCHIVES, amend["cik"], clean)
            return price, source, link

        time.sleep(0.15)

    return None, None, None


# ---------------------------------------------------------------------------
# Historical price from Yahoo Finance
# ---------------------------------------------------------------------------

def get_historical_price(ticker, target_date):
    """
    Return (closing_price, actual_date) for the last trading day strictly
    before target_date. Uses Yahoo Finance period1/period2 API.
    Returns (None, None) if unavailable.
    """
    # period2 = midnight UTC on target_date (exclude target_date itself)
    period2 = int(datetime(target_date.year, target_date.month,
                           target_date.day, 0, 0, 0).timestamp())
    period1 = period2 - (14 * 86400)  # up to 14 days before

    try:
        url = YAHOO_CHART_URL.format(ticker=ticker)
        resp = requests.get(
            url,
            headers=_YAHOO_HEADERS,
            timeout=15,
            params={"interval": "1d", "period1": period1, "period2": period2},
        )
        if resp.status_code != 200:
            return None, None

        data = resp.json()
        result = data.get("chart", {}).get("result")
        if not result:
            return None, None

        timestamps = result[0].get("timestamp", [])
        quotes = result[0].get("indicators", {}).get("quote", [{}])
        closes = quotes[0].get("close", []) if quotes else []

        if not timestamps or not closes:
            return None, None

        # Find the latest valid closing price before target_date
        best_price = None
        best_date = None
        for ts, price in zip(timestamps, closes):
            if price is not None and ts < period2:
                best_price = round(float(price), 4)
                best_date = date.fromtimestamp(ts).isoformat()

        return best_price, best_date

    except Exception:
        return None, None


def get_exchange(ticker):
    """Check whether ticker is on NYSE/NASDAQ/AMEX and return display name, or None."""
    try:
        url = YAHOO_CHART_URL.format(ticker=ticker)
        resp = requests.get(
            url,
            headers=_YAHOO_HEADERS,
            timeout=15,
            params={"interval": "1d", "range": "1d"},
        )
        if resp.status_code != 200:
            return None
        data = resp.json()
        result = data.get("chart", {}).get("result")
        if not result:
            return None
        exchange = result[0].get("meta", {}).get("exchangeName", "")
        return EXCHANGE_DISPLAY.get(exchange) if exchange in VALID_EXCHANGES else None
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Main backtest
# ---------------------------------------------------------------------------

def main():
    today = date.today()
    end = today.isoformat()

    print("[INFO] Searching EDGAR for SC TO-I filings from {} to {}".format(
        BACKTEST_START, end), file=sys.stderr)
    filing_hits = _search_edgar_form(BACKTEST_START, end, "SC TO-I")
    print("[INFO] Found {} SC TO-I hits".format(len(filing_hits)), file=sys.stderr)

    print("[INFO] Searching EDGAR for SC TO-I/A amendments from {} to {}".format(
        BACKTEST_START, end), file=sys.stderr)
    amendment_hits = _search_edgar_form(BACKTEST_START, end, "SC TO-I/A")
    print("[INFO] Found {} SC TO-I/A amendment hits".format(len(amendment_hits)), file=sys.stderr)

    amendment_index = build_amendment_index(amendment_hits)
    print("[INFO] Amendment index: {} unique CIKs".format(len(amendment_index)), file=sys.stderr)

    # Group SC TO-I hits by accession number (EDGAR returns one hit per exhibit)
    filings = {}
    for hit in filing_hits:
        meta = parse_hit(hit)
        if not meta:
            continue
        adsh = meta["adsh"]
        if adsh not in filings:
            filings[adsh] = {"meta": meta, "filenames": []}
        if meta.get("direct_filename"):
            filings[adsh]["filenames"].append(meta["direct_filename"])

    print("[INFO] Unique SC TO-I filings: {}".format(len(filings)), file=sys.stderr)

    session = _make_session()
    trades = []
    skipped = 0

    for i, (adsh, filing) in enumerate(filings.items(), 1):
        meta = filing["meta"]
        ticker = meta.get("ticker")
        cik = meta["cik"]

        print("[{}/{}] Processing {} ({})".format(i, len(filings), ticker or "?", adsh[:20]),
              file=sys.stderr)

        try:
            if not ticker:
                print("  [SKIP] No ticker", file=sys.stderr)
                skipped += 1
                continue

            # Fetch the primary SC TO-I document
            best_filename = _pick_offer_document(filing["filenames"])
            meta["direct_filename"] = best_filename
            html = fetch_primary_document(
                session, cik, meta["adsh"], meta["clean_adsh"],
                agent_cik=meta.get("agent_cik"),
                direct_filename=best_filename,
            )
            if not html:
                print("  [SKIP] No document", file=sys.stderr)
                skipped += 1
                continue

            offer = extract_offer_details(html)
            if offer["price_upper"] is None:
                print("  [SKIP] No offer price extracted", file=sys.stderr)
                skipped += 1
                continue

            expiry = offer["expiry"]
            if expiry is None:
                print("  [SKIP] No expiry date", file=sys.stderr)
                skipped += 1
                continue

            # Only include completed offers
            if expiry >= today:
                print("  [SKIP] Offer not yet expired ({})".format(expiry), file=sys.stderr)
                skipped += 1
                continue

            # Check exchange (rate-limited Yahoo call)
            time.sleep(0.5)
            exchange = get_exchange(ticker)
            if not exchange:
                print("  [SKIP] Not on NYSE/NASDAQ/AMEX", file=sys.stderr)
                skipped += 1
                continue

            # T-1 historical price (day before expiry)
            t1_price, t1_date = get_historical_price(ticker, expiry)
            time.sleep(0.5)
            if t1_price is None:
                print("  [SKIP] No historical price for T-1 ({})".format(expiry - timedelta(days=1)),
                      file=sys.stderr)
                skipped += 1
                continue

            # Sanity check: T-1 price must be within 3x of offer price.
            # A wider ratio indicates either a bad offer price extraction (generic
            # regex grabbed a fee/compensation figure) or ticker reuse (same ticker
            # now belongs to a different company with a different price level).
            offer_ref = offer["price_upper"]
            if not (offer_ref / 3.0 < t1_price < offer_ref * 3.0):
                print("  [SKIP] T-1 ${} vs offer ${} ratio implausible — stale ticker or bad extraction".format(
                    t1_price, offer_ref), file=sys.stderr)
                skipped += 1
                continue

            # Determine clearing price
            is_dutch = offer["price_lower"] != offer["price_upper"]

            if not is_dutch:
                # Fixed-price offer: clearing price = offer price
                clearing_price = offer["price_upper"]
                clearing_source = "fixed"
                amendment_link = None
            else:
                # Dutch auction: try to find actual clearing price from amendment
                clearing_price, clearing_source, amendment_link = \
                    find_clearing_price_from_amendments(session, cik, expiry, amendment_index)
                time.sleep(0.15)

                if clearing_price is None:
                    # Fall back to upper bound of range
                    clearing_price = offer["price_upper"]
                    clearing_source = "upper_bound_fallback"
                    amendment_link = None
                    print("  [WARN] No amendment clearing price found for {} — using upper bound".format(
                        ticker), file=sys.stderr)

            realized_pnl = round(99 * (clearing_price - t1_price), 2)

            trade = {
                "ticker": ticker,
                "company_name": meta["company_name"],
                "exchange": exchange,
                "price_lower": offer["price_lower"],
                "price_upper": offer["price_upper"],
                "clearing_price": clearing_price,
                "clearing_price_source": clearing_source,
                "t1_price": t1_price,
                "t1_date": t1_date,
                "expiry": expiry.isoformat(),
                "filed_date": meta["filed_date"].isoformat() if meta.get("filed_date") else None,
                "realized_pnl": realized_pnl,
                "filing_link": meta["filing_link"],
                "amendment_link": amendment_link,
            }
            trades.append(trade)
            print("  [OK] P&L ${:.2f} | clearing ${:.2f} ({}) | T-1 ${:.2f} | expiry {}".format(
                realized_pnl, clearing_price, clearing_source, t1_price, expiry),
                file=sys.stderr)

        except Exception as e:
            print("  [ERROR] {}: {}".format(adsh, e), file=sys.stderr)
            skipped += 1
            continue

    # Sort by expiry date ascending
    trades.sort(key=lambda x: x["expiry"])

    total_pnl = round(sum(t["realized_pnl"] for t in trades), 2)
    print("\n[INFO] Complete: {} trades, {} skipped, total P&L ${:.2f}".format(
        len(trades), skipped, total_pnl), file=sys.stderr)

    JSON_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "trade_count": len(trades),
        "total_pnl": total_pnl,
        "trades": trades,
    }
    JSON_PATH.write_text(json.dumps(payload, indent=2))
    print("[INFO] Wrote {} trades to {}".format(len(trades), JSON_PATH), file=sys.stderr)


if __name__ == "__main__":
    main()
