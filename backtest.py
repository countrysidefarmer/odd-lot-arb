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
import pathlib
import re
import sys
import time
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
    _try_index,
    _valid_price,
    extract_offer_details,
    fetch_primary_document,
    parse_hit,
)

EDGAR_SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{}.json"

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
# Amendment discovery via EDGAR submissions API
# ---------------------------------------------------------------------------

def _fetch_submissions_amendments(session, padded_cik):
    """
    Fetch all SC TO-I/A filings for a CIK from the EDGAR submissions API.
    Returns list of dicts: {accession, filed_date, primary_doc}.
    Handles pagination via the filings.files array for older filings.
    """
    url = EDGAR_SUBMISSIONS_URL.format(padded_cik)
    try:
        resp = _sec_get(session, url)
        data = resp.json()
    except Exception as e:
        print("  [WARN] Submissions API failed for CIK {}: {}".format(padded_cik, e),
              file=sys.stderr)
        return []

    results = []

    def _extract_from_block(block):
        forms = block.get("form", [])
        accessions = block.get("accessionNumber", [])
        dates = block.get("filingDate", [])
        primary_docs = block.get("primaryDocument", [])
        for form, acc, dt, doc in zip(forms, accessions, dates, primary_docs):
            if form == "SC TO-I/A":
                results.append({"accession": acc, "filed_date": dt, "primary_doc": doc})

    # Recent filings block
    recent = data.get("filings", {}).get("recent", {})
    if recent:
        _extract_from_block(recent)

    # Older filing bundles
    for file_meta in data.get("filings", {}).get("files", []):
        file_name = file_meta.get("name")
        if not file_name:
            continue
        try:
            file_url = "https://data.sec.gov/submissions/{}".format(file_name)
            resp2 = _sec_get(session, file_url)
            _extract_from_block(resp2.json())
            time.sleep(0.1)
        except Exception:
            pass

    return results


# ---------------------------------------------------------------------------
# Clearing price extraction from SC TO-I/A amendments
# ---------------------------------------------------------------------------

# Pattern 1: explicit "clearing price" mention
_CP_CLEARING = re.compile(
    r'clearing\s+price.{0,200}?\$\s*(\d{1,4}(?:\.\d{1,2})?)',
    re.IGNORECASE,
)
# Pattern 2: "accepted for payment/purchase ... at [a price of] $X per [Class A] Share"
_CP_ACCEPTED = re.compile(
    r'accepted\s+for\s+(?:payment|purchase).{0,300}?'
    r'at\s+(?:a\s+(?:purchase\s+)?price\s+of\s+)?\$\s*(\d{1,4}(?:\.\d{1,2})?)\s+per\s+(?:\w+\s+)*[Ss]hare',
    re.IGNORECASE,
)
# Pattern 3: "Purchase Price ... determined to be $X"
_CP_DETERMINED = re.compile(
    r'[Pp]urchase\s+[Pp]rice.{0,300}?determined.{0,200}?\$\s*(\d{1,4}(?:\.\d{1,2})?)',
    re.IGNORECASE,
)
# Pattern 4: "[final] purchase price of $X [per share]"  (catches CRAI, CNNE)
_CP_PURCHASE_OF = re.compile(
    r'(?:final\s+)?purchase\s+price\s+of\s+\$\s*(\d{1,4}(?:\.\d{1,2})?)',
    re.IGNORECASE,
)
# Pattern 5: "at a price of $X per [Class A/B/C] Share"  (catches RAMP, AMCX)
_CP_AT_PRICE = re.compile(
    r'at\s+a\s+price\s+of\s+\$\s*(\d{1,4}(?:\.\d{1,2})?)\s+per\s+(?:\w+\s+)*[Ss]hare',
    re.IGNORECASE,
)
# Pattern 6: "acquired/purchased ... at [a price of] $X per [Class A] Share"  (catches RGP)
_CP_ACQUIRED = re.compile(
    r'(?:acquired|purchased).{0,200}?'
    r'at\s+(?:a\s+price\s+of\s+)?\$\s*(\d{1,4}(?:\.\d{1,2})?)\s+per\s+(?:\w+\s+)*[Ss]hare',
    re.IGNORECASE,
)
# Pattern 7: "will pay/purchase $X per [Class A] Share"
_CP_WILL_PAY = re.compile(
    r'will\s+(?:pay|purchase)\s+\$\s*(\d{1,4}(?:\.\d{1,2})?)\s+per\s+(?:\w+\s+)*[Ss]hare',
    re.IGNORECASE,
)
# Pattern 8: generic "$X per [Class A] Share" — last resort
_CP_GENERIC = re.compile(
    r'\$\s*(\d{1,4}(?:\.\d{1,2})?)\s+per\s+(?:\w+\s+)*[Ss]hare',
    re.IGNORECASE,
)


def extract_clearing_price(html):
    """
    Extract the final clearing price from an SC TO-I/A amendment HTML.
    Returns (price: float, source: str) or (None, None).
    """
    text = _strip_html(html)

    for pat, label in [
        (_CP_CLEARING,    "amendment"),
        (_CP_ACCEPTED,    "amendment"),
        (_CP_DETERMINED,  "amendment"),
        (_CP_PURCHASE_OF, "amendment"),
        (_CP_AT_PRICE,    "amendment"),
        (_CP_ACQUIRED,    "amendment"),
        (_CP_WILL_PAY,    "amendment"),
        (_CP_GENERIC,     "amendment"),
    ]:
        m = pat.search(text)
        if m:
            p = float(m.group(1))
            if _valid_price(p):
                return p, label

    return None, None


_TERMINATION_RE = re.compile(
    r'terminated?\s+the\s+(?:tender\s+)?offer|termination\s+of\s+(?:the\s+)?(?:tender\s+)?offer',
    re.IGNORECASE,
)


def _fetch_exhibit_htmls(session, cik, agent_cik, adsh, clean_adsh):
    """
    Return HTML text for each EX-99.* exhibit document in the filing index.
    These are press release exhibits that often contain the actual clearing price
    when the primary SC TO-I/A document is just a stub incorporating by reference.
    Falls back to parsing the HTML index for older filings where -index.json is absent.
    """
    import re as _re

    documents = _try_index(session, cik, adsh, clean_adsh)
    active_cik = cik
    if documents is None and agent_cik and agent_cik != cik:
        documents = _try_index(session, agent_cik, adsh, clean_adsh)
        active_cik = agent_cik

    exhibit_urls = []

    if documents:
        for doc in documents:
            doc_type = doc.get("type", "").upper()
            url = doc.get("documentUrl", "")
            if doc_type.startswith("EX-99") and url.endswith((".htm", ".html", ".txt")):
                exhibit_urls.append(url)
    else:
        # Older filings lack -index.json; parse the HTML index instead
        for try_cik in ([cik] + ([agent_cik] if agent_cik and agent_cik != cik else [])):
            html_idx_url = "{}/{}/{}/{}-index.htm".format(
                EDGAR_ARCHIVES, try_cik, clean_adsh, adsh)
            try:
                resp = session.get(html_idx_url, timeout=20)
                if not resp.ok:
                    continue
                # Index HTML has href before type: <a href="/Archives/...htm">...</a></td><td>EX-99...</td>
                hrefs = _re.findall(
                    r'href="(/Archives/edgar/data/[^"]+\.htm)"[^>]*>[^<]*</a></td>\s*<td[^>]*>\s*EX-99',
                    resp.text, _re.IGNORECASE,
                )
                if hrefs:
                    exhibit_urls = ["https://www.sec.gov" + h for h in hrefs]
                    break
                time.sleep(0.15)
            except Exception:
                pass

    exhibits = []
    for url in exhibit_urls:
        try:
            resp = _sec_get(session, url)
            exhibits.append(resp.text)
            time.sleep(0.15)
        except Exception:
            pass
    return exhibits


def find_clearing_price_from_submissions(session, cik, expiry, price_lower, price_upper):
    """
    Use the EDGAR submissions API to find SC TO-I/A filings for this CIK
    filed within 90 days after the offer expiry, then extract the clearing price.
    When the primary document yields nothing, searches EX-99 press release exhibits.
    Validates the extracted price is within the offer range (±5% slack).
    Returns (clearing_price, source, amendment_link) or (None, None, None).
    """
    padded_cik = str(int(cik)).zfill(10)
    amendments = _fetch_submissions_amendments(session, padded_cik)

    expiry_str = expiry.isoformat()
    limit_str = (expiry + timedelta(days=90)).isoformat()

    candidates = [
        a for a in amendments
        if expiry_str <= a["filed_date"] <= limit_str
    ]
    candidates.sort(key=lambda x: x["filed_date"])

    # Acceptable range: [lower_bound * 0.95, upper_bound * 1.05]
    lo = (price_lower if price_lower is not None else price_upper * 0.5) * 0.95
    hi = price_upper * 1.05

    for cand in candidates:
        adsh = cand["accession"]                    # e.g. "0001193125-18-324979"
        clean_adsh = adsh.replace("-", "")          # e.g. "000119312518324979"
        # Agent CIK is the numeric prefix of the accession number
        agent_cik = str(int(adsh.split("-")[0]))

        html = fetch_primary_document(
            session, cik, adsh, clean_adsh,
            agent_cik=agent_cik,
            direct_filename=cand["primary_doc"] or None,
        )
        if not html:
            time.sleep(0.15)
            continue

        # Detect terminated offers — stop searching this filing and move on
        if _TERMINATION_RE.search(_strip_html(html)):
            print("  [SKIP] Offer terminated per amendment — no clearing price", file=sys.stderr)
            return None, None, None

        # Try primary document first
        price, source = extract_clearing_price(html)
        if price is not None and lo <= price <= hi:
            link = "{}/{}/{}/".format(EDGAR_ARCHIVES, cik, clean_adsh)
            return price, source, link
        if price is not None:
            print("  [WARN] Amendment clearing ${} outside offer range [${}, ${}] — skipping".format(
                price, price_lower, price_upper), file=sys.stderr)

        # Primary doc yielded nothing valid — try EX-99 press release exhibits
        for ex_html in _fetch_exhibit_htmls(session, cik, agent_cik, adsh, clean_adsh):
            if _TERMINATION_RE.search(_strip_html(ex_html)):
                print("  [SKIP] Offer terminated per exhibit — no clearing price", file=sys.stderr)
                return None, None, None
            price, source = extract_clearing_price(ex_html)
            if price is not None and lo <= price <= hi:
                link = "{}/{}/{}/".format(EDGAR_ARCHIVES, cik, clean_adsh)
                return price, source, link
            if price is not None:
                print("  [WARN] Exhibit clearing ${} outside offer range [${}, ${}] — skipping".format(
                    price, price_lower, price_upper), file=sys.stderr)

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
                # Fixed-price offer: clearing price = offer price (known)
                clearing_price = offer["price_upper"]
                clearing_source = "fixed"
                amendment_link = None
            else:
                # Dutch auction: look up actual clearing price via EDGAR submissions API
                clearing_price, clearing_source, amendment_link = \
                    find_clearing_price_from_submissions(
                        session, cik, expiry, offer["price_lower"], offer["price_upper"])
                time.sleep(0.15)

                if clearing_price is None:
                    # No confirmed clearing price — skip this trade rather than estimate
                    print("  [SKIP] Dutch auction {} — no amendment clearing price found".format(
                        ticker), file=sys.stderr)
                    skipped += 1
                    continue

            # Strategy rule: only enter if stock was trading below the offer price at T-1.
            # If stock was already above or at the offer ceiling, the trade makes no sense
            # (odd-lot holders would not benefit — offer likely to be withdrawn or repriced).
            if t1_price >= offer["price_upper"]:
                print("  [SKIP] {} T-1 ${} >= offer ${} — stock above range, no entry".format(
                    ticker, t1_price, offer["price_upper"]), file=sys.stderr)
                skipped += 1
                continue

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

    # Deduplicate by (ticker, expiry) — multiple EDGAR hits can map to the
    # same underlying offer. Prefer "fixed" over "amendment" source; otherwise
    # keep the entry with the highest clearing price (most conservative).
    source_rank = {"fixed": 0, "amendment": 1}
    seen: dict[tuple, dict] = {}
    for t in trades:
        key = (t["ticker"], t["expiry"])
        if key not in seen:
            seen[key] = t
        else:
            existing = seen[key]
            if (source_rank.get(t["clearing_price_source"], 2) <
                    source_rank.get(existing["clearing_price_source"], 2)):
                seen[key] = t  # better source
    trades = list(seen.values())

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
