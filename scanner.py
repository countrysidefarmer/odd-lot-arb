"""
SEC EDGAR Odd-Lot Tender Offer Scanner
Queries EDGAR for SC TO-I filings with "odd lot" provisions filed in the last 7 days,
extracts offer details, filters to exchange-listed opportunities, and emails a summary.
"""

from __future__ import annotations

import html as html_lib
import json
import os
import pathlib
import re
import smtplib
import ssl
import sys
import time
from datetime import date, datetime, timedelta
from email.mime.text import MIMEText

import requests

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

EDGAR_SEARCH_URL = "https://efts.sec.gov/LATEST/search-index"
EDGAR_ARCHIVES = "https://www.sec.gov/Archives/edgar/data"
YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"

VALID_EXCHANGES = {"NYQ", "NMS", "NGM", "NCM", "ASE"}
EXCHANGE_DISPLAY = {
    "NYQ": "NYSE",
    "NMS": "NASDAQ",
    "NGM": "NASDAQ",
    "NCM": "NASDAQ",
    "ASE": "AMEX",
}

# Price patterns (tried in order; first match wins)
_RANGE_TO = re.compile(
    r"\$\s*(\d{1,4}(?:\.\d{1,2})?)\s+to\s+\$\s*(\d{1,4}(?:\.\d{1,2})?)\s+per\s+[Ss]hare",
    re.IGNORECASE,
)
_RANGE_NOT_GT = re.compile(
    r"not\s+(?:greater|more)\s+than\s+\$\s*(\d{1,4}(?:\.\d{1,2})?)"
    r"\s+nor\s+less\s+than\s+\$\s*(\d{1,4}(?:\.\d{1,2})?)",
    re.IGNORECASE,
)
_RANGE_BETWEEN = re.compile(
    r"between\s+\$\s*(\d{1,4}(?:\.\d{1,2})?)\s+and\s+\$\s*(\d{1,4}(?:\.\d{1,2})?)"
    r"\s+per\s+[Ss]hare",
    re.IGNORECASE,
)
_FIXED = re.compile(
    r"\$\s*(\d{1,4}(?:\.\d{1,2})?)\s+per\s+[Ss]hare",
    re.IGNORECASE,
)
# Month name group reused across patterns
_MONTH = (r"(?:January|February|March|April|May|June|July|August|"
          r"September|October|November|December)")
_DATE_GROUP = r"({month}\s+\d{{1,2}}\s*,\s*\d{{4}})".format(month=_MONTH)

# All patterns use .{{0,N}}? (any char, lazy) — NOT [^.] — so "5:00 P.M." dots
# don't stop the match before the date is reached.

# Pattern 1: "EXPIRE … ON [weekday,] DATE"  (handles "P.M., CITY TIME, ON DATE")
_EXPIRY = re.compile(
    r"expir\w*.{{0,300}}?\bon\s+(?:(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),?\s+)?"
    .format() + _DATE_GROUP,
    re.IGNORECASE,
)
# Pattern 2: "PRORATION PERIOD … EXPIRE … DATE"  (YEXT-style all-caps header)
_EXPIRY_PRORATION = re.compile(
    r"PRORATION\s+PERIOD.{{0,400}}?" .format() + _DATE_GROUP,
    re.IGNORECASE,
)
# Pattern 3: "Expiration Date/Time" as a defined term followed by date
_EXPIRY_DEF = re.compile(
    r"Expiration\s+(?:Date|Time).{{0,300}}?".format() + _DATE_GROUP,
    re.IGNORECASE,
)
# Pattern 4: fallback — "offer … expire … DATE" with wide window
_EXPIRY_FALLBACK = re.compile(
    r"\boffer\b.{{0,400}}?expir\w*.{{0,300}}?\bon\s+".format() + _DATE_GROUP,
    re.IGNORECASE,
)
_TICKER_RE = re.compile(r"\(([A-Z]{1,5})\)\s*\(CIK")


# ---------------------------------------------------------------------------
# 1. Date range
# ---------------------------------------------------------------------------

def get_date_range(days=7):
    today = date.today()
    start = today - timedelta(days=days)
    return start.isoformat(), today.isoformat()


# ---------------------------------------------------------------------------
# 2. EDGAR search
# ---------------------------------------------------------------------------

def _make_session():
    session = requests.Session()
    email_from = os.environ.get("EMAIL_FROM", "scanner@example.com")
    session.headers.update({
        "User-Agent": "OddLotScanner/1.0 ({})".format(email_from),
        "Accept-Encoding": "gzip, deflate",
    })
    return session


def _sec_get(session, url, **kwargs):
    """GET with retry on 429 and 500."""
    for attempt in range(4):
        resp = session.get(url, timeout=30, **kwargs)
        if resp.status_code == 429:
            time.sleep(2 ** attempt * 3)
            continue
        if resp.status_code == 500:
            wait = 2 ** attempt * 5  # 5s, 10s, 20s, 40s
            print("[WARN] EDGAR 500, retrying in {}s (attempt {}/4)".format(wait, attempt + 1),
                  file=sys.stderr)
            time.sleep(wait)
            continue
        resp.raise_for_status()
        return resp
    resp.raise_for_status()
    return resp


def search_edgar(start, end):
    session = _make_session()
    hits = []
    offset = 0
    page_size = 100

    while True:
        params = {
            "q": '"odd lot"',
            "forms": "SC TO-I",
            "dateRange": "custom",
            "startdt": start,
            "enddt": end,
            "from": offset,
        }
        resp = _sec_get(session, EDGAR_SEARCH_URL, params=params)
        data = resp.json()

        page_hits = data.get("hits", {}).get("hits", [])
        total = data.get("hits", {}).get("total", {}).get("value", 0)

        hits.extend(page_hits)

        if len(hits) >= total or not page_hits:
            break

        offset += page_size
        time.sleep(0.15)

    return hits


# ---------------------------------------------------------------------------
# 3. Parse hit metadata
# ---------------------------------------------------------------------------

def parse_hit(hit):
    src = hit.get("_source", {})
    if not src:
        return None

    adsh = src.get("adsh", "")
    if not adsh:
        return None

    ciks = src.get("ciks", [])
    if not ciks:
        return None
    cik = str(int(ciks[0]))  # primary: company CIK from search metadata
    # secondary: filer CIK embedded in accession number (used when filing agent submitted)
    agent_cik = str(int(adsh.split("-")[0]))

    display_names = src.get("display_names", [])
    display = display_names[0] if display_names else ""

    m = _TICKER_RE.search(display)
    ticker = m.group(1) if m else None

    company_name = display.split(" (")[0].strip() if display else ""

    clean_adsh = adsh.replace("-", "")
    filing_link = "{}/{}/{}/".format(EDGAR_ARCHIVES, cik, clean_adsh)

    # Extract filename directly from _id ("accessionNo:filename.htm") for fallback fetching
    hit_id = hit.get("_id", "")
    direct_filename = hit_id.split(":")[-1] if ":" in hit_id else None

    filed_date_str = src.get("file_date", "")
    try:
        filed_date = datetime.strptime(filed_date_str, "%Y-%m-%d").date()
    except (ValueError, TypeError):
        filed_date = None

    return {
        "adsh": adsh,
        "clean_adsh": clean_adsh,
        "cik": cik,
        "agent_cik": agent_cik,
        "ticker": ticker,
        "company_name": company_name,
        "filing_link": filing_link,
        "direct_filename": direct_filename,
        "filed_date": filed_date,
    }


# ---------------------------------------------------------------------------
# 4. Fetch primary document
# ---------------------------------------------------------------------------

def _try_index(session, cik, adsh, clean_adsh):
    """Try fetching the filing index JSON for a given CIK. Returns document list or None."""
    index_url = "{}/{}/{}/{}-index.json".format(EDGAR_ARCHIVES, cik, clean_adsh, adsh)
    try:
        resp = session.get(index_url, timeout=30)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        time.sleep(0.15)
        return resp.json().get("documents", [])
    except Exception:
        return None


def fetch_primary_document(session, cik, adsh, clean_adsh, agent_cik=None, direct_filename=None):
    # Try company CIK first, then filing-agent CIK
    documents = _try_index(session, cik, adsh, clean_adsh)
    active_cik = cik
    if documents is None and agent_cik and agent_cik != cik:
        documents = _try_index(session, agent_cik, adsh, clean_adsh)
        active_cik = agent_cik

    primary_url = None

    if documents is not None:
        for doc in documents:
            if doc.get("type", "").upper() == "SC TO-I":
                primary_url = doc.get("documentUrl")
                break
        if not primary_url:
            for doc in sorted(documents, key=lambda d: int(d.get("sequence", 999))):
                url = doc.get("documentUrl", "")
                if url.endswith((".htm", ".html", ".txt")):
                    primary_url = url
                    break

    # Last resort: use the filename from the search hit _id directly
    if not primary_url and direct_filename and direct_filename.endswith((".htm", ".html", ".txt")):
        primary_url = "{}/{}/{}/{}".format(EDGAR_ARCHIVES, cik, clean_adsh, direct_filename)

    if not primary_url:
        print("[WARN] No document found for {}".format(adsh), file=sys.stderr)
        return None

    try:
        resp = _sec_get(session, primary_url)
        time.sleep(0.15)
        return resp.text
    except Exception as e:
        print("[WARN] Document fetch failed for {}: {}".format(adsh, e), file=sys.stderr)
        return None


# ---------------------------------------------------------------------------
# 5. Extract offer details from filing HTML
# ---------------------------------------------------------------------------

def _strip_html(raw):
    text = re.sub(r"<[^>]+>", " ", raw)
    text = html_lib.unescape(text)
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r"\s+,", ",", text)   # "April 1 , 2026" → "April 1, 2026"
    text = re.sub(r",\s{2,}", ", ", text)  # normalise post-comma spacing
    return text


def _parse_date(raw_str):
    """Parse a date string that may have irregular spacing around the comma."""
    s = raw_str.strip()
    s = re.sub(r"\s*,\s*", ", ", s)   # normalise comma spacing
    s = re.sub(r"\s+", " ", s)
    try:
        return datetime.strptime(s, "%B %d, %Y").date()
    except ValueError:
        return None


def _valid_price(p):
    return 0.10 < p < 10_000


def extract_offer_details(html):
    text = _strip_html(html)
    price_lower = price_upper = None
    expiry = None

    m = _RANGE_TO.search(text)
    if m:
        lo, hi = float(m.group(1)), float(m.group(2))
        if _valid_price(lo) and _valid_price(hi):
            price_lower, price_upper = min(lo, hi), max(lo, hi)

    if price_upper is None:
        m = _RANGE_NOT_GT.search(text)
        if m:
            hi, lo = float(m.group(1)), float(m.group(2))
            if _valid_price(lo) and _valid_price(hi):
                price_lower, price_upper = min(lo, hi), max(lo, hi)

    if price_upper is None:
        m = _RANGE_BETWEEN.search(text)
        if m:
            lo, hi = float(m.group(1)), float(m.group(2))
            if _valid_price(lo) and _valid_price(hi):
                price_lower, price_upper = min(lo, hi), max(lo, hi)

    if price_upper is None:
        m = _FIXED.search(text)
        if m:
            p = float(m.group(1))
            if _valid_price(p):
                price_lower = price_upper = p

    for pat in (_EXPIRY, _EXPIRY_PRORATION, _EXPIRY_DEF, _EXPIRY_FALLBACK):
        m = pat.search(text)
        if m:
            expiry = _parse_date(m.group(1))
            if expiry:
                break

    return {"price_lower": price_lower, "price_upper": price_upper, "expiry": expiry}


# ---------------------------------------------------------------------------
# 6. Market data via Yahoo Finance API (no yfinance dependency)
# ---------------------------------------------------------------------------

_YAHOO_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept": "application/json",
}


def get_market_data(ticker):
    try:
        url = YAHOO_CHART_URL.format(ticker=ticker)
        resp = requests.get(url, headers=_YAHOO_HEADERS, timeout=15,
                            params={"interval": "1d", "range": "1d"})
        if resp.status_code != 200:
            return None

        data = resp.json()
        result = data.get("chart", {}).get("result")
        if not result:
            return None

        meta = result[0].get("meta", {})
        exchange = meta.get("exchangeName", "")
        if exchange not in VALID_EXCHANGES:
            return None

        price = meta.get("regularMarketPrice")
        if not price or price <= 0:
            return None

        return {
            "exchange": EXCHANGE_DISPLAY.get(exchange, exchange),
            "current_price": round(float(price), 4),
        }
    except Exception:
        return None


# ---------------------------------------------------------------------------
# 7. Profit calculation
# ---------------------------------------------------------------------------

def calculate_profit(price_upper, current_price):
    return 99 * (price_upper - current_price)


# ---------------------------------------------------------------------------
# 8. Email
# ---------------------------------------------------------------------------

def _format_price(lower, upper):
    if lower is None or upper is None:
        return "N/A"
    if lower == upper:
        return "${:.2f}".format(lower)
    return "${:.2f}-${:.2f}".format(lower, upper)


def _format_table(opportunities):
    col_widths = {
        "ticker": 7,
        "exchange": 8,
        "offer": 17,
        "current": 13,
        "profit": 20,
        "filed": 12,
        "expiry": 12,
    }

    header = (
        "{:<{w0}} | {:<{w1}} | {:<{w2}} | {:<{w3}} | {:<{w4}} | {:<{w5}} | {:<{w6}} | {}".format(
            "Ticker", "Exchange", "Offer Price", "Current Price",
            "Max Profit (99 sh)", "Filed", "Expiry", "Filing",
            w0=col_widths["ticker"], w1=col_widths["exchange"],
            w2=col_widths["offer"], w3=col_widths["current"],
            w4=col_widths["profit"], w5=col_widths["filed"],
            w6=col_widths["expiry"],
        )
    )
    sep = "-" * len(header)

    rows = [header, sep]
    for op in opportunities:
        offer_str = _format_price(op["price_lower"], op["price_upper"])
        expiry_str = op["expiry"].isoformat() if op["expiry"] else "Unknown"
        filed_str = op["filed_date"].isoformat() if op.get("filed_date") else "Unknown"
        profit_str = "${:.2f}".format(op["max_profit"])
        current_str = "${:.2f}".format(op["current_price"])

        row = (
            "{:<{w0}} | {:<{w1}} | {:<{w2}} | {:<{w3}} | {:<{w4}} | {:<{w5}} | {:<{w6}} | {}".format(
                op["ticker"], op["exchange"], offer_str, current_str,
                profit_str, filed_str, expiry_str, op["filing_link"],
                w0=col_widths["ticker"], w1=col_widths["exchange"],
                w2=col_widths["offer"], w3=col_widths["current"],
                w4=col_widths["profit"], w5=col_widths["filed"],
                w6=col_widths["expiry"],
            )
        )
        rows.append(row)

    return "\n".join(rows)


def build_and_send_email(opportunities, all_opportunities=None):
    if not opportunities:
        return

    body = _format_table(opportunities)

    email_from = os.environ["EMAIL_FROM"]
    email_to = os.environ["EMAIL_TO"]
    password = os.environ["EMAIL_PASSWORD"]
    host = os.environ["SMTP_HOST"]
    port = int(os.environ["SMTP_PORT"])

    msg = MIMEText(body, "plain")
    msg["Subject"] = "Odd-Lot Tender Offers — {} ({} new/updated)".format(
        date.today().isoformat(), len(opportunities)
    )
    msg["From"] = email_from
    msg["To"] = email_to

    if port == 465:
        ctx = ssl.create_default_context()
        with smtplib.SMTP_SSL(host, port, context=ctx) as server:
            server.login(email_from, password)
            server.sendmail(email_from, [email_to], msg.as_string())
    else:
        with smtplib.SMTP(host, port) as server:
            server.ehlo()
            server.starttls()
            server.login(email_from, password)
            server.sendmail(email_from, [email_to], msg.as_string())

    print("[INFO] Email sent: {} opportunities".format(len(opportunities)), file=sys.stderr)


# ---------------------------------------------------------------------------
# 9. JSON output
# ---------------------------------------------------------------------------

JSON_PATH = pathlib.Path("data/opportunities.json")


def write_json(opportunities):
    """Serialise opportunities to data/opportunities.json for the website."""
    JSON_PATH.parent.mkdir(parents=True, exist_ok=True)

    def _serial(obj):
        if isinstance(obj, date):
            return obj.isoformat()
        raise TypeError("Not serialisable: {}".format(type(obj)))

    payload = {
        "last_updated": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "opportunities": [
            {
                "ticker": op["ticker"],
                "company_name": op["company_name"],
                "exchange": op["exchange"],
                "price_lower": op["price_lower"],
                "price_upper": op["price_upper"],
                "current_price": op["current_price"],
                "max_profit": round(op["max_profit"], 2),
                "expiry": op["expiry"].isoformat() if op["expiry"] else None,
                "filed_date": op["filed_date"].isoformat() if op.get("filed_date") else None,
                "filing_link": op["filing_link"],
            }
            for op in opportunities
        ],
    }
    JSON_PATH.write_text(json.dumps(payload, default=_serial, indent=2))
    print("[INFO] Wrote {} opportunities to {}".format(len(opportunities), JSON_PATH),
          file=sys.stderr)


# ---------------------------------------------------------------------------
# 10. State — track seen opportunities to avoid duplicate emails
# ---------------------------------------------------------------------------

STATE_PATH = pathlib.Path("data/seen_state.json")


def load_state():
    if STATE_PATH.exists():
        return json.loads(STATE_PATH.read_text())
    return {"opportunities": {}}


def save_state(state, opportunities):
    active_adshs = {op["adsh"] for op in opportunities}
    state["opportunities"] = {
        k: v for k, v in state["opportunities"].items()
        if k in active_adshs
    }
    state["last_updated"] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(json.dumps(state, indent=2))


# ---------------------------------------------------------------------------
# 11. Main
# ---------------------------------------------------------------------------

def _pick_offer_document(filenames):
    """
    Given a list of exhibit filenames for a filing, return the one most likely
    to be the Offer to Purchase — the document with the complete price range and
    expiry language.  Priority:
      1. Filename contains 'a1a', 'a-1-a', 'a_1_a', or 'offer' (Offer to Purchase)
      2. Filename contains 'a1b' (Letter of Transmittal — also has good date language)
      3. First filename in the list (fallback)
    """
    if not filenames:
        return None
    for fn in filenames:
        fn_lower = fn.lower()
        if re.search(r'a[-_]?1[-_]?a|offertopur|offer.to.purchase', fn_lower):
            return fn
    for fn in filenames:
        if re.search(r'a[-_]?1[-_]?b', fn.lower()):
            return fn
    return filenames[0]


def _send_test_email():
    email_from = os.environ["EMAIL_FROM"]
    email_to = os.environ["EMAIL_TO"]
    password = os.environ["EMAIL_PASSWORD"]
    host = os.environ["SMTP_HOST"]
    port = int(os.environ["SMTP_PORT"])

    msg = MIMEText(
        "Your odd-lot tender offer scanner is configured correctly.\n\n"
        "You will receive weekly emails every Monday listing actionable opportunities.\n"
        "If no opportunities exist that week, no email is sent.",
        "plain",
    )
    msg["Subject"] = "Odd-Lot Scanner — test email OK ({})".format(date.today().isoformat())
    msg["From"] = email_from
    msg["To"] = email_to

    if port == 465:
        ctx = ssl.create_default_context()
        with smtplib.SMTP_SSL(host, port, context=ctx) as server:
            server.login(email_from, password)
            server.sendmail(email_from, [email_to], msg.as_string())
    else:
        with smtplib.SMTP(host, port) as server:
            server.ehlo()
            server.starttls()
            server.login(email_from, password)
            server.sendmail(email_from, [email_to], msg.as_string())

    print("[INFO] Test email sent to {}".format(email_to), file=sys.stderr)


def main():
    dry_run = "--dry-run" in sys.argv
    test_email = "--test-email" in sys.argv
    output_json = "--json" in sys.argv
    days = 180

    if test_email:
        _send_test_email()
        sys.exit(0)

    if dry_run:
        print("[INFO] Dry-run mode: widening window to 90 days, skipping email", file=sys.stderr)

    start, end = get_date_range(days=days)
    print("[INFO] Scanning EDGAR for SC TO-I filings from {} to {}".format(start, end),
          file=sys.stderr)

    try:
        hits = search_edgar(start, end)
    except Exception as e:
        print("[ERROR] EDGAR search failed after retries: {}".format(e), file=sys.stderr)
        print("[INFO] Skipping scan — site data unchanged", file=sys.stderr)
        sys.exit(0)
    print("[INFO] Found {} raw hits".format(len(hits)), file=sys.stderr)

    session = _make_session()
    opportunities = []

    # Group all EFTS hits by accession number, collecting every document filename.
    # EDGAR returns one hit per exhibit, so a single filing may have many hits.
    filings = {}  # adsh -> {"meta": ..., "filenames": [...]}
    for hit in hits:
        meta = parse_hit(hit)
        if not meta:
            continue
        adsh = meta["adsh"]
        if adsh not in filings:
            filings[adsh] = {"meta": meta, "filenames": []}
        if meta.get("direct_filename"):
            filings[adsh]["filenames"].append(meta["direct_filename"])

    for adsh, filing in filings.items():
        try:
            meta = filing["meta"]
            if not meta["ticker"]:
                print("[SKIP] No ticker for {}".format(meta["company_name"]), file=sys.stderr)
                continue

            # Pick the best document: prefer the Offer to Purchase (a1a / ex99a1a)
            # over other exhibits, since it has the complete price and expiry language.
            best_filename = _pick_offer_document(filing["filenames"])
            meta["direct_filename"] = best_filename

            html = fetch_primary_document(
                session, meta["cik"], meta["adsh"], meta["clean_adsh"],
                agent_cik=meta.get("agent_cik"),
                direct_filename=best_filename,
            )
            if not html:
                print("[SKIP] No document for {}".format(meta["adsh"]), file=sys.stderr)
                continue

            offer = extract_offer_details(html)
            if offer["price_upper"] is None:
                print("[SKIP] No price found for {}".format(meta["ticker"]), file=sys.stderr)
                continue

            time.sleep(0.5)  # Yahoo Finance rate limit buffer
            market = get_market_data(meta["ticker"])
            if not market:
                print("[SKIP] {} not on NYSE/NASDAQ/AMEX or price unavailable".format(
                    meta["ticker"]), file=sys.stderr)
                continue

            # Skip if we know the expiry has already passed
            if offer["expiry"] and offer["expiry"] < date.today():
                print("[SKIP] {} offer already expired ({})".format(
                    meta["ticker"], offer["expiry"]), file=sys.stderr)
                continue

            profit = calculate_profit(offer["price_upper"], market["current_price"])

            opportunities.append({
                "adsh": adsh,
                "ticker": meta["ticker"],
                "company_name": meta["company_name"],
                "exchange": market["exchange"],
                "price_lower": offer["price_lower"],
                "price_upper": offer["price_upper"],
                "current_price": market["current_price"],
                "max_profit": profit,
                "expiry": offer["expiry"],
                "filed_date": meta.get("filed_date"),
                "filing_link": meta["filing_link"],
            })
            print("[OK]   {} — profit ${:.2f} — expiry {}".format(
                meta["ticker"], profit, offer["expiry"]), file=sys.stderr)

        except Exception as e:
            print("[ERROR] {}: {}".format(adsh, e), file=sys.stderr)
            continue

    # Sort: soonest expiry first (None at end), then highest profit
    far_future = date(9999, 12, 31)
    opportunities.sort(key=lambda x: (x["expiry"] or far_future, -x["max_profit"]))
    print("[INFO] {} actionable opportunities found".format(len(opportunities)), file=sys.stderr)

    if output_json:
        write_json(opportunities)

    # Determine which opportunities warrant an email alert.
    state = load_state()
    seen = state.setdefault("opportunities", {})

    to_email = []
    for op in opportunities:
        adsh = op["adsh"]
        entry = seen.get(adsh)
        if entry is None:
            to_email.append(op)
        elif op["max_profit"] >= 100 and not entry.get("threshold_alerted"):
            to_email.append(op)

    for op in opportunities:
        adsh = op["adsh"]
        if adsh not in seen:
            seen[adsh] = {
                "ticker": op["ticker"],
                "first_seen": date.today().isoformat(),
                "threshold_alerted": False,
            }
        if op["max_profit"] >= 100:
            seen[adsh]["threshold_alerted"] = True

    save_state(state, opportunities)

    if dry_run:
        if to_email:
            print("\n[INFO] Would email {} opportunity/ies (new or crossed $100):".format(
                len(to_email)))
            print(_format_table(to_email))
        else:
            print("[INFO] No new opportunities or threshold crossings — no email would be sent.")
    else:
        build_and_send_email(to_email)

    sys.exit(0)


if __name__ == "__main__":
    main()
