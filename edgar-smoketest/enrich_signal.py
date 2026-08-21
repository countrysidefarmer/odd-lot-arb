"""Literature-grounded enrichment for the insider-buy signal, EDGAR-only.

- Routine vs opportunistic (Cohen, Malloy & Pomorski 2012): pull each insider's
  full Form 4 filing history from data.sec.gov/submissions and flag insiders who
  trade in the same calendar month across the prior 3 years as "routine".
- Small-cap tilt: market cap ~= insider purchase price x shares outstanding,
  shares from data.sec.gov XBRL company-concept (open-market P fills near market).
"""
import json
from collections import defaultdict

SUBMISSIONS = "https://data.sec.gov/submissions/CIK{cik:010d}.json"
CONCEPT = "https://data.sec.gov/api/xbrl/companyconcept/CIK{cik:010d}/{taxo}/{tag}.json"
FACTS = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik:010d}.json"
SHARE_CONCEPTS = [("dei", "EntityCommonStockSharesOutstanding"),
                  ("us-gaap", "CommonStockSharesOutstanding"),
                  ("us-gaap", "CommonStockSharesIssued")]


def _latest_from_body(body):
    if not body:
        return None, None
    vals = [x for u in body.get("units", {}).values() for x in u
            if x.get("val") and x.get("end")]
    if not vals:
        return None, None
    last = max(vals, key=lambda x: x["end"])
    return last["val"], last["end"]


def _cik_int(cik):
    try:
        return int(str(cik).lstrip("0") or "0")
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Routine vs opportunistic
# ---------------------------------------------------------------------------
def insider_months_by_year(client, cik):
    """Return {year: set(month)} of the insider's Form 4/4A filings, or None."""
    n = _cik_int(cik)
    if n is None:
        return None
    url = SUBMISSIONS.format(cik=n)
    text, _ = client.fetch(url, f"{client.cache_dir}/submissions_{n:010d}.json")
    if text is None:
        return None
    try:
        rec = json.loads(text).get("filings", {}).get("recent", {})
    except (ValueError, AttributeError):
        return None
    forms = rec.get("form", [])
    dates = rec.get("filingDate", [])
    out = defaultdict(set)
    for f, d in zip(forms, dates):
        if f in ("4", "4/A") and len(d) >= 7:
            out[int(d[:4])].add(int(d[5:7]))
    return dict(out)


def routine_months(months_by_year, buy_year, lookback=5, min_years=3):
    """The insider's habitual trading months: those recurring in >= min_years of
    the traded years within the lookback. None if too little history to judge."""
    if not months_by_year:
        return None
    yrs = [y for y in range(buy_year - lookback, buy_year) if y in months_by_year]
    if len(yrs) < min_years:
        return None
    counts = defaultdict(int)
    for y in yrs:
        for m in months_by_year[y]:
            counts[m] += 1
    return {m for m, c in counts.items() if c >= min_years}


def classify_trade(months_by_year, buy_year, buy_months):
    """Cohen-Malloy-Pomorski, trade-level: a buy in a habitual month is 'routine';
    a buy that breaks the pattern is 'opportunistic' (the informative one). An
    insider with too little history is 'unknown'. If ANY of the insider's scored
    buys is off-pattern, treat the insider as opportunistic."""
    rm = routine_months(months_by_year, buy_year)
    if rm is None:
        return "unknown"
    if any(m not in rm for m in buy_months):
        return "opportunistic"
    return "routine"


def classify_insiders(client, cik_buymonths, buy_year):
    """cik_buymonths: {rptOwnerCik: set(month_int)} of the buys being scored."""
    out = {}
    for cik, months in cik_buymonths.items():
        out[cik] = classify_trade(insider_months_by_year(client, cik), buy_year, months)
    return out


# ---------------------------------------------------------------------------
# Shares outstanding -> market-cap estimate
# ---------------------------------------------------------------------------
def shares_outstanding(client, issuer_cik):
    """Return (shares, as_of_date). Tries the cheap company-concept endpoint,
    then falls back to full company-facts (concept is empty for some large filers)."""
    n = _cik_int(issuer_cik)
    if n is None:
        return None, None
    # 1) cheap per-concept endpoint (works for most small caps)
    for taxo, tag in SHARE_CONCEPTS:
        url = CONCEPT.format(cik=n, taxo=taxo, tag=tag)
        text, _ = client.fetch(url, f"{client.cache_dir}/shares_{n:010d}_{tag}.json")
        if text is None:
            continue
        try:
            val, end = _latest_from_body(json.loads(text))
        except ValueError:
            val = None
        if val:
            return val, end
    # 2) fallback: full company-facts (reliable but a larger download)
    text, _ = client.fetch(FACTS.format(cik=n), f"{client.cache_dir}/facts_{n:010d}.json")
    if text is None:
        return None, None
    try:
        facts = json.loads(text).get("facts", {})
    except ValueError:
        return None, None
    for taxo, tag in SHARE_CONCEPTS:
        val, end = _latest_from_body(facts.get(taxo, {}).get(tag))
        if val:
            return val, end
    return None, None


def market_caps(client, issuer_price):
    """issuer_price: {issuerCik: representative_price}. Returns
    {issuerCik: (mktcap, shares, as_of)}."""
    out = {}
    for cik, price in issuer_price.items():
        shares, asof = shares_outstanding(client, cik)
        mc = shares * price if (shares and price) else None
        out[cik] = (mc, shares, asof)
    return out
