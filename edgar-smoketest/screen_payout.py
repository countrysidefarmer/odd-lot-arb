"""Net-payout / share-buyback screen (EDGAR-only, price-free).

Grounded in:
- Pontiff & Woodgate (2008): net stock issuance (NSI) predicts the cross-section
  of returns; firms that shrink share count outperform firms that dilute.
- Ikenberry, Lakonishok & Vermaelen (1995): open-market buyback firms earn
  positive long-run abnormal returns (the buyback signal).

Data source: SEC XBRL *frames* endpoints (one HTTP call per tag/period, each
covering ~2000+ filers), so the whole cross-section is built from a handful of
requests. No prices required — payout is normalized per share.

Signal:
  net_payout           = repurchase + dividends - issuance   ($ returned)
  net_payout_per_share = net_payout / shares_now             (price-free norm.)
  nsi                  = (shares_now - shares_prev)/shares_prev
                         (NEGATIVE = net buyer = good; |nsi|>0.4 => likely
                          stock split -> nsi=None, neither reward nor punish)
  score = z(net_payout_per_share) - z(nsi_if_present)

Missing components are tolerated: a firm is scored on whatever it has.
"""
import json
import math

SCREEN_NAME = "payout"

# detail keys shown by the orchestrator's table
COLUMNS = ["net_payout", "buyback", "issuance", "nsi_pct", "fiscal_year"]

FRAMES = "https://data.sec.gov/api/xbrl/frames/{taxo}/{tag}/{unit}/{period}.json"
TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"

CACHE_DIR = "/Users/thomasfielden/claude-projects/edgar-smoketest/cache"

# tag -> (taxonomy, unit)
FLOW_TAGS = {
    "repurchase": ("us-gaap", "PaymentsForRepurchaseOfCommonStock", "USD"),
    "issuance": ("us-gaap", "ProceedsFromIssuanceOfCommonStock", "USD"),
    "dividends": ("us-gaap", "PaymentsOfDividendsCommonStock", "USD"),
}
SHARES_TAG = ("dei", "EntityCommonStockSharesOutstanding", "shares")

# a fiscal year is "good enough" if the repurchase frame has at least this many
# filers; otherwise fall back to the prior year.
MIN_COVERAGE = 1500
SPLIT_THRESHOLD = 0.4  # |nsi| above this is treated as a split, not issuance


def _build_client():
    from edgar_client import EdgarClient, TokenBucket
    bucket = TokenBucket(rate=10, capacity=10)
    return EdgarClient(bucket, cache_dir=CACHE_DIR)


def _fetch_json(client, url, cache_path):
    text, _ = client.fetch(url, cache_path)
    if text is None:
        return None
    try:
        return json.loads(text)
    except (ValueError, TypeError):
        return None


def _frame(client, taxo, tag, unit, period):
    """Return {cik(int): (val, entityName)} for one XBRL frame, or {}.

    A CIK can appear multiple times in a frame (e.g. overlapping periods for a
    changed fiscal year-end); keep the largest-magnitude value, which is the
    full-year figure for flows and the point-in-time count for shares.
    """
    url = FRAMES.format(taxo=taxo, tag=tag, unit=unit, period=period)
    cache = "{}/frames_{}_{}.json".format(CACHE_DIR, tag, period)
    data = _fetch_json(client, url, cache)
    if not data:
        return {}
    out = {}
    for row in data.get("data", []):
        cik = row.get("cik")
        val = row.get("val")
        if cik is None or val is None:
            continue
        prev = out.get(cik)
        if prev is None or abs(val) > abs(prev[0]):
            out[cik] = (val, row.get("entityName"))
    return out


def _pick_year(client):
    """Choose the most recent complete fiscal year with good coverage."""
    for year in (2025, 2024):
        f = _frame(client, "us-gaap", "PaymentsForRepurchaseOfCommonStock",
                   "USD", "CY{}".format(year))
        if len(f) >= MIN_COVERAGE:
            return year, f
    # nothing well-covered: use the best of what we saw (fall back to 2024)
    return 2024, f


def _load_tickers(client):
    """Return {cik(int): (ticker_upper, title)}."""
    data = _fetch_json(client, TICKERS_URL, "{}/company_tickers.json".format(CACHE_DIR))
    out = {}
    if not data:
        return out
    rows = data.values() if isinstance(data, dict) else data
    for row in rows:
        try:
            cik = int(row.get("cik_str"))
        except (TypeError, ValueError):
            continue
        tkr = row.get("ticker")
        if cik not in out and tkr:  # first (primary) ticker wins
            out[cik] = (str(tkr).upper(), row.get("title"))
    return out


def _zscores(values):
    """values: list of (float | None). Returns list of z (None where input None)."""
    present = [v for v in values if v is not None]
    if len(present) < 2:
        return [0.0 if v is not None else None for v in values]
    mean = sum(present) / len(present)
    var = sum((v - mean) ** 2 for v in present) / len(present)
    sd = math.sqrt(var)
    if sd == 0:
        return [0.0 if v is not None else None for v in values]
    return [((v - mean) / sd) if v is not None else None for v in values]


def run_screen(client=None, **opts):
    if client is None:
        client = _build_client()

    year, repurchase = _pick_year(client)
    prev_year = year - 1

    issuance = _frame(client, *FLOW_TAGS["issuance"], "CY{}".format(year))
    dividends = _frame(client, *FLOW_TAGS["dividends"], "CY{}".format(year))
    shares_now = _frame(client, *SHARES_TAG, "CY{}Q4I".format(year))
    shares_prev = _frame(client, *SHARES_TAG, "CY{}Q4I".format(prev_year))

    tickers = _load_tickers(client)

    # universe: any firm appearing in any payout flow or the current share count
    ciks = set(repurchase) | set(issuance) | set(dividends) | set(shares_now)

    rows = []
    for cik in ciks:
        rep = repurchase.get(cik, (None, None))
        iss = issuance.get(cik, (None, None))
        div = dividends.get(cik, (None, None))
        sn = shares_now.get(cik, (None, None))
        sp = shares_prev.get(cik, (None, None))

        # entityName: prefer whichever frame gave one
        name = (rep[1] or iss[1] or div[1] or sn[1]
                or (tickers.get(cik, (None, None))[1]))

        rep_v = rep[0]
        iss_v = iss[0]
        div_v = div[0]
        shares = sn[0]
        shares_p = sp[0]

        # net payout: use 0 for missing flow components (absent buyback/div/iss
        # in XBRL generally means the firm reported none), but require at least
        # one payout flow to be present so we don't score pure noise.
        if rep_v is None and iss_v is None and div_v is None:
            continue
        net_payout = (rep_v or 0) + (div_v or 0) - (iss_v or 0)

        nps = None
        if shares and shares > 0:
            nps = net_payout / shares

        nsi = None
        if shares and shares_p and shares_p > 0:
            nsi = (shares - shares_p) / shares_p
            if abs(nsi) > SPLIT_THRESHOLD:  # likely a stock split, not issuance
                nsi = None

        rows.append({
            "cik": cik,
            "name": name,
            "nps": nps,
            "nsi": nsi,
            "net_payout": net_payout,
            "buyback": rep_v,
            "issuance": iss_v,
            "shares_now": shares,
        })

    # cross-sectional z-scores
    z_nps = _zscores([r["nps"] for r in rows])
    z_nsi = _zscores([r["nsi"] for r in rows])

    records = []
    for r, znp, zns in zip(rows, z_nps, z_nsi):
        # buying back -> nsi negative -> -z(nsi) positive -> higher score
        score = (znp or 0.0) - (zns or 0.0)
        cik = r["cik"]
        tkr = tickers.get(cik, (None, None))[0]
        records.append({
            "cik": str(cik),  # digits, no leading zeros
            "ticker": tkr,
            "cusip": None,
            "name": r["name"] or (tkr or str(cik)),
            "score": float(score),
            "rank": 0,
            "detail": {
                "net_payout": r["net_payout"],
                "buyback": r["buyback"],
                "issuance": r["issuance"],
                "nsi_pct": (r["nsi"] * 100.0) if r["nsi"] is not None else None,
                "fiscal_year": year,
            },
        })

    records.sort(key=lambda x: x["score"], reverse=True)
    for i, rec in enumerate(records, start=1):
        rec["rank"] = i
    return records


def _fmt_money(v):
    if v is None:
        return "-"
    a = abs(v)
    for div, suf in ((1e12, "T"), (1e9, "B"), (1e6, "M"), (1e3, "K")):
        if a >= div:
            return "{:.2f}{}".format(v / div, suf)
    return "{:.0f}".format(v)


def _print_table(records, n=15):
    print(SCREEN_NAME)
    top = records[:n]
    headers = ["rank", "ticker", "name", "score", "net_payout",
               "buyback", "issuance", "nsi_pct", "fy"]
    table = []
    for r in top:
        d = r["detail"]
        table.append([
            str(r["rank"]),
            r["ticker"] or "-",
            (r["name"] or "")[:28],
            "{:.2f}".format(r["score"]),
            _fmt_money(d["net_payout"]),
            _fmt_money(d["buyback"]),
            _fmt_money(d["issuance"]),
            ("{:.1f}".format(d["nsi_pct"]) if d["nsi_pct"] is not None else "-"),
            str(d["fiscal_year"]),
        ])
    widths = [max(len(headers[i]), max((len(row[i]) for row in table), default=0))
              for i in range(len(headers))]
    line = "  ".join(h.ljust(widths[i]) for i, h in enumerate(headers))
    print(line)
    print("  ".join("-" * widths[i] for i in range(len(headers))))
    for row in table:
        print("  ".join(row[i].ljust(widths[i]) for i in range(len(headers))))


if __name__ == "__main__":
    recs = run_screen()
    _print_table(recs, 15)
    print("\n{} filers ranked".format(len(recs)))
