"""Spin-off screen (Cusatis-Miles-Woolridge 1993).

Newly spun-off companies get dumped by forced/indifferent sellers of the
parent's shareholders, then tend to outperform over the following ~1-2 years.
A spin-off registers its shares with a Form 10 ("10-12B", "10-12B/A",
"10-12G"). We find recent Form 10 filers via EDGAR full-text search, dedup by
CIK keeping the EARLIEST registration (the spin milestone), and score by a
smooth bump function of days-since-registration that peaks in the classic
early forced-selling / drift window (~1-6 months).

EDGAR-only. Reuses edgar_client.EdgarClient. Standalone smoke test at bottom.
"""
import json
import math
import re
import sys
from datetime import date, datetime, timedelta

CACHE_DIR = "/Users/thomasfielden/claude-projects/edgar-smoketest/cache"

SCREEN_NAME = "spinoff"
COLUMNS = ["registration_date", "days_since", "form", "parent", "accession"]

# Form 10 variants that mark a spin-off registration. EDGAR's full-text
# `forms` filter matches by root form, so "10-12B" also returns "10-12B/A".
FORM_ROOTS = ["10-12B", "10-12G"]

FTS_URL = "https://efts.sec.gov/LATEST/search-index"
TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"

# Scoring window (days since registration).
PEAK_LO = 30      # start of peak plateau
PEAK_HI = 180     # end of peak plateau
RAMP_UP = 30      # days over which score ramps from 0 -> 1 before PEAK_LO
TAPER = 240       # days over which score tapers 1 -> 0 after PEAK_HI
MAX_DAYS = PEAK_HI + TAPER  # hard cutoff


def _build_client():
    from edgar_client import EdgarClient, TokenBucket
    bucket = TokenBucket(rate=10, capacity=10)
    return EdgarClient(bucket, cache_dir=CACHE_DIR)


_CIK_RE = re.compile(r"\(CIK\s*(\d{10})\)", re.IGNORECASE)


def _parse_display(display_names, ciks):
    """Return (cik_str, company_name) from a hit's display_names/ciks."""
    name = None
    cik = None
    if display_names:
        dn = display_names[0]
        m = _CIK_RE.search(dn)
        if m:
            cik = m.group(1)
        name = _CIK_RE.sub("", dn).strip().rstrip("(").strip()
    if cik is None and ciks:
        cik = str(ciks[0]).zfill(10)
    return cik, name


def _fetch_form10_hits(client, startdt, enddt):
    """Page through EDGAR full-text search for each Form 10 root; return hits."""
    all_hits = []
    for root in FORM_ROOTS:
        frm = 0
        while True:
            url = (f"{FTS_URL}?q=&forms={root}"
                   f"&startdt={startdt}&enddt={enddt}&from={frm}")
            cache_path = (f"{CACHE_DIR}/spinoff_fts_{root.replace('/', '_')}"
                          f"_{startdt}_{enddt}_{frm}.json")
            text, _ = client.fetch(url, cache_path)
            if not text:
                break
            try:
                data = json.loads(text)
            except (ValueError, TypeError):
                break
            hits = data.get("hits", {}).get("hits", [])
            all_hits.extend(hits)
            total = data.get("hits", {}).get("total", {}).get("value", 0)
            frm += len(hits)
            if not hits or frm >= total or frm >= 1000:  # FTS caps at 1000
                break
    return all_hits


def _load_ticker_map(client):
    """cik(int) -> (ticker upper, title). Best-effort; None on failure."""
    text, _ = client.fetch(TICKERS_URL, f"{CACHE_DIR}/company_tickers.json")
    if not text:
        return {}
    try:
        data = json.loads(text)
    except (ValueError, TypeError):
        return {}
    out = {}
    for row in data.values():
        try:
            out[int(row["cik_str"])] = (
                str(row.get("ticker", "")).upper() or None,
                row.get("title"),
            )
        except (KeyError, ValueError, TypeError):
            continue
    return out


def _score(days_since):
    """Smooth bump: 0 before spin, ramps up, plateaus in the drift window,
    tapers to 0 by MAX_DAYS. Returns a float in [0, 1]."""
    if days_since is None or days_since < 0:
        return 0.0
    if days_since >= MAX_DAYS:
        return 0.0
    if days_since < PEAK_LO:
        # ramp up over the very-recent (not-yet-trading) window using a
        # raised-cosine so it is smooth. Below (PEAK_LO - RAMP_UP) -> partial.
        x = max(0.0, (days_since - (PEAK_LO - RAMP_UP)) / RAMP_UP)
        x = min(1.0, x)
        return 0.5 * (1 - math.cos(math.pi * x))
    if days_since <= PEAK_HI:
        return 1.0
    # taper down after the peak plateau
    x = (days_since - PEAK_HI) / TAPER
    x = min(1.0, max(0.0, x))
    return 0.5 * (1 + math.cos(math.pi * x))


def run_screen(client=None, **opts):
    """Return spin-off records sorted by score desc with rank filled.

    opts:
      lookback_days: window size (default 365)
      asof: 'YYYY-MM-DD' end date (default today)
    """
    if client is None:
        client = _build_client()

    lookback_days = int(opts.get("lookback_days", 365))
    asof_str = opts.get("asof")
    asof = (datetime.strptime(asof_str, "%Y-%m-%d").date()
            if asof_str else date.today())
    startdt = (asof - timedelta(days=lookback_days)).isoformat()
    enddt = asof.isoformat()

    hits = _fetch_form10_hits(client, startdt, enddt)
    ticker_map = _load_ticker_map(client)

    # Dedup by CIK, keep earliest filing (the registration milestone).
    by_cik = {}
    for hit in hits:
        src = hit.get("_source", {})
        cik, name = _parse_display(src.get("display_names"), src.get("ciks"))
        if not cik:
            continue
        fdate = src.get("file_date")
        if not fdate:
            continue
        rec = {
            "cik": cik,
            "name": name,
            "file_date": fdate,
            "form": src.get("form"),
            "accession": src.get("adsh"),
        }
        prev = by_cik.get(cik)
        if prev is None or fdate < prev["file_date"]:
            by_cik[cik] = rec

    records = []
    for cik, rec in by_cik.items():
        try:
            reg = datetime.strptime(rec["file_date"], "%Y-%m-%d").date()
            days_since = (asof - reg).days
        except (ValueError, TypeError):
            days_since = None

        score = _score(days_since)
        tick, title = ticker_map.get(int(cik), (None, None))
        name = rec["name"] or title

        records.append({
            "cik": cik,
            "ticker": tick,
            "cusip": None,
            "name": name or "",
            "score": float(round(score, 6)),
            "rank": 0,
            "detail": {
                "registration_date": rec["file_date"],
                "days_since": days_since,
                "form": rec["form"],
                "parent": None,  # best-effort; not inferred in v1
                "accession": rec["accession"],
            },
        })

    # Sort by score desc, then most-recent registration as tiebreak.
    records.sort(
        key=lambda r: (r["score"], r["detail"]["registration_date"] or ""),
        reverse=True,
    )
    for i, r in enumerate(records, 1):
        r["rank"] = i
    return records


def _print_table(records, screen_name, top=15):
    print(screen_name)
    cols = ["rank", "score", "ticker", "cik", "name",
            "registration_date", "days_since", "form"]
    rows = []
    for r in records[:top]:
        d = r["detail"]
        rows.append([
            str(r["rank"]),
            f"{r['score']:.3f}",
            r["ticker"] or "-",
            r["cik"] or "-",
            (r["name"] or "")[:38],
            d.get("registration_date") or "-",
            str(d.get("days_since")) if d.get("days_since") is not None else "-",
            d.get("form") or "-",
        ])
    widths = [len(c) for c in cols]
    for row in rows:
        for j, cell in enumerate(row):
            widths[j] = max(widths[j], len(cell))
    fmt = "  ".join("{:<" + str(w) + "}" for w in widths)
    print(fmt.format(*cols))
    for row in rows:
        print(fmt.format(*row))


if __name__ == "__main__":
    recs = run_screen()
    _print_table(recs, SCREEN_NAME, top=15)
    print(f"\n{len(recs)} spin-offs found in window", file=sys.stderr)
