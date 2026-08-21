"""Main pipeline: fetch (resumable, rate-limited), parse Form 4 + SC 13D,
enrich, and write form4_purchases.csv / sc13d_filings.csv / shortlist.md."""
import os
import csv
import json
import math
import time
import threading
import argparse
import datetime as dt
from collections import defaultdict, Counter
from concurrent.futures import ThreadPoolExecutor, as_completed

from edgar_client import TokenBucket, EdgarClient
from parsers import (extract_ownership_xml, parse_form4, detect_10b5_1,
                     parse_sc13d, TENB51_ELEMENTS)
from step1_index import business_days, fetch_indexes
from enrich_signal import classify_insiders, market_caps
import screens as screens_mod

BASE = os.path.dirname(os.path.abspath(__file__))


def to_float(s):
    if s is None:
        return None
    try:
        return float(str(s).replace(",", "").strip())
    except ValueError:
        return None


def seniority(rec):
    title = (rec.get("officerTitle") or "").lower()
    if "chief executive" in title or "ceo" in title.split():
        return "CEO"
    if "chief financial" in title or "cfo" in title.split():
        return "CFO"
    if rec.get("isOfficer"):
        return "other-officer"
    if rec.get("isDirector"):
        return "director"
    return "other"


# ---------------------------------------------------------------------------
# Fetch + parse (concurrent)
# ---------------------------------------------------------------------------
def process_all(rows, client, workers=8):
    form4_ok, form4_fail = [], 0
    sc13d_ok, sc13d_fail = [], 0
    done = 0
    lock = threading.Lock()
    stats = Counter()

    def work(r):
        try:
            text, _ = client.fetch(r["url"], f"{BASE}/cache/{r['accession']}.txt")
        except Exception as e:  # don't let one bad filing kill the batch
            return ("fetch_error", r, str(e))
        if text is None:
            return ("fetch_none", r, None)
        if r["form_class"] == "4":
            xml = extract_ownership_xml(text)
            if not xml:
                return ("f4_noxml", r, None)
            rec = parse_form4(xml)
            if rec is None:
                return ("f4_badxml", r, None)
            flag, src = detect_10b5_1(xml, TENB51_ELEMENTS)
            rec["is_10b5_1"] = flag
            rec["flag_source"] = src
            rec["accession"] = r["accession"]
            rec["index_date"] = r.get("index_date")
            return ("f4_ok", r, rec)
        else:
            rec = parse_sc13d(text)
            rec["form_class"] = r["form_class"]
            rec["index_date"] = r.get("index_date")
            if not rec.get("accession"):
                rec["accession"] = r["accession"]
            return ("sc13d_ok", r, rec)

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = [ex.submit(work, r) for r in rows]
        for fut in as_completed(futs):
            kind, r, rec = fut.result()
            with lock:
                done += 1
                stats[kind] += 1
                if kind == "f4_ok":
                    form4_ok.append(rec)
                elif kind == "sc13d_ok":
                    sc13d_ok.append(rec)
                elif kind.startswith("f4") or kind == "fetch_none" and r["form_class"] == "4":
                    form4_fail += 1
                if done % 500 == 0:
                    print(f"  ...processed {done}/{len(rows)}  "
                          f"(net={client.net_hits} cache={client.cache_hits} "
                          f"404={client.not_found})", flush=True)

    # recount failures cleanly by form class
    f4_total = sum(1 for r in rows if r["form_class"] == "4")
    sc_total = sum(1 for r in rows if r["form_class"] != "4")
    return {
        "client": client, "stats": stats,
        "form4_ok": form4_ok, "sc13d_ok": sc13d_ok,
        "f4_total": f4_total, "sc_total": sc_total,
    }


# ---------------------------------------------------------------------------
# Enrichment
# ---------------------------------------------------------------------------
def build_purchases(form4_ok):
    """Flatten to one row per open-market purchase (code P, acquired A)."""
    purchases = []
    for rec in form4_ok:
        for t in rec["transactions"]:
            if t["transactionCode"] != "P" or t["acquiredDisposedCode"] != "A":
                continue
            shares = to_float(t["transactionShares"])
            price = to_float(t["transactionPricePerShare"])
            owned_after = to_float(t["sharesOwnedFollowingTransaction"])
            trade_value = shares * price if (shares is not None and price is not None) else None
            pct_inc = None
            if shares is not None and owned_after is not None:
                before = owned_after - shares
                if before > 0:
                    pct_inc = shares / before
            purchases.append({
                "accession": rec["accession"],
                "index_date": rec.get("index_date"),
                "issuerCik": rec["issuerCik"],
                "issuerName": rec["issuerName"],
                "issuerTradingSymbol": rec["issuerTradingSymbol"],
                "rptOwnerCik": rec["rptOwnerCik"],
                "rptOwnerName": rec["rptOwnerName"],
                "isDirector": rec["isDirector"],
                "isOfficer": rec["isOfficer"],
                "isTenPercentOwner": rec["isTenPercentOwner"],
                "officerTitle": rec["officerTitle"],
                "seniority": seniority(rec),
                "is_10b5_1": rec["is_10b5_1"],
                "flag_source": rec["flag_source"],
                "transactionDate": t["transactionDate"],
                "transactionCode": t["transactionCode"],
                "acquiredDisposedCode": t["acquiredDisposedCode"],
                "transactionShares": shares,
                "transactionPricePerShare": price,
                "sharesOwnedFollowingTransaction": owned_after,
                "trade_value": trade_value,
                "pct_holding_increase": pct_inc,
            })
    _add_cluster_counts(purchases)
    return purchases


def _parse_date(s):
    try:
        return dt.datetime.strptime(s, "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None


def _add_cluster_counts(purchases):
    """cluster_count = distinct rptOwnerCik with a P purchase at the same issuer
    within a trailing 30-day window ending at this transaction's date."""
    by_issuer = defaultdict(list)
    for p in purchases:
        by_issuer[p["issuerCik"]].append(p)
    for issuer, plist in by_issuer.items():
        dated = [(p, _parse_date(p["transactionDate"])) for p in plist]
        for p, d in dated:
            if d is None:
                p["cluster_count"] = None
                continue
            owners = set()
            for q, qd in dated:
                if qd is not None and dt.timedelta(0) <= (d - qd) <= dt.timedelta(days=30):
                    owners.add(q["rptOwnerCik"])
            p["cluster_count"] = len(owners)


# ---------------------------------------------------------------------------
# Output writers
# ---------------------------------------------------------------------------
PURCHASE_COLS = [
    "accession", "index_date", "issuerCik", "issuerName", "issuerTradingSymbol",
    "rptOwnerCik", "rptOwnerName", "isDirector", "isOfficer", "isTenPercentOwner",
    "officerTitle", "seniority", "is_10b5_1", "flag_source", "transactionDate",
    "transactionCode", "acquiredDisposedCode", "transactionShares",
    "transactionPricePerShare", "sharesOwnedFollowingTransaction", "trade_value",
    "pct_holding_increase", "cluster_count",
]

SC13D_COLS = [
    "accession", "form_class", "date_filed", "subject_name", "subject_cik",
    "filer_name", "filer_cik", "cusip", "pct_of_class", "parse_mode", "item4",
]


def write_csv(path, cols, rows):
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)


def write_shortlist(path, purchases, sc13d_ok):
    # qualifying transactions
    qual = []
    for p in purchases:
        flag_ok = p["is_10b5_1"] in (False, None)  # false or absent
        role_ok = (p["isOfficer"] or p["isDirector"]) and not p["isTenPercentOwner"]
        val_ok = p["trade_value"] is not None and p["trade_value"] > 100000
        clust_ok = p["cluster_count"] is not None and p["cluster_count"] >= 2
        if flag_ok and role_ok and val_ok and clust_ok:
            qual.append(p)

    by_issuer = defaultdict(list)
    for p in qual:
        by_issuer[p["issuerCik"]].append(p)

    issuers = []
    for cik, plist in by_issuer.items():
        issuers.append({
            "issuerCik": cik,
            "issuerName": plist[0]["issuerName"],
            "ticker": plist[0]["issuerTradingSymbol"],
            "cluster_count": max(p["cluster_count"] for p in plist),
            "total_trade_value": sum(p["trade_value"] for p in plist),
            "insiders": sorted({p["rptOwnerName"] for p in plist}),
            "n_txn": len(plist),
        })
    issuers.sort(key=lambda x: (-x["cluster_count"], -x["total_trade_value"]))

    lines = []
    lines.append("# Form 4 Cluster-Buy Shortlist (5 business-day smoke test)\n")
    lines.append("**Scope caveat:** This is a one-month/5-day exploratory run. The "
                 "routine-vs-opportunistic *calendar* classification is deliberately "
                 "OUT OF SCOPE — it needs ~3y of per-insider trade history, which one "
                 "month cannot support. The **10b5-1 flag (`aff10b5One`) is the only "
                 "routine proxy used here.** Market-cap / liquidity filtering is also "
                 "out of scope (EDGAR has no price data); tickers are left raw for "
                 "Bloomberg cross-reference.\n")
    lines.append("**Filters (ALL required):** 10b5-1 flag false/absent · "
                 "(officer OR director) AND NOT 10% owner · cluster_count ≥ 2 · "
                 "trade_value > $100,000. Ranked by cluster_count desc, then total "
                 "trade_value desc.\n")
    lines.append(f"**Issuers matching: {len(issuers)}**\n")

    if issuers:
        lines.append("| # | Issuer | Ticker | Cluster | Total $ | Txns | Insiders |")
        lines.append("|---|--------|--------|---------|---------|------|----------|")
        for i, it in enumerate(issuers, 1):
            insiders = "; ".join(it["insiders"])[:120]
            lines.append(
                f"| {i} | {it['issuerName']} | {it['ticker'] or '-'} | "
                f"{it['cluster_count']} | ${it['total_trade_value']:,.0f} | "
                f"{it['n_txn']} | {insiders} |")
    else:
        lines.append("_No issuers met all criteria in this window._")

    # SC 13D originals (not /A)
    origs = [s for s in sc13d_ok if s.get("form_class") == "SC 13D"]
    lines.append(f"\n---\n\n## SC 13D filings (originals only, not /A): {len(origs)}\n")
    for s in origs:
        snippet = (s.get("item4") or "").replace("\n", " ").strip()
        snippet = (snippet[:400] + "…") if len(snippet) > 400 else snippet
        lines.append(f"- **{s.get('filer_name') or '?'}** → subject "
                     f"*{s.get('subject_name') or '?'}* "
                     f"(CUSIP {s.get('cusip') or 'n/a'}, "
                     f"{s.get('pct_of_class') or '?'}% of class)")
        lines.append(f"  - Item 4: {snippet or '_(not extracted)_'}")

    with open(path, "w") as f:
        f.write("\n".join(lines) + "\n")
    return issuers


# ---------------------------------------------------------------------------
# Signal ranking: keep only genuine open-market insider buys, score & rank.
# This bakes in the noise-filtering worked out by hand: drop PIPE/financing
# (same price shared by 2+ owners), SPAC founder / no-price rows, and pure
# 10%-owner fund accumulation. Rank the rest by breadth + seniority + size.
# ---------------------------------------------------------------------------
def _rp(p):
    return round(p, 4) if p is not None else None


def _median(xs):
    xs = sorted(x for x in xs if x is not None)
    if not xs:
        return None
    n = len(xs)
    return xs[n // 2] if n % 2 else 0.5 * (xs[n // 2 - 1] + xs[n // 2])


def _clamp(x, lo, hi):
    return max(lo, min(hi, x))


# Literature-grounded weights (see README "Scoring"). Opportunistic tilt is the
# dominant documented factor (Cohen-Malloy-Pomorski 2012); consensus and
# conviction next; small-cap tilt (Lakonishok-Lee) and role secondary. CEO/CFO
# are treated equally (Wang-Shin-Francis: CFO trades >= CEO in informativeness).
W_OPP = 4.0           # per opportunistic insider
W_ROUTINE = -3.0      # per routine insider (penalty; ~0 alpha in CMP)
W_CONSENSUS = 1.5     # per distinct insider, capped
CONSENSUS_CAP = 5
W_CONVICTION = 3.0    # x clamp(median % holdings increase, 0..1)
W_SMALLCAP = 1.0      # x smallcap factor (0..3)
W_ROLE_EXEC = 1.0     # senior officer (CEO/CFO/other-officer) present
W_ROLE_DIR = 0.5      # directors only


def _smallcap_factor(mktcap):
    if not mktcap or mktcap <= 0:
        return 1.0  # unknown -> neutral (don't reward or punish)
    return _clamp(9.5 - math.log10(mktcap), 0.0, 3.0)  # ~micro=3, $1B~0.5, >$3B~0


def classify_and_rank(purchases):
    """Tag every purchase; aggregate genuine ones per issuer (scored later)."""
    # financing-like = same (rounded) price shared by >=2 distinct owners at an issuer
    price_owners = defaultdict(lambda: defaultdict(set))
    for p in purchases:
        pr = p["transactionPricePerShare"]
        if pr:
            price_owners[p["issuerCik"]][_rp(pr)].add(p["rptOwnerCik"])

    for p in purchases:
        pr = p["transactionPricePerShare"]
        role_ok = (p["isOfficer"] or p["isDirector"]) and not p["isTenPercentOwner"]
        financing = bool(pr) and len(price_owners[p["issuerCik"]][_rp(pr)]) >= 2
        p["_genuine"] = bool(role_ok and pr and pr > 0 and not financing)
        if p["_genuine"]:
            p["_class"] = "insider-open-market"
        elif not pr or pr == 0:
            p["_class"] = "no-price (SPAC/plan)"
        elif financing:
            p["_class"] = "uniform-price (PIPE/financing)"
        elif p["isTenPercentOwner"] and not (p["isOfficer"] or p["isDirector"]):
            p["_class"] = "fund / 10%-owner"
        else:
            p["_class"] = "other-excluded"

    by_issuer = defaultdict(list)
    for p in purchases:
        if p["_genuine"]:
            by_issuer[p["issuerCik"]].append(p)

    sig = []
    for cik, ps in by_issuer.items():
        buyers = {p["rptOwnerCik"] for p in ps}
        value = sum(p["trade_value"] or 0 for p in ps)
        sen = {p["seniority"] for p in ps}
        top_sen = ("CEO" if "CEO" in sen else "CFO" if "CFO" in sen else
                   "other-officer" if "other-officer" in sen else "director")
        has_exec = bool({"CEO", "CFO", "other-officer"} & sen)
        nonplan = any(p["is_10b5_1"] in (False, None) for p in ps)
        # conviction = median % increase vs prior holding; first buys (null) -> max (1.0)
        convs = [1.0 if p["pct_holding_increase"] is None else p["pct_holding_increase"]
                 for p in ps]
        conviction = _clamp(_median(convs) or 0.0, 0.0, 1.0)
        rep_price = _median([p["transactionPricePerShare"] for p in ps])
        # months of each insider's genuine buys here (for routine/opportunistic)
        insider_months = defaultdict(set)
        for p in ps:
            d = p["transactionDate"]
            if d and len(d) >= 7:
                insider_months[p["rptOwnerCik"]].add(int(d[5:7]))
        sig.append({
            "cik": cik,
            "ticker": ps[0]["issuerTradingSymbol"] or "-",
            "name": ps[0]["issuerName"] or "?",
            "buyers": len(buyers),
            "insider_ciks": sorted(buyers),
            "insider_months": {k: v for k, v in insider_months.items()},
            "value": value,
            "txns": len(ps),
            "seniority": top_sen,
            "has_exec": has_exec,
            "nonplan": nonplan,
            "conviction": conviction,
            "rep_price": rep_price,
            "insiders": sorted({p["rptOwnerName"] for p in ps}),
        })
    return sig


def enrich_and_score(sig, client, buy_year, do_enrich=True):
    """Attach routine/opportunistic mix + market-cap estimate, score, and sort."""
    insider_class, mcaps = {}, {}
    if do_enrich and sig:
        # merge each insider's scored-buy months across issuers
        cik_buymonths = defaultdict(set)
        for s in sig:
            for c, months in s["insider_months"].items():
                cik_buymonths[c] |= months
        print(f"  enriching: {len(cik_buymonths)} insiders (routine/opportunistic) + "
              f"{len(sig)} issuers (market cap) via data.sec.gov ...", flush=True)
        insider_class = classify_insiders(client, dict(cik_buymonths), buy_year)
        mcaps = market_caps(client, {s["cik"]: s["rep_price"] for s in sig})

    for s in sig:
        cls = [insider_class.get(c, "unknown") for c in s["insider_ciks"]]
        s["opp"] = cls.count("opportunistic")
        s["routine"] = cls.count("routine")
        s["unknown"] = cls.count("unknown")
        mc, shares, asof = mcaps.get(s["cik"], (None, None, None))
        s["mktcap"] = mc
        s["mktcap_asof"] = asof
        s["score"] = round(
            W_OPP * s["opp"] + W_ROUTINE * s["routine"]
            + W_CONSENSUS * min(s["buyers"], CONSENSUS_CAP)
            + W_CONVICTION * s["conviction"]
            + W_SMALLCAP * _smallcap_factor(mc)
            + (W_ROLE_EXEC if s["has_exec"] else W_ROLE_DIR), 2)
    sig.sort(key=lambda s: (-s["score"], s["routine"], -s["buyers"], -s["value"]))
    return sig


def _fmt_mc(mc):
    if not mc:
        return "n/a"
    if mc >= 1e9:
        return f"${mc / 1e9:.1f}B"
    if mc >= 1e6:
        return f"${mc / 1e6:.0f}M"
    return f"${mc:,.0f}"


def write_signal(path, sig, purchases, top=None):
    class_counts = Counter(p["_class"] for p in purchases)
    lines = ["# Highest-signal insider buying (genuine open-market only)\n"]
    lines.append("Literature-grounded **signal score** (higher = stronger):")
    lines.append(f"- **opportunistic tilt** (Cohen-Malloy-Pomorski 2012): "
                 f"+{W_OPP:g}/opportunistic insider, {W_ROUTINE:g}/routine insider "
                 f"(routine calendar traders ~ 0 alpha)")
    lines.append(f"- **consensus**: +{W_CONSENSUS:g}/distinct insider (capped at {CONSENSUS_CAP})")
    lines.append(f"- **conviction**: +{W_CONVICTION:g} x median buy as % of prior holding (Seyhun)")
    lines.append(f"- **small-cap tilt** (Lakonishok-Lee): +0..{3.0*W_SMALLCAP:g}, larger for smaller mktcap")
    lines.append(f"- **role**: +{W_ROLE_EXEC:g} senior officer / +{W_ROLE_DIR:g} director-only "
                 f"(CEO=CFO, per Wang-Shin-Francis)\n")
    lines.append("Excludes PIPE/financing (one price shared by 2+ owners), SPAC / no-price "
                 "rows, and pure 10%-owner fund accumulation. Market cap is an EDGAR-only "
                 "estimate = buy price x shares outstanding (data.sec.gov).\n")
    lines.append("| # | Score | Ticker | Company | Insiders | Opp/Rout/Unk | Conv% | Mkt cap | Total $ | Role | Names |")
    lines.append("|---|------:|--------|---------|:-------:|:-----------:|:----:|--------:|--------:|------|-------|")
    rows = sig if top is None else sig[:top]
    for i, s in enumerate(rows, 1):
        names = "; ".join(s["insiders"])[:80]
        lines.append(f"| {i} | {s['score']:.1f} | {s['ticker']} | {s['name'][:32]} | "
                     f"{s['buyers']} | {s['opp']}/{s['routine']}/{s['unknown']} | "
                     f"{100*s['conviction']:.0f} | {_fmt_mc(s['mktcap'])} | "
                     f"${s['value']:,.0f} | {s['seniority']} | {names} |")
    lines.append("\n## Purchase classification (all P/A rows)\n")
    for cls, n in class_counts.most_common():
        lines.append(f"- {cls}: {n}")
    with open(path, "w") as f:
        f.write("\n".join(lines) + "\n")


def print_top_table(sig, top=25):
    print("\n" + "=" * 92)
    print(f"TOP {min(top, len(sig))} HIGHEST-SIGNAL INSIDER BUYS "
          f"(of {len(sig)} issuers; opp/routine + conviction + small-cap weighted)")
    print("=" * 92)
    print(f"{'#':>2} {'SCORE':>5} {'TICKER':<7} {'INS':>3} {'O/R/U':>7} {'CONV':>4} "
          f"{'MKTCAP':>8} {'TOTAL $':>12} {'COMPANY':<28}")
    print("-" * 92)
    for i, s in enumerate(sig[:top], 1):
        orc = f"{s['opp']}/{s['routine']}/{s['unknown']}"
        print(f"{i:>2} {s['score']:>5.1f} {s['ticker']:<7} {s['buyers']:>3} {orc:>7} "
              f"{100*s['conviction']:>3.0f}% {_fmt_mc(s['mktcap']):>8} "
              f"${s['value']:>11,.0f} {s['name'][:28]}")


# ---------------------------------------------------------------------------
def get_filings(client, end, calendar_days, recent_bdays):
    days = business_days(end, calendar_days)
    if recent_bdays > 0:
        days = days[:recent_bdays]
    print(f"Fetching {len(days)} business-day indexes "
          f"({days[-1]} .. {days[0]}) ...", flush=True)
    rows, per_day = fetch_indexes(client, days)
    found = sum(1 for v in per_day.values() if v is not None)
    print(f"  indexes: {found}/{len(days)} posted "
          f"(net={client.net_hits} cache={client.cache_hits})")
    return rows


# ---------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(
        description="EDGAR insider/activist scan -> ranked highest-signal table")
    ap.add_argument("--days", type=int, default=30,
                    help="calendar days back from --end (default 30)")
    ap.add_argument("--end", default=dt.date.today().isoformat(),
                    help="window end date YYYY-MM-DD (default: today)")
    ap.add_argument("--recent-bdays", type=int, default=0,
                    help="cap to N most recent business days (0 = use full window)")
    ap.add_argument("--top", type=int, default=25, help="rows in the printed table")
    ap.add_argument("--no-extra-screens", action="store_true",
                    help="skip the payout / smartmoney / spinoff screens + overlap")
    ap.add_argument("--min-value", type=float, default=25000,
                    help="materiality floor: drop issuers whose total genuine buying "
                         "is below this $ (default 25000)")
    ap.add_argument("--no-enrich", action="store_true",
                    help="skip data.sec.gov enrichment (routine/opportunistic + market cap)")
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--filings", default=None,
                    help="use a prebuilt filings JSON instead of fetching indexes")
    args = ap.parse_args()

    bucket = TokenBucket(rate=10, capacity=10)
    client = EdgarClient(bucket, cache_dir=f"{BASE}/cache")

    # ---- get the filing list (fetch indexes unless a JSON is supplied) ----
    if args.filings:
        with open(args.filings) as f:
            raw_rows = json.load(f)
        print(f"Loaded {len(raw_rows)} index rows from {os.path.basename(args.filings)}")
    else:
        end = dt.datetime.strptime(args.end, "%Y-%m-%d").date()
        raw_rows = get_filings(client, end, args.days, args.recent_bdays)
        with open(f"{BASE}/filings.json", "w") as f:
            json.dump(raw_rows, f)

    # The daily index emits one row per (filing x party): a Form 4 accession
    # appears once per issuer and once per reporting owner. Dedup by accession
    # so each filing/document is fetched and counted exactly once.
    seen = set()
    rows = []
    for r in raw_rows:
        if r["accession"] in seen:
            continue
        seen.add(r["accession"])
        rows.append(r)
    kept = Counter(r["form_class"] for r in rows)
    print(f"{len(raw_rows)} index rows -> {len(rows)} distinct filings "
          f"(Form 4={kept.get('4',0)}, SC 13D={kept.get('SC 13D',0)}, "
          f"SC 13D/A={kept.get('SC 13D/A',0)})")
    if kept.get("4", 0) > 20000:
        print(f"  NOTE: {kept['4']} Form 4s -> ~{kept['4']//10}s of fetching. "
              f"Ctrl-C is safe; the cache makes re-runs resume instantly.")

    t0 = time.time()
    res = process_all(rows, client, workers=args.workers)
    dt_s = time.time() - t0

    purchases = build_purchases(res["form4_ok"])
    write_csv(f"{BASE}/form4_purchases.csv", PURCHASE_COLS, purchases)
    write_csv(f"{BASE}/sc13d_filings.csv", SC13D_COLS, res["sc13d_ok"])
    issuers = write_shortlist(f"{BASE}/shortlist.md", purchases, res["sc13d_ok"])
    sig_all = classify_and_rank(purchases)
    sig = [s for s in sig_all if s["value"] >= args.min_value]  # materiality floor
    n_dropped = len(sig_all) - len(sig)
    buy_year = dt.datetime.strptime(args.end, "%Y-%m-%d").date().year
    sig = enrich_and_score(sig, client, buy_year, do_enrich=not args.no_enrich)
    write_signal(f"{BASE}/signal.md", sig, purchases)

    # ---- summary ----
    f4_ok = len(res["form4_ok"])
    sc_ok = len(res["sc13d_ok"])
    flag_resolved = sum(1 for r in res["form4_ok"] if r["is_10b5_1"] is not None)
    flag_true = sum(1 for r in res["form4_ok"] if r["is_10b5_1"] is True)
    flag_false = sum(1 for r in res["form4_ok"] if r["is_10b5_1"] is False)
    flag_footnote = sum(1 for r in res["form4_ok"] if r["flag_source"] == "footnote")

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"elapsed: {dt_s:.0f}s | net fetches: {client.net_hits} | "
          f"cache hits: {client.cache_hits} | 403/404: {client.not_found}")
    print(f"Form 4:   {f4_ok}/{res['f4_total']} parsed "
          f"({100*f4_ok/max(res['f4_total'],1):.1f}%)")
    print(f"SC 13D*:  {sc_ok}/{res['sc_total']} parsed "
          f"({100*sc_ok/max(res['sc_total'],1):.1f}%)")
    print(f"10b5-1 flag resolvable: {flag_resolved}/{f4_ok} "
          f"({100*flag_resolved/max(f4_ok,1):.1f}%)  "
          f"[true={flag_true} false={flag_false} via-footnote={flag_footnote}]")
    print(f"Open-market P purchases (rows): {len(purchases)}")
    print(f"Issuers in shortlist: {len(issuers)}")
    print(f"Issuers with genuine open-market buying: {len(sig_all)} "
          f"(-{n_dropped} below ${args.min_value:,.0f} floor -> {len(sig)} ranked)")
    sc_orig = sum(1 for s in res['sc13d_ok'] if s.get('form_class') == 'SC 13D')
    sc_amend = sc_ok - sc_orig
    print(f"SC 13D originals: {sc_orig} | amendments: {sc_amend}")
    print(f"parse-stage breakdown: {dict(res['stats'])}")

    # data-quality report saved for the writeup
    _data_quality(res, purchases)

    # ---- screen 1: the insider signal ----
    print_top_table(sig, top=args.top)

    # ---- screens 2-4 + multi-screen overlap ----
    outputs = ["form4_purchases.csv", "sc13d_filings.csv", "shortlist.md",
               "signal.md", "data_quality.json"]
    if not args.no_extra_screens:
        print("\nRunning payout / smartmoney / spinoff screens ...", flush=True)
        all_screens, cols = screens_mod.run_all_screens(client, sig)
        for name in ("payout", "smartmoney", "spinoff"):
            screens_mod.print_screen(name, all_screens[name], cols[name], top=args.top)
        overlap = screens_mod.build_overlap(all_screens)
        screens_mod.print_overlap(overlap, top=args.top)
        screens_mod.write_screens_md(f"{BASE}/screens.md", all_screens, cols,
                                     overlap, top=max(args.top, 25))
        outputs.append("screens.md")
    print("\nWrote: " + ", ".join(outputs))


def _data_quality(res, purchases):
    f4 = res["form4_ok"]
    dq = {}
    dq["form4_missing_ticker"] = sum(1 for r in f4 if not r["issuerTradingSymbol"])
    dq["form4_no_relationship"] = sum(
        1 for r in f4 if r["isOfficer"] is None and r["isDirector"] is None)
    dq["purchases_missing_price"] = sum(1 for p in purchases if p["transactionPricePerShare"] is None)
    dq["purchases_missing_value"] = sum(1 for p in purchases if p["trade_value"] is None)
    dq["purchases_neg_or_null_pct"] = sum(1 for p in purchases if p["pct_holding_increase"] is None)
    sc = res["sc13d_ok"]
    dq["sc13d_missing_cusip"] = sum(1 for s in sc if not s.get("cusip"))
    dq["sc13d_missing_pct"] = sum(1 for s in sc if not s.get("pct_of_class"))
    dq["sc13d_missing_item4"] = sum(1 for s in sc if not s.get("item4"))
    dq["sc13d_missing_filer"] = sum(1 for s in sc if not s.get("filer_name"))
    with open(f"{BASE}/data_quality.json", "w") as f:
        json.dump(dq, f, indent=2)
    print("\nData-quality flags (see data_quality.json):")
    for k, v in dq.items():
        print(f"  {k}: {v}")


if __name__ == "__main__":
    main()
