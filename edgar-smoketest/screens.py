"""Orchestrate the four screens (insider + payout + smartmoney + spinoff),
print each ranked table, and surface names that appear in more than one.

Each screen module exposes run_screen(client)->list[record], SCREEN_NAME, COLUMNS.
Records: {cik, ticker, cusip, name, score, rank, detail}. We join across screens
on the best available key (ticker > cik > cusip > normalized name) and combine
via reciprocal-rank fusion (RRF)."""
import re
from collections import defaultdict

import screen_payout
import screen_smartmoney
import screen_spinoffs

# Non-operating issuers whose XBRL create/redeem flows pollute the payout screen.
_FUND_RE = re.compile(
    r"\b(ETF|ETN|TRUST|FUND|ISHARES|SPDR|PROSHARES|INVESCO|GRAYSCALE|GALAXY|"
    r"BITCOIN|ETHER|COMMODITIES?|CURRENCYSHARES|INDEX|ACQUISITION|SPAC|"
    r"BLANK CHECK)\b", re.I)


def _clean_ticker(t):
    t = (t or "").strip().upper()
    return t if t and t != "-" else None


def _norm_cik(cik):
    try:
        return str(int(str(cik).lstrip("0") or "0")) if cik else None
    except (ValueError, TypeError):
        return None


def _join_key(rec):
    """Best available cross-screen identity key."""
    if rec.get("ticker"):
        return ("ticker", rec["ticker"].upper())
    if rec.get("cik"):
        return ("cik", _norm_cik(rec["cik"]))
    if rec.get("cusip"):
        return ("cusip", rec["cusip"])
    return ("name", re.sub(r"[^A-Z0-9]", "", (rec.get("name") or "").upper())[:18])


# ---------------------------------------------------------------------------
# Adapters / per-screen cleanups
# ---------------------------------------------------------------------------
def insider_to_records(sig):
    recs = []
    for i, s in enumerate(sig, 1):
        recs.append({
            "cik": _norm_cik(s.get("cik")),
            "ticker": _clean_ticker(s.get("ticker")),
            "cusip": None,
            "name": s.get("name") or "?",
            "score": s.get("score", 0.0),
            "rank": i,
            "detail": {"insiders": s.get("buyers"), "opp": s.get("opp"),
                       "routine": s.get("routine"), "value": s.get("value"),
                       "mktcap": s.get("mktcap")},
        })
    return recs


def _clean_payout(records):
    out = [r for r in records if not _FUND_RE.search(r.get("name") or "")]
    for i, r in enumerate(out, 1):
        r["rank"] = i
    return out


def _rerank_spinoffs(records):
    """Favour exchange-listed 10-12B (true spin-offs) over 10-12G shells/funds,
    then the module's recency score, breaking plateau ties by freshness."""
    def key(r):
        d = r.get("detail", {})
        form = (d.get("form") or "")
        return (0 if form == "10-12B" else 1, -r.get("score", 0.0),
                d.get("days_since") if d.get("days_since") is not None else 9999)
    out = sorted(records, key=key)
    for i, r in enumerate(out, 1):
        r["rank"] = i
    return out


# ---------------------------------------------------------------------------
def run_all_screens(client, insider_sig):
    screens, cols = {}, {}
    screens["insider"] = insider_to_records(insider_sig)
    cols["insider"] = ["insiders", "opp", "routine", "value", "mktcap"]

    for mod in (screen_payout, screen_smartmoney, screen_spinoffs):
        name = mod.SCREEN_NAME
        try:
            recs = mod.run_screen(client)
        except Exception as e:  # a screen failing shouldn't kill the rest
            print(f"  [screen '{name}' failed: {type(e).__name__}: {e}]")
            recs = []
        if name == "payout":
            recs = _clean_payout(recs)
        elif name == "spinoff":
            recs = _rerank_spinoffs(recs)
        screens[name] = recs
        cols[name] = getattr(mod, "COLUMNS", [])
    return screens, cols


def build_overlap(screens, cutoff=150):
    """Return names in >=2 screens' TOP-`cutoff`, ranked by (#screens, RRF).
    The cutoff is what makes overlap mean 'ranks well in both', not merely
    'appears somewhere in the long tail'."""
    key_recs = defaultdict(dict)   # key -> {screen: record}
    for sname, recs in screens.items():
        for r in recs:
            if r["rank"] <= cutoff:
                key_recs[_join_key(r)].setdefault(sname, r)
    multi = []
    for key, present in key_recs.items():
        if len(present) < 2:
            continue
        rrf = sum(1.0 / (r["rank"] + 10) for r in present.values())  # damped RRF
        disp = next(iter(present.values()))
        multi.append({
            "key": key, "present": present, "rrf": rrf,
            "n": len(present),
            "ticker": next((r["ticker"] for r in present.values() if r.get("ticker")), None),
            "name": disp.get("name"),
        })
    multi.sort(key=lambda m: (-m["n"], -m["rrf"]))
    return multi


# ---------------------------------------------------------------------------
# Printing
# ---------------------------------------------------------------------------
def _fmt_val(v):
    if isinstance(v, float):
        if abs(v) >= 1e9:
            return f"{v/1e9:.1f}B"
        if abs(v) >= 1e6:
            return f"{v/1e6:.1f}M"
        return f"{v:.2f}"
    if isinstance(v, list):
        return ", ".join(str(x) for x in v)[:28]
    return "-" if v is None else str(v)


def print_screen(name, recs, columns, top=15):
    print(f"\n{'='*92}\nSCREEN: {name.upper()}  ({len(recs)} ranked)\n{'='*92}")
    header = f"{'#':>2} {'SCORE':>6} {'TICKER':<7} {'NAME':<30} " + \
             " ".join(f"{c[:12]:>12}" for c in columns[:4])
    print(header)
    print("-" * len(header))
    for r in recs[:top]:
        d = r.get("detail", {})
        cells = " ".join(f"{_fmt_val(d.get(c)):>12}" for c in columns[:4])
        print(f"{r['rank']:>2} {r['score']:>6.2f} {(r.get('ticker') or '-'):<7} "
              f"{(r.get('name') or '?')[:30]:<30} {cells}")


def print_overlap(multi, top=25):
    print(f"\n{'='*92}\nMULTI-SCREEN OVERLAP  ({len(multi)} names in >=2 screens)\n{'='*92}")
    if not multi:
        print("  (no overlap this run — the four screens target different universes)")
        return
    print(f"{'#':>2} {'TICKER':<7} {'NAME':<32} {'SCREENS':>3}  DETAIL (screen:rank)")
    print("-" * 92)
    for i, m in enumerate(multi[:top], 1):
        detail = "  ".join(f"{s}:#{r['rank']}" for s, r in sorted(m["present"].items()))
        print(f"{i:>2} {(m['ticker'] or '-'):<7} {(m['name'] or '?')[:32]:<32} "
              f"{m['n']:>3}  {detail}")


def write_screens_md(path, screens, cols, multi, top=25):
    L = ["# Multi-strategy screens\n",
         "Four independent, weakly-correlated reads on 'informed parties think this "
         "is cheap' — all EDGAR-only. Names surfacing in >=2 are the high-conviction set.\n"]
    L.append("## Multi-screen overlap (ranked by # of screens, then reciprocal-rank fusion)\n")
    if multi:
        L.append("| # | Ticker | Name | #Screens | Screens (rank) |")
        L.append("|---|--------|------|:--------:|----------------|")
        for i, m in enumerate(multi[:top], 1):
            d = ", ".join(f"{s} #{r['rank']}" for s, r in sorted(m["present"].items()))
            L.append(f"| {i} | {m['ticker'] or '-'} | {(m['name'] or '?')[:40]} | {m['n']} | {d} |")
    else:
        L.append("_No name appeared in 2+ screens this run._")
    for name, recs in screens.items():
        L.append(f"\n## {name} (top {top})\n")
        c = cols.get(name, [])
        L.append("| # | Ticker | Name | Score | " + " | ".join(c[:4]) + " |")
        L.append("|---|--------|------|------:|" + "|".join(["---"] * min(4, len(c))) + "|")
        for r in recs[:top]:
            dd = r.get("detail", {})
            vals = " | ".join(_fmt_val(dd.get(x)) for x in c[:4])
            L.append(f"| {r['rank']} | {r.get('ticker') or '-'} | "
                     f"{(r.get('name') or '?')[:34]} | {r['score']:.2f} | {vals} |")
    with open(path, "w") as f:
        f.write("\n".join(L) + "\n")
