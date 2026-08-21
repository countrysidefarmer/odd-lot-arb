"""Smart-money 13F screen (Cohen-Polk-Silli "Best Ideas" + multi-manager consensus).

Aggregates the latest 13F-HR long-common holdings of a curated universe of
well-known value/activist managers and scores each security by:
  - how many managers hold it (consensus),
  - whether it is a "best idea" (a manager's top-5 portfolio weight),
  - new buys this quarter (held now, not in that manager's prior 13F),
  - the max single-manager portfolio weight (conviction).

EDGAR / data.sec.gov only. Standalone: `/usr/bin/python3 screen_smartmoney.py`.
"""
import json
import re
import sys
import xml.etree.ElementTree as ET

from edgar_client import EdgarClient, TokenBucket

BASE = "/Users/thomasfielden/claude-projects/edgar-smoketest"

SCREEN_NAME = "smartmoney"
COLUMNS = ["n_managers", "managers", "max_weight_pct", "best_idea", "n_new_buys"]

# Curated value/activist manager universe. CIKs were resolved + verified as
# 13F-HR filers via EDGAR company search; editable here.
MANAGERS = {
    "Berkshire Hathaway": "0001067983",
    "Pershing Square": "0001336528",
    "Third Point": "0001040273",
    "ValueAct Capital": "0001418814",  # ValueAct Holdings, L.P. (current filer)
    "Greenlight Capital": "0001079114",
    "Baupost Group": "0001061768",
    "Appaloosa": "0001656456",
    "Scion Asset Management": "0001649339",
    "Trian Fund Management": "0001345471",
    "Starboard Value": "0001517137",
    "Elliott Investment Management": "0001791786",
    "Himalaya Capital": "0001709323",
}

TOP_N_BEST_IDEA = 5

# ---- scoring weights ----
W_MANAGERS = 3.0      # per additional manager holding
W_BEST_IDEA = 4.0     # per manager for whom this is a top-5 weight
W_NEW_BUY = 2.0       # per new-buy this quarter
W_MAX_WEIGHT = 5.0    # times max single-manager weight (fraction 0..1)


# --------------------------------------------------------------------------
# EDGAR fetch helpers
# --------------------------------------------------------------------------
def _submissions(client, cik):
    n = int(cik)
    url = "https://data.sec.gov/submissions/CIK%010d.json" % n
    txt, _ = client.fetch(url, "%s/cache/submissions_%010d.json" % (BASE, n))
    if not txt:
        return None
    try:
        return json.loads(txt)
    except ValueError:
        return None


def _recent_13f_accessions(subs, limit=2):
    """Return up to `limit` most recent 13F-HR accession numbers (dashed)."""
    r = subs.get("filings", {}).get("recent", {})
    forms = r.get("form", [])
    accs = r.get("accessionNumber", [])
    dates = r.get("filingDate", [])
    out = []
    for i, f in enumerate(forms):
        if f == "13F-HR":
            out.append((dates[i] if i < len(dates) else "", accs[i]))
    out.sort(key=lambda x: x[0], reverse=True)
    return [a for _, a in out[:limit]]


def _infotable_url(client, cik, accession):
    """Locate the information-table xml within a 13F filing via index.json."""
    n = int(cik)
    accnd = accession.replace("-", "")
    iurl = "https://www.sec.gov/Archives/edgar/data/%d/%s/index.json" % (n, accnd)
    itxt, _ = client.fetch(iurl, "%s/cache/13fidx_%d_%s.json" % (BASE, n, accnd))
    if not itxt:
        return None
    try:
        ij = json.loads(itxt)
    except ValueError:
        return None
    items = [it.get("name", "") for it in ij.get("directory", {}).get("item", [])]
    base = "https://www.sec.gov/Archives/edgar/data/%d/%s/" % (n, accnd)
    # Prefer explicit "infotable"/"informationtable" filenames.
    for name in items:
        low = name.lower()
        if low.endswith(".xml") and ("infotable" in low or "informationtable" in low
                                     or "info_table" in low or "form13f" in low):
            return base + name
    # Fallback: any xml that isn't primary_doc.
    for name in items:
        low = name.lower()
        if low.endswith(".xml") and "primary" not in low:
            return base + name
    return None


def _localname(tag):
    return tag.rsplit("}", 1)[-1] if "}" in tag else tag


def _parse_infotable(text):
    """Parse the 13F information table. Return list of holding dicts (long common
    only; put/call rows skipped)."""
    holdings = []
    try:
        root = ET.fromstring(text)
    except ET.ParseError:
        return holdings
    for el in root.iter():
        if _localname(el.tag) != "infoTable":
            continue
        rec = {"name": None, "cusip": None, "value": 0.0, "shares": 0.0,
               "putCall": None}
        for child in el.iter():
            ln = _localname(child.tag)
            txt = (child.text or "").strip()
            if ln == "nameOfIssuer":
                rec["name"] = txt
            elif ln == "cusip":
                rec["cusip"] = txt.upper() if txt else None
            elif ln == "value":
                try:
                    rec["value"] = float(txt.replace(",", ""))
                except ValueError:
                    pass
            elif ln == "sshPrnamt":
                try:
                    rec["shares"] = float(txt.replace(",", ""))
                except ValueError:
                    pass
            elif ln == "putCall":
                rec["putCall"] = txt.upper() or None
        if rec["putCall"]:  # skip options, keep only long common
            continue
        if not rec["cusip"] or rec["value"] <= 0:
            continue
        holdings.append(rec)
    return holdings


def _load_holdings(client, cik, accession):
    url = _infotable_url(client, cik, accession)
    if not url:
        return None
    n = int(cik)
    accnd = accession.replace("-", "")
    txt, _ = client.fetch(url, "%s/cache/13finfo_%d_%s.xml" % (BASE, n, accnd))
    if not txt:
        return None
    return _parse_infotable(txt)


# --------------------------------------------------------------------------
# issuer name -> ticker/cik mapping
# --------------------------------------------------------------------------
_SUFFIXES = (" inc", " incorporated", " corp", " corporation", " co", " company",
             " ltd", " limited", " plc", " lp", " llc", " sa", " nv", " ag",
             " holdings", " hldgs", " group", " grp", " the", " new", " cl a",
             " cl b", " cl c", " com", " ord", " adr", " ads")


def _normalize(name):
    if not name:
        return ""
    s = name.lower()
    s = re.sub(r"[.,/&]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    # strip common corporate suffixes repeatedly
    changed = True
    while changed:
        changed = False
        for suf in _SUFFIXES:
            if s.endswith(suf):
                s = s[: -len(suf)].strip()
                changed = True
    return re.sub(r"\s+", " ", s).strip()


def _build_ticker_index(client):
    txt, _ = client.fetch("https://www.sec.gov/files/company_tickers.json",
                          "%s/cache/company_tickers.json" % BASE)
    idx = {}
    if not txt:
        return idx
    try:
        j = json.loads(txt)
    except ValueError:
        return j and {} or {}
    for row in j.values():
        norm = _normalize(row.get("title", ""))
        if not norm:
            continue
        # first writer wins; company_tickers is roughly market-cap ordered
        idx.setdefault(norm, {
            "ticker": (row.get("ticker") or "").upper() or None,
            "cik": "%010d" % int(row.get("cik_str")) if row.get("cik_str") else None,
        })
    return idx


def _match_issuer(idx, issuer):
    norm = _normalize(issuer)
    if not norm:
        return None, None
    hit = idx.get(norm)
    if hit:
        return hit["ticker"], hit["cik"]
    # try first two words (handles trailing class/descriptors)
    parts = norm.split()
    if len(parts) >= 2:
        hit = idx.get(" ".join(parts[:2]))
        if hit:
            return hit["ticker"], hit["cik"]
    return None, None


# --------------------------------------------------------------------------
# main screen
# --------------------------------------------------------------------------
def run_screen(client=None, **opts):
    if client is None:
        bucket = TokenBucket(rate=10, capacity=10)
        client = EdgarClient(bucket, cache_dir="%s/cache" % BASE)

    top_n = int(opts.get("top_n_best_idea", TOP_N_BEST_IDEA))

    # per-cusip aggregate accumulator
    agg = {}  # cusip -> dict

    def _slot(cusip, name):
        s = agg.get(cusip)
        if s is None:
            s = {"cusip": cusip, "name": name, "managers": set(),
                 "best_idea_count": 0, "n_new_buys": 0, "max_weight": 0.0}
            agg[cusip] = s
        elif not s["name"] and name:
            s["name"] = name
        return s

    resolved = {}
    failed = {}

    for mname, cik in MANAGERS.items():
        subs = _submissions(client, cik)
        if not subs:
            failed[mname] = "no submissions"
            continue
        accs = _recent_13f_accessions(subs, limit=2)
        if not accs:
            failed[mname] = "no 13F-HR"
            continue
        cur = _load_holdings(client, cik, accs[0])
        if not cur:
            failed[mname] = "no holdings parsed"
            continue
        resolved[mname] = cik

        prior_cusips = set()
        if len(accs) > 1:
            prior = _load_holdings(client, cik, accs[1])
            if prior:
                prior_cusips = {h["cusip"] for h in prior}

        # collapse multiple share classes / lots of same cusip into one value
        by_cusip = {}
        for h in cur:
            c = h["cusip"]
            e = by_cusip.get(c)
            if e is None:
                by_cusip[c] = {"name": h["name"], "value": h["value"]}
            else:
                e["value"] += h["value"]
        total = sum(e["value"] for e in by_cusip.values()) or 1.0

        weighted = sorted(by_cusip.items(), key=lambda kv: kv[1]["value"],
                          reverse=True)
        best_idea_cusips = {c for c, _ in weighted[:top_n]}

        for c, e in by_cusip.items():
            weight = e["value"] / total
            slot = _slot(c, e["name"])
            slot["managers"].add(mname)
            if weight > slot["max_weight"]:
                slot["max_weight"] = weight
            if c in best_idea_cusips:
                slot["best_idea_count"] += 1
            if prior_cusips and c not in prior_cusips:
                slot["n_new_buys"] += 1

    # resolve tickers/ciks
    tidx = _build_ticker_index(client)

    records = []
    for cusip, s in agg.items():
        n_managers = len(s["managers"])
        best_cnt = s["best_idea_count"]
        n_new = s["n_new_buys"]
        max_w = s["max_weight"]
        score = (W_MANAGERS * n_managers
                 + W_BEST_IDEA * best_cnt
                 + W_NEW_BUY * n_new
                 + W_MAX_WEIGHT * max_w)
        ticker, ci = _match_issuer(tidx, s["name"])
        records.append({
            "cik": ci,
            "ticker": ticker,
            "cusip": cusip,
            "name": s["name"],
            "score": round(score, 4),
            "rank": 0,
            "detail": {
                "n_managers": n_managers,
                "managers": sorted(s["managers"]),
                "max_weight_pct": round(max_w * 100.0, 2),
                "best_idea": best_cnt,
                "n_new_buys": n_new,
            },
        })

    records.sort(key=lambda r: (r["score"], r["detail"]["n_managers"],
                                r["detail"]["max_weight_pct"]), reverse=True)
    for i, r in enumerate(records, 1):
        r["rank"] = i

    run_screen._resolved = resolved
    run_screen._failed = failed
    return records


def _print_table(records, limit=15):
    print(SCREEN_NAME)
    hdr = ("%-4s %-6s %-9s %-30s %6s %4s %4s %4s %4s %7s"
           % ("rank", "tkr", "cusip", "name", "score", "mgr", "best", "new",
              "", "maxw%"))
    print(hdr)
    print("-" * len(hdr))
    for r in records[:limit]:
        d = r["detail"]
        print("%-4d %-6s %-9s %-30s %6.2f %4d %4d %4d %4s %7.2f" % (
            r["rank"],
            (r["ticker"] or "-")[:6],
            (r["cusip"] or "-")[:9],
            (r["name"] or "-")[:30],
            r["score"],
            d["n_managers"],
            d["best_idea"],
            d["n_new_buys"],
            "",
            d["max_weight_pct"],
        ))


if __name__ == "__main__":
    recs = run_screen()
    resolved = getattr(run_screen, "_resolved", {})
    failed = getattr(run_screen, "_failed", {})
    _print_table(recs, 15)
    print()
    print("resolved managers: %d / %d" % (len(resolved), len(MANAGERS)))
    if failed:
        print("failed: " + ", ".join("%s (%s)" % (k, v) for k, v in failed.items()))
    print("securities ranked: %d" % len(recs), file=sys.stderr)
