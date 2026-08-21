# EDGAR insider / activist smoke test (throwaway)

Exploratory scraper for one month of Form 4 (insider) and SC 13D (activist)
filings. Goal: see the *shape* of the data. Correctness of parsing > coverage.

## Status (last worked 2026-08-21)
Working end-to-end, EDGAR-only, runnable via `./scan.sh --days 30`. Four ranked
screens (insider / payout / smartmoney / spinoff) + a multi-screen overlap.
**Cache is not committed** — the first run after a fresh clone refetches (~15 min
for 30 days). **Where we left off / next steps:**
- add an optional IBKR price layer (momentum, 52-wk, 13D & spin-off event-drift) —
  IBKR is only reachable in a Claude session, so keep it optional/off for standalone.
- turn the multi-screen overlap into a real composite score (not just set-intersection).
- add the deferred value / contrarian-past-return factor.
- curate the smart-money manager list in `screen_smartmoney.py`.

## Run (one command)
```bash
./scan.sh                 # last 30 days -> prints TOP-25 highest-signal table
./scan.sh --days 30 --top 40
./scan.sh --days 7        # quick week
```
`scan.sh` pins `/usr/bin/python3` (3.9, has `requests`) because the default
`python3` on this Mac is a broken 2018 build. It fetches the daily indexes for
the window, dedups to distinct filings, pulls + parses every filing, and writes
the outputs below. **First 30-day run pulls ~8-9k filings (~15 min at 10 req/s);
Ctrl-C is safe — the cache resumes instantly.**

The headline output is the ranked table printed to your terminal (and `signal.md`):
genuine open-market insider buys only, literature-scored (see below). PIPEs, SPAC
founder shares and 10%-owner fund flows are filtered out automatically.

Useful flags: `--min-value 50000` (materiality floor, default 25k), `--top 40`,
`--no-enrich` (skip the data.sec.gov calls if you just want the raw buy list).

SEC requires every request to declare a real contact, and there is no default. Set it once:

```bash
export SEC_UA="Your Name you@example.com"
# or, persisted locally and gitignored:
echo 'SEC_UA="Your Name you@example.com"' > edgar-smoketest/.env
```
Uses `requests` + stdlib `xml.etree` (no lxml). Cache in `./cache/{accession}.txt`.
Global 10 req/s token bucket, 8 worker threads.

## Scoring (grounded in the insider-alpha literature)
Only *genuine* open-market insider buys are ranked (officer/director, not 10%
owner, real price, not a PIPE where one price is shared by 2+ owners). A
materiality floor drops sub-$25k issuers. Each surviving issuer scores:

- **opportunistic tilt** (Cohen-Malloy-Pomorski 2012) — the dominant factor.
  We pull each insider's *full* Form 4 history from `data.sec.gov/submissions`,
  find their habitual trading months, and classify **this** buy trade-level:
  off-pattern = *opportunistic* (+4, ~10%/yr alpha in CMP), habitual-month =
  *routine* (-3, ~0 alpha), too little history = *unknown* (0, neutral).
- **consensus** — +1.5 per distinct insider, capped at 5 (Seyhun; Lakonishok-Lee).
- **conviction** — +3 x buy size as a % of the insider's prior holding, not raw $
  (relative size is the informative measure; raw $ only gates materiality).
- **small-cap tilt** — up to +3, larger for smaller market cap (Lakonishok-Lee:
  insider alpha concentrates in small firms). Market cap is EDGAR-only:
  buy price x shares outstanding (`data.sec.gov` XBRL).
- **role** — +1 senior officer / +0.5 director-only. CEO == CFO (Wang-Shin-Francis).

Weights are constants at the top of `run.py` — tune freely. **Deliberately NOT
included** (need price/fundamental data you set aside): value/book-to-market and
the contrarian past-return overlay.

## Strategy screens (multi-signal)
`run.py` also runs three more independent, literature-backed screens (all
EDGAR-only, add `--no-extra-screens` to skip) and prints each ranked, plus a
**multi-screen overlap** — names that rank in the top-150 of ≥2 screens, which is
where corroboration (and the real edge) lives:

- **payout** – net share buyback / issuance (Pontiff-Woodgate 2008; Ikenberry
  1995), from XBRL frames. Higher = net buyer, split-adjusted, funds/SPACs filtered.
- **smartmoney** – 13F "best ideas" + consensus across a curated set of skilled
  value/activist managers (Cohen-Polk-Silli 2010). Edit the manager list in
  `screen_smartmoney.py`.
- **spinoff** – recently registered spin-offs (Form 10-12B), the forced-seller
  discount (Cusatis-Miles-Woolridge 1993). Exchange-listed 10-12B ranked above
  10-12G shells.

Overlap is combined by reciprocal-rank fusion → written to `screens.md`.

Caveats: the opportunistic/routine split needs ~3y of an insider's history, so
brand-new filers show as *unknown*; the market-cap estimate assumes the open-
market fill is near the current price and uses the latest tagged shares figure
(occasionally stale or missing -> neutral small-cap weight).

Sub-scripts still runnable standalone: `step1_index.py` (index + counts only),
`probe_10b5.py` (re-discover the 10b5-1 element, dumps `sample_form4.xml`).

## Files
- `edgar_client.py` – token bucket + caching HTTP client (retries 429/5xx, treats 403/404 as "absent")
- `parsers.py` – daily index, Form 4 ownership XML, SC 13D structured XML
- `enrich_signal.py` – data.sec.gov enrichment: routine/opportunistic classifier + shares-outstanding/market-cap
- `screen_payout.py` / `screen_smartmoney.py` / `screen_spinoffs.py` – the three extra strategy screens (standalone-runnable)
- `screens.py` – orchestrates all four screens + the multi-screen overlap
- `step1_index.py` – build filing list, counts, >15k Form-4 stop rule
- `probe_10b5.py` – empirical discovery of the 10b5-1 checkbox element
- `run.py` – fetch + parse + enrich + write outputs
- `scan.sh` – one-command wrapper (pins the working Python + default User-Agent)
- Outputs: **`signal.md`** (ranked highest-signal table), `form4_purchases.csv`,
  `sc13d_filings.csv`, `shortlist.md`, `data_quality.json`

## What the run of 2026-07-29 .. 2026-08-03 (4 posted business days) found
- 3,442 index rows = **1,666 distinct filings** (index lists one row per party).
- 1,590 Form 4, 76 SC 13D (20 original / 56 amendment). 100% parse both.
- 10b5-1 element = **`<aff10b5One>`** (values 0/1/true/false). Present on 100% of
  Form 4s; footnote fallback never needed. 182/1,590 (11%) under a plan.
- 179 open-market `P` purchases (code P & acquired A). 4 issuers in shortlist.

## Known scope limits
- Routine/opportunistic *calendar* classification is OUT (needs ~3y history);
  the 10b5-1 flag is the only routine proxy.
- No market-cap / liquidity filter (EDGAR has no prices). Tickers left raw.
- SC 13D/A amendments often don't restate Item 4 → item4 legitimately null.
- `P` code is self-reported: PIPE/financing rounds and SPAC founder shares can be
  mislabeled as open-market purchases (see Scribe Therapeutics in shortlist).
