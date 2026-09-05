# Odd-Lot Tender Offer Scanner

Scans SEC EDGAR weekly for SC TO-I (issuer tender offer) filings that include odd-lot provisions, filters to exchange-listed opportunities with positive profit potential, and emails a ranked plain-text summary.

## How it works

1. Queries the EDGAR full-text search API for SC TO-I filings containing "odd lot" filed in the last 7 days
2. Fetches each filing and extracts: company name, ticker, offer price (or Dutch auction range), and expiration date
3. Fetches the current market price via yfinance
4. Calculates max profit = 99 × (upper offer price − current price)
5. Filters out tickers not listed on NYSE, NASDAQ, or AMEX (non-traded REITs, BDCs, OTC stocks)
6. Sends a plain-text email table sorted by max profit descending — only if ≥1 opportunity exists

## Local setup

```bash
pip install -r requirements.txt

export EMAIL_FROM=you@example.com
export EMAIL_TO=fieldenthomas@gmail.com
export EMAIL_PASSWORD=your-app-password
export SMTP_HOST=smtp.gmail.com
export SMTP_PORT=465

python scanner.py
```

Progress and skip reasons are printed to stderr. The email is sent only when there are actionable opportunities.

**Tip for testing:** temporarily change `timedelta(days=7)` to `timedelta(days=30)` in `get_date_range()` to increase the chance of finding real filings during testing.

## Deploy on Railway

### 1. Create a Railway project

1. Go to [railway.app](https://railway.app) and create a new project
2. Choose **Deploy from GitHub repo** and select the repo containing this code
3. Railway auto-detects Python via `requirements.txt` — no Dockerfile needed

### 2. Configure environment variables

In the Railway dashboard → your service → **Variables**, add:

| Variable | Example value | Description |
|---|---|---|
| `EMAIL_FROM` | `scanner@gmail.com` | Sender email address |
| `EMAIL_TO` | `fieldenthomas@gmail.com` | Recipient email address |
| `EMAIL_PASSWORD` | `abcd efgh ijkl mnop` | App password (not your login password) |
| `SMTP_HOST` | `smtp.gmail.com` | SMTP server hostname |
| `SMTP_PORT` | `465` | 465 for SSL, 587 for STARTTLS |

**Gmail users:** Generate an [App Password](https://myaccount.google.com/apppasswords) (requires 2FA enabled). Use that as `EMAIL_PASSWORD`, not your Google account password.

### 3. Set the cron schedule

In the Railway dashboard → your service → **Settings** → **Start Command**:

```
python scanner.py
```

Then under **Cron Schedule**, enter:

```
0 8 * * 1
```

This runs every Monday at 08:00 UTC. Adjust the hour to match your preferred timezone:
- 08:00 UTC = 03:00 ET / 04:00 ET (depending on DST)
- For 09:00 ET use `0 14 * * 1`

### 4. Verify deployment

After saving, trigger a manual run from the Railway dashboard (Deployments → **Trigger Deploy**). Check the deploy logs — you should see `[INFO]` lines for each filing processed and `[OK]` lines for any opportunities found. If zero opportunities exist that week, no email is sent (this is expected).

## SMTP notes

- Port **465**: uses `SMTP_SSL` (direct TLS connection) — recommended for Gmail
- Port **587**: uses `SMTP` + `STARTTLS` — works with most providers
- Port **25**: plain SMTP with STARTTLS — rarely needed

## Files

```
scanner.py        # all logic, single entry point
requirements.txt  # requests, yfinance
nixpacks.toml     # pins Python 3.11 on Railway
README.md         # this file
```
