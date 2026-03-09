# Grade Opportunity Scanner

Scans eBay hourly for raw cards that are worth grading based on `mv_grade_premiums` data.

## How it works

1. Loads all cards from `mv_grade_premiums` where `grading_score >= 70` and both `raw_price` and `psa10_price` exist
2. Searches eBay by sport/category for raw listings (BIN + auction)
3. Fuzzy matches listing titles to cards in the DB
4. Alerts to Discord if:
   - Listing price is within 5% of DB raw median
   - Estimated PSA 10 net profit (after grading cost) is >= $50
   - Listing hasn't been alerted before

## Alert format

Each Discord alert shows:
- Card name, set, year
- eBay price vs DB raw median
- PSA 9 and PSA 10 values
- Estimated net profit after grading
- Grading score (0-100)
- Hard-to-grade warning if PSA9/raw >= 5x

## Setup

### 1. Environment variables (set in Railway)

```
EBAY_CLIENT_ID=
EBAY_CLIENT_SECRET=
SUPABASE_URL=
SUPABASE_KEY=          # use service role key
DISCORD_WEBHOOK_GRADE_ALERTS=
```

### 2. Discord webhook
- Go to your Discord server → channel settings → Integrations → Webhooks
- Create a new webhook and copy the URL
- Set as `DISCORD_WEBHOOK_GRADE_ALERTS` in Railway

### 3. Deploy to Railway
- Create a new Railway project
- Connect to this GitHub repo
- Add environment variables
- Railway will auto-detect the Procfile and start the worker

## Tuning

In `scanner.py` at the top:

| Variable | Default | Description |
|---|---|---|
| `PRICE_THRESHOLD` | 1.05 | Alert if eBay price <= raw_price × this (1.05 = within 5%) |
| `MIN_GRADING_SCORE` | 70 | Minimum grading score to consider |
| `MIN_PROFIT` | 50 | Minimum estimated PSA 10 net profit to alert |

## Categories scanned

- NFL (Football)
- NBA (Basketball)  
- MLB (Baseball)
- NHL (Hockey)
- Pokemon
