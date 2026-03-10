import os
import re
import time
import base64
import sqlite3
import logging
import schedule
import requests
from datetime import datetime
from dotenv import load_dotenv
from supabase import create_client, Client
from rapidfuzz import fuzz, process as fuzz_process

load_dotenv()

# ===========================================================================
# Config
# ===========================================================================

EBAY_CLIENT_ID     = os.getenv("EBAY_CLIENT_ID")
EBAY_CLIENT_SECRET = os.getenv("EBAY_CLIENT_SECRET")
SUPABASE_URL       = os.getenv("SUPABASE_URL")
SUPABASE_KEY       = os.getenv("SUPABASE_KEY")
DISCORD_WEBHOOK    = os.getenv("DISCORD_WEBHOOK_GRADE_ALERTS")

EBAY_TOKEN_URL = "https://api.ebay.com/identity/v1/oauth2/token"
EBAY_SEARCH_URL = "https://api.ebay.com/buy/browse/v1/item_summary/search"
EBAY_SCOPE = "https://api.ebay.com/oauth/api_scope"

# Price threshold: alert if eBay listing <= raw_price * this multiplier
# 1.05 = within 5% above DB median raw price
PRICE_THRESHOLD = 1.05

# Minimum grading score to consider
MIN_GRADING_SCORE = 70

# Minimum PSA 10 profit after grading to alert
MIN_PROFIT = 50

# eBay search config per category
EXCL = (
    '-"you pick" -"lot of" -"choose your" -"complete your set" -"u pick"'
    ' -"card lot" -"cards lot" -"pack of" -"box of" -"blaster" -"hobby box"'
    ' -"factory sealed" -"sealed box" -"sealed pack" -"complete set"'
    ' -"mystery" -"random" -"bundle" -"collection" -"bulk"'
    ' -PSA -BGS -SGC -CGC -graded -autograph -auto'
)

CATEGORIES = {
    "NFL": {
        "sport":          "NFL",
        "ebay_query":     f"football {EXCL}",
        "ebay_category":  "261328",   # Sports Trading Card Singles
        "aspect_filter":  "categoryId:261328,Sport:{Football}",
        "discord_emoji":  "🏈",
        "color":          0x013369,
    },
    "NBA": {
        "sport":          "NBA",
        "ebay_query":     f"basketball {EXCL}",
        "ebay_category":  "261328",   # Sports Trading Card Singles
        "aspect_filter":  "categoryId:261328,Sport:{Basketball}",
        "discord_emoji":  "🏀",
        "color":          0xC9082A,
    },
    "MLB": {
        "sport":          "MLB",
        "ebay_query":     f"baseball {EXCL}",
        "ebay_category":  "261328",   # Sports Trading Card Singles
        "aspect_filter":  "categoryId:261328,Sport:{Baseball}",
        "discord_emoji":  "⚾",
        "color":          0x002D72,
    },
    "NHL": {
        "sport":          "NHL",
        "ebay_query":     f"hockey {EXCL}",
        "ebay_category":  "261328",   # Sports Trading Card Singles
        "aspect_filter":  "categoryId:261328,Sport:{Ice Hockey}",
        "discord_emoji":  "🏒",
        "color":          0x000000,
    },
    "Pokemon": {
        "sport":          "Pokemon",
        "ebay_query":     EXCL,
        "ebay_category":  "183454",   # Pokemon Cards
        "aspect_filter":  None,
        "discord_emoji":  "⚡",
        "color":          0xFFCC00,
    },
}

# ===========================================================================
# Logging
# ===========================================================================

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger(__name__)

# ===========================================================================
# Supabase
# ===========================================================================

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ===========================================================================
# Local SQLite alert log (prevents duplicate alerts)
# ===========================================================================

def init_alert_db():
    conn = sqlite3.connect("alerts.db")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS alert_log (
            item_url TEXT PRIMARY KEY,
            alerted_at TEXT
        )
    """)
    conn.commit()
    conn.close()

def has_alerted(url: str) -> bool:
    conn = sqlite3.connect("alerts.db")
    row = conn.execute("SELECT 1 FROM alert_log WHERE item_url = ?", (url,)).fetchone()
    conn.close()
    return row is not None

def record_alert(url: str):
    conn = sqlite3.connect("alerts.db")
    conn.execute(
        "INSERT OR IGNORE INTO alert_log (item_url, alerted_at) VALUES (?, ?)",
        (url, datetime.utcnow().isoformat())
    )
    conn.commit()
    conn.close()

# ===========================================================================
# eBay OAuth token (cached)
# ===========================================================================

_ebay_token = None
_ebay_token_expiry = 0

def get_ebay_token() -> str:
    global _ebay_token, _ebay_token_expiry
    if _ebay_token and time.time() < _ebay_token_expiry:
        return _ebay_token

    auth = base64.b64encode(f"{EBAY_CLIENT_ID}:{EBAY_CLIENT_SECRET}".encode()).decode()
    resp = requests.post(
        EBAY_TOKEN_URL,
        headers={
            "Authorization": f"Basic {auth}",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        data=f"grant_type=client_credentials&scope={EBAY_SCOPE}",
    )
    resp.raise_for_status()
    data = resp.json()
    _ebay_token = data["access_token"]
    _ebay_token_expiry = time.time() + data["expires_in"] - 60
    log.info("Got new eBay token")
    return _ebay_token

# ===========================================================================
# eBay search
# ===========================================================================

def search_ebay(category_config: dict, listing_type: str) -> list:
    """Search eBay for raw cards. listing_type: 'bin' or 'auction'"""
    token = get_ebay_token()
    items = []

    for page in range(2):  # max 2 pages = 200 listings per type
        params = {
            "q":            category_config["ebay_query"],
            "category_ids": category_config["ebay_category"],
            "limit":        "100",
            "offset":       str(page * 100),
            "sort":         "-newlyListed" if listing_type == "bin" else "-endingSoonest",
        }

        # conditionIds: 1000=New, 2750=Like New only — anything worse won't grade well
        if listing_type == "bin":
            params["filter"] = "buyingOptions:{FIXED_PRICE},price:[10..],conditionIds:{1000|2750}"
        else:
            from datetime import timezone
            six_hours = datetime.now(timezone.utc).replace(microsecond=0)
            from datetime import timedelta
            six_hours = (datetime.now(timezone.utc) + timedelta(hours=6)).strftime("%Y-%m-%dT%H:%M:%SZ")
            params["filter"] = f"buyingOptions:{{AUCTION}},price:[10..],conditionIds:{{1000|2750}},itemEndDate:[..{six_hours}]"

        if category_config.get("aspect_filter"):
            params["aspect_filter"] = category_config["aspect_filter"]

        resp = requests.get(
            EBAY_SEARCH_URL,
            headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
            params=params,
        )

        if not resp.ok:
            log.error(f"eBay error: {resp.status_code} {resp.text[:200]}")
            break

        batch = resp.json().get("itemSummaries", [])
        items.extend(batch)
        log.info(f"  Fetched {len(batch)} {listing_type} items (page {page+1})")
        if len(batch) < 100:
            break

    return items

# ===========================================================================
# Load gradeable cards from Supabase
# ===========================================================================

_card_cache = {}  # sport -> list of card dicts

def load_gradeable_cards(sport: str) -> list:
    """Load cards with grading_score >= MIN_GRADING_SCORE for a given sport."""
    if sport in _card_cache:
        return _card_cache[sport]

    log.info(f"Loading gradeable cards for {sport}...")
    all_cards = []
    batch_size = 1000
    offset = 0

    while True:
        result = supabase.table("mv_grade_premiums") \
            .select("player_name, set_name, set_year, card_number, variation, "
                    "canonical_name, is_rookie, raw_price, psa9_price, psa10_price, "
                    "grading_score, raw_to_psa9_mult") \
            .eq("sport", sport) \
            .gte("grading_score", MIN_GRADING_SCORE) \
            .not_.is_("raw_price", "null") \
            .not_.is_("psa10_price", "null") \
            .range(offset, offset + batch_size - 1) \
            .execute()

        if not result.data:
            break

        all_cards.extend(result.data)
        if len(result.data) < batch_size:
            break
        offset += batch_size

    log.info(f"  Loaded {len(all_cards)} gradeable {sport} cards")
    _card_cache[sport] = all_cards
    return all_cards

# ===========================================================================
# Player name matching
# ===========================================================================

SUFFIX_RE = re.compile(r'\b(II|III|IV|Jr\.?|Sr\.?)$', re.IGNORECASE)

def strip_suffix(name: str) -> str:
    return SUFFIX_RE.sub('', name).strip()

def build_player_index(cards: list) -> dict:
    """Build cleaned_name -> original_name lookup for partial_ratio matching."""
    index = {}
    seen = set()
    for card in cards:
        name = card.get("player_name", "")
        if not name or name in seen:
            continue
        seen.add(name)
        cleaned = strip_suffix(name).lower()
        index[cleaned] = name
    return index

def get_candidate_players(title: str, index: dict) -> list:
    """Find players whose name appears in the eBay title using partial_ratio."""
    title_lower = title.lower()
    matches = []
    for cleaned_name, original_name in index.items():
        score = fuzz.partial_ratio(cleaned_name, title_lower)
        if score >= 90:
            matches.append((original_name, score))
    # Return sorted by score descending, deduplicated
    matches.sort(key=lambda x: -x[1])
    return [m[0] for m in matches]

# ===========================================================================
# Title parsing helpers
# ===========================================================================

def parse_grade(title: str) -> str:
    """Return grader+grade if present, else 'Raw'."""
    # All known grading companies including obscure ones
    GRADERS = (
        "PSA|BGS|SGC|CGC|CSG|HGA|GAI|GMA|KSA|WCG|BVG|CCG|CGA|CCA|OCE|"
        "PGS|OCG|AGS|TAG|ISA|BCCG|GAS|PTA|DGA|AFA|MNT|GEM MINT"
    )
    match = re.search(rf'\b({GRADERS})\s*(\d+\.?\d*)', title.upper())
    if match:
        return f"{match.group(1)} {match.group(2)}"
    # Also catch "Beckett X" and "Beckett NM" style grades
    if re.search(r'\bBECKETT\b', title.upper()):
        return "Beckett"
    return "Raw"

def clean_title(title: str) -> str:
    import re
    return re.sub(r'\b(RC|SP|SSP|rookie|card|lot|pack)\b', '', title, flags=re.IGNORECASE).strip()

# ===========================================================================
# Discord alert
# ===========================================================================

def post_discord_alert(card: dict, item: dict, listing_type: str,
                       ebay_price: float, category_config: dict):
    if not DISCORD_WEBHOOK:
        log.warning("No Discord webhook configured")
        return

    raw_price  = float(card["raw_price"])
    psa10      = float(card["psa10_price"])
    psa9       = float(card.get("psa9_price") or 0)
    grade_cost = 27.99  # PSA Value tier default
    net_profit = psa10 - ebay_price - grade_cost
    psa9_mult  = float(card.get("raw_to_psa9_mult") or 0)

    type_label = "🏷️ Buy It Now" if listing_type == "bin" else "⏱️ Auction"
    emoji      = category_config["discord_emoji"]
    rookie_tag = " 🌟 RC" if card.get("is_rookie") else ""

    hard_grade_warning = ""
    if psa9_mult >= 5.0:
        hard_grade_warning = f"\n⚠️ PSA 9 is {psa9_mult:.1f}x raw — historically difficult to grade"

    description = (
        f"**eBay:** ${ebay_price:.2f}  ·  "
        f"**DB Raw Median:** ${raw_price:.2f}  ·  "
        f"**Grading Score:** {card['grading_score']:.0f}/100\n"
        f"**PSA 9:** ${psa9:.2f}  ·  **PSA 10:** ${psa10:.2f}\n"
        f"**Est. Net Profit (PSA 10 via eBay):** ${net_profit:.2f} after ${grade_cost} grading\n"
        f"{hard_grade_warning}"
    )

    embed = {
        "title":       f"{emoji} Grading Opportunity — {card['canonical_name']}{rookie_tag}",
        "description": description,
        "url":         item.get("itemWebUrl", ""),
        "color":       category_config["color"],
        "fields": [
            {"name": "Set",          "value": f"{card['set_year']} {card['set_name']}", "inline": True},
            {"name": "Listing Type", "value": type_label,                               "inline": True},
            {"name": "Card #",       "value": str(card.get("card_number", "N/A")),      "inline": True},
        ],
        "footer": {"text": "Prices from DB (30-day median). Always verify condition before grading."},
    }

    if item.get("image", {}).get("imageUrl"):
        embed["thumbnail"] = {"url": item["image"]["imageUrl"]}

    resp = requests.post(
        DISCORD_WEBHOOK,
        json={"embeds": [embed]},
        headers={"Content-Type": "application/json"},
    )
    if not resp.ok:
        log.error(f"Discord webhook error: {resp.status_code} {resp.text}")

# ===========================================================================
# Process items for one category
# ===========================================================================

def process_items(items: list, listing_type: str, cards: list,
                  player_index: dict, category_config: dict):
    if not items:
        return

    log.info(f"Processing {len(items)} {listing_type} items...")
    alerts_sent = 0

    # Build canonical_name list for card-level fuzzy matching
    canonical_names = [c["canonical_name"] for c in cards if c.get("canonical_name")]

    # Debug: show sample player names and eBay titles
    sample_players = list({c["player_name"] for c in cards if c.get("player_name")})[:10]
    log.info(f"  Sample DB players: {sample_players}")
    raw_titles = [item.get("title","") for item in items if parse_grade(item.get("title","")) == "Raw"]
    log.info(f"  Sample raw eBay titles: {raw_titles[:5]}")

    skipped_graded = 0
    no_candidates  = 0
    no_player      = 0
    no_card        = 0
    price_too_high = 0
    low_profit     = 0

    for item in items:
        title = item.get("title", "")
        if not title:
            continue

        # Skip graded listings
        if parse_grade(title) != "Raw":
            skipped_graded += 1
            continue

        # Step 1: find candidate players from title words
        candidates = get_candidate_players(title, player_index)
        if not candidates:
            no_candidates += 1
            continue

        # Step 2: use best candidate (already scored by partial_ratio in get_candidate_players)
        matched_player = candidates[0]

        # Step 3: get cards for this player
        player_cards = [c for c in cards if c.get("player_name") == matched_player]
        if not player_cards:
            no_player += 1
            continue

        # Step 4: fuzzy match to specific card canonical name
        player_canonicals = [c["canonical_name"] for c in player_cards if c.get("canonical_name")]
        if not player_canonicals:
            no_card += 1
            continue

        card_match = fuzz_process.extractOne(
            clean_title(title.lower()),
            player_canonicals,
            scorer=fuzz.token_set_ratio,
            score_cutoff=70,
        )
        if not card_match:
            no_card += 1
            continue

        matched_canonical = card_match[0]
        matched_card = next((c for c in player_cards if c["canonical_name"] == matched_canonical), None)
        if not matched_card:
            no_card += 1
            continue

        # Strict validation: extract identifiers from eBay title
        # If none found → too vague to trust, skip
        # If found → must match DB card exactly
        ebay_year_match  = re.search(r'\b(19|20)\d{2}\b', title)
        ebay_card_match  = re.search(r'#\s*(\w+)', title)
        ebay_year        = int(ebay_year_match.group()) if ebay_year_match else None
        ebay_card_num    = ebay_card_match.group(1).lstrip('0') if ebay_card_match else None

        set_year         = int(matched_card["set_year"]) if matched_card.get("set_year") else None
        db_card_num      = str(matched_card.get("card_number") or "").lstrip('0')

        # If no year AND no card number found in title → skip (too vague)
        if not ebay_year and not ebay_card_num:
            log.info(f"  VAGUE SKIP: no year or card# in title — \"{title}\"")
            no_card += 1
            continue

        # Year must match exactly if present
        if ebay_year and set_year and ebay_year != set_year:
            log.info(f"  YEAR SKIP: eBay {ebay_year} != DB {set_year} — \"{title}\"")
            no_card += 1
            continue

        # Card number must match exactly if present in both
        if ebay_card_num and db_card_num and ebay_card_num != db_card_num:
            log.info(f"  CARD# SKIP: eBay #{ebay_card_num} != DB #{db_card_num} — \"{title}\"")
            no_card += 1
            continue

        # Set name validation: extract known brand/set keywords from eBay title
        # At least one must appear in the matched canonical name
        SET_KEYWORDS = [
            "prizm", "chrome", "topps", "bowman", "donruss", "optic", "mosaic",
            "select", "contenders", "score", "fleer", "upper deck", "skybox",
            "stadium club", "finest", "heritage", "archives", "series",
            "hoops", "panini", "sp authentic", "exquisite", "immaculate",
            "national treasures", "phoenix", "obsidian", "spectra", "revolution",
            "absolute", "certified", "playbook", "prestige", "legacy",
            "base set", "jungle", "fossil", "team rocket", "gym", "neo",
            "ex", "diamond", "legend", "platinum",
        ]
        title_lower_set = title.lower()
        canonical_lower = matched_canonical.lower()
        title_set_keywords = [kw for kw in SET_KEYWORDS if kw in title_lower_set]

        if title_set_keywords:
            if not any(kw in canonical_lower for kw in title_set_keywords):
                log.info(f"  SET SKIP: title keywords {title_set_keywords} not in \"{matched_canonical}\"")
                no_card += 1
                continue

        log.info(f"CARD MATCH: \"{title}\" -> {matched_canonical}")

        # Step 5: get eBay price
        if listing_type == "bin":
            price = float(item.get("price", {}).get("value", 0))
        else:
            price = float(item.get("currentBidPrice", {}).get("value", 0) or
                         item.get("price", {}).get("value", 0))

        if price <= 0:
            continue

        raw_median = float(matched_card["raw_price"])
        if raw_median <= 0:
            continue

        # Step 6: check price threshold (within 5% of DB median)
        if price > raw_median * PRICE_THRESHOLD:
            price_too_high += 1
            log.info(f"  PRICE SKIP: eBay ${price:.2f} > threshold ${raw_median * PRICE_THRESHOLD:.2f} (median ${raw_median:.2f})")
            continue

        # Step 7: check minimum profit
        psa10 = float(matched_card.get("psa10_price") or 0)
        net_profit = psa10 - price - 27.99
        if net_profit < MIN_PROFIT:
            low_profit += 1
            log.info(f"  PROFIT SKIP: net profit ${net_profit:.2f} < min ${MIN_PROFIT}")
            continue

        # Step 8: skip if already alerted
        url = item.get("itemWebUrl", "")
        if has_alerted(url):
            continue

        log.info(f"DEAL: {matched_canonical} | eBay: ${price:.2f} | Raw median: ${raw_median:.2f} | PSA10: ${psa10:.2f} | Profit: ${net_profit:.2f}")

        record_alert(url)
        post_discord_alert(matched_card, item, listing_type, price, category_config)
        alerts_sent += 1
        time.sleep(0.5)  # small delay between Discord posts

    log.info(f"  Sent {alerts_sent} alerts for {listing_type} | graded={skipped_graded} no_candidates={no_candidates} no_player={no_player} no_card={no_card} price_high={price_too_high} low_profit={low_profit}")

# ===========================================================================
# Main scan job
# ===========================================================================

def run_scan():
    log.info("=" * 60)
    log.info(f"Starting scan — {datetime.utcnow().isoformat()}")
    log.info("=" * 60)

    # Clear card cache each run so prices stay fresh
    _card_cache.clear()

    for cat_name, cat_config in CATEGORIES.items():
        log.info(f"\n--- Scanning {cat_name} ---")

        try:
            # Load gradeable cards for this sport
            cards = load_gradeable_cards(cat_config["sport"])
            if not cards:
                log.info(f"No gradeable cards found for {cat_name}, skipping")
                continue

            # Build player index
            player_index = build_player_index(cards)

            # Search eBay — BIN and auction in parallel would be nice but
            # keeping sequential to avoid rate limit issues
            bin_items     = search_ebay(cat_config, "bin")
            auction_items = search_ebay(cat_config, "auction")

            # Process results
            process_items(bin_items,     "bin",     cards, player_index, cat_config)
            process_items(auction_items, "auction", cards, player_index, cat_config)

        except Exception as e:
            log.error(f"Error scanning {cat_name}: {e}", exc_info=True)
            continue

    log.info("\nScan complete")

# ===========================================================================
# Entry point — runs once immediately then hourly
# ===========================================================================

if __name__ == "__main__":
    init_alert_db()
    log.info("Grade opportunity scanner starting...")

    # Run immediately on startup
    run_scan()

    # Then schedule hourly
    schedule.every(1).hours.do(run_scan)

    while True:
        schedule.run_pending()
        time.sleep(30)
