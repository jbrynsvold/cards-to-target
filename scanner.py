import os
import re
import time
import base64
import sqlite3
import logging
import schedule
import requests
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from supabase import create_client, Client
from rapidfuzz import fuzz

load_dotenv()

# ===========================================================================
# Config
# ===========================================================================

EBAY_CLIENT_ID     = os.getenv("EBAY_CLIENT_ID")
EBAY_CLIENT_SECRET = os.getenv("EBAY_CLIENT_SECRET")
SUPABASE_URL       = os.getenv("SUPABASE_URL")
SUPABASE_KEY       = os.getenv("SUPABASE_KEY")
DISCORD_WEBHOOK    = os.getenv("DISCORD_WEBHOOK_GRADE_ALERTS")

EBAY_TOKEN_URL  = "https://api.ebay.com/identity/v1/oauth2/token"
EBAY_SEARCH_URL = "https://api.ebay.com/buy/browse/v1/item_summary/search"
EBAY_SCOPE      = "https://api.ebay.com/oauth/api_scope"

# Price threshold: alert if eBay listing <= raw_price * this multiplier
PRICE_THRESHOLD = 1.25

# ROI-based card inclusion filters
MIN_ROI_MULTIPLE  = 2.5
MAX_ROI_MULTIPLE  = 500
MIN_NET_PROFIT    = 20
MIN_RAW_SALES_30D = 5
MIN_PSA10_SALES   = 2
GRADING_COST      = 28

# Minimum PSA 10 profit after grading to alert
MIN_PROFIT = 50

# Minimum score for a card to be considered matched
MIN_MATCH_SCORE     = 65
MIN_MATCH_SCORE_TCG = 65

# Minimum word length for player name index
MIN_WORD_LEN = 4

# eBay search exclusions
EXCL = (
    '-"you pick" -"lot of" -"choose your" -"complete your set" -"u pick"'
    ' -"card lot" -"pack of" -"box of" -"blaster" -"hobby box"'
    ' -"factory sealed" -"sealed box" -"sealed pack" -"complete set"'
    ' -"mystery" -"random" -"bundle" -"collection" -"bulk"'
    ' -"pick a card" -"pick your card" -"you choose" -"choose from"'
    ' -"art card" -"fan art" -"custom card" -"custom slab" -"custom art"'
    ' -"uncut" -"panels" -"stamp card" -"holographic"'
    ' -"tcg pocket" -"pocket" -"japanese" -"chinese" -"korean"'
    ' -PSA -BGS -SGC -CGC -graded -autograph -auto'
    ' -cards'
)

EXCL_KEYWORDS = [
    "you pick", "lot of", "choose your", "complete your set", "u pick",
    "card lot", "pack of", "box of", "blaster", "hobby box",
    "factory sealed", "sealed box", "sealed pack", "complete set",
    "mystery", "random", "bundle", "collection", "bulk",
    "pick a card", "pick your card", "you choose", "choose from",
    "art card", "fan art", "custom card", "custom slab", "custom art",
    "uncut", "panels", "stamp card", "holographic",
    "tcg pocket", "pocket",
    "japanese", "chinese", "korean",
]

JAPANESE_SET_CODE_RE = re.compile(
    r'\b(sv\d+[a-zA-Z]*|SV-P|SV[0-9]+[a-zA-Z]|s\d+[a-zA-Z]|SM\d+|XY\d+|BW\d+)\b'
)

TEAM_NAMES = {
    # MLB
    "Baltimore Orioles", "Boston Red Sox", "New York Yankees", "Los Angeles Dodgers",
    "Chicago Cubs", "Houston Astros", "Atlanta Braves", "San Francisco Giants",
    "St. Louis Cardinals", "Philadelphia Phillies", "New York Mets", "Los Angeles Angels",
    "Seattle Mariners", "Toronto Blue Jays", "Tampa Bay Rays", "Minnesota Twins",
    "Cleveland Guardians", "Detroit Tigers", "Kansas City Royals", "Chicago White Sox",
    "Texas Rangers", "Oakland Athletics", "San Diego Padres", "Colorado Rockies",
    "Arizona Diamondbacks", "Miami Marlins", "Pittsburgh Pirates", "Cincinnati Reds",
    "Milwaukee Brewers", "Washington Nationals",
    # NBA
    "Los Angeles Lakers", "Boston Celtics", "Chicago Bulls", "Golden State Warriors",
    "Miami Heat", "San Antonio Spurs", "Dallas Mavericks", "Phoenix Suns",
    "Denver Nuggets", "Milwaukee Bucks", "Brooklyn Nets", "Philadelphia 76ers",
    "Toronto Raptors", "New York Knicks", "Cleveland Cavaliers", "Oklahoma City Thunder",
    "Memphis Grizzlies", "New Orleans Pelicans", "Sacramento Kings", "Utah Jazz",
    "Portland Trail Blazers", "Indiana Pacers", "Atlanta Hawks", "Charlotte Hornets",
    "Detroit Pistons", "Washington Wizards", "Orlando Magic", "Minnesota Timberwolves",
    "Houston Rockets", "Los Angeles Clippers",
    # NFL
    "Kansas City Chiefs", "San Francisco 49ers", "Dallas Cowboys", "New England Patriots",
    "Green Bay Packers", "Pittsburgh Steelers", "Baltimore Ravens", "Buffalo Bills",
    "Philadelphia Eagles", "Cincinnati Bengals", "Los Angeles Rams", "Miami Dolphins",
    "Las Vegas Raiders", "Denver Broncos", "Seattle Seahawks", "Tampa Bay Buccaneers",
    "New Orleans Saints", "Minnesota Vikings", "Chicago Bears", "New York Giants",
    "New York Jets", "Washington Commanders", "Carolina Panthers", "Atlanta Falcons",
    "Detroit Lions", "Arizona Cardinals", "Los Angeles Chargers", "Indianapolis Colts",
    "Tennessee Titans", "Jacksonville Jaguars", "Cleveland Browns", "Houston Texans",
    # NHL
    "Toronto Maple Leafs", "Montreal Canadiens", "Boston Bruins", "New York Rangers",
    "Chicago Blackhawks", "Detroit Red Wings", "Philadelphia Flyers", "Edmonton Oilers",
    "Pittsburgh Penguins", "Colorado Avalanche", "Tampa Bay Lightning", "Vegas Golden Knights",
    "Carolina Hurricanes", "Florida Panthers", "New York Islanders", "Washington Capitals",
    "Minnesota Wild", "St. Louis Blues", "Nashville Predators", "Winnipeg Jets",
    "Calgary Flames", "Vancouver Canucks", "Ottawa Senators", "Buffalo Sabres",
    "New Jersey Devils", "Columbus Blue Jackets", "San Jose Sharks", "Anaheim Ducks",
    "Seattle Kraken", "Dallas Stars", "Arizona Coyotes", "Quebec Nordiques",
    "Hartford Whalers", "Atlanta Thrashers",
}

CATEGORIES = {
    "NFL": {
        "sport":         "NFL",
        "ebay_query":    f"football {EXCL}",
        "ebay_category": "261328",
        "aspect_filter": "categoryId:261328,Sport:{Football},Graded:{No}",
        "discord_emoji": "🏈",
        "color":         0x013369,
    },
    "NBA": {
        "sport":         "NBA",
        "ebay_query":    f"basketball {EXCL}",
        "ebay_category": "261328",
        "aspect_filter": "categoryId:261328,Sport:{Basketball},Graded:{No}",
        "discord_emoji": "🏀",
        "color":         0xC9082A,
    },
    "MLB": {
        "sport":         "MLB",
        "ebay_query":    f"baseball {EXCL}",
        "ebay_category": "261328",
        "aspect_filter": "categoryId:261328,Sport:{Baseball},Graded:{No}",
        "discord_emoji": "⚾",
        "color":         0x002D72,
    },
    "NHL": {
        "sport":         "NHL",
        "ebay_query":    f"hockey {EXCL}",
        "ebay_category": "261328",
        "aspect_filter": "categoryId:261328,Sport:{Ice Hockey},Graded:{No}",
        "discord_emoji": "🏒",
        "color":         0x000000,
    },
    "Pokemon": {
        "sport":         "Pokemon",
        "ebay_query":    f'pokemon -"magic the gathering" -MTG -yugioh -lorcana -"one piece" -"dragon ball" -vanguard {EXCL}',
        "ebay_category": "183454",
        "aspect_filter": "categoryId:183454,Graded:{No}",
        "discord_emoji": "⚡",
        "color":         0xFFCC00,
        "is_tcg":        True,
        "min_year":      2010,
    },
    "Yu-Gi-Oh": {
        "sport":         "Yu-Gi-Oh",
        "ebay_query":    f"yugioh {EXCL}",
        "ebay_category": "183454",
        "aspect_filter": "categoryId:183454,Graded:{No}",
        "discord_emoji": "🃏",
        "color":         0x6A0DAD,
        "is_tcg":        True,
        "min_year":      2010,
    },
}

# ===========================================================================
# Logging
# ===========================================================================

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

_scan_start_time = None

def log_elapsed(message: str):
    if _scan_start_time is not None:
        elapsed = time.time() - _scan_start_time
        mins = int(elapsed // 60)
        secs = int(elapsed % 60)
        log.info(f"[+{mins:02d}:{secs:02d}] {message}")
    else:
        log.info(message)

# ===========================================================================
# Supabase
# ===========================================================================

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ===========================================================================
# SQLite alert log
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
        (url, datetime.now(timezone.utc).isoformat())
    )
    conn.commit()
    conn.close()

# ===========================================================================
# eBay token
# ===========================================================================

_ebay_token        = None
_ebay_token_expiry = 0

def get_ebay_token() -> str:
    global _ebay_token, _ebay_token_expiry
    if _ebay_token and time.time() < _ebay_token_expiry:
        return _ebay_token

    auth = base64.b64encode(
        f"{EBAY_CLIENT_ID}:{EBAY_CLIENT_SECRET}".encode()
    ).decode()
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
    _ebay_token        = data["access_token"]
    _ebay_token_expiry = time.time() + data["expires_in"] - 60
    log.info("Got new eBay token")
    return _ebay_token

# ===========================================================================
# eBay search
# ===========================================================================

def search_ebay(category_config: dict, listing_type: str) -> list:
    token = get_ebay_token()
    items = []

    for page in range(2):
        params = {
            "q":            category_config["ebay_query"],
            "category_ids": category_config["ebay_category"],
            "limit":        "100",
            "offset":       str(page * 100),
            "sort":         "-newlyListed" if listing_type == "bin" else "endingSoonest",
        }

        if category_config.get("aspect_filter"):
            params["aspect_filter"] = category_config["aspect_filter"]

        if listing_type == "bin":
            params["filter"] = "buyingOptions:{FIXED_PRICE},price:[10..],conditionIds:{3000|4000}"
        else:
            six_hours = (datetime.now(timezone.utc) + timedelta(hours=6)).strftime("%Y-%m-%dT%H:%M:%SZ")
            now_str   = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            params["filter"] = f"buyingOptions:{{AUCTION}},price:[10..],conditionIds:{{3000|4000}},itemEndDate:[{now_str}..{six_hours}]"

        time.sleep(1)
        resp = requests.get(
            EBAY_SEARCH_URL,
            headers={"Authorization": f"Bearer {get_ebay_token()}", "X-EBAY-C-MARKETPLACE-ID": "EBAY_US"},
            params=params,
        )

        if not resp.ok:
            log.error(f"eBay error: {resp.status_code} {resp.text[:200]}")
            break

        batch = resp.json().get("itemSummaries", [])
        items.extend(batch)
        log_elapsed(f"Fetched {len(batch)} {listing_type} items (page {page+1})")
        if len(batch) < 100:
            break

    return items

# ===========================================================================
# Load gradeable cards
# ===========================================================================

_card_cache = {}

def load_gradeable_cards(sport: str, min_year: int = None) -> list:
    cache_key = f"{sport}:{min_year}"
    if cache_key in _card_cache:
        return _card_cache[cache_key]

    log_elapsed(f"Loading gradeable cards for {sport}...")
    all_cards = []
    batch_size = 1000
    offset = 0

    while True:
        result = supabase.table("mv_grade_premiums") \
            .select("player_name, set_name, set_year, card_number, variation, "
                    "canonical_name, is_rookie, raw_price, psa9_price, psa10_price, "
                    "grading_score, raw_to_psa9_mult, psa10_sale_count_30d, raw_sale_count_30d") \
            .eq("sport", sport) \
            .not_.is_("raw_price", "null") \
            .not_.is_("psa10_price", "null") \
            .gte("raw_sale_count_30d", MIN_RAW_SALES_30D) \
            .gte("psa10_sale_count_30d", MIN_PSA10_SALES) \
            .gte("raw_price", 10) \
            .range(offset, offset + batch_size - 1) \
            .execute()

        if not result.data:
            break

        all_cards.extend(result.data)
        if len(result.data) < batch_size:
            break
        offset += batch_size

    filtered = []
    for c in all_cards:
        try:
            raw   = float(c["raw_price"])
            psa10 = float(c["psa10_price"])
            roi   = psa10 / (raw + GRADING_COST)
            net   = psa10 - raw - GRADING_COST
            if min_year and c.get("set_year") and int(c["set_year"]) < min_year:
                continue
            if roi >= MIN_ROI_MULTIPLE and roi <= MAX_ROI_MULTIPLE and net >= MIN_NET_PROFIT:
                filtered.append(c)
        except (TypeError, ValueError, ZeroDivisionError):
            continue

    log_elapsed(f"Loaded {len(all_cards)} cards, {len(filtered)} passed ROI filter for {sport}")
    _card_cache[cache_key] = filtered
    return filtered

# ===========================================================================
# Player name index — two-stage (inverted index + partial_ratio)
# ===========================================================================

SUFFIX_RE = re.compile(r'\b(II|III|IV|Jr\.?|Sr\.?)$', re.IGNORECASE)

def strip_suffix(name: str) -> str:
    return SUFFIX_RE.sub('', name).strip()

# sport -> { word -> set(player_names) }
_word_to_players: dict = {}
# sport -> { cleaned_name -> original_name }
_cleaned_to_original: dict = {}
_player_index_loaded: set = set()

def load_player_index(sport: str):
    if sport in _player_index_loaded:
        return

    log.info(f"Loading {sport} player names...")
    result = supabase.table("player_name_index") \
        .select("player_name") \
        .eq("sport", sport) \
        .limit(50000) \
        .execute()
    all_names = [r["player_name"] for r in (result.data or []) if r.get("player_name")]

    word_map    = {}
    cleaned_map = {}
    seen        = set()

    for name in all_names:
        if name in seen:
            continue
        seen.add(name)
        cleaned = strip_suffix(name).lower()
        cleaned_map[cleaned] = name
        for word in cleaned.split():
            if len(word) >= MIN_WORD_LEN:
                word_map.setdefault(word, set()).add(name)

    _word_to_players[sport]     = word_map
    _cleaned_to_original[sport] = cleaned_map
    _player_index_loaded.add(sport)
    log.info(f"{sport}: loaded {len(cleaned_map)} players, {len(word_map)} index words")


def get_candidate_players(title: str, sport: str) -> list:
    """Two-stage: fast inverted index then partial_ratio on small candidate set."""
    title_lower = title.lower()
    word_map    = _word_to_players.get(sport, {})

    title_words   = [w for w in re.split(r'\W+', title_lower) if len(w) >= MIN_WORD_LEN]
    candidate_set = set()
    for word in title_words:
        for player in word_map.get(word, []):
            candidate_set.add(player)

    if not candidate_set:
        return []

    matches = []
    for original_name in candidate_set:
        if original_name in TEAM_NAMES:
            continue
        cleaned = strip_suffix(original_name).lower()
        score   = fuzz.partial_ratio(cleaned, title_lower)
        if score >= 92:
            matches.append((original_name, score))

    matches.sort(key=lambda x: -x[1])
    return [m[0] for m in matches]

# ===========================================================================
# Set / variation token matching
# ===========================================================================

SET_NOISE_WORDS = {
    "basketball", "football", "baseball", "hockey",
    "trading", "card", "cards", "tcg", "nfl", "nba", "mlb", "nhl",
}

BASE_VARIATIONS = {"", "base", "none", "base card", "n/a"}

STRONG_NON_BASE = {
    "silver", "gold", "refractor", "prizm", "holo", "foil",
    "rainbow", "atomic", "laser", "hyper", "mojo", "cracked",
    "shimmer", "wave", "pulsar", "disco", "glossy", "lazer",
    "stamped", "prerelease", "shadowless", "cosmos",
    "reverse", "fullart", "altart", "promo", "kaboom", "horizontal",
}

def tokenize(text: str, min_len: int = 3) -> list:
    return [w.lower() for w in re.split(r'[\W_]+', text) if len(w) >= min_len]

def set_tokens(set_name: str) -> list:
    return [t for t in tokenize(set_name) if t not in SET_NOISE_WORDS]

def variation_tokens(variation: str) -> list:
    stop = {"and", "the", "of", "for", "a"}
    return [t for t in tokenize(variation, min_len=2) if t not in stop]

def score_card_match(title_lower: str, card: dict,
                     ebay_year: int, ebay_year2: int,
                     ebay_card_num: str) -> float:
    set_year    = int(card["set_year"]) if card.get("set_year") else None
    db_card_num = str(card.get("card_number") or "").lstrip("0")
    set_name    = (card.get("set_name") or "")
    variation   = (card.get("variation") or "").strip()
    is_base     = variation.lower() in BASE_VARIATIONS

    # Hard filter: year
    if set_year and (ebay_year or ebay_year2):
        if ebay_year != set_year and ebay_year2 != set_year:
            return -1.0

    # Hard filter: card number
    if ebay_card_num and db_card_num:
        if ebay_card_num != db_card_num:
            return -1.0

    score = 0.0

    # Set name matching
    s_tokens = set_tokens(set_name)
    if s_tokens:
        found = [t for t in s_tokens if t in title_lower]
        score += (len(found) / len(s_tokens)) * 60
        if len(found) == len(s_tokens):
            score += 20
        if not found:
            score -= 20
    else:
        score += 10

    # Variation matching
    if not is_base:
        v_tokens = variation_tokens(variation)
        if v_tokens:
            found_v = [t for t in v_tokens if t in title_lower]
            ratio_v = len(found_v) / len(v_tokens)
            if ratio_v <= 0.5:
                return -1.0
            score += ratio_v * 60
            if len(found_v) == len(v_tokens):
                score += 20
        else:
            if variation.lower() in title_lower:
                score += 20
            else:
                score -= 10
    else:
        title_tokens = set(tokenize(title_lower))
        if title_tokens & STRONG_NON_BASE:
            score -= 40

# Canonical name extra token check
        canonical = (card.get("canonical_name") or "").lower()
        set_name_lower = set_name.lower()
        canonical_extra = [
            t for t in tokenize(canonical)
            if t not in tokenize(set_name_lower)
            and t not in SET_NOISE_WORDS
            and len(t) >= 4
        ]
        if canonical_extra:
            missing = [t for t in canonical_extra if t not in title_lower]
            if missing and len(missing) / len(canonical_extra) >= 0.5:
                return -1.0
                         
    # Year bonus
    if set_year and (ebay_year == set_year or ebay_year2 == set_year):
        score += 10

    return score

# ===========================================================================
# Title parsing
# ===========================================================================

def parse_grade(title: str) -> str:
    GRADERS = (
        "PSA|BGS|SGC|CGC|CSG|HGA|GAI|GMA|KSA|WCG|BVG|CCG|CGA|CCA|OCE|"
        "PGS|OCG|AGS|TAG|ISA|BCCG|GAS|PTA|DGA|AFA|MNT|GEM MINT"
    )
    t = title.upper()
    match = re.search(rf'\b({GRADERS})\s*(\d+\.?\d*)', t)
    if match:
        return f"{match.group(1)} {match.group(2)}"
    if re.search(r'\bBECKETT\b', t):
        return "Beckett"
    if re.search(r'\bGEM\s*(MINT|MT)?\s*\d+', t):
        return "Graded"
    if re.search(r'\b(9\.5|10)\s*(MINT|GEM)\b', t):
        return "Graded"
    return "Raw"


def parse_title_years(title: str):
    """Extract ebay_year, ebay_year2, ebay_card_num from an eBay title."""
    ebay_year  = None
    ebay_year2 = None

    full_year = re.search(r'\b(19|20)\d{2}\b', title)
    if full_year:
        ebay_year = int(full_year.group())
        # Handle hockey-style 1995-96 hyphenated years
        hockey_year = re.search(r'\b(19|20)(\d{2})-(\d{2})\b', title)
        if hockey_year:
            suffix = int(hockey_year.group(3))
            ebay_year2 = 2000 + suffix if suffix <= 30 else 1900 + suffix
    else:
        short = re.search(r'\b(\d{2})-(\d{2})\b', title)
        if short:
            y1, y2 = int(short.group(1)), int(short.group(2))
            if (y1 >= 90 or y1 <= 26) and (y2 >= 90 or y2 <= 26):
                ebay_year  = (1900 if y1 >= 90 else 2000) + y1
                ebay_year2 = 2000 + y2

    card_num_match = re.search(r'#\s*(\w+)', title)
    ebay_card_num  = card_num_match.group(1).lstrip('0') if card_num_match else None

    return ebay_year, ebay_year2, ebay_card_num


def format_time_remaining(end_time_str: str) -> str:
    if not end_time_str:
        return ""
    try:
        end_dt     = datetime.fromisoformat(end_time_str.replace("Z", "+00:00"))
        now_dt     = datetime.now(timezone.utc)
        delta      = end_dt - now_dt
        if delta.total_seconds() <= 0:
            return "ended"
        total_secs = int(delta.total_seconds())
        hours      = total_secs // 3600
        minutes    = (total_secs % 3600) // 60
        seconds    = total_secs % 60
        if hours > 0:
            return f"{hours}h {minutes}m"
        elif minutes > 0:
            return f"{minutes}m {seconds}s"
        else:
            return f"{seconds}s"
    except Exception:
        return ""

# ===========================================================================
# Discord
# ===========================================================================

def post_discord_alert(card: dict, item: dict, listing_type: str,
                       ebay_price: float, category_config: dict):
    if not DISCORD_WEBHOOK:
        log.warning("No Discord webhook configured")
        return

    raw_price  = float(card["raw_price"])
    psa10      = float(card["psa10_price"])
    psa9       = float(card.get("psa9_price") or 0)
    grade_cost = 27.99
    net_profit = psa10 - ebay_price - grade_cost
    psa9_mult  = float(card.get("raw_to_psa9_mult") or 0)

    type_label = "🏷️ Buy It Now" if listing_type == "bin" else "⏱️ Auction"
    emoji      = category_config["discord_emoji"]
    rookie_tag = " 🌟 RC" if card.get("is_rookie") else ""

    hard_grade_warning = ""
    if psa9_mult >= 5.0:
        hard_grade_warning = f"\n⚠️ PSA 9 is {psa9_mult:.1f}x raw — historically difficult to grade"

    time_remaining_str = ""
    if listing_type == "auction":
        tr = format_time_remaining(item.get("itemEndDate", ""))
        if tr:
            time_remaining_str = f"\n⏳ **Time Remaining:** {tr}"

    description = (
        f"**eBay:** ${ebay_price:.2f}  ·  "
        f"**DB Raw Median:** ${raw_price:.2f}  ·  "
        f"**Grading Score:** {card['grading_score']:.0f}/100\n"
        f"**PSA 9:** ${psa9:.2f}  ·  **PSA 10:** ${psa10:.2f}\n"
        f"**Est. Net Profit (PSA 10 via eBay):** ${net_profit:.2f} after ${grade_cost} grading"
        f"{time_remaining_str}"
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
# Process items
# ===========================================================================

def process_items(items: list, listing_type: str, cards: list,
                  sport: str, category_config: dict):
    if not items:
        return

    is_tcg        = category_config.get("is_tcg", False)
    min_score     = MIN_MATCH_SCORE_TCG if is_tcg else MIN_MATCH_SCORE
    section_start = time.time()

    log_elapsed(f"Processing {len(items)} {listing_type} items...")

    skipped_graded = 0
    no_candidates  = 0
    no_player      = 0
    no_card        = 0
    price_too_high = 0
    low_profit     = 0
    alerts_sent    = 0

    for item in items:
        title = item.get("title", "")
        if not title:
            continue

        if parse_grade(title) != "Raw":
            skipped_graded += 1
            continue

        title_lower_excl = title.lower()
        if any(kw in title_lower_excl for kw in EXCL_KEYWORDS):
            no_candidates += 1
            continue

        if is_tcg and JAPANESE_SET_CODE_RE.search(title):
            no_candidates += 1
            continue

        title_words = [w for w in re.split(r'\W+', title) if len(w) >= 4]
        if len(title_words) < 2:
            no_candidates += 1
            continue

        ebay_year, ebay_year2, ebay_card_num = parse_title_years(title)

        if not is_tcg and not ebay_year and not ebay_card_num:
            no_candidates += 1
            log_elapsed(f"NO_CANDIDATE [no_year_or_cardnum]: {title}")
            continue

        # Two-stage player matching
        candidates = get_candidate_players(title, sport)
        if not candidates:
            no_candidates += 1
            log_elapsed(f"NO_CANDIDATE [no_player_match]: {title}")
            continue

        matched_player = candidates[0]

        player_cards = [c for c in cards if c.get("player_name") == matched_player]
        if not player_cards:
            no_player += 1
            continue

        title_lower  = title.lower()
        matched_card = None
        best_score   = 0.0

        for card in player_cards:
            s = score_card_match(title_lower, card, ebay_year, ebay_year2, ebay_card_num)
            if s < 0:
                continue
            if s > best_score:
                best_score   = s
                matched_card = card

        if not matched_card or best_score < min_score:
            no_card += 1
            log_elapsed(
                f"NO_CARD: \"{title}\" -> player={matched_player} "
                f"best_score={best_score:.0f} cards_checked={len(player_cards)}"
            )
            continue

        log_elapsed(
            f"CARD MATCH: \"{title}\" -> {matched_card['canonical_name']} "
            f"(score={best_score:.0f} variation={matched_card.get('variation') or 'base'})"
        )

        price = float(item.get("price", {}).get("value", 0))
        if price <= 0:
            continue

        raw_median = float(matched_card["raw_price"])
        if raw_median <= 0:
            continue

        if price > raw_median * PRICE_THRESHOLD:
            price_too_high += 1
            log_elapsed(
                f"  PRICE SKIP: {matched_card['canonical_name']} | "
                f"eBay: ${price:.2f} | Market: ${raw_median:.2f} | "
                f"Threshold: ${raw_median * PRICE_THRESHOLD:.2f}"
            )
            continue

        psa10      = float(matched_card.get("psa10_price") or 0)
        net_profit = psa10 - price - 27.99
        if net_profit < MIN_PROFIT:
            low_profit += 1
            log_elapsed(f"  PROFIT SKIP: net profit ${net_profit:.2f} < min ${MIN_PROFIT}")
            continue

        url = item.get("itemWebUrl", "")
        if has_alerted(url):
            continue

        time_remaining_log = ""
        if listing_type == "auction":
            tr = format_time_remaining(item.get("itemEndDate", ""))
            if tr:
                time_remaining_log = f" | Time remaining: {tr}"

        log_elapsed(
            f"DEAL: {matched_card['canonical_name']} | eBay: ${price:.2f} | "
            f"Raw median: ${raw_median:.2f} | PSA10: ${psa10:.2f} | "
            f"Profit: ${net_profit:.2f}{time_remaining_log}"
        )

        record_alert(url)
        post_discord_alert(matched_card, item, listing_type, price, category_config)
        alerts_sent += 1
        time.sleep(0.5)

    elapsed_sec = time.time() - section_start
    log_elapsed(
        f"Sent {alerts_sent} alerts for {listing_type} [{elapsed_sec:.1f}s] | "
        f"graded={skipped_graded} no_candidates={no_candidates} "
        f"no_player={no_player} no_card={no_card} "
        f"price_high={price_too_high} low_profit={low_profit}"
    )

# ===========================================================================
# Main scan
# ===========================================================================

def run_scan():
    global _scan_start_time
    _scan_start_time = time.time()

    log.info("=" * 60)
    log.info(f"Starting grading scan — {datetime.now(timezone.utc).isoformat()}")
    log.info("=" * 60)

    _card_cache.clear()

    for cat_name, cat_config in CATEGORIES.items():
        sport = cat_config["sport"]
        log_elapsed(f"\n--- Scanning {cat_name} ---")
        time.sleep(5)

        try:
            load_player_index(sport)

            cards = load_gradeable_cards(sport, min_year=cat_config.get("min_year"))
            if not cards:
                log_elapsed(f"No gradeable cards found for {cat_name}, skipping")
                continue

            bin_items     = search_ebay(cat_config, "bin")
            auction_items = search_ebay(cat_config, "auction")

            process_items(bin_items,     "bin",     cards, sport, cat_config)
            process_items(auction_items, "auction", cards, sport, cat_config)

        except Exception as e:
            log.error(f"Error scanning {cat_name}: {e}", exc_info=True)
            continue

    total_elapsed = time.time() - _scan_start_time
    log.info(f"\nScan complete — total time: {total_elapsed:.1f}s ({total_elapsed/60:.1f}m)")

# ===========================================================================
# Entry point — runs once immediately then every 30 minutes
# ===========================================================================

if __name__ == "__main__":
    init_alert_db()
    log.info("Grading opportunity scanner starting...")

    run_scan()

    schedule.every(30).minutes.do(run_scan)

    while True:
        schedule.run_pending()
        time.sleep(30)
