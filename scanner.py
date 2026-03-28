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

PRICE_THRESHOLD     = 1.25
MIN_ROI_MULTIPLE    = 2.5
MAX_ROI_MULTIPLE    = 500
MIN_NET_PROFIT      = 20
MIN_RAW_SALES_30D   = 3
MIN_PSA10_SALES     = 1
GRADING_COST        = 28
MIN_PROFIT          = 50
MIN_MATCH_SCORE     = 65
MIN_MATCH_SCORE_TCG = 55  # slightly lower for TCG since set name matching is looser
MIN_WORD_LEN        = 4
EBAY_FEE_PCT        = 0.1287
MIN_PRICE_PCT       = 0.30  # eBay price must be at least 30% of raw median


EXCL = (
    '-"you pick" -"lot of" -"choose your" -"complete your set" -"u pick"'
    ' -"card lot" -"pack of" -"box of" -"blaster" -"hobby box"'
    ' -"factory sealed" -"sealed box" -"sealed pack" -"complete set"'
    ' -"mystery" -"random" -"bundle" -"collection" -"bulk"'
    ' -"pick a card" -"pick your card" -"you choose" -"choose from"'
    ' -"art card" -"fan art" -"custom card" -"custom slab" -"custom art"'
    ' -"uncut" -"panels" -"stamp card" -"holographic"'
    ' -"tcg pocket" -"pocket" -"japanese" -"chinese" -"korean"'
    ' -"pick your player" -"pick & choose" -"pick from list"'
    ' -"fill your set" -"build a lot" -"set break" -"card pick"'
    ' -"complete a set" -"complete the set" -"take your pick"'
    ' -"buy more" -"free ship" -"flat s/h"'
    ' -PSA -BGS -SGC -CGC -graded'
    ' -"cards!" -"jumbo"'
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
    "pick your player", "pick & choose", "pick from list",
    "fill your set", "build a lot", "set break",
    "card pick", "singles", "jumbo",
]

JAPANESE_SET_CODE_RE = re.compile(
    r'\b(sv\d+[a-zA-Z]*|SV-P|SV[0-9]+[a-zA-Z]|s\d+[a-zA-Z]|SM\d+|XY\d+|BW\d+)\b'
)

# Yu-Gi-Oh set code pattern e.g. LOB-005, MRD-126, RA01-EN008
YGO_SET_CODE_RE = re.compile(
    r'\b([A-Z]{2,6}\d*-[A-Z]{0,2}\d{3})\b'
)

BASE_VARIATIONS = {"", "base", "none", "base card", "n/a"}

SET_NOISE_WORDS = {
    "basketball", "football", "baseball", "hockey",
    "trading", "card", "cards", "tcg", "nfl", "nba", "mlb", "nhl",
}

STRONG_NON_BASE = {
    "silver", "gold", "refractor", "prizm", "holo", "foil",
    "rainbow", "atomic", "laser", "hyper", "mojo", "cracked",
    "shimmer", "wave", "pulsar", "disco", "glossy", "lazer",
    "stamped", "prerelease", "shadowless", "cosmos",
    "reverse", "fullart", "altart", "promo", "kaboom", "horizontal",
}

POKEMON_GENERATION_TOKENS = {
    "scarlet", "violet", "sword", "shield", "sun", "moon",
    "black", "white", "diamond", "pearl", "heartgold", "soulsilver",
    "winds", "waves", "mega", "evolution",
}

# Pokemon set name aliases — maps canonical set name keywords to alternate
# short names sellers use in titles. Keys are substrings of the DB set name
# (lowercase), values are lists of alternate terms sellers use.
POKEMON_SET_ALIASES = {
    "surging sparks":       ["sv08"],
    "stellar crown":        ["sv07"],
    "twilight masquerade":  ["sv06"],
    "shrouded fable":       ["sv06.5"],
    "paldean fates":        ["sv04.5"],
    "paradox rift":         ["sv04"],
    "obsidian flames":      ["sv03", "obsidian"],
    "paldea evolved":       ["sv02"],
    "scarlet violet":       ["sv01"],
    "prismatic evolutions": ["sv08.5", "prismatic evo"],
    "ascended heroes":      ["ascended heros"],
    "phantasmal flames":    ["phantasmal"],
    "lost origin":          ["lost origin"],
    "crown zenith":         ["crown zenith"],
    "brilliant stars":      ["brilliant stars"],
    "fusion strike":        ["fusion strike"],
    "battle styles":        ["battle styles"],
    "vivid voltage":        ["vivid voltage"],
    "darkness ablaze":      ["darkness ablaze"],
    "rebel clash":          ["rebel clash"],
    "team up":              ["team up"],
    "unbroken bonds":       ["unbroken bonds"],
    "cosmic eclipse":       ["cosmic eclipse"],
    "hidden fates":         ["hidden fates"],
    "ultra prism":          ["ultra prism"],
    "forbidden light":      ["forbidden light"],
    "celestial storm":      ["celestial storm"],
    "lost thunder":         ["lost thunder"],
    "dragon majesty":       ["dragon majesty"],
    "roaring skies":        ["roaring skies"],
    "ancient origins":      ["ancient origins"],
    "primal clash":         ["primal clash"],
    "phantom forces":       ["phantom forces"],
    "furious fists":        ["furious fists"],
    "flashfire":            ["flashfire"],
    "plasma blast":         ["plasma blast"],
    "plasma freeze":        ["plasma freeze"],
    "plasma storm":         ["plasma storm"],
    "boundaries crossed":   ["boundaries crossed"],
    "dragons exalted":      ["dragons exalted"],
    "dark explorers":       ["dark explorers"],
    "next destinies":       ["next destinies"],
    "noble victories":      ["noble victories"],
    "emerging powers":      ["emerging powers"],
    "black star promo":     ["swsh promo", "sm promo", "xy promo", "black star"],
}

# City/partial team names that pollute the player index
CITY_FRAGMENTS = {
    "Los Angeles", "New York", "San Francisco", "Washington Senators",
    "Washington", "Chicago", "Boston", "Oakland", "Detroit",
    "Cleveland", "Seattle", "Minnesota", "Houston", "Atlanta",
    "Philadelphia", "Cincinnati", "Milwaukee", "Pittsburgh",
    "San Diego", "Colorado", "Arizona", "Miami", "Tampa Bay",
    "Kansas City", "St. Louis", "Toronto", "Baltimore",
    "Golden State", "New Orleans", "Oklahoma City", "Salt Lake",
    "Las Vegas", "Sacramento", "Memphis", "Portland",
}

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
        "sport": "NFL", "ebay_query": f"football {EXCL}",
        "ebay_category": "261328", "aspect_filter": "categoryId:261328,Sport:{Football},Graded:{No}",
        "discord_emoji": "🏈", "color": 0x013369,
    },
    "NBA": {
        "sport": "NBA", "ebay_query": f"basketball {EXCL}",
        "ebay_category": "261328", "aspect_filter": "categoryId:261328,Sport:{Basketball},Graded:{No}",
        "discord_emoji": "🏀", "color": 0xC9082A,
    },
    "MLB": {
        "sport": "MLB", "ebay_query": f"baseball {EXCL}",
        "ebay_category": "261328", "aspect_filter": "categoryId:261328,Sport:{Baseball},Graded:{No}",
        "discord_emoji": "⚾", "color": 0x002D72,
    },
    "NHL": {
        "sport": "NHL", "ebay_query": f"hockey {EXCL}",
        "ebay_category": "261328", "aspect_filter": "categoryId:261328,Sport:{Ice Hockey},Graded:{No}",
        "discord_emoji": "🏒", "color": 0x000000,
    },
    "Pokemon": {
        "sport": "Pokemon",
        "ebay_query": f'pokemon -"magic the gathering" -MTG -yugioh -lorcana -"one piece" -"dragon ball" -vanguard {EXCL}',
        "ebay_category": "183454", "aspect_filter": "categoryId:183454,Graded:{No}",
        "discord_emoji": "⚡", "color": 0xFFCC00, "is_tcg": True, "min_year": 2010,
    },
    "Yu-Gi-Oh": {
        "sport": "Yu-Gi-Oh", "ebay_query": f"yugioh {EXCL}",
        "ebay_category": "183454", "aspect_filter": "categoryId:183454,Graded:{No}",
        "discord_emoji": "🃏", "color": 0x6A0DAD, "is_tcg": True,
        # No min_year — eligible YGO cards are mostly 2002-2004
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

def fmt(n: float) -> str:
    return f"${n:,.2f}"

def get_item_url(item: dict) -> str:
    item_id = item.get("itemId", "")
    if item_id:
        numeric = re.search(r'\d{8,}', item_id)
        if numeric:
            return f"https://www.ebay.com/itm/{numeric.group()}"
    return item.get("itemWebUrl", "")

# ===========================================================================
# SQLite alert dedup
# ===========================================================================

def init_alert_db():
    conn = sqlite3.connect("alerts.db")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS alert_log (
            item_url   TEXT PRIMARY KEY,
            alerted_at TEXT
        )
    """)
    conn.commit()
    conn.close()

def extract_item_id(url: str) -> str:
    match = re.search(r'/itm/(\d+)', url)
    return match.group(1) if match else url

def has_alerted(url: str) -> bool:
    item_id = extract_item_id(url)
    result = supabase.table("alert_log") \
        .select("item_url") \
        .eq("item_url", item_id) \
        .gte("alerted_at", (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()) \
        .execute()
    return bool(result.data)

def record_alert(url: str):
    item_id = extract_item_id(url)
    supabase.table("alert_log") \
        .upsert({"item_url": item_id, "scanner": "grading", "alerted_at": datetime.now(timezone.utc).isoformat()}) \
        .execute()

# ===========================================================================
# eBay token
# ===========================================================================

_ebay_token        = None
_ebay_token_expiry = 0

def get_ebay_token() -> str:
    global _ebay_token, _ebay_token_expiry
    if _ebay_token and time.time() < _ebay_token_expiry:
        return _ebay_token
    auth = base64.b64encode(f"{EBAY_CLIENT_ID}:{EBAY_CLIENT_SECRET}".encode()).decode()
    resp = requests.post(
        EBAY_TOKEN_URL,
        headers={"Authorization": f"Basic {auth}", "Content-Type": "application/x-www-form-urlencoded"},
        data=f"grant_type=client_credentials&scope={EBAY_SCOPE}",
    )
    resp.raise_for_status()
    data = resp.json()
    _ebay_token        = data["access_token"]
    _ebay_token_expiry = time.time() + data["expires_in"] - 60
    log.info("Got new eBay token")
    return _ebay_token

# ===========================================================================
# Title normalization
# ===========================================================================

TITLE_EXPANSIONS = [
    (r'\bS&V\b',                         'scarlet violet'),
    (r'\bScarlet\s*&\s*Violet\b',        'scarlet violet'),
    (r'\bSV\s+(?=\d)',                   'scarlet violet '),
    (r'\bSWSH\b',                        'sword shield'),
    (r'\bSword\s*&\s*Shield\b',          'sword shield'),
    (r'\bS&S\b',                         'sword shield'),
    (r'\bSun\s*&\s*Moon\b',              'sun moon'),
    (r'\bS&M\b',                         'sun moon'),
    (r'\bME:\s*',                        'mega evolution '),
    (r'\bMega\s+Evo\b',                  'mega evolution'),
    (r'\bB&W\b',                         'black white'),
    (r'\bBlack\s*&\s*White\b',           'black white'),
    (r'\bBW\b(?=\s)',                    'black white'),
    (r'\bD&P\b',                         'diamond pearl'),
    (r'\bDiamond\s*&\s*Pearl\b',         'diamond pearl'),
    (r'\bHG\s*SS\b',                     'heartgold soulsilver'),
    (r'\bHeartGold\s*&?\s*SoulSilver\b', 'heartgold soulsilver'),
    (r'\bEvo\s+Skies\b',                'evolving skies'),
    (r'\bPrismatic\s+Evo\b',            'prismatic evolutions'),
    (r'\bW&W\b',                         'winds waves'),
    (r'\bWinds\s*&\s*Waves\b',           'winds waves'),
    (r'\bWW\b(?=\s+\d)',                'winds waves '),
    (r'\bUD\b',                          'upper deck'),
    (r'\bU\.D\.\b',                      'upper deck'),
    (r'\bBCP\b',                         'bowman chrome prospects'),
    (r'\bBDP\b',                         'bowman draft picks'),
    (r'\bBC\b(?=\s+(?:Pros|Draft|Prospect))', 'bowman chrome'),
    (r'\bA&G\b',                         'allen ginter'),
    (r'\bSP\s+Auth\b',                  'sp authentic'),
    (r'\s*&\s*',                         ' '),
]

def normalize_title(title: str) -> str:
    result = title
    for pattern, replacement in TITLE_EXPANSIONS:
        result = re.sub(pattern, replacement, result, flags=re.IGNORECASE)
    return result

def expand_pokemon_set_aliases(title_lower: str, set_name: str) -> str:
    """
    If the DB set name contains a known alias key, check if the seller used
    an alias in the title and inject the canonical key words so token matching
    succeeds. Also works in reverse — canonical words in title, inject aliases.
    """
    set_name_lower = set_name.lower()
    for key, aliases in POKEMON_SET_ALIASES.items():
        if key in set_name_lower:
            for alias in aliases:
                if alias in title_lower:
                    return title_lower + " " + key
            if key in title_lower:
                return title_lower + " " + " ".join(aliases)
    return title_lower

# ===========================================================================
# Token helpers
# ===========================================================================

SUFFIX_RE = re.compile(r'\b(II|III|IV|Jr\.?|Sr\.?)$', re.IGNORECASE)

def strip_suffix(name: str) -> str:
    return SUFFIX_RE.sub('', name).strip()

def tokenize(text: str, min_len: int = 3) -> list:
    return [w.lower() for w in re.split(r'[\W_]+', text) if len(w) >= min_len]

def set_tokens(set_name: str, is_tcg: bool = False) -> tuple:
    """Returns (required_tokens, optional_tokens)."""
    if is_tcg:
        TCG_NOISE     = SET_NOISE_WORDS | {"pokemon"}
        all_tokens    = [t for t in tokenize(set_name) if t not in TCG_NOISE]
        year_tokens   = [t for t in all_tokens if re.match(r'^\d{4}$', t)]
        non_year      = [t for t in all_tokens if not re.match(r'^\d{4}$', t)]
        gen_tokens    = [t for t in non_year if t in POKEMON_GENERATION_TOKENS]
        unique_tokens = [t for t in non_year if t not in POKEMON_GENERATION_TOKENS]
        if unique_tokens:
            # Sub-set — unique name tokens required, generation and year optional
            required = unique_tokens
            optional = gen_tokens + year_tokens
        else:
            # Base set — generation tokens are all we have, require them
            required = gen_tokens
            optional = year_tokens
    else:
        all_tokens = [t for t in tokenize(set_name) if t not in SET_NOISE_WORDS]
        required   = all_tokens
        optional   = []
    return required, optional

def variation_tokens(variation: str) -> list:
    stop = {"and", "the", "of", "for", "a"}
    return [t for t in tokenize(variation, min_len=2) if t not in stop]

# ===========================================================================
# Player name index
# ===========================================================================

_word_to_players:     dict = {}
_cleaned_to_original: dict = {}
_player_index_loaded: set  = set()

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
        cleaned = strip_suffix(name).strip().lower()
        cleaned_map[cleaned] = name
        for word in cleaned.split():
            if len(word) >= MIN_WORD_LEN:
                word_map.setdefault(word, set()).add(name)
    _word_to_players[sport]     = word_map
    _cleaned_to_original[sport] = cleaned_map
    _player_index_loaded.add(sport)
    log.info(f"{sport}: loaded {len(cleaned_map)} players, {len(word_map)} index words")

def get_candidate_players(title: str, sport: str) -> list:
    title_lower = normalize_title(title).lower()
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
        if original_name in CITY_FRAGMENTS:
            continue
        cleaned = strip_suffix(original_name).strip().lower()
        score   = fuzz.partial_ratio(cleaned, title_lower)
        if score >= 92:
            matches.append((original_name, score))
    matches.sort(key=lambda x: -x[1])
    return [m[0] for m in matches]

# ===========================================================================
# Supabase
# ===========================================================================

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ===========================================================================
# Load gradeable cards
# ===========================================================================

_card_cache = {}

def load_gradeable_cards(sport: str, min_year: int = None) -> list:
    cache_key = f"{sport}:{min_year}"
    if cache_key in _card_cache:
        return _card_cache[cache_key]
    log_elapsed(f"Loading gradeable cards for {sport}...")
    all_cards  = []
    batch_size = 1000
    offset     = 0
    while True:
        result = supabase.table("mv_grade_premiums") \
            .select("player_name, set_name, set_year, card_number, variation, "
                    "canonical_name, is_rookie, raw_price, psa9_price, psa10_price, "
                    "grading_score, raw_to_psa9_mult, psa10_sale_count_30d, "
                    "raw_sale_count_30d, sport") \
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
# Card scoring
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
    title      = normalize_title(title)
    ebay_year  = None
    ebay_year2 = None
    full_year  = re.search(r'\b(19|20)\d{2}\b', title)
    if full_year:
        ebay_year = int(full_year.group())
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

def extract_ygo_set_code(title: str) -> str | None:
    """Extract Yu-Gi-Oh set code like LOB-005 or RA01-EN008 from title."""
    match = YGO_SET_CODE_RE.search(title.upper())
    return match.group(1) if match else None

def build_card_debug(card: dict, title_lower: str, ebay_year, ebay_year2) -> str:
    set_name_norm = normalize_title(card.get("set_name") or "")
    card_sport    = card.get("sport", "")
    card_is_tcg   = card_sport in {"Pokemon", "Yu-Gi-Oh", "Other TCG", "Non-Sport Vintage"}
    req, opt      = set_tokens(set_name_norm, is_tcg=card_is_tcg)
    found_req     = [t for t in req if t in title_lower]
    missing_req   = [t for t in req if t not in title_lower]
    found_opt     = [t for t in opt if t in title_lower]
    return (
        f"set={card.get('set_name')} | "
        f"req_found={found_req} req_missing={missing_req} opt_found={found_opt} | "
        f"variation={card.get('variation') or 'base'} | "
        f"year={card.get('set_year')} ebay_year={ebay_year}/{ebay_year2}"
    )

def score_card_match(title_lower: str, card: dict,
                     ebay_year: int, ebay_year2: int,
                     ebay_card_num: str,
                     ygo_set_code: str = None) -> float:
    set_year    = int(card["set_year"]) if card.get("set_year") else None
    db_card_num = str(card.get("card_number") or "").strip().upper()
    set_name    = (card.get("set_name") or "")
    variation   = (card.get("variation") or "").strip()
    is_base     = variation.lower() in BASE_VARIATIONS
    sport       = card.get("sport", "")
    is_tcg      = sport in {"Pokemon", "Yu-Gi-Oh", "Other TCG", "Non-Sport Vintage"}
    is_ygo      = sport == "Yu-Gi-Oh"

    # ===========================================================
    # Yu-Gi-Oh — card number is the primary match signal
    # ===========================================================
    if is_ygo:
        if not ygo_set_code or not db_card_num:
            return -1.0
        if ygo_set_code.upper() != db_card_num:
            return -1.0
        # Card number matches — now check 1st Edition variation
        title_is_1st = bool(re.search(r'\b1st\b|\b1st\s+ed', title_lower, re.IGNORECASE))
        db_is_1st    = "1st" in variation.lower()
        if db_is_1st and not title_is_1st:
            return -1.0
        if title_is_1st and not db_is_1st:
            return -1.0
        # Strong match
        score = 100.0
        preferred_year = ebay_year
        if set_year and preferred_year and preferred_year == set_year:
            score += 10
        return score

    # ===========================================================
    # Auto/autograph hard filter
    # ===========================================================
    combined_db   = (set_name + " " + variation).lower()
    db_is_auto    = any(w in combined_db for w in ["autograph", " auto", "/a "])
    title_is_auto = any(w in title_lower for w in ["autograph", "/a ", " auto "])
    if db_is_auto and not title_is_auto:
        return -1.0
    if title_is_auto and not db_is_auto:
        return -1.0

    # ===========================================================
    # X-Fractor hard filter
    # ===========================================================
    db_is_xfractor    = "x-fractor" in combined_db or "xfractor" in combined_db
    title_is_xfractor = "x-fractor" in title_lower or "xfractor" in title_lower
    if db_is_xfractor and not title_is_xfractor:
        return -1.0
    if title_is_xfractor and not db_is_xfractor:
        return -1.0

    # ===========================================================
    # Panini sub-brand mismatch hard filter (sports only)
    # ===========================================================                         
                         
    PANINI_BRANDS = {"prizm", "select", "optic", "mosaic", "chronicles",
                     "contenders", "donruss", "prestige", "spectra", "flawless"}
    if not is_tcg:
        title_brands = PANINI_BRANDS & set(tokenize(title_lower))
        db_brands    = PANINI_BRANDS & set(tokenize(combined_db))
        if title_brands - db_brands:
            return -1.0

    # ===========================================================
    # Year filter — hard for sports, bonus only for TCG
    # ===========================================================

    preferred_year = ebay_year
    if not is_tcg:
        if set_year and (ebay_year or ebay_year2):
            if ebay_year != set_year and ebay_year2 != set_year:
                return -1.0

    # ===========================================================
    # Card number hard filter (sports only)
    # ===========================================================
    if ebay_card_num and db_card_num and not is_ygo:
        if ebay_card_num.upper() != db_card_num:
            return -1.0

    score = 0.0

    # ===========================================================
    # Set name matching — with Pokemon alias expansion
    # ===========================================================
    set_name_normalized = normalize_title(set_name)
    effective_title = title_lower
    if sport == "Pokemon":
        effective_title = expand_pokemon_set_aliases(title_lower, set_name_normalized)

    required_tokens, optional_tokens = set_tokens(set_name_normalized, is_tcg=is_tcg)

    if required_tokens:
        found_req   = [t for t in required_tokens if t in effective_title]
        match_ratio = len(found_req) / len(required_tokens)
        score += match_ratio * 60
        if match_ratio == 1.0:
            score += 20
        elif match_ratio < 0.5:
            return -1.0
        elif match_ratio < 0.6:
            score -= 10
    else:
        score += 10

    if optional_tokens:
        found_opt = [t for t in optional_tokens if t in effective_title]
        score += (len(found_opt) / len(optional_tokens)) * 15

    # Year bonus for TCG (not hard filter)
    if is_tcg and set_year and (preferred_year == set_year or ebay_year == set_year):
        score += 15

    # ===========================================================
    # Variation matching
    # ===========================================================
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
        canonical      = (card.get("canonical_name") or "").lower()
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

    # Card number penalty (sports only)
    if not is_tcg and db_card_num and not ebay_card_num:
        score -= 30

    # Year bonus (sports)
    if not is_tcg and set_year and (preferred_year == set_year or ebay_year == set_year):
        score += 10

    return score

# ===========================================================================
# eBay search
# ===========================================================================

def search_ebay(category_config: dict, listing_type: str) -> list:
    items = []
    pages = 2 if listing_type == "bin" else 1
    for page in range(pages):
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
# Discord
# ===========================================================================

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

def post_discord_alert(card: dict, item: dict, listing_type: str,
                       ebay_price: float, category_config: dict):
    if not DISCORD_WEBHOOK:
        log.warning("No Discord webhook configured")
        return

    raw_price  = float(card["raw_price"])
    psa10      = float(card["psa10_price"])
    psa9       = float(card.get("psa9_price") or 0)
    grade_cost = 27.99
    ebay_fees  = round(psa10 * EBAY_FEE_PCT, 2)
    net_profit = psa10 - ebay_price - grade_cost - ebay_fees

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

    set_year_str = str(card.get("set_year") or "")
    set_name_str = card.get("set_name") or ""
    if set_name_str.startswith(set_year_str):
        set_display = set_name_str
    else:
        set_display = f"{set_year_str} {set_name_str}".strip()

    description = (
        f"**eBay:** {fmt(ebay_price)}  ·  **GIGA Median:** {fmt(raw_price)}\n"
        f"**PSA 9:** {fmt(psa9)}  ·  **PSA 10:** {fmt(psa10)}\n"
        f"**Est. Net Profit:** {fmt(net_profit)} "
        f"_(after {fmt(grade_cost)} grading + {fmt(ebay_fees)} eBay fees)_"
        f"{time_remaining_str}"
        f"{hard_grade_warning}"
    )

    embed = {
        "title":       f"{emoji} Grading Opportunity — {card['canonical_name']}{rookie_tag}",
        "description": description,
        "url": get_item_url(item),
        "color":       category_config["color"],
        "fields": [
            {"name": "Set",          "value": set_display or "Unknown",              "inline": True},
            {"name": "Listing Type", "value": type_label,                            "inline": True},
            {"name": "Card #",       "value": str(card.get("card_number") or "N/A"), "inline": True},
        ],
        "footer": {"text": "Always verify condition before grading."},
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
    is_ygo        = sport == "Yu-Gi-Oh"
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

        # Japanese set code filter — Pokemon only, not YGO
        if is_tcg and not is_ygo and JAPANESE_SET_CODE_RE.search(title):
            no_candidates += 1
            continue

        title_words = [w for w in re.split(r'\W+', title) if len(w) >= 4]
        if len(title_words) < 2:
            no_candidates += 1
            continue

        ebay_year, ebay_year2, ebay_card_num = parse_title_years(title)

        # Extract YGO set code — required for Yu-Gi-Oh matching
        ygo_set_code = extract_ygo_set_code(title) if is_ygo else None

        # Sports require year or card number
        if not is_tcg and not ebay_year and not ebay_card_num:
            no_candidates += 1
            log_elapsed(f"NO_CANDIDATE [no_year_or_cardnum]: {title}")
            continue

        # Yu-Gi-Oh requires a set code
        if is_ygo and not ygo_set_code:
            no_candidates += 1
            log_elapsed(f"NO_CANDIDATE [no_ygo_set_code]: {title}")
            continue

        candidates = get_candidate_players(title, sport)
        if not candidates:
            no_candidates += 1
            log_elapsed(f"NO_CANDIDATE [no_player_match]: {title}")
            continue

        matched_player = candidates[0]

        player_cards = [c for c in cards if c.get("player_name") == matched_player]
        if not player_cards:
            no_player += 1
            log_elapsed(f"NO_PLAYER: \"{title}\" → player={matched_player} (not in mv_grade_premiums ROI filter)")
            continue

        title_lower  = normalize_title(title).lower()
        matched_card = None
        best_score   = 0.0
        best_debug   = ""
        reject_debug = ""
        reject_score = -999.0

        for card in player_cards:
            s = score_card_match(
                title_lower, card, ebay_year, ebay_year2, ebay_card_num,
                ygo_set_code=ygo_set_code
            )
            if s < 0:
                if s > reject_score:
                    reject_score = s
                    reject_debug = "HARD_REJECT " + build_card_debug(card, title_lower, ebay_year, ebay_year2)
                continue
            if s > best_score:
                best_score   = s
                matched_card = card
                best_debug   = build_card_debug(card, title_lower, ebay_year, ebay_year2)

        if not matched_card or best_score < min_score:
            no_card += 1
            debug_line = best_debug if best_debug else reject_debug
            # If score is exactly 0 with many cards checked, none passed even basic set token matching
            if best_score == 0 and len(player_cards) > 5:
                # Sample the first card to show what tokens are expected
                sample = player_cards[0]
                sn = normalize_title(sample.get("set_name") or "")
                cs = sample.get("sport", "")
                tcg = cs in {"Pokemon", "Yu-Gi-Oh", "Other TCG", "Non-Sport Vintage"}
                req, _ = set_tokens(sn, is_tcg=tcg)
                eff = expand_pokemon_set_aliases(title_lower, sn) if cs == "Pokemon" else title_lower
                found = [t for t in req if t in eff]
                debug_line = (
                    f"ZERO_SCORE ({len(player_cards)} cards) — "
                    f"sample set={sample.get('set_name')} | "
                    f"req={req} found={found}"
                )
            log_elapsed(
                f"NO_CARD: \"{title}\" → player={matched_player} "
                f"best_score={best_score:.0f}/{min_score} cards_checked={len(player_cards)}\n"
                f"          BEST: {debug_line}"
            )
            continue

        log_elapsed(
            f"CARD MATCH: \"{title}\" → {matched_card['canonical_name']} "
            f"(score={best_score:.0f} | {best_debug})"
        )

        if listing_type == "auction":
            price_data = item.get("currentBidPrice") or item.get("price") or {}
        else:
            price_data = item.get("price") or {}
        price = float(price_data.get("value", 0))
        if price <= 0:
            log_elapsed(f"PRICE_ZERO: {matched_card['canonical_name']} | no price on listing")
            continue

        raw_median = float(matched_card["raw_price"])
        if raw_median <= 0:
            log_elapsed(f"NO_RAW_PRICE: {matched_card['canonical_name']} | raw_price is zero or null")
            continue

        if price < raw_median * MIN_PRICE_PCT:
            log_elapsed(
                f"PRICE_TOO_LOW: {matched_card['canonical_name']} | "
                f"eBay: {fmt(price)} | GIGA Median: {fmt(raw_median)} | "
                f"Floor: {fmt(raw_median * MIN_PRICE_PCT)}"
            )
            continue

        if price > raw_median * PRICE_THRESHOLD:
            price_too_high += 1
            log_elapsed(
                f"PRICE_HIGH: {matched_card['canonical_name']} | "
                f"eBay: {fmt(price)} | GIGA Median: {fmt(raw_median)} | "
                f"Threshold: {fmt(raw_median * PRICE_THRESHOLD)}"
            )
            continue

        psa10      = float(matched_card.get("psa10_price") or 0)
        net_profit = psa10 - price - 27.99 - round(psa10 * EBAY_FEE_PCT, 2)
        if net_profit < MIN_PROFIT:
            low_profit += 1
            log_elapsed(
                f"LOW_PROFIT: {matched_card['canonical_name']} | "
                f"Net: {fmt(net_profit)} | PSA10: {fmt(psa10)} | eBay: {fmt(price)}"
            )
            continue

        url = get_item_url(item)
        if has_alerted(url):
            continue

        time_remaining_log = ""
        if listing_type == "auction":
            tr = format_time_remaining(item.get("itemEndDate", ""))
            if tr:
                time_remaining_log = f" | Time remaining: {tr}"

        log_elapsed(
            f"DEAL: {matched_card['canonical_name']} | eBay: {fmt(price)} | "
            f"GIGA Median: {fmt(raw_median)} | PSA10: {fmt(psa10)} | "
            f"Net Profit: {fmt(net_profit)}{time_remaining_log}"
        )

        record_alert(url)
        post_discord_alert(matched_card, item, listing_type, price, category_config)
        alerts_sent += 1
        time.sleep(0.5)

    elapsed_sec = time.time() - section_start
    log_elapsed(
        f"Sent {alerts_sent} alerts for {listing_type} [{elapsed_sec:.1f}s]\n"
        f"  graded={skipped_graded} | no_candidates={no_candidates} | "
        f"no_player={no_player} | no_card={no_card} | "
        f"price_high={price_too_high} | low_profit={low_profit}"
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
# Entry point
# ===========================================================================

if __name__ == "__main__":
    init_alert_db()
    log.info("Grading opportunity scanner starting...")
    run_scan()
    schedule.every(10).minutes.do(run_scan)
    while True:
        schedule.run_pending()
        time.sleep(30)
