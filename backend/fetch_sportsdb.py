"""
TheSportsDB fetcher — completely FREE, no rate limit.
Fetches COMPLETE season data round-by-round (bypasses 15-event limit).
Covers: EPL, La Liga, Serie A, Bundesliga, Ligue 1, Champions League

Usage:
  python fetch_sportsdb.py backfill              # All leagues, 2020-2026 (full)
  python fetch_sportsdb.py season 4328 2024-2025 # Specific league + season
  python fetch_sportsdb.py live                   # Recent + upcoming per league
"""
import requests
import sqlite3
import datetime
import time
import os
import sys
import logging
import threading

log = logging.getLogger("fetch_sdb")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")

_here = os.path.dirname(os.path.abspath(__file__))
_parent = os.path.dirname(_here)
DB_PATH = os.path.join(_here, "soccer.db") if os.path.exists(os.path.join(_here, "soccer.db")) else os.path.join(_parent, "soccer.db")

API_KEY = "123"  # Free public key
BASE_URL = f"https://www.thesportsdb.com/api/v1/json/{API_KEY}"

# TheSportsDB league IDs -> (DB league name, max rounds per season)
LEAGUES = {
    "4328": ("English Premier League", 38),
    "4335": ("Spain laLiga", 38),
    "4332": ("Italy_ Serie A", 38),
    "4331": ("Germany Bundesliga 1", 34),
    "4334": ("France ligue 1", 38),   # 38 until 2022-23, then 34 from 2023-24
    "4480": ("UEFA Champions League", 15),  # group+knockout rounds vary
}

# Cup competitions — 2025-2026 only
CUP_LEAGUES = {
    "4482": ("FA Cup", 13),
    "4570": ("EFL Cup", 7),
    "4571": ("FA Community Shield", 1),
    "4483": ("Copa del Rey", 7),
    "4506": ("Coppa Italia", 8),
    "4507": ("Supercoppa Italiana", 1),
    "4484": ("Coupe de France", 10),
    "4485": ("DFB-Pokal", 6),
    "5071": ("UEFA Conference League", 15),
}

# Team name normalization (variants → canonical names)
# Shared across all data sources for consistency
TEAM_NAME_NORMALIZATION = {
    # === SPANISH LA LIGA ===
    "Club Atlético de Madrid": "Atlético Madrid",
    "Atletico Madrid": "Atlético Madrid",
    "Real Betis Balompié": "Real Betis",
    "Betis": "Real Betis",
    "Real Sociedad de Fútbol": "Real Sociedad",
    "Sociedad": "Real Sociedad",
    "Levante UD": "Levante",
    "RCD Mallorca": "Mallorca",
    "Real Madrid CF": "Real Madrid",
    "RCD Espanyol de Barcelona": "Espanyol",
    "Espanyol Barcelona": "Espanyol",
    "Espanol": "Espanyol",
    "FC Barcelona": "Barcelona",
    "Athletic Bilbao": "Athletic Bilbao",
    "Ath Bilbao": "Athletic Bilbao",
    "Ath Madrid": "Atlético Madrid",
    "Atl. Madrid": "Atlético Madrid",
    "Alaves": "Deportivo Alavés",
    "Alavés": "Deportivo Alavés",
    "Cadiz": "Cádiz CF",
    "Cadiz CF": "Cádiz CF",
    "Granada CF": "Granada",
    "Vallecano": "Rayo Vallecano",
    "Celta": "Celta Vigo",
    "Vigo": "Celta Vigo",
    "Sevilla FC": "Sevilla",
    "Valencia CF": "Valencia",
    "Villarreal CF": "Villarreal",
    "Getafe CF": "Getafe",
    "Girona FC": "Girona",
    "Osasuna": "CA Osasuna",
    "Las Palmas": "UD Las Palmas",
    "Leganes": "Leganés",
    "Elche CF": "Elche",
    "Almeria": "UD Almería",
    "Almería": "UD Almería",
    "Huesca": "SD Huesca",
    "Eibar": "SD Eibar",
    "Valladolid": "Real Valladolid",
    "Oviedo": "Real Oviedo",

    # === ENGLISH PREMIER LEAGUE ===
    "Brighton": "Brighton and Hove Albion",
    "Leeds": "Leeds United",
    "Manchester Utd": "Manchester United",
    "Newcastle": "Newcastle United",
    "Nottingham": "Nottingham Forest",
    "Sheffield Utd": "Sheffield United",
    "Tottenham": "Tottenham Hotspur",
    "West Ham": "West Ham United",
    "Wolves": "Wolverhampton Wanderers",
    "West Brom": "West Bromwich Albion",
    "Leicester": "Leicester City",
    "Luton": "Luton Town",
    "Ipswich": "Ipswich Town",
    "Norwich": "Norwich City",

    # === GERMAN BUNDESLIGA ===
    "Augsburg": "FC Augsburg",
    "Dortmund": "Borussia Dortmund",
    "B. Monchengladbach": "Borussia Mönchengladbach",
    "Monchengladbach": "Borussia Mönchengladbach",
    "Mönchengladbach": "Borussia Mönchengladbach",
    "Ein Frankfurt": "Eintracht Frankfurt",
    "FC Koln": "FC Köln",
    "Hertha": "Hertha Berlin",
    "Hertha BSC": "Hertha Berlin",
    "Leverkusen": "Bayer Leverkusen",
    "Bayer 04 Leverkusen": "Bayer Leverkusen",
    "Bielefeld": "Arminia Bielefeld",
    "Schalke": "Schalke 04",
    "Schalke 04": "Schalke 04",
    "FC Schalke 04": "Schalke 04",
    "RasenBallsport Leipzig": "RB Leipzig",
    "Greuther Furth": "Greuther Fürth",
    "Greuther Fuerth": "Greuther Fürth",
    "SPVGG Greuther Fürth": "Greuther Fürth",
    "Bayern Munich": "Bayern München",
    "Bayern": "Bayern München",
    "FC Bayern Munich": "Bayern München",
    "FC Bayern München": "Bayern München",
    "Wolfsburg": "VfL Wolfsburg",
    "VfL Wolfsburg": "VfL Wolfsburg",
    "Stuttgart": "VfB Stuttgart",
    "Hoffenheim": "TSG Hoffenheim",
    "TSG 1899 Hoffenheim": "TSG Hoffenheim",
    "Freiburg": "SC Freiburg",
    "Union Berlin": "1. FC Union Berlin",
    "Werder Bremen": "SV Werder Bremen",
    "Bremen": "SV Werder Bremen",
    "Mainz": "1. FSV Mainz 05",
    "Mainz 05": "1. FSV Mainz 05",
    "1. FSV Mainz": "1. FSV Mainz 05",
    "Bochum": "VfL Bochum",
    "Darmstadt": "SV Darmstadt 98",
    "Heidenheim": "1. FC Heidenheim",
    "Heidenheim 1846": "1. FC Heidenheim",
    "1. FC Heidenheim 1846": "1. FC Heidenheim",
    "FC Heidenheim": "1. FC Heidenheim",
    "Hamburg": "Hamburger SV",
    "Dusseldorf": "Fortuna Düsseldorf",
    "Fortuna Dusseldorf": "Fortuna Düsseldorf",
    "St Pauli": "FC St. Pauli",
    "St. Pauli": "FC St. Pauli",
    "Hannover": "Hannover 96",
    "Hannover 96": "Hannover 96",
    "Kiel": "Holstein Kiel",
    "Holstein Kiel": "Holstein Kiel",
    "SV Elversberg": "Elversberg",

    # === ITALIAN SERIE A ===
    "Inter": "Inter Milan",
    "FC Internazionale Milano": "Inter Milan",
    "Internazionale": "Inter Milan",
    "AC Milan": "Milan",
    "Milan": "Milan",
    "Verona": "Hellas Verona",
    "Hellas": "Hellas Verona",
    "AS Roma": "Roma",
    "Roma": "Roma",
    "Parma": "Parma",
    "Parma Calcio 1913": "Parma",
    "Lazio": "Lazio",
    "SS Lazio": "Lazio",
    "Napoli": "Napoli",
    "SSC Napoli": "Napoli",
    "Juventus": "Juventus",
    "Juve": "Juventus",
    "Atalanta": "Atalanta",
    "Atalanta BC": "Atalanta",
    "Fiorentina": "Fiorentina",
    "ACF Fiorentina": "Fiorentina",
    "Torino": "Torino",
    "Torino FC": "Torino",
    "Udinese": "Udinese",
    "Udinese Calcio": "Udinese",
    "Bologna": "Bologna",
    "Bologna FC": "Bologna",
    "Sassuolo": "Sassuolo",
    "Sampdoria": "Sampdoria",
    "Cagliari": "Cagliari",
    "Cagliari Calcio": "Cagliari",
    "Genoa": "Genoa",
    "Genoa CFC": "Genoa",
    "Empoli": "Empoli",
    "Empoli FC": "Empoli",
    "Monza": "Monza",
    "AC Monza": "Monza",
    "Salernitana": "Salernitana",
    "US Salernitana": "Salernitana",
    "Lecce": "Lecce",
    "US Lecce": "Lecce",
    "Spezia": "Spezia",
    "Spezia Calcio": "Spezia",
    "Frosinone": "Frosinone",
    "Frosinone Calcio": "Frosinone",
    "Cremonese": "Cremonese",
    "US Cremonese": "Cremonese",
    "Crotone": "Crotone",
    "FC Crotone": "Crotone",
    "Benevento": "Benevento",
    "Benevento Calcio": "Benevento",
    "Venezia": "Venezia",
    "Venezia FC": "Venezia",
    "Pisa": "Pisa",
    "Pisa SC": "Pisa",
    "Como": "Como",
    "Como 1907": "Como",

    # === FRENCH LIGUE 1 ===
    "Paris Saint Germain": "Paris Saint-Germain",
    "Paris S-G": "Paris Saint-Germain",
    "PSG": "Paris Saint-Germain",
    "Paris SG": "Paris Saint-Germain",
    "Paris": "Paris Saint-Germain",
    "St Etienne": "Saint-Étienne",
    "Saint-Etienne": "Saint-Étienne",
    "AS Saint-Étienne": "Saint-Étienne",
    "Marseille": "Olympique Marseille",
    "OM": "Olympique Marseille",
    "Lyon": "Olympique Lyonnais",
    "Olympique Lyon": "Olympique Lyonnais",
    "OL": "Olympique Lyonnais",
    "Clermont": "Clermont Foot",
    "Clermont Foot 63": "Clermont Foot",
    "Lens": "Lens",
    "RC Lens": "Lens",
    "Lille": "Lille",
    "LOSC Lille": "Lille",
    "Monaco": "Monaco",
    "AS Monaco": "Monaco",
    "Rennes": "Rennes",
    "Stade Rennais": "Rennes",
    "Stade Rennais FC": "Rennes",
    "Nice": "Nice",
    "OGC Nice": "Nice",
    "Strasbourg": "Strasbourg",
    "RC Strasbourg": "Strasbourg",
    "RC Strasbourg Alsace": "Strasbourg",
    "Nantes": "Nantes",
    "FC Nantes": "Nantes",
    "Reims": "Reims",
    "Stade de Reims": "Reims",
    "Montpellier": "Montpellier",
    "Montpellier Hérault": "Montpellier",
    "Montpellier HSC": "Montpellier",
    "Angers": "Angers",
    "Angers SCO": "Angers",
    "Troyes": "Troyes",
    "ESTAC Troyes": "Troyes",
    "Lorient": "Lorient",
    "FC Lorient": "Lorient",
    "Brest": "Brest",
    "Stade Brestois": "Brest",
    "Stade Brestois 29": "Brest",
    "Metz": "Metz",
    "FC Metz": "Metz",
    "Auxerre": "Auxerre",
    "AJ Auxerre": "Auxerre",
    "Ajaccio": "AC Ajaccio",
    "AC Ajaccio": "AC Ajaccio",
    "Le Havre": "Le Havre",
    "Havre AC": "Le Havre",
    "Nimes": "Nîmes",
    "Nîmes": "Nîmes",
    "Nîmes Olympique": "Nîmes",
    "Dijon": "Dijon",
    "Dijon FCO": "Dijon",
    "Bordeaux": "Bordeaux",
    "FC Girondins de Bordeaux": "Bordeaux",
    "Girondins de Bordeaux": "Bordeaux",
    "Sochaux": "Sochaux",
    "FC Sochaux-Montbéliard": "Sochaux",
    "Grenoble": "Grenoble",
    "Grenoble Foot": "Grenoble",
    "Rodez": "Rodez",
    "Rodez Aveyron": "Rodez",
}

# Seasons to backfill
SEASONS = [
    "2020-2021", "2021-2022", "2022-2023",
    "2023-2024", "2024-2025", "2025-2026",
]


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def api_get(endpoint, retries=3):
    """GET request to TheSportsDB with retry on 429."""
    url = f"{BASE_URL}/{endpoint}"
    for attempt in range(retries):
        try:
            resp = requests.get(url, timeout=15)
        except requests.RequestException as e:
            log.error(f"Request failed: {e}")
            return None
        if resp.status_code == 429:
            wait = 10 * (attempt + 1)
            log.warning(f"Rate limited, waiting {wait}s (attempt {attempt+1}/{retries})")
            time.sleep(wait)
            continue
        if resp.status_code != 200:
            log.error(f"HTTP {resp.status_code}")
            return None
        return resp.json()
    log.error(f"Failed after {retries} retries: {endpoint}")
    return None


def parse_timestamp(ts_str):
    """Parse TheSportsDB timestamp to epoch."""
    if not ts_str:
        return None
    try:
        dt = datetime.datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        return int(dt.timestamp())
    except Exception:
        try:
            dt = datetime.datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%S")
            return int(dt.timestamp())
        except Exception:
            return None


def upsert_match(cursor, ev, league_name):
    """Insert or update a match from TheSportsDB event."""
    raw_home = ev.get("strHomeTeam", "Unknown")
    raw_away = ev.get("strAwayTeam", "Unknown")
    # Normalize team names
    home = TEAM_NAME_NORMALIZATION.get(raw_home, raw_home)
    away = TEAM_NAME_NORMALIZATION.get(raw_away, raw_away)
    round_str = f"Round {ev.get('intRound', '?')}"
    season = ev.get("strSeason", "")
    date_str = ev.get("dateEvent", "")
    kickoff = parse_timestamp(ev.get("strTimestamp"))
    venue = ev.get("strVenue", "")
    video = ev.get("strVideo", "")

    status_raw = ev.get("strStatus", "")
    status_map = {
        "Match Finished": "finished", "Not Started": "notstarted",
        "FT": "finished", "NS": "notstarted",
        "1H": "inprogress", "2H": "inprogress", "HT": "inprogress",
        "Match Postponed": "cancelled", "Match Cancelled": "cancelled",
        "AET": "finished", "PEN": "finished", "FT/P": "finished",
        "AOT": "finished", "AP": "finished",
    }
    status = status_map.get(status_raw, status_raw.lower() if status_raw else "unknown")

    home_score = ev.get("intHomeScore")
    away_score = ev.get("intAwayScore")
    try:
        home_score = int(home_score) if home_score is not None and home_score != "" else -1
    except (ValueError, TypeError):
        home_score = -1
    try:
        away_score = int(away_score) if away_score is not None and away_score != "" else -1
    except (ValueError, TypeError):
        away_score = -1

    sdb_id = ev.get("idEvent")
    event_id = int(sdb_id) if sdb_id else None

    # Check if match already exists by event_id
    if event_id:
        cursor.execute("SELECT id FROM matches WHERE event_id = ?", (event_id,))
        existing = cursor.fetchone()
        if existing:
            cursor.execute("""
                UPDATE matches SET
                    status=?, home_score=?, away_score=?, kickoff_timestamp=?
                WHERE event_id=? AND (status != ? OR home_score != ? OR away_score != ?)
            """, (status, home_score, away_score, kickoff,
                  event_id, status, home_score, away_score))
            return "updated" if cursor.rowcount > 0 else "unchanged"

    # Check by home_team + away_team + date to link with existing data
    if date_str:
        cursor.execute("""
            SELECT id FROM matches
            WHERE home_team = ? AND away_team = ? AND date = ? AND league = ?
        """, (home, away, date_str, league_name))
        dup = cursor.fetchone()
        if dup:
            cursor.execute("""
                UPDATE matches SET
                    event_id = COALESCE(event_id, ?),
                    status = COALESCE(?, status),
                    kickoff_timestamp = COALESCE(?, kickoff_timestamp),
                    home_score = CASE WHEN ? >= 0 THEN ? ELSE home_score END,
                    away_score = CASE WHEN ? >= 0 THEN ? ELSE away_score END
                WHERE id = ?
            """, (event_id, status, kickoff,
                  home_score, home_score, away_score, away_score, dup["id"]))
            return "linked"

    cursor.execute("""
        INSERT OR IGNORE INTO matches
        (league, season, round, date, home_team, away_team, home_score, away_score,
         event_id, status, kickoff_timestamp)
        VALUES (?,?,?,?,?,?,?,?,?,?,?)
    """, (league_name, season, round_str, date_str,
          home, away, home_score, away_score,
          event_id, status, kickoff))
    return "inserted" if cursor.rowcount > 0 else "skipped"


# ---------------------------------------------------------------------------
# Fetch commands
# ---------------------------------------------------------------------------
def fetch_season_full(league_id, season, silent=False):
    """Fetch ALL matches for a league + season using round-by-round endpoint."""
    lid = str(league_id)
    league_name, max_rounds = LEAGUES.get(lid, (f"League {lid}", 38))
    if not silent:
        log.info(f"Fetching {league_name} {season} (up to {max_rounds} rounds)")

    conn = get_db()
    cursor = conn.cursor()
    total_events, ins, upd, link = 0, 0, 0, 0
    empty_streak = 0

    for rnd in range(1, max_rounds + 1):
        data = api_get(f"eventsround.php?id={lid}&r={rnd}&s={season}")
        events = (data.get("events") or []) if data else []

        if not events:
            empty_streak += 1
            if empty_streak >= 3:
                break  # no more rounds
            continue
        empty_streak = 0
        total_events += len(events)

        for ev in events:
            result = upsert_match(cursor, ev, league_name)
            if result == "inserted":
                ins += 1
            elif result == "updated":
                upd += 1
            elif result == "linked":
                link += 1

        # Commit every few rounds to avoid large transactions
        if rnd % 5 == 0:
            conn.commit()

        time.sleep(3)  # respect rate limits

    conn.commit()
    conn.close()
    if not silent:
        log.info(f"  {total_events} events across {rnd} rounds -> {ins} new, {upd} updated, {link} linked")
    return ins, upd


def fetch_live(silent=False):
    """Fetch next and past 15 events per league (covers today's matches)."""
    total_ins, total_upd = 0, 0
    conn = get_db()
    cursor = conn.cursor()

    all_leagues = {**LEAGUES, **CUP_LEAGUES}
    for lid, (lname, _) in all_leagues.items():
        # Past 15 (recently finished)
        data = api_get(f"eventspastleague.php?id={lid}")
        if data and data.get("events"):
            for ev in data["events"]:
                r = upsert_match(cursor, ev, lname)
                if r == "inserted":
                    total_ins += 1
                elif r == "updated":
                    total_upd += 1

        # Next 15 (upcoming)
        data = api_get(f"eventsnextleague.php?id={lid}")
        if data and data.get("events"):
            for ev in data["events"]:
                r = upsert_match(cursor, ev, lname)
                if r == "inserted":
                    total_ins += 1
                elif r == "updated":
                    total_upd += 1

    conn.commit()
    conn.close()
    if not silent:
        log.info(f"Live refresh: {total_ins} new, {total_upd} updated")
    return total_ins, total_upd


def backfill_all():
    """Fetch all leagues × all seasons, round by round (complete data)."""
    total_combos = len(LEAGUES) * len(SEASONS)
    done = 0
    grand_ins, grand_upd = 0, 0
    log.info(f"=== FULL BACKFILL: {len(LEAGUES)} leagues × {len(SEASONS)} seasons ===")
    log.info(f"=== Fetching round-by-round for complete season data ===")
    for season in SEASONS:
        for lid in LEAGUES:
            done += 1
            lname = LEAGUES[lid][0]
            log.info(f"[{done}/{total_combos}] {lname} {season}")
            ins, upd = fetch_season_full(lid, season)
            grand_ins += ins
            grand_upd += upd
    log.info(f"=== BACKFILL COMPLETE: {grand_ins} new, {grand_upd} updated ===")
    return grand_ins, grand_upd


def backfill_cups(season="2025-2026"):
    """Fetch all cup competitions for a given season."""
    total_combos = len(CUP_LEAGUES)
    done = 0
    grand_ins, grand_upd = 0, 0
    log.info(f"=== CUP BACKFILL: {total_combos} cups for {season} ===")
    for lid in CUP_LEAGUES:
        done += 1
        lname = CUP_LEAGUES[lid][0]
        log.info(f"[{done}/{total_combos}] {lname} {season}")
        ins, upd = fetch_season_full(lid, season, silent=False)
        grand_ins += ins
        grand_upd += upd
    log.info(f"=== CUP BACKFILL COMPLETE: {grand_ins} new, {grand_upd} updated ===")
    return grand_ins, grand_upd


# ---------------------------------------------------------------------------
# SMART AUTO-REFRESH (runs as background thread from FastAPI)
# ---------------------------------------------------------------------------
# Strategy: fetch live matches every 60 seconds from all leagues
# TheSportsDB has rate limits but is more generous than Football-Data.org
# ---------------------------------------------------------------------------

_scheduler_running = False
_scheduler_thread = None
_refresh_stats = {"last_refresh": None, "live_count": 0, "total_refreshes": 0, "last_error": None}


def _scheduler_loop():
    """Background loop: fetch live matches every 60 seconds."""
    global _scheduler_running
    log.info("TheSportsDB auto-refresh scheduler started (60s interval)")

    cycle = 0
    while _scheduler_running:
        cycle += 1
        try:
            now = datetime.datetime.utcnow()
            today = now.strftime("%Y-%m-%d")

            # Fetch recent and upcoming matches from all leagues
            sdb_ins, sdb_upd = fetch_live(silent=True)

            # Count live matches for today
            live_count = 0
            conn = get_db()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT COUNT(*) FROM matches 
                WHERE status = 'inprogress' 
                AND date = ?
            """, (today,))
            live_count = cursor.fetchone()[0]
            conn.close()

            _refresh_stats["last_refresh"] = now.isoformat()
            _refresh_stats["live_count"] = live_count
            _refresh_stats["total_refreshes"] = cycle

            if sdb_ins or sdb_upd or live_count:
                log.info(f"Refresh #{cycle}: {sdb_ins} new, {sdb_upd} updated, {live_count} live")

            _refresh_stats["last_error"] = None
        except Exception as e:
            _refresh_stats["last_error"] = str(e)
            log.error(f"Scheduler error: {e}")

        # Wait ~12 seconds before next cycle (5 times per minute)
        for _ in range(12):
            if not _scheduler_running:
                break
            time.sleep(1)

    log.info("TheSportsDB auto-refresh scheduler stopped")


def start_scheduler():
    """Start the background auto-refresh thread."""
    global _scheduler_running, _scheduler_thread
    if _scheduler_running:
        return
    _scheduler_running = True
    _scheduler_thread = threading.Thread(target=_scheduler_loop, daemon=True, name="sdb-scheduler")
    _scheduler_thread.start()


def stop_scheduler():
    """Stop the background auto-refresh thread."""
    global _scheduler_running
    _scheduler_running = False


def get_scheduler_status():
    """Return current scheduler state for the API."""
    return {
        "running": _scheduler_running,
        "source": "TheSportsDB",
        **_refresh_stats,
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "backfill"

    if cmd == "backfill":
        backfill_all()
    elif cmd == "cups":
        s = sys.argv[2] if len(sys.argv) > 2 else "2025-2026"
        backfill_cups(s)
    elif cmd == "season":
        lid = sys.argv[2] if len(sys.argv) > 2 else "4328"
        s = sys.argv[3] if len(sys.argv) > 3 else "2024-2025"
        fetch_season_full(lid, s)
    elif cmd == "live" or cmd == "today":
        fetch_live()
    else:
        print("Usage:")
        print("  python fetch_sportsdb.py backfill")
        print("  python fetch_sportsdb.py cups [2025-2026]")
        print("  python fetch_sportsdb.py season 4328 2024-2025")
        print("  python fetch_sportsdb.py live")
