"""
Football-Data.org fetcher — FREE data source (10 req/min).
Covers: EPL, La Liga, Serie A, Bundesliga, Ligue 1, Champions League

Usage:
  python fetch_footballdata.py daily                # Fetch today's matches
  python fetch_footballdata.py season PL 2024       # Fetch PL 2024-25
  python fetch_footballdata.py season all            # Current season, all leagues
  python fetch_footballdata.py backfill              # All leagues, 2020-2026
  python fetch_footballdata.py standings PL          # League standings
"""
import requests
import sqlite3
import datetime
import time
import os
import sys
import threading
import logging

log = logging.getLogger("fetch_fd")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")

_here = os.path.dirname(os.path.abspath(__file__))
_parent = os.path.dirname(_here)
DB_PATH = os.path.join(_here, "soccer.db") if os.path.exists(os.path.join(_here, "soccer.db")) else os.path.join(_parent, "soccer.db")

API_KEY = os.environ.get("FOOTBALL_DATA_API_KEY", "140aade9f1ec461bbfe524ddc740bf94")
BASE_URL = "https://api.football-data.org/v4"

# Competition codes -> DB league names
COMPETITIONS = {
    "PL":  "English Premier League",
    "PD":  "Spain laLiga",
    "SA":  "Italy_ Serie A",
    "BL1": "Germany Bundesliga 1",
    "FL1": "France ligue 1",
    "CL":  "UEFA Champions League",
    "EL":  "UEFA Europa League",
    "CONFERENCE": "UEFA Conference League",
    "FAC": "FA Cup",
    "CDR": "Copa del Rey",
    "CI":  "Coppa Italia",
    "DFB": "DFB-Pokal",
    "FRA_CUP": "Coupe de France",
}

# Team name normalization (variants → canonical names)
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
    "Stuttgart": "VfB Stuttgart",
    "Hoffenheim": "TSG Hoffenheim",
    "Freiburg": "SC Freiburg",
    "Union Berlin": "1. FC Union Berlin",
    "Werder Bremen": "SV Werder Bremen",
    "Bremen": "SV Werder Bremen",
    "Mainz": "1. FSV Mainz 05",
    "Mainz 05": "1. FSV Mainz 05",
    "Bochum": "VfL Bochum",
    "Darmstadt": "SV Darmstadt 98",
    "Heidenheim": "1. FC Heidenheim",
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

# Seasons to backfill — free tier only allows current season
# Older seasons (2020-2024) require a paid plan; we already have that data from other sources
HISTORY_SEASONS = [2024, 2025, 2026]

HEADERS = {"X-Auth-Token": API_KEY}

# Track last refresh time and live match state for the scheduler
_last_refresh = 0
_live_matches_active = False


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------
def api_get(endpoint):
    """Rate-limited GET with retry on 429."""
    url = f"{BASE_URL}{endpoint}"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
    except requests.RequestException as e:
        log.error(f"Request failed: {e}")
        return None
    if resp.status_code == 429:
        wait = int(resp.headers.get("X-RequestCounter-Reset", 60))
        log.warning(f"Rate limited, waiting {wait}s...")
        time.sleep(wait)
        try:
            resp = requests.get(url, headers=HEADERS, timeout=15)
        except requests.RequestException as e:
            log.error(f"Retry failed: {e}")
            return None
    if resp.status_code != 200:
        log.error(f"HTTP {resp.status_code}: {resp.text[:200]}")
        return None
    return resp.json()


def parse_utc_date(date_str):
    if not date_str:
        return None
    try:
        dt = datetime.datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        return int(dt.timestamp())
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Upsert
# ---------------------------------------------------------------------------
def upsert_match(cursor, m, league_name, season_str):
    """Insert or update a match from Football-Data.org response."""
    raw_home = m.get("homeTeam", {}).get("name", "Unknown")
    raw_away = m.get("awayTeam", {}).get("name", "Unknown")
    # Normalize team names
    home = TEAM_NAME_NORMALIZATION.get(raw_home, raw_home)
    away = TEAM_NAME_NORMALIZATION.get(raw_away, raw_away)
    matchday = m.get("matchday", "?")
    kickoff = parse_utc_date(m.get("utcDate"))

    status_map = {
        "FINISHED": "finished", "TIMED": "notstarted", "SCHEDULED": "notstarted",
        "IN_PLAY": "inprogress", "PAUSED": "inprogress",
        "POSTPONED": "cancelled", "CANCELLED": "cancelled",
        "SUSPENDED": "cancelled", "AWARDED": "finished",
    }
    status = status_map.get(m.get("status", ""), m.get("status", "").lower())

    score = m.get("score", {})
    ft = score.get("fullTime", {})
    ht = score.get("halfTime", {})
    home_score = ft.get("home") if ft.get("home") is not None else -1
    away_score = ft.get("away") if ft.get("away") is not None else -1
    home_ht = ht.get("home")
    away_ht = ht.get("away")
    fd_id = m.get("id")

    # Try update first (by event_id)
    cursor.execute("SELECT id, status FROM matches WHERE event_id = ?", (fd_id,))
    existing = cursor.fetchone()
    if existing:
        # Only update if something changed (avoid pointless writes)
        cursor.execute("""
            UPDATE matches SET
                status=?, home_score=?, away_score=?,
                home_score_ht=?, away_score_ht=?, kickoff_timestamp=?
            WHERE event_id=? AND (status != ? OR home_score != ? OR away_score != ?)
        """, (status, home_score, away_score, home_ht, away_ht, kickoff,
              fd_id, status, home_score, away_score))
        return "updated" if cursor.rowcount > 0 else "unchanged"

    date_str = ""
    if kickoff:
        date_str = datetime.datetime.fromtimestamp(kickoff).strftime("%Y-%m-%d")

    cursor.execute("""
        INSERT OR IGNORE INTO matches
        (league, season, round, date, home_team, away_team, home_score, away_score,
         event_id, status, kickoff_timestamp, home_score_ht, away_score_ht)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (league_name, season_str, f"Round {matchday}", date_str,
          home, away, home_score, away_score,
          fd_id, status, kickoff, home_ht, away_ht))
    return "inserted" if cursor.rowcount > 0 else "skipped"


# ---------------------------------------------------------------------------
# Fetch commands
# ---------------------------------------------------------------------------
def fetch_today(date_str=None, silent=False):
    """Fetch all matches for a date across tracked leagues. Returns counts."""
    global _live_matches_active
    if not date_str:
        date_str = datetime.datetime.utcnow().strftime("%Y-%m-%d")
    if not silent:
        log.info(f"Fetching matches for {date_str}")
    data = api_get(f"/matches?date={date_str}")
    if not data:
        return 0, 0, 0
    matches = data.get("matches", [])

    conn = get_db()
    cursor = conn.cursor()
    ins, upd, live = 0, 0, 0
    for m in matches:
        comp_code = m.get("competition", {}).get("code", "")
        league_name = COMPETITIONS.get(comp_code)
        if not league_name:
            continue
        season_str = str(m.get("season", {}).get("startDate", "")[:4])
        result = upsert_match(cursor, m, league_name, season_str)
        if result == "inserted":
            ins += 1
        elif result == "updated":
            upd += 1
        if m.get("status") in ("IN_PLAY", "PAUSED"):
            live += 1
    conn.commit()
    conn.close()
    _live_matches_active = live > 0
    if not silent:
        log.info(f"Done: {ins} new, {upd} updated, {live} live")
    return ins, upd, live


def fetch_season(comp_code="PL", season_year=None):
    """Fetch all matches for a competition + season. season_year=None → current."""
    league_name = COMPETITIONS.get(comp_code, comp_code)
    season_param = f"?season={season_year}" if season_year else ""
    label = f"{league_name} ({comp_code}) {season_year or 'current'}"
    log.info(f"Fetching {label}")
    data = api_get(f"/competitions/{comp_code}/matches{season_param}")
    if not data:
        return
    matches = data.get("matches", [])
    season_str = str(data.get("filters", {}).get("season", season_year or ""))
    log.info(f"  {len(matches)} matches for season {season_str}")

    conn = get_db()
    cursor = conn.cursor()
    ins, upd, skip = 0, 0, 0
    for m in matches:
        result = upsert_match(cursor, m, league_name, season_str)
        if result == "inserted":
            ins += 1
        elif result == "updated":
            upd += 1
        else:
            skip += 1
    conn.commit()
    conn.close()
    log.info(f"  Done: {ins} new, {upd} updated, {skip} skipped")


def backfill_all():
    """Fetch all 6 leagues × 7 seasons (2020-2026). Respects rate limit."""
    total_calls = len(COMPETITIONS) * len(HISTORY_SEASONS)
    done = 0
    log.info(f"=== BACKFILL: {total_calls} API calls across {len(COMPETITIONS)} leagues × {len(HISTORY_SEASONS)} seasons ===")
    for season_year in HISTORY_SEASONS:
        for comp_code in COMPETITIONS:
            done += 1
            log.info(f"[{done}/{total_calls}] {comp_code} {season_year}-{season_year+1}")
            fetch_season(comp_code, season_year)
            time.sleep(7)  # ~8-9 req/min to stay safely under 10
    log.info("=== BACKFILL COMPLETE ===")


def fetch_standings(comp_code="PL"):
    data = api_get(f"/competitions/{comp_code}/standings")
    if not data:
        return
    for group in data.get("standings", []):
        if group.get("type") == "TOTAL":
            print(f"\n{'Pos':<4} {'Team':<30} {'P':<4} {'W':<4} {'D':<4} {'L':<4} {'GF':<5} {'GA':<5} {'GD':<5} {'Pts':<5}")
            print("-" * 76)
            for t in group.get("table", []):
                print(f"{t['position']:<4} {t['team']['name']:<30} {t['playedGames']:<4} {t['won']:<4} {t['draw']:<4} {t['lost']:<4} {t['goalsFor']:<5} {t['goalsAgainst']:<5} {t['goalDifference']:<5} {t['points']:<5}")


# ---------------------------------------------------------------------------
# SMART AUTO-REFRESH (runs as background thread from FastAPI)
# ---------------------------------------------------------------------------
# Strategy (10 req/min budget):
#   - Every 60s: fetch today's matches (1 req) → live scores, new fixtures
#   - If live matches detected: also fetch tomorrow's matches (1 req) for context
#   - Remaining budget (~8/min) is kept as headroom / burst protection
#
# This means during a match day we get score updates every 60 seconds
# while using only 1-2 of our 10 req/min allowance.
# ---------------------------------------------------------------------------

_scheduler_running = False
_scheduler_thread = None
_refresh_stats = {"last_refresh": None, "live_count": 0, "total_refreshes": 0, "last_error": None}


def _scheduler_loop():
    """Background loop: dual-source refresh every 60 seconds.

    Budget per cycle:
      - Football-Data.org: 1 req (today's matches) — rate-limited to 10/min
      - TheSportsDB: ~12 req (past+next 15 per league) — FREE, no limit
    """
    global _scheduler_running
    log.info("Auto-refresh scheduler started (60s interval, dual-source)")

    # Import TheSportsDB fetcher
    try:
        import fetch_sportsdb as sdb
        has_sdb = True
    except ImportError:
        has_sdb = False
        log.warning("fetch_sportsdb not available — using Football-Data.org only")

    cycle = 0
    while _scheduler_running:
        cycle += 1
        try:
            now = datetime.datetime.utcnow()
            today = now.strftime("%Y-%m-%d")

            # 1. Football-Data.org: fetch today's matches (1 req/min)
            fd_ins, fd_upd, live = fetch_today(today, silent=True)

            # 2. TheSportsDB: fetch recent + upcoming per league (free, no limit)
            sdb_ins, sdb_upd = 0, 0
            if has_sdb:
                try:
                    sdb_ins, sdb_upd = sdb.fetch_live(silent=True)
                except Exception as e:
                    log.error(f"TheSportsDB error: {e}")

            total_ins = fd_ins + sdb_ins
            total_upd = fd_upd + sdb_upd

            _refresh_stats["last_refresh"] = now.isoformat()
            _refresh_stats["live_count"] = live
            _refresh_stats["total_refreshes"] = cycle

            if total_ins or total_upd or live:
                log.info(f"Refresh #{cycle}: {total_ins} new, {total_upd} updated, {live} live "
                         f"(FD: {fd_ins}/{fd_upd}, SDB: {sdb_ins}/{sdb_upd})")

            # 3. If live matches, also pre-fetch tomorrow (1 extra FD req)
            if live > 0:
                tomorrow = (now + datetime.timedelta(days=1)).strftime("%Y-%m-%d")
                fetch_today(tomorrow, silent=True)

            _refresh_stats["last_error"] = None
        except Exception as e:
            _refresh_stats["last_error"] = str(e)
            log.error(f"Scheduler error: {e}")

        # Wait 60 seconds before next cycle
        for _ in range(60):
            if not _scheduler_running:
                break
            time.sleep(1)

    log.info("Auto-refresh scheduler stopped")


def start_scheduler():
    """Start the background auto-refresh thread."""
    global _scheduler_running, _scheduler_thread
    if _scheduler_running:
        return
    _scheduler_running = True
    _scheduler_thread = threading.Thread(target=_scheduler_loop, daemon=True, name="fd-scheduler")
    _scheduler_thread.start()


def stop_scheduler():
    """Stop the background auto-refresh thread."""
    global _scheduler_running
    _scheduler_running = False


def get_scheduler_status():
    """Return current scheduler state for the API."""
    return {
        "running": _scheduler_running,
        **_refresh_stats,
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    if API_KEY == "YOUR_API_KEY_HERE":
        print("Set FOOTBALL_DATA_API_KEY or edit this file. Register free: https://www.football-data.org/")
        sys.exit(1)

    cmd = sys.argv[1] if len(sys.argv) > 1 else "daily"

    if cmd == "daily":
        fetch_today(sys.argv[2] if len(sys.argv) > 2 else None)
    elif cmd == "season":
        comp = sys.argv[2] if len(sys.argv) > 2 else "PL"
        year = int(sys.argv[3]) if len(sys.argv) > 3 else None
        if comp.lower() == "all":
            for code in COMPETITIONS:
                fetch_season(code, year)
                time.sleep(7)
        else:
            fetch_season(comp.upper(), year)
    elif cmd == "backfill":
        backfill_all()
    elif cmd == "standings":
        fetch_standings((sys.argv[2] if len(sys.argv) > 2 else "PL").upper())
    else:
        print("Usage:")
        print("  python fetch_footballdata.py daily [YYYY-MM-DD]")
        print("  python fetch_footballdata.py season PL|all [2024]")
        print("  python fetch_footballdata.py backfill")
        print("  python fetch_footballdata.py standings PL")
