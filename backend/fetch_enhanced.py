"""
Enhanced data fetcher for the soccer analyzer.
- Fetches today's scheduled matches
- Fetches detailed match statistics (cards, corners, shots, possession)
- Fills missing 2024-25 season data
- Supports 2025-26 daily auto-update
"""
import requests
import sqlite3
import datetime
import time
import os
import sys

_here = os.path.dirname(os.path.abspath(__file__))
_parent = os.path.dirname(_here)
DB_PATH = os.path.join(_here, "soccer.db") if os.path.exists(os.path.join(_here, "soccer.db")) else os.path.join(_parent, "soccer.db")
RAPID_API_KEY = os.environ.get("RAPIDAPI_KEY", "71f1cb10a2msh46e08954191fd4dp1e55cajsne7b50af76124")
RAPID_API_HOST = "sportapi7.p.rapidapi.com"
BASE_URL = f"https://{RAPID_API_HOST}/api/v1"

TOURNAMENTS = {
    17: "English Premier League",
    8: "Spain laLiga",
    23: "Italy_ Serie A",
    35: "Germany Bundesliga 1",
    34: "France ligue 1",
    7: "UEFA Champions League",
    52: "Trendyol Super Lig",
    238: "Liga Portugal",
    # Cup competitions
    29: "FA Cup",
    27: "EFL Cup",
    329: "Copa del Rey",
    328: "Coppa Italia",
    335: "Coupe de France",
    119: "DFB-Pokal",
    17015: "UEFA Conference League",
    488: "Supercopa de España",
    382: "Supercoppa Italiana",
    529: "DFL-Supercup",
    487: "FA Community Shield",
}

TARGET_SEASON_YEARS = ["20/21", "21/22", "22/23", "23/24", "24/25", "25/26",
                       "2020", "2021", "2022", "2023", "2024", "2025"]

HEADERS = {
    "x-rapidapi-key": RAPID_API_KEY,
    "x-rapidapi-host": RAPID_API_HOST,
}


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def api_get(path, retries=2):
    """GET request with retry and rate-limit handling."""
    url = f"{BASE_URL}{path}"
    for attempt in range(retries + 1):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=15)
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 429:
                print(f"  Rate limited. Waiting 5s...")
                time.sleep(5)
                continue
            print(f"  HTTP {resp.status_code} for {path}")
            return None
        except Exception as e:
            print(f"  Request error: {e}")
            time.sleep(2)
    return None


# ---------------------------------------------------------------------------
# 1. FETCH TODAY'S MATCHES
# ---------------------------------------------------------------------------
def fetch_scheduled_matches(date_str=None):
    """Fetch all scheduled matches for a given date (YYYY-MM-DD). Defaults to today."""
    if date_str is None:
        date_str = datetime.datetime.now().strftime("%Y-%m-%d")

    data = api_get(f"/sport/football/scheduled-events/{date_str}")
    if not data:
        return []

    events = data.get("events", [])
    targeted = [
        e for e in events
        if e.get("tournament", {}).get("uniqueTournament", {}).get("id") in TOURNAMENTS
    ]
    print(f"[{date_str}] {len(events)} global events -> {len(targeted)} in target leagues")
    return targeted


# ---------------------------------------------------------------------------
# 2. FETCH EVENT STATISTICS (cards, corners, shots, possession)
# ---------------------------------------------------------------------------
def fetch_event_statistics(event_id):
    """Fetch detailed statistics for a single match event."""
    data = api_get(f"/event/{event_id}/statistics")
    if not data:
        return {}

    stats = {}
    # The API returns statistics grouped by period. We want "ALL" (full match).
    for group in data.get("statistics", []):
        period = group.get("period", "")
        if period == "ALL":
            for item in group.get("groups", []):
                for stat_item in item.get("statisticsItems", []):
                    key = stat_item.get("name", "")
                    home_val = stat_item.get("home", "")
                    away_val = stat_item.get("away", "")
                    stats[key] = {"home": home_val, "away": away_val}
            break
    return stats


def parse_stat_int(stats, key, side):
    try:
        return int(stats.get(key, {}).get(side, 0))
    except (ValueError, TypeError):
        return 0


def parse_stat_float(stats, key, side):
    try:
        val = str(stats.get(key, {}).get(side, "0"))
        return float(val.replace("%", ""))
    except (ValueError, TypeError):
        return 0.0


# ---------------------------------------------------------------------------
# 3. SAVE / UPSERT A MATCH
# ---------------------------------------------------------------------------
def upsert_match(cursor, match_data):
    """Insert or update a match record."""
    # Check if we already have this event_id
    event_id = match_data.get("event_id")
    if event_id:
        cursor.execute("SELECT id FROM matches WHERE event_id = ?", (event_id,))
        existing = cursor.fetchone()
        if existing:
            # Update existing record
            cursor.execute("""
                UPDATE matches SET
                    status=?, home_score=?, away_score=?,
                    home_score_ht=?, away_score_ht=?,
                    home_yellow_cards=?, away_yellow_cards=?,
                    home_red_cards=?, away_red_cards=?,
                    home_corners=?, away_corners=?,
                    home_shots=?, away_shots=?,
                    home_shots_on_target=?, away_shots_on_target=?,
                    home_possession=?, away_possession=?,
                    home_fouls=?, away_fouls=?,
                    kickoff_timestamp=?
                WHERE event_id=?
            """, (
                match_data.get("status"), match_data.get("home_score"), match_data.get("away_score"),
                match_data.get("home_score_ht"), match_data.get("away_score_ht"),
                match_data.get("home_yellow_cards", 0), match_data.get("away_yellow_cards", 0),
                match_data.get("home_red_cards", 0), match_data.get("away_red_cards", 0),
                match_data.get("home_corners", 0), match_data.get("away_corners", 0),
                match_data.get("home_shots", 0), match_data.get("away_shots", 0),
                match_data.get("home_shots_on_target", 0), match_data.get("away_shots_on_target", 0),
                match_data.get("home_possession"), match_data.get("away_possession"),
                match_data.get("home_fouls", 0), match_data.get("away_fouls", 0),
                match_data.get("kickoff_timestamp"),
                event_id,
            ))
            return "updated"

    # Insert new record
    cursor.execute("""
        INSERT OR IGNORE INTO matches
        (league, season, round, date, home_team, away_team, home_score, away_score,
         event_id, status, kickoff_timestamp,
         home_score_ht, away_score_ht,
         home_yellow_cards, away_yellow_cards, home_red_cards, away_red_cards,
         home_corners, away_corners, home_shots, away_shots,
         home_shots_on_target, away_shots_on_target,
         home_possession, away_possession, home_fouls, away_fouls)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        match_data.get("league"), match_data.get("season"), match_data.get("round"),
        match_data.get("date"), match_data.get("home_team"), match_data.get("away_team"),
        match_data.get("home_score"), match_data.get("away_score"),
        match_data.get("event_id"), match_data.get("status"), match_data.get("kickoff_timestamp"),
        match_data.get("home_score_ht"), match_data.get("away_score_ht"),
        match_data.get("home_yellow_cards", 0), match_data.get("away_yellow_cards", 0),
        match_data.get("home_red_cards", 0), match_data.get("away_red_cards", 0),
        match_data.get("home_corners", 0), match_data.get("away_corners", 0),
        match_data.get("home_shots", 0), match_data.get("away_shots", 0),
        match_data.get("home_shots_on_target", 0), match_data.get("away_shots_on_target", 0),
        match_data.get("home_possession"), match_data.get("away_possession"),
        match_data.get("home_fouls", 0), match_data.get("away_fouls", 0),
    ))
    return "inserted" if cursor.rowcount > 0 else "skipped"


def parse_event(event, fetch_stats=False):
    """Parse a raw API event object into a flat dict for our DB."""
    t_id = event.get("tournament", {}).get("uniqueTournament", {}).get("id")
    league_name = TOURNAMENTS.get(t_id, "Unknown")
    season_year = event.get("season", {}).get("year", "")
    round_num = event.get("roundInfo", {}).get("round", "Unknown")
    start_ts = event.get("startTimestamp", 0)

    status_desc = event.get("status", {}).get("description", "").lower()
    if "finished" in status_desc or "ended" in status_desc:
        status = "finished"
    elif "not" in status_desc:
        status = "notstarted"
    elif "progress" in status_desc or "1st" in status_desc or "2nd" in status_desc:
        status = "inprogress"
    else:
        status = status_desc or "unknown"

    home_score = event.get("homeScore", {}).get("current")
    away_score = event.get("awayScore", {}).get("current")
    home_score_ht = event.get("homeScore", {}).get("period1")
    away_score_ht = event.get("awayScore", {}).get("period1")

    match_data = {
        "league": league_name,
        "season": str(season_year),
        "round": f"Round {round_num}",
        "date": str(start_ts),
        "home_team": event.get("homeTeam", {}).get("name", "Unknown"),
        "away_team": event.get("awayTeam", {}).get("name", "Unknown"),
        "home_score": home_score if home_score is not None else -1,
        "away_score": away_score if away_score is not None else -1,
        "event_id": event.get("id"),
        "status": status,
        "kickoff_timestamp": start_ts,
        "home_score_ht": home_score_ht,
        "away_score_ht": away_score_ht,
        "home_yellow_cards": 0, "away_yellow_cards": 0,
        "home_red_cards": 0, "away_red_cards": 0,
        "home_corners": 0, "away_corners": 0,
        "home_shots": 0, "away_shots": 0,
        "home_shots_on_target": 0, "away_shots_on_target": 0,
        "home_possession": None, "away_possession": None,
        "home_fouls": 0, "away_fouls": 0,
    }

    # Fetch detailed stats for finished matches
    if fetch_stats and status == "finished" and match_data["event_id"]:
        stats = fetch_event_statistics(match_data["event_id"])
        if stats:
            match_data["home_yellow_cards"] = parse_stat_int(stats, "Yellow cards", "home")
            match_data["away_yellow_cards"] = parse_stat_int(stats, "Yellow cards", "away")
            match_data["home_red_cards"] = parse_stat_int(stats, "Red cards", "home")
            match_data["away_red_cards"] = parse_stat_int(stats, "Red cards", "away")
            match_data["home_corners"] = parse_stat_int(stats, "Corner kicks", "home")
            match_data["away_corners"] = parse_stat_int(stats, "Corner kicks", "away")
            match_data["home_shots"] = parse_stat_int(stats, "Total shots", "home")
            match_data["away_shots"] = parse_stat_int(stats, "Total shots", "away")
            match_data["home_shots_on_target"] = parse_stat_int(stats, "Shots on target", "home")
            match_data["away_shots_on_target"] = parse_stat_int(stats, "Shots on target", "away")
            match_data["home_possession"] = parse_stat_float(stats, "Ball possession", "home")
            match_data["away_possession"] = parse_stat_float(stats, "Ball possession", "away")
            match_data["home_fouls"] = parse_stat_int(stats, "Fouls", "home")
            match_data["away_fouls"] = parse_stat_int(stats, "Fouls", "away")
            time.sleep(0.3)

    return match_data


# ---------------------------------------------------------------------------
# 4. DAILY UPDATE: fetch today + update results for recent unfinished
# ---------------------------------------------------------------------------
def daily_update():
    """Pull today's matches and update any previously-unfinished ones."""
    print("=== DAILY UPDATE ===")
    conn = get_db()
    cursor = conn.cursor()

    # A) Fetch today's scheduled matches
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    events = fetch_scheduled_matches(today)
    inserted, updated = 0, 0
    for event in events:
        md = parse_event(event, fetch_stats=True)
        result = upsert_match(cursor, md)
        if result == "inserted":
            inserted += 1
        elif result == "updated":
            updated += 1
    print(f"Today: {inserted} new, {updated} updated")

    # B) Also fetch yesterday (to catch late-finishing matches)
    yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    events_y = fetch_scheduled_matches(yesterday)
    for event in events_y:
        md = parse_event(event, fetch_stats=True)
        result = upsert_match(cursor, md)
        if result == "updated":
            updated += 1

    # C) Update any DB records still marked 'notstarted' or 'inprogress' that are >3h old
    three_hours_ago = int(time.time()) - 3 * 3600
    cursor.execute("""
        SELECT event_id FROM matches
        WHERE status IN ('notstarted', 'inprogress')
          AND kickoff_timestamp IS NOT NULL
          AND kickoff_timestamp < ?
          AND event_id IS NOT NULL
        LIMIT 50
    """, (three_hours_ago,))
    stale_events = [row["event_id"] for row in cursor.fetchall()]
    if stale_events:
        print(f"Checking {len(stale_events)} stale unfinished matches...")
        for eid in stale_events:
            event_data = api_get(f"/event/{eid}")
            if event_data and "event" in event_data:
                md = parse_event(event_data["event"], fetch_stats=True)
                upsert_match(cursor, md)
                time.sleep(0.3)

    conn.commit()
    cursor.execute("SELECT count(*) as c FROM matches")
    print(f"Total matches in DB: {cursor.fetchone()['c']}")
    conn.close()
    print("=== DAILY UPDATE COMPLETE ===")


# ---------------------------------------------------------------------------
# 5. BACKFILL: fill missing seasons from the API
# ---------------------------------------------------------------------------
def get_seasons_for_tournament(tournament_id):
    data = api_get(f"/unique-tournament/{tournament_id}/seasons")
    if not data:
        return []
    seasons = []
    for s in data.get("seasons", []):
        year_str = str(s.get("year", ""))
        for target in TARGET_SEASON_YEARS:
            if target in year_str:
                seasons.append({"id": s["id"], "name": s["name"], "year": s["year"]})
                break
    return seasons


def get_events_for_season(tournament_id, season_id, page_direction="last"):
    """Fetch all events for a season, paginating."""
    events = []
    page = 0
    while True:
        data = api_get(f"/unique-tournament/{tournament_id}/season/{season_id}/events/{page_direction}/{page}")
        if not data:
            break
        page_events = data.get("events", [])
        if not page_events:
            break
        events.extend(page_events)
        if data.get("hasNextPage"):
            page += 1
            time.sleep(0.5)
        else:
            break
    return events


def backfill_seasons(fetch_stats=False, tournaments=None):
    """Backfill historical data for all target seasons."""
    print("=== BACKFILL SEASONS ===")
    conn = get_db()
    cursor = conn.cursor()
    target_tournaments = tournaments or TOURNAMENTS

    for t_id, t_name in target_tournaments.items():
        print(f"\n--- {t_name} (ID: {t_id}) ---")
        seasons = get_seasons_for_tournament(t_id)
        print(f"  Found {len(seasons)} target seasons")

        for s in seasons:
            print(f"  Season {s['name']} ({s['year']})...")
            events = get_events_for_season(t_id, s["id"])
            inserted, updated, skipped = 0, 0, 0
            for event in events:
                md = parse_event(event, fetch_stats=fetch_stats)
                # Skip matches with no score and not upcoming
                if md["home_score"] is None and md["status"] == "finished":
                    skipped += 1
                    continue
                result = upsert_match(cursor, md)
                if result == "inserted":
                    inserted += 1
                elif result == "updated":
                    updated += 1
                else:
                    skipped += 1

            conn.commit()
            print(f"    -> {len(events)} events: {inserted} inserted, {updated} updated, {skipped} skipped")
            time.sleep(1)

    cursor.execute("SELECT count(*) as c FROM matches")
    print(f"\nTotal matches in DB: {cursor.fetchone()['c']}")
    conn.close()
    print("=== BACKFILL COMPLETE ===")


def backfill_stats_for_existing():
    """For matches already in DB with event_id but missing stats, fetch stats."""
    print("=== BACKFILL STATS ===")
    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT id, event_id FROM matches
        WHERE event_id IS NOT NULL
          AND status = 'finished'
          AND home_shots = 0
          AND home_corners = 0
          AND home_possession IS NULL
        ORDER BY id DESC
        LIMIT 200
    """)
    rows = cursor.fetchall()
    print(f"Found {len(rows)} matches needing stats backfill")

    for i, row in enumerate(rows):
        stats = fetch_event_statistics(row["event_id"])
        if stats:
            cursor.execute("""
                UPDATE matches SET
                    home_yellow_cards=?, away_yellow_cards=?,
                    home_red_cards=?, away_red_cards=?,
                    home_corners=?, away_corners=?,
                    home_shots=?, away_shots=?,
                    home_shots_on_target=?, away_shots_on_target=?,
                    home_possession=?, away_possession=?,
                    home_fouls=?, away_fouls=?
                WHERE id=?
            """, (
                parse_stat_int(stats, "Yellow cards", "home"),
                parse_stat_int(stats, "Yellow cards", "away"),
                parse_stat_int(stats, "Red cards", "home"),
                parse_stat_int(stats, "Red cards", "away"),
                parse_stat_int(stats, "Corner kicks", "home"),
                parse_stat_int(stats, "Corner kicks", "away"),
                parse_stat_int(stats, "Total shots", "home"),
                parse_stat_int(stats, "Total shots", "away"),
                parse_stat_int(stats, "Shots on target", "home"),
                parse_stat_int(stats, "Shots on target", "away"),
                parse_stat_float(stats, "Ball possession", "home"),
                parse_stat_float(stats, "Ball possession", "away"),
                parse_stat_int(stats, "Fouls", "home"),
                parse_stat_int(stats, "Fouls", "away"),
                row["id"],
            ))
        if (i + 1) % 10 == 0:
            conn.commit()
            print(f"  Processed {i+1}/{len(rows)}")
        time.sleep(0.4)

    conn.commit()
    conn.close()
    print("=== STATS BACKFILL COMPLETE ===")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "daily"

    if cmd == "daily":
        daily_update()
    elif cmd == "backfill":
        fetch_stats_flag = "--stats" in sys.argv
        backfill_seasons(fetch_stats=fetch_stats_flag)
    elif cmd == "backfill-stats":
        backfill_stats_for_existing()
    else:
        print("Usage: python fetch_enhanced.py [daily|backfill|backfill-stats]")
        print("  daily          - Fetch today's matches + update recent results")
        print("  backfill       - Fill missing historical seasons (add --stats for detailed stats)")
        print("  backfill-stats - Fetch detailed stats for existing matches missing them")
