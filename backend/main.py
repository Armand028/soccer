import os
import time
import datetime
from zoneinfo import ZoneInfo
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import sqlite3
import analysis_engine_v2 as v2
import match_analyzer
import fetch_footballdata as fd


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: begin auto-refresh (1 API call every 60s for live scores)
    fd.start_scheduler()
    yield
    # Shutdown: stop the scheduler thread
    fd.stop_scheduler()


app = FastAPI(title="Soccer Bets Analyzer API", lifespan=lifespan)

@app.get("/health")
def health():
    return {"status": "ok"}

# Allow CORS for Next.js frontend
# In production, replace "*" with your Vercel domain
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://soccer-bets.vercel.app", "http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Look for soccer.db in same dir first (Render), then parent dir (local dev)
_here = os.path.dirname(os.path.abspath(__file__))
_parent = os.path.dirname(_here)
DB_PATH = os.path.join(_here, "soccer.db") if os.path.exists(os.path.join(_here, "soccer.db")) else os.path.join(_parent, "soccer.db")
print("USING DATABASE AT:", DB_PATH)

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

@app.get("/api/leagues")
def get_leagues():
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT league FROM matches ORDER BY league")
    leagues = [row["league"] for row in cursor.fetchall()]
    conn.close()
    return {"leagues": leagues}

@app.get("/api/teams")
def get_teams(league: str = None):
    conn = get_db()
    cursor = conn.cursor()
    
    if league:
        cursor.execute('''
        SELECT DISTINCT home_team as team FROM matches WHERE league = ?
        UNION
        SELECT DISTINCT away_team as team FROM matches WHERE league = ?
        ORDER BY team
        ''', (league, league))
    else:
        cursor.execute('''
        SELECT DISTINCT home_team as team FROM matches 
        UNION 
        SELECT DISTINCT away_team as team FROM matches 
        ORDER BY team
        ''')
        
    teams = [row["team"] for row in cursor.fetchall()]
    conn.close()
    return {"teams": teams}

@app.get("/api/analysis/predict")
def predict_match(home_team: str, away_team: str, league: str):
    """Match prediction using v2 engine for consistency with the rest of the app."""
    try:
        analysis = v2.generate_full_analysis(home_team, away_team, league)
        xg = analysis["expected_goals"]
        if not xg:
            raise HTTPException(status_code=404, detail="Not enough data to calculate probabilities for these teams in this league.")
        return {
            "home_team": home_team,
            "away_team": away_team,
            "league": league,
            "form": {
                "home": analysis["form"]["home_overall"],
                "away": analysis["form"]["away_overall"],
            },
            "head_to_head": analysis["head_to_head"],
            "expected_goals": xg,
            "probabilities": xg["probabilities"],
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/analysis/h2h")
def get_h2h(team_a: str, team_b: str):
    try:
        return v2.get_h2h(team_a, team_b, limit=10)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/analysis/form")
def get_form(team: str, limit: int = 10):
    try:
        return v2.get_team_form(team, limit=limit)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/stats")
def get_stats():
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) as cnt FROM matches")
    total_matches = cursor.fetchone()["cnt"]
    cursor.execute("SELECT COUNT(DISTINCT league) as cnt FROM matches")
    total_leagues = cursor.fetchone()["cnt"]
    cursor.execute("""
        SELECT COUNT(*) as cnt FROM (
            SELECT DISTINCT home_team as team FROM matches
            UNION
            SELECT DISTINCT away_team as team FROM matches
        )
    """)
    total_teams = cursor.fetchone()["cnt"]
    conn.close()
    return {"total_matches": total_matches, "total_leagues": total_leagues, "total_teams": total_teams}


ALL_MATCH_COLS = """id, league, season, round, date, home_team, away_team,
    home_score, away_score, event_id, status, kickoff_timestamp,
    home_score_ht, away_score_ht,
    home_yellow_cards, away_yellow_cards, home_red_cards, away_red_cards,
    home_corners, away_corners, home_shots, away_shots,
    home_shots_on_target, away_shots_on_target,
    home_possession, away_possession, home_fouls, away_fouls"""


@app.get("/api/matches/recent")
def get_recent_matches(limit: int = 50, league: str = None):
    conn = get_db()
    cursor = conn.cursor()
    order = "ORDER BY COALESCE(kickoff_timestamp, id) DESC"
    if league:
        cursor.execute(f'SELECT {ALL_MATCH_COLS} FROM matches WHERE league = ? AND home_score >= 0 {order} LIMIT ?',
                       (league, limit))
    else:
        cursor.execute(f'SELECT {ALL_MATCH_COLS} FROM matches WHERE home_score >= 0 {order} LIMIT ?',
                       (limit,))
    matches = _add_ottawa_time([dict(row) for row in cursor.fetchall()])
    conn.close()
    return {"matches": matches}


OTTAWA_TZ = ZoneInfo("America/Toronto")


def _add_ottawa_time(matches: list) -> list:
    """Add kickoff_ottawa (human-readable Ottawa time) to each match dict."""
    for m in matches:
        ts = m.get("kickoff_timestamp")
        if ts:
            dt = datetime.datetime.fromtimestamp(ts, tz=OTTAWA_TZ)
            m["kickoff_ottawa"] = dt.strftime("%Y-%m-%d %H:%M")
        else:
            m["kickoff_ottawa"] = None
    return matches


@app.get("/api/matches/today")
def get_today_matches(date: str = None):
    """Return matches for a given date (YYYY-MM-DD, in Ottawa time). Defaults to today in Ottawa."""
    if not date:
        date = datetime.datetime.now(tz=OTTAWA_TZ).strftime("%Y-%m-%d")
    # Convert date to start/end epoch anchored in Ottawa timezone
    try:
        dt = datetime.datetime.strptime(date, "%Y-%m-%d").replace(tzinfo=OTTAWA_TZ)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD.")
    start_ts = int(dt.timestamp())
    end_ts = start_ts + 86400
    conn = get_db()
    cursor = conn.cursor()
    # Dedup safety: if same teams play on same date from different seasons, keep only the latest season
    cursor.execute(f"""
        SELECT {ALL_MATCH_COLS} FROM matches
        WHERE kickoff_timestamp >= ? AND kickoff_timestamp < ?
          AND id IN (
            SELECT MAX(id) FROM matches 
            WHERE kickoff_timestamp >= ? AND kickoff_timestamp < ?
            GROUP BY home_team, away_team, date
          )
        ORDER BY kickoff_timestamp ASC
    """, (start_ts, end_ts, start_ts, end_ts))
    matches = _add_ottawa_time([dict(row) for row in cursor.fetchall()])
    conn.close()
    return {"date": date, "matches": matches}


@app.get("/api/matches/{match_id}")
def get_match_by_id(match_id: int):
    """Return a single match by its DB id."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(f"SELECT {ALL_MATCH_COLS} FROM matches WHERE id = ?", (match_id,))
    row = cursor.fetchone()
    conn.close()
    if not row:
        raise HTTPException(status_code=404, detail="Match not found")
    return _add_ottawa_time([dict(row)])[0]


@app.get("/api/matches/{match_id}/analysis")
def get_match_analysis(match_id: int):
    """
    Deep analysis for a specific match.
    Uses the v2 analysis engine: form, goal trends, H2H, xG, league position, pattern detection.
    """
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(f"SELECT {ALL_MATCH_COLS} FROM matches WHERE id = ?", (match_id,))
    row = cursor.fetchone()
    conn.close()
    if not row:
        raise HTTPException(status_code=404, detail="Match not found")
    match = dict(row)
    try:
        analysis = v2.generate_full_analysis(match["home_team"], match["away_team"], match["league"])
        analysis["match"] = match
        return analysis
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/matches/{match_id}/report")
def get_match_report(match_id: int):
    """Generate an intelligent, structured match analysis report."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(f"SELECT {ALL_MATCH_COLS} FROM matches WHERE id = ?", (match_id,))
    row = cursor.fetchone()
    conn.close()
    if not row:
        raise HTTPException(status_code=404, detail="Match not found")
    match = dict(row)
    try:
        result = match_analyzer.generate_match_report(match["home_team"], match["away_team"], match["league"])
        result["match"] = match
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/analysis/report")
def get_analysis_report(home_team: str, away_team: str, league: str):
    """Generate an intelligent match report for an arbitrary matchup."""
    try:
        return match_analyzer.generate_match_report(home_team, away_team, league)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/analysis/full")
def get_full_analysis(home_team: str, away_team: str, league: str):
    """Deep analysis for an arbitrary home/away matchup."""
    try:
        return v2.generate_full_analysis(home_team, away_team, league)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/league-table")
def get_league_table(league: str, season: str = None):
    try:
        table = v2.get_league_table(league, season)
        return {"league": league, "table": table}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/data/refresh")
def trigger_daily_refresh():
    """Trigger an immediate data refresh (fetch today's matches)."""
    try:
        ins, upd, live = fd.fetch_today()
        return {"status": "ok", "inserted": ins, "updated": upd, "live": live}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/data/scheduler")
def scheduler_status():
    """Return the auto-refresh scheduler status."""
    return fd.get_scheduler_status()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
