import os
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import sqlite3
import analysis_engine

app = FastAPI(title="Soccer Bets Analyzer API")

# Allow CORS for Next.js frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # In production, restrict this to the frontend URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "soccer.db")
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
    try:
        prediction = analysis_engine.generate_match_prediction(home_team, away_team, league)
        if not prediction.get('expected_goals'):
            raise HTTPException(status_code=404, detail="Not enough data to calculate probabilities for these teams in this league.")
        return prediction
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/matches/recent")
def get_recent_matches(limit: int = 50, league: str = None):
    conn = get_db()
    cursor = conn.cursor()
    
    # We sort by id DESC as a proxy for date
    if league:
        cursor.execute('''
        SELECT id, league, season, round, date, home_team, away_team, home_score, away_score 
        FROM matches 
        WHERE league = ?
        ORDER BY id DESC LIMIT ?
        ''', (league, limit))
    else:
        cursor.execute('''
        SELECT id, league, season, round, date, home_team, away_team, home_score, away_score 
        FROM matches 
        ORDER BY id DESC LIMIT ?
        ''', (limit,))
        
    matches = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return {"matches": matches}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
