import requests
import json
import sqlite3
import time

DB_PATH = "soccer.db"
RAPID_API_KEY = "71f1cb10a2msh46e08954191fd4dp1e55cajsne7b50af76124"
RAPID_API_HOST = "sportapi7.p.rapidapi.com"

TOURNAMENTS = {
    7: "UEFA Champions League",
    52: "Trendyol Super Lig",
    238: "Liga Portugal"
}

TARGET_YEARS = ["20/21", "21/22", "22/23", "23/24", "24/25", "2020", "2021", "2022", "2023", "2024"]

def get_headers():
    return {
        "x-rapidapi-key": RAPID_API_KEY,
        "x-rapidapi-host": RAPID_API_HOST
    }

def get_seasons_for_tournament(tournament_id):
    url = f"https://sportapi7.p.rapidapi.com/api/v1/unique-tournament/{tournament_id}/seasons"
    response = requests.get(url, headers=get_headers())
    if response.status_code != 200:
        return []
    data = response.json()
    seasons = []
    if 'seasons' in data:
        for s in data['seasons']:
            year_str = str(s.get('year', ''))
            for target in TARGET_YEARS:
                if target in year_str:
                    seasons.append({
                        "id": s['id'],
                        "name": s['name'],
                        "year": s['year']
                    })
                    break
    return seasons

def get_events_for_season(tournament_id, season_id):
    events = []
    page = 0
    while True:
        url = f"https://sportapi7.p.rapidapi.com/api/v1/unique-tournament/{tournament_id}/season/{season_id}/events/last/{page}"
        response = requests.get(url, headers=get_headers())
        if response.status_code != 200:
            print(f"Failed page {page} for T:{tournament_id} S:{season_id} - status {response.status_code}")
            break
        
        data = response.json()
        if 'events' in data and len(data['events']) > 0:
            events.extend(data['events'])
            if data.get('hasNextPage'):
                page += 1
                time.sleep(0.5)
            else:
                break
        else:
            break
            
    return events

def setup_db():
    conn = sqlite3.connect(DB_PATH)
    return conn

def main():
    conn = setup_db()
    cursor = conn.cursor()
    
    for t_id, t_name in TOURNAMENTS.items():
        print(f"Fetching seasons for {t_name} ({t_id})...")
        seasons = get_seasons_for_tournament(t_id)
        
        for s in seasons:
            print(f"  Fetching events for {t_name} season {s['year']} (ID: {s['id']})...")
            events = get_events_for_season(t_id, s['id'])
            if events:
                count_inserted = 0
                for event in events:
                    try:
                        home_team = event['homeTeam']['name']
                        away_team = event['awayTeam']['name']
                        home_score = event.get('homeScore', {}).get('current')
                        away_score = event.get('awayScore', {}).get('current')
                        round_num = event.get('roundInfo', {}).get('round', 'Unknown')
                        date = event.get('startTimestamp', 0)
                        
                        if home_score is not None and away_score is not None:
                            match_tuple = (t_name, str(s['year']), f"Round {round_num}", str(date), home_team, away_team, home_score, away_score)
                            cursor.execute('''
                            INSERT OR IGNORE INTO matches 
                            (league, season, round, date, home_team, away_team, home_score, away_score)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                            ''', match_tuple)
                            if cursor.rowcount > 0:
                                count_inserted += 1
                    except Exception as e:
                        pass
                print(f"  -> Inserted {count_inserted} matches into DB.")
            else:
                print("  -> No events.")
            
            time.sleep(1)
            
    conn.commit()
    cursor.execute("SELECT count(*) FROM matches")
    print(f"Total matches in database: {cursor.fetchone()[0]}")
    conn.close()

if __name__ == "__main__":
    main()
