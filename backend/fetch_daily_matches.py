import requests
import sqlite3
import datetime
import time
import os

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "soccer.db")
RAPID_API_KEY = "71f1cb10a2msh46e08954191fd4dp1e55cajsne7b50af76124"
RAPID_API_HOST = "sportapi7.p.rapidapi.com"

# The 5 specific tournament IDs we care about (mapping from previous searches)
TOURNAMENTS = {
    17: "English Premier League",
    34: "France ligue 1",
    35: "Germany Bundesliga 1",
    23: "Italy Serie A",
    8: "Spain LaLiga",
    7: "UEFA Champions League",
    52: "Trendyol Super Lig",
    238: "Liga Portugal"
}

def get_headers():
    return {
        "x-rapidapi-key": RAPID_API_KEY,
        "x-rapidapi-host": RAPID_API_HOST
    }

def get_db_connection():
    return sqlite3.connect(DB_PATH)

def fetch_daily_matches(date_str):
    """
    Fetches scheduled events for a specific date (YYYY-MM-DD).
    NOTE: Endpoint uses the SoFaScore clone sportapi7 schema.
    """
    url = f"https://sportapi7.p.rapidapi.com/api/v1/sport/football/scheduled-events/{date_str}"
    print(f"Fetching matches for {date_str}...")
    response = requests.get(url, headers=get_headers())
    
    if response.status_code == 429:
        print("ERROR: RapidAPI Monthly or Daily Quota Exceeded!")
        return []
        
    if response.status_code != 200:
        print(f"Failed to fetch data: {response.status_code}")
        print(response.text)
        return []

    data = response.json()
    events = data.get('events', [])
    print(f"Found {len(events)} total global football events today.")
    
    # Filter only for our targeted leagues
    targeted_matches = [e for e in events if e.get('tournament', {}).get('uniqueTournament', {}).get('id') in TOURNAMENTS]
    print(f"Filtered to {len(targeted_matches)} matches in our targeted top leagues.")
    return targeted_matches

def save_daily_matches(events):
    conn = get_db_connection()
    cursor = conn.cursor()
    count = 0
    
    for event in events:
        try:
            t_id = event['tournament']['uniqueTournament']['id']
            league_name = TOURNAMENTS[t_id]
            season_year = event.get('season', {}).get('year', str(datetime.datetime.now().year))
            round_num = event.get('roundInfo', {}).get('round', 'Unknown')
            date_time = str(event.get('startTimestamp', 0))
            home_team = event['homeTeam']['name']
            away_team = event['awayTeam']['name']
            home_score = event.get('homeScore', {}).get('current', -1) # Default to -1 if match hasn't finished
            away_score = event.get('awayScore', {}).get('current', -1)
            
            # Additional logic to extract Red/Yellow cards if available in the event properties
            # Note: Detailed card stats might require a secondary call to /api/v1/event/{id}/statistics
            home_red_cards = event.get('homeRedCards', 0)
            away_red_cards = event.get('awayRedCards', 0)
            
            match_tuple = (league_name, season_year, f"Round {round_num}", date_time, home_team, away_team, home_score, away_score)
            cursor.execute('''
            INSERT OR IGNORE INTO matches 
            (league, season, round, date, home_team, away_team, home_score, away_score)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', match_tuple)
            
            if cursor.rowcount > 0:
                count += 1
                
        except Exception as e:
            print(f"Error parsing event: {e}")
            
    conn.commit()
    conn.close()
    print(f"Successfully saved {count} new matches to the database.")

if __name__ == "__main__":
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    matches = fetch_daily_matches(today)
    if matches:
        save_daily_matches(matches)
