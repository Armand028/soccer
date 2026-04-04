"""
Insert all cup competition data from CSV files into the database
"""
import sqlite3
import os
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "soccer.db")

def parse_datetime(dt_str):
    """Parse datetime string and return date and unix timestamp."""
    try:
        dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
        return dt.strftime("%Y-%m-%d"), int(dt.timestamp())
    except:
        return dt_str[:10] if len(dt_str) >= 10 else dt_str, 0

def insert_cup_data(csv_file_path, league_name, season="2024-25"):
    """Insert cup matches from a CSV file into the database."""
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        # Check how many matches already exist
        cursor.execute("SELECT COUNT(*) FROM matches WHERE league = ?", (league_name,))
        existing_count = cursor.fetchone()[0]
        print(f"Existing {league_name} matches in DB: {existing_count}")
        
        # Read CSV file
        with open(csv_file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        header = lines[0].strip()
        data_lines = lines[1:]
        
        inserted = 0
        skipped = 0
        errors = 0
        
        for line in data_lines:
            line = line.strip()
            if not line:
                continue
            
            parts = line.split(',')
            if len(parts) < 7:
                continue
            
            try:
                event_id = int(parts[0])
                timestamp_str = parts[1]
                round_name = parts[2]
                home_team = parts[3]
                home_score_str = parts[4]
                away_team = parts[5]
                away_score_str = parts[6]
                
                # Parse date and timestamp
                date, kickoff_timestamp = parse_datetime(timestamp_str)
                
                # Determine status and scores
                if home_score_str == '' or away_score_str == '':
                    home_score = -1
                    away_score = -1
                    status = 'SCHEDULED'
                else:
                    home_score = int(home_score_str)
                    away_score = int(away_score_str)
                    status = 'FINISHED'
                
                # Check for duplicate (same league + event_id)
                cursor.execute("SELECT id FROM matches WHERE event_id = ? AND league = ?", (event_id, league_name))
                if cursor.fetchone():
                    skipped += 1
                    continue
                
                # Insert match
                cursor.execute('''
                    INSERT INTO matches (
                        date, league, season, round, home_team, away_team, 
                        home_score, away_score, event_id, status, kickoff_timestamp
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    date, league_name, season, round_name, home_team, away_team,
                    home_score, away_score, event_id, status, kickoff_timestamp
                ))
                inserted += 1
                
            except Exception as e:
                errors += 1
                print(f"Error: {e}")
                continue
        
        conn.commit()
        print(f"✅ Inserted {inserted} new {league_name} matches")
        print(f"⏭️ Skipped {skipped} duplicates")
        print(f"❌ Errors: {errors}")
        print(f"📊 Total {league_name} matches in DB: {existing_count + inserted}")
        
    except Exception as e:
        conn.rollback()
        print(f"❌ Error: {e}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    import sys
    
    # Insert all cup data
    cups = [
        ("fa_cup_data.csv", "FA Cup"),
        ("coppa_italia_data.csv", "Coppa Italia"),
        ("coupe_de_france_data.csv", "Coupe de France"),
        ("uefa_europa_league_data.csv", "UEFA Europa League"),
        ("uefa_conference_league_data.csv", "UEFA Conference League"),
    ]
    
    for csv_file, league_name in cups:
        csv_path = os.path.join(os.path.dirname(__file__), csv_file)
        if os.path.exists(csv_path):
            print(f"\n=== {league_name} ===")
            insert_cup_data(csv_path, league_name)
        else:
            print(f"\n⚠️ {csv_file} not found, skipping {league_name}")
