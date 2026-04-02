import os
import re
import sqlite3
import glob

DB_PATH = "soccer.db"
RAW_DATA_DIR = r"C:\Users\armgu\OneDrive\Desktop\My files\TRADING\soccer\raw_data - Copy"

def setup_database():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS matches (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        league TEXT,
        season TEXT,
        round TEXT,
        date TEXT,
        home_team TEXT,
        away_team TEXT,
        home_score INTEGER,
        away_score INTEGER,
        UNIQUE(league, season, round, home_team, away_team)
    )
    ''')
    conn.commit()
    return conn

def parse_file(filepath, conn):
    with open(filepath, 'r', encoding='utf-8') as f:
        lines = [line.strip() for line in f.readlines()]
        
    league = "Unknown"
    season = "Unknown"
    current_round = "Unknown"
    
    matches_to_insert = []
    
    i = 0
    while i < len(lines):
        line = lines[i]
        
        # Match League and Season: === English Premier League 2020-2021 ===
        header_match = re.match(r"===\s*(.+?)\s+(\d{4}-\d{4})\s*===", line)
        if header_match:
            league = header_match.group(1).strip()
            season = header_match.group(2).strip()
            i += 1
            continue
            
        # Match Round
        if line.startswith("Round "):
            current_round = line.strip()
            i += 1
            continue
            
        # Match Date pattern: DD.MM. HH:MM
        date_match = re.match(r"\d{2}\.\d{2}\.\s+\d{2}:\d{2}", line)
        if date_match:
            if i + 6 < len(lines):
                match_date = line
                home_1 = lines[i+1]
                home_2 = lines[i+2]
                away_1 = lines[i+3]
                away_2 = lines[i+4]
                score_1 = lines[i+5]
                score_2 = lines[i+6]
                
                # Check for "Postponed" or "Cancel" instead of score
                if not score_1.isdigit() or not score_2.isdigit():
                    # Might not be a valid score. If so, skip.
                    i += 1
                    continue
                
                if home_1 == home_2 and away_1 == away_2:
                    matches_to_insert.append((
                        league, season, current_round, match_date,
                        home_1, away_1, int(score_1), int(score_2)
                    ))
                    i += 7  # skip the parsed lines
                    continue
        i += 1

    cursor = conn.cursor()
    inserted = 0
    for match in matches_to_insert:
        try:
            cursor.execute('''
            INSERT OR IGNORE INTO matches 
            (league, season, round, date, home_team, away_team, home_score, away_score)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', match)
            if cursor.rowcount > 0:
                inserted += 1
        except Exception as e:
            print(f"Error inserting match: {e}")
            
    conn.commit()
    print(f"Parsed {filepath} - Found {len(matches_to_insert)} matches, Inserted {inserted}")

def main():
    print(f"Setting up database at {os.path.abspath(DB_PATH)}")
    conn = setup_database()
    
    file_pattern = os.path.join(RAW_DATA_DIR, "*.txt")
    files = glob.glob(file_pattern)
    print(f"Found {len(files)} text files to parse.")
    
    for file in files:
        parse_file(file, conn)
        
    cursor = conn.cursor()
    cursor.execute("SELECT count(*) FROM matches")
    print(f"Total matches in database: {cursor.fetchone()[0]}")
    conn.close()

if __name__ == "__main__":
    main()
