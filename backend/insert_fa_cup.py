"""
Insert FA Cup matches into the database
"""
import sqlite3
import os
from datetime import datetime

# Database setup
_here = os.path.dirname(os.path.abspath(__file__))
_parent = os.path.dirname(_here)
DB_PATH = os.path.join(_here, "soccer.db") if os.path.exists(os.path.join(_here, "soccer.db")) else os.path.join(_parent, "soccer.db")

# FA Cup match data from user - stored in separate file to keep this manageable
# The data will be read from a CSV file

def parse_datetime(dt_str):
    """Parse datetime string and return date and unix timestamp."""
    try:
        dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
        return dt.strftime("%Y-%m-%d"), int(dt.timestamp())
    except:
        return dt_str[:10] if len(dt_str) >= 10 else dt_str, 0


def insert_fa_cup_from_csv(csv_file_path):
    """Insert FA Cup matches from a CSV file into the database."""
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        # Check how many FA Cup matches already exist
        cursor.execute("SELECT COUNT(*) FROM matches WHERE league = ?", ("FA Cup",))
        existing_count = cursor.fetchone()[0]
        print(f"Existing FA Cup matches in DB: {existing_count}")
        
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
                    # Future match
                    home_score = None
                    away_score = None
                    status = 'SCHEDULED'
                else:
                    home_score = int(home_score_str)
                    away_score = int(away_score_str)
                    status = 'FINISHED'
                
                # Check for duplicate (same league + event_id)
                cursor.execute("SELECT id FROM matches WHERE event_id = ? AND league = ?", (event_id, "FA Cup"))
                if cursor.fetchone():
                    skipped += 1
                    continue
                
                # Insert match
                cursor.execute('''
                    INSERT INTO matches (
                        date, league, home_team, away_team, 
                        home_score, away_score, event_id, status, kickoff_timestamp
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    date, "FA Cup", home_team, away_team,
                    home_score, away_score, event_id, status, kickoff_timestamp
                ))
                inserted += 1
                
            except Exception as e:
                errors += 1
                print(f"Error processing line: {line[:50]}... - {e}")
                continue
        
        conn.commit()
        print(f"\n✅ Inserted {inserted} new FA Cup matches")
        print(f"⏭️ Skipped {skipped} duplicates")
        print(f"❌ Errors: {errors}")
        print(f"📊 Total FA Cup matches in DB: {existing_count + inserted}")
        
    except Exception as e:
        conn.rollback()
        print(f"❌ Error: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python insert_fa_cup.py <csv_file_path>")
        print("Example: python insert_fa_cup.py fa_cup_data.csv")
        sys.exit(1)
    
    insert_fa_cup_from_csv(sys.argv[1])
