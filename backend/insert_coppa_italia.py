"""
Insert Coppa Italia matches into the database
"""
import sqlite3
import os
from datetime import datetime
import time

# Database setup
_here = os.path.dirname(os.path.abspath(__file__))
_parent = os.path.dirname(_here)
DB_PATH = os.path.join(_here, "soccer.db") if os.path.exists(os.path.join(_here, "soccer.db")) else os.path.join(_parent, "soccer.db")

# Coppa Italia match data from user
COPPA_ITALIA_DATA = """idEvent,strTimestamp,Round,Home Team,Home Score,Away Team,Away Score,Poster,Thumb
2281622,2025-08-09 18:30:00,Round 400,Virtus Entella,4,Ternana,0,,https://r2.thesportsdb.com/images/media/event/thumb/gzp0qb1755264960.jpg
2281623,2025-08-10 17:30:00,Round 400,Padova,0,Vicenza,2,,https://r2.thesportsdb.com/images/media/event/thumb/7qem8l1755264964.jpg
2281624,2025-08-10 18:30:00,Round 400,Avellino,1,Audace Cerignola,0,,https://r2.thesportsdb.com/images/media/event/thumb/5cwqs11755264967.jpg
2281625,2025-08-10 18:30:00,Round 400,Pescara,1,Rimini,0,,https://r2.thesportsdb.com/images/media/event/thumb/t1c67h1755264971.jpg
2308595,2025-08-10 18:30:00,Round 400,Audace Cerignola,0,Avellino,1,,https://r2.thesportsdb.com/images/media/event/thumb/u3kkrz1755265020.jpg
2281626,2025-08-15 16:00:00,Round 1,Empoli,1,Reggiana,1,,https://r2.thesportsdb.com/images/media/event/thumb/mn15ea1755264975.jpg
2281627,2025-08-15 16:30:00,Round 1,Sassuolo,1,Catanzaro,0,,https://r2.thesportsdb.com/images/media/event/thumb/rtxdls1755264979.jpg
2281628,2025-08-15 18:45:00,Round 1,Lecce,2,Juve Stabia,0,,https://r2.thesportsdb.com/images/media/event/thumb/p3h5jf1755264982.jpg
2308895,2025-08-15 19:15:00,Round 1,Genoa,3,Vicenza,0,,https://r2.thesportsdb.com/images/media/event/thumb/shxq6d1755265028.jpg
2281629,2025-08-16 16:00:00,Round 1,Venezia,4,Mantova,0,,https://r2.thesportsdb.com/images/media/event/thumb/tu3gbs1755264986.jpg
2281630,2025-08-16 16:30:00,Round 1,Como,3,Südtirol,1,,https://r2.thesportsdb.com/images/media/event/thumb/vcap9e1755264990.jpg
2308613,2025-08-16 18:45:00,Round 1,Cagliari,1,Virtus Entella,1,,https://r2.thesportsdb.com/images/media/event/thumb/axbepf1755265024.jpg
2281631,2025-08-16 19:15:00,Round 1,Cremonese,0,Palermo,0,,https://r2.thesportsdb.com/images/media/event/thumb/t99nmy1755264993.jpg
2281632,2025-08-17 16:00:00,Round 1,Monza,0,Frosinone,1,,https://r2.thesportsdb.com/images/media/event/thumb/ouqnri1755264997.jpg
2308896,2025-08-17 16:30:00,Round 1,Parma,2,Pescara,0,,https://r2.thesportsdb.com/images/media/event/thumb/42jtzv1755265031.jpg
2314560,2025-08-17 18:45:00,Round 1,Cesena,0,Pisa,0,,https://r2.thesportsdb.com/images/media/event/thumb/yd7nso1758607201.jpg
2281634,2025-08-17 19:15:00,Round 1,AC Milan,2,Bari,0,,https://r2.thesportsdb.com/images/media/event/thumb/2zna361755265005.jpg
2314978,2025-08-18 16:00:00,Round 1,Audace Cerignola,1,Hellas Verona,1,,https://r2.thesportsdb.com/images/media/event/thumb/w4iyzo1758607204.jpg
2281635,2025-08-18 16:30:00,Round 1,Spezia,1,Sampdoria,1,,https://r2.thesportsdb.com/images/media/event/thumb/yg7wlc1755265009.jpg
2281636,2025-08-18 18:45:00,Round 1,Udinese,2,Carrarese,0,,https://r2.thesportsdb.com/images/media/event/thumb/47oggy1755265012.jpg
2281637,2025-08-18 19:15:00,Round 1,Torino,1,Modena,0,,https://r2.thesportsdb.com/images/media/event/thumb/vh67q71755265016.jpg
2321153,2025-09-23 15:00:00,Round 2,Cagliari,4,Frosinone,1,,https://r2.thesportsdb.com/images/media/event/thumb/6yinu81758607226.jpg
2334068,2025-09-23 16:30:00,Round 2,Udinese,2,Palermo,1,,https://r2.thesportsdb.com/images/media/event/thumb/aqe1nd1758607234.jpg
2321154,2025-09-23 19:00:00,Round 2,AC Milan,3,Lecce,0,,https://r2.thesportsdb.com/images/media/event/thumb/zp3uje1758607230.jpg
2321152,2025-09-24 15:00:00,Round 2,Parma,2,Spezia,2,,https://r2.thesportsdb.com/images/media/event/thumb/73gne01758607222.jpg
2321150,2025-09-24 16:30:00,Round 2,Hellas Verona,0,Venezia,0,,https://r2.thesportsdb.com/images/media/event/thumb/clqkht1758607215.jpg
2321151,2025-09-24 19:00:00,Round 2,Como,3,Sassuolo,0,,https://r2.thesportsdb.com/images/media/event/thumb/299kqz1758607219.jpg
2321149,2025-09-25 16:30:00,Round 2,Genoa,3,Empoli,1,,https://r2.thesportsdb.com/images/media/event/thumb/nkjluz1758607212.jpg
2321148,2025-09-25 19:00:00,Round 2,Torino,1,Pisa,0,,https://r2.thesportsdb.com/images/media/event/thumb/b57o6s1758607208.jpg
2360717,2025-12-02 20:00:00,Round 16,Juventus,2,Udinese,0,,https://r2.thesportsdb.com/images/media/event/thumb/efoxkf1764658657.jpg
2360713,2025-12-03 14:00:00,Round 16,Atalanta,4,Genoa,0,,https://r2.thesportsdb.com/images/media/event/thumb/43yanm1764658643.jpg
2360719,2025-12-03 17:00:00,Round 16,Napoli,1,Cagliari,1,,https://r2.thesportsdb.com/images/media/event/thumb/6nv3ei1764658664.jpg
2360716,2025-12-03 20:00:00,Round 16,Inter Milan,5,Venezia,1,,https://r2.thesportsdb.com/images/media/event/thumb/2fvcrx1764658654.jpg
2360714,2025-12-04 17:00:00,Round 16,Bologna,2,Parma,1,,https://r2.thesportsdb.com/images/media/event/thumb/suyf121764658646.jpg
2360718,2025-12-04 20:00:00,Round 16,Lazio,1,AC Milan,0,,https://r2.thesportsdb.com/images/media/event/thumb/9hoe7u1764658661.jpg
2360712,2026-01-13 20:00:00,Round 16,Roma,2,Torino,3,,https://r2.thesportsdb.com/images/media/event/thumb/c3pmyr1765442728.jpg
2360715,2026-01-27 20:00:00,Round 16,Fiorentina,1,Como,3,,https://r2.thesportsdb.com/images/media/event/thumb/cqjy541764658650.jpg
2415865,2026-02-04 20:00:00,Round 125,Inter Milan,2,Torino,1,,https://r2.thesportsdb.com/images/media/event/thumb/9f52aw1770705098.jpg
2428740,2026-02-05 20:00:00,Round 125,Atalanta,3,Juventus,0,,https://r2.thesportsdb.com/images/media/event/thumb/b0q36t1770705120.jpg
2425249,2026-02-10 20:00:00,Round 125,Napoli,1,Como,1,,https://r2.thesportsdb.com/images/media/event/thumb/3zaxd11770705113.jpg
2391460,2026-02-11 20:00:00,Round 125,Bologna,1,Lazio,1,,https://r2.thesportsdb.com/images/media/event/thumb/sxq0yd1770705037.jpg
2432481,2026-03-03 20:00:00,Round 150,Como,0,Inter Milan,0,,https://r2.thesportsdb.com/images/media/event/thumb/vb5gs31772524899.jpg
2443833,2026-03-04 20:00:00,Round 0,Lazio,2,Atalanta,2,,
2432482,2026-04-22 16:00:00,Round 150,Inter Milan,,Como,,,https://r2.thesportsdb.com/images/media/event/thumb/9rwp3j1772524900.jpg
2443834,2026-04-22 16:00:00,Round 0,Lazio,,Atalanta,,,"""


def parse_datetime(dt_str):
    """Parse datetime string and return date and unix timestamp."""
    try:
        dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
        return dt.strftime("%Y-%m-%d"), int(dt.timestamp())
    except:
        return dt_str[:10] if len(dt_str) >= 10 else dt_str, 0


def insert_coppa_italia():
    """Insert Coppa Italia matches into the database."""
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        # Check how many Coppa Italia matches already exist
        cursor.execute("SELECT COUNT(*) FROM matches WHERE league = ?", ("Coppa Italia",))
        existing_count = cursor.fetchone()[0]
        print(f"Existing Coppa Italia matches in DB: {existing_count}")
        
        # Parse CSV data
        lines = COPPA_ITALIA_DATA.strip().split('\n')
        header = lines[0]
        data_lines = lines[1:]
        
        inserted = 0
        skipped = 0
        
        for line in data_lines:
            if not line.strip():
                continue
                
            parts = line.split(',')
            if len(parts) < 8:
                continue
            
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
            cursor.execute("SELECT id FROM matches WHERE event_id = ? AND league = ?", (event_id, "Coppa Italia"))
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
                date, "Coppa Italia", home_team, away_team,
                home_score, away_score, event_id, status, kickoff_timestamp
            ))
            inserted += 1
            print(f"Inserted: {home_team} vs {away_team} ({round_name})")
        
        conn.commit()
        print(f"\n✅ Inserted {inserted} new Coppa Italia matches")
        print(f"⏭️ Skipped {skipped} duplicates")
        print(f"📊 Total Coppa Italia matches in DB: {existing_count + inserted}")
        
    except Exception as e:
        conn.rollback()
        print(f"❌ Error: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    insert_coppa_italia()
