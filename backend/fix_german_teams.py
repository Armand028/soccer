"""
Fix German team names in the database
"""
import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "soccer.db")

# German team name normalization map (must match fetch_footballdata.py)
GERMAN_TEAM_MAP = {
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
    "Bayer 04 Leverkusen": "Bayer Leverkusen",
    "Bielefeld": "Arminia Bielefeld",
    "Schalke": "Schalke 04",
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
    "TSG 1899 Hoffenheim": "TSG Hoffenheim",
    "Freiburg": "SC Freiburg",
    "Union Berlin": "1. FC Union Berlin",
    "Werder Bremen": "SV Werder Bremen",
    "Bremen": "SV Werder Bremen",
    "Mainz": "1. FSV Mainz 05",
    "Mainz 05": "1. FSV Mainz 05",
    "1. FSV Mainz": "1. FSV Mainz 05",
    "Bochum": "VfL Bochum",
    "Darmstadt": "SV Darmstadt 98",
    "Heidenheim": "1. FC Heidenheim",
    "Heidenheim 1846": "1. FC Heidenheim",
    "1. FC Heidenheim 1846": "1. FC Heidenheim",
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
}

def fix_german_team_names():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Get all unique team names from German Bundesliga
    cursor.execute('''
        SELECT DISTINCT home_team FROM matches WHERE league = 'Germany Bundesliga 1'
        UNION
        SELECT DISTINCT away_team FROM matches WHERE league = 'Germany Bundesliga 1'
    ''')
    
    teams = [row[0] for row in cursor.fetchall()]
    print(f"Found {len(teams)} unique German teams in database")
    
    # Track what needs fixing
    to_fix = []
    for team in teams:
        if team in GERMAN_TEAM_MAP:
            canonical = GERMAN_TEAM_MAP[team]
            if team != canonical:
                to_fix.append((team, canonical))
                print(f"  {team} → {canonical}")
    
    if not to_fix:
        print("✅ No German team names need fixing!")
        conn.close()
        return
    
    print(f"\n🔧 Fixing {len(to_fix)} team name variants...")
    
    # Apply fixes
    total_updated = 0
    for old_name, new_name in to_fix:
        # Update home_team
        cursor.execute('''
            UPDATE matches 
            SET home_team = ? 
            WHERE league = 'Germany Bundesliga 1' AND home_team = ?
        ''', (new_name, old_name))
        home_updated = cursor.rowcount
        
        # Update away_team
        cursor.execute('''
            UPDATE matches 
            SET away_team = ? 
            WHERE league = 'Germany Bundesliga 1' AND away_team = ?
        ''', (new_name, old_name))
        away_updated = cursor.rowcount
        
        total_updated += home_updated + away_updated
        print(f"  {old_name} → {new_name} ({home_updated + away_updated} rows)")
    
    conn.commit()
    conn.close()
    
    print(f"\n✅ Done! Updated {total_updated} match records")

if __name__ == "__main__":
    fix_german_team_names()
