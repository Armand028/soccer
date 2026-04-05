"""
Fix team names in cup competitions to match canonical names from leagues.
This ensures consistency across all competitions.
"""
import sqlite3
import os
import sys

# Add backend to path to import the normalization map
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from fetch_sportsdb import TEAM_NAME_NORMALIZATION

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "soccer.db")

# Cup competitions to fix
CUP_LEAGUES = [
    "FA Cup",
    "Coppa Italia", 
    "Coupe de France",
    "Copa del Rey",
    "DFB-Pokal",
    "EFL Cup",
    "FA Community Shield",
    "Supercoppa Italiana",
    "UEFA Europa League",
    "UEFA Conference League",
    "UEFA Champions League"
]

def get_all_teams_in_cups(cursor):
    """Get all unique team names from cup competitions."""
    teams = set()
    for league in CUP_LEAGUES:
        cursor.execute('''
            SELECT DISTINCT home_team FROM matches WHERE league = ?
            UNION
            SELECT DISTINCT away_team FROM matches WHERE league = ?
        ''', (league, league))
        for row in cursor.fetchall():
            if row[0]:
                teams.add(row[0])
    return sorted(teams)

def fix_cup_team_names():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    print("=== Checking cup competitions for unnormalized team names ===\n")
    
    # Get all teams in cups
    cup_teams = get_all_teams_in_cups(cursor)
    print(f"Found {len(cup_teams)} unique teams in cup competitions\n")
    
    # Find teams that need fixing
    to_fix = []  # (league, old_name, new_name, is_home)
    
    for team in cup_teams:
        if team in TEAM_NAME_NORMALIZATION:
            canonical = TEAM_NAME_NORMALIZATION[team]
            if team != canonical:
                # Find which leagues this team appears in
                for league in CUP_LEAGUES:
                    cursor.execute('''
                        SELECT COUNT(*) FROM matches 
                        WHERE league = ? AND (home_team = ? OR away_team = ?)
                    ''', (league, team, team))
                    count = cursor.fetchone()[0]
                    if count > 0:
                        to_fix.append((league, team, canonical, count))
                        print(f"Found: '{team}' → '{canonical}' in {league} ({count} matches)")
    
    if not to_fix:
        print("✅ All cup team names are already normalized!")
        conn.close()
        return
    
    print(f"\n🔧 Fixing {len(to_fix)} team name variants...\n")
    
    total_updated = 0
    for league, old_name, new_name, count in to_fix:
        # Update home_team
        cursor.execute('''
            UPDATE matches 
            SET home_team = ? 
            WHERE league = ? AND home_team = ?
        ''', (new_name, league, old_name))
        home_updated = cursor.rowcount
        
        # Update away_team
        cursor.execute('''
            UPDATE matches 
            SET away_team = ? 
            WHERE league = ? AND away_team = ?
        ''', (new_name, league, old_name))
        away_updated = cursor.rowcount
        
        total_updated += home_updated + away_updated
        print(f"  {league}: '{old_name}' → '{new_name}' ({home_updated + away_updated} rows)")
    
    conn.commit()
    conn.close()
    
    print(f"\n✅ Done! Updated {total_updated} match records in cup competitions")

if __name__ == "__main__":
    fix_cup_team_names()
