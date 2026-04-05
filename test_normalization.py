"""Test team name normalization"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'backend'))

from main import normalize_team_name, normalize_matches, normalize_match_teams
from fetch_sportsdb import TEAM_NAME_NORMALIZATION
import sqlite3

print('=== TEST 1: Normalization Functions ===')
test_names = ['Freiburg', 'Bayern Munich', 'Hoffenheim', 'Mainz', 'Bayer Leverkusen', 'Wolfsburg']
for name in test_names:
    normalized = normalize_team_name(name)
    expected = TEAM_NAME_NORMALIZATION.get(name, name)
    status = '✅' if normalized == expected else '❌'
    print(f'  {status} {name} -> {normalized}')

print('\n=== TEST 2: German Bundesliga Teams in DB ===')
conn = sqlite3.connect('soccer.db')
cursor = conn.cursor()
cursor.execute("""
    SELECT DISTINCT home_team FROM matches WHERE league = 'Germany Bundesliga 1'
    UNION
    SELECT DISTINCT away_team FROM matches WHERE league = 'Germany Bundesliga 1'
    ORDER BY 1
""")
teams = [row[0] for row in cursor.fetchall()]
print(f'Found {len(teams)} teams in database')

print('\n=== TEST 3: Checking for Unnormalized Variants ===')
unnormalized = []
for team in teams:
    if team in TEAM_NAME_NORMALIZATION and TEAM_NAME_NORMALIZATION[team] != team:
        unnormalized.append((team, TEAM_NAME_NORMALIZATION[team]))

if unnormalized:
    print('❌ Found unnormalized team names in DB:')
    for team, canonical in unnormalized:
        print(f'  {team} should be {canonical}')
else:
    print('✅ All German team names in DB are already normalized!')

print('\n=== TEST 4: API Response Normalization ===')
test_match = {
    'home_team': 'Freiburg',
    'away_team': 'Bayern Munich',
    'home_score': 2,
    'away_score': 1
}
normalized = normalize_match_teams(test_match.copy())
print(f"  Original: {test_match['home_team']} vs {test_match['away_team']}")
print(f"  Normalized: {normalized['home_team']} vs {normalized['away_team']}")
if normalized['home_team'] == 'SC Freiburg' and normalized['away_team'] == 'Bayern München':
    print('  ✅ API response normalization working!')
else:
    print('  ❌ API response normalization failed!')

conn.close()
print('\n=== TEST COMPLETE ===')
