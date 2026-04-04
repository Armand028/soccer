import sqlite3
conn = sqlite3.connect('soccer.db')
cursor = conn.cursor()

print('=== CHECKING FOR NUMERIC LEAGUES ===')
cursor.execute("SELECT DISTINCT league FROM matches WHERE league LIKE 'League %' ORDER BY league")
numeric_leagues = cursor.fetchall()

if numeric_leagues:
    print('Found numeric leagues:')
    for league in numeric_leagues:
        league_name = league[0]
        cursor.execute('SELECT COUNT(*) FROM matches WHERE league = ?', (league_name,))
        count = cursor.fetchone()[0]
        print(f'  {league_name}: {count} matches')
else:
    print('✅ No numeric leagues found - all leagues have proper names!')

print('\n=== ALL LEAGUES IN DATABASE ===')
cursor.execute('SELECT DISTINCT league, COUNT(*) as cnt FROM matches GROUP BY league ORDER BY cnt DESC')
for row in cursor.fetchall():
    print(f'  {row[0]}: {row[1]} matches')

conn.close()
