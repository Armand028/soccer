import sqlite3
conn = sqlite3.connect('soccer.db')
c = conn.cursor()
c.execute("SELECT DISTINCT home_team FROM matches WHERE league = 'Germany Bundesliga 1' UNION SELECT DISTINCT away_team FROM matches WHERE league = 'Germany Bundesliga 1' ORDER BY 1")
print('=== GERMAN TEAMS IN DATABASE ===')
for row in c.fetchall():
    print(row[0])
conn.close()
