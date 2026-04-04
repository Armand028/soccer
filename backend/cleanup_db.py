"""Clean up remaining duplicate matches"""
import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "soccer.db")
conn = sqlite3.connect(DB_PATH)
c = conn.cursor()

print("Cleaning remaining duplicates...")

# 1. Paris Saint-Germain vs Toulouse - keep ID 16372 (higher event_id)
c.execute("DELETE FROM matches WHERE id = 27839")
print(f"Removed duplicate PSG vs Toulouse (ID 27839)")

# 2. Rayo Vallecano vs Elche - keep ID 16366 (higher event_id)
c.execute("DELETE FROM matches WHERE id = 27840")
print(f"Removed duplicate Rayo Vallecano vs Elche (ID 27840)")

# 3. Levante vs Sevilla - remove matches with weird date format
c.execute("DELETE FROM matches WHERE date LIKE '21.04%' OR date LIKE '%13:00%'")
print(f"Removed matches with invalid date format")

conn.commit()

# Verify
c.execute("SELECT COUNT(*) FROM matches")
print(f"\nTotal matches after cleanup: {c.fetchone()[0]}")

# Check for any remaining duplicates
c.execute("""
    SELECT league, date, home_team, away_team, COUNT(*) as cnt
    FROM matches
    WHERE date NOT LIKE '%:%'
    GROUP BY league, date, home_team, away_team
    HAVING cnt > 1
    LIMIT 5
""")
dupes = c.fetchall()
if dupes:
    print("\nRemaining duplicates:")
    for d in dupes:
        print(f"  {d[0]} {d[1]}: {d[2]} vs {d[3]} ({d[4]} copies)")
else:
    print("\nNo duplicates remaining!")

conn.close()
