"""
Database migration script to expand the matches table with richer data columns.
Run once to add new columns. Safe to re-run (uses IF NOT EXISTS / try-except).
"""
import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "soccer.db")

NEW_COLUMNS = [
    ("event_id", "INTEGER"),           # SoFaScore event ID for API lookups
    ("status", "TEXT DEFAULT 'finished'"),  # 'finished', 'notstarted', 'inprogress', 'cancelled'
    ("kickoff_timestamp", "INTEGER"),  # Unix epoch for precise kickoff time
    ("home_score_ht", "INTEGER"),      # Halftime scores
    ("away_score_ht", "INTEGER"),
    ("home_yellow_cards", "INTEGER DEFAULT 0"),
    ("away_yellow_cards", "INTEGER DEFAULT 0"),
    ("home_red_cards", "INTEGER DEFAULT 0"),
    ("away_red_cards", "INTEGER DEFAULT 0"),
    ("home_corners", "INTEGER DEFAULT 0"),
    ("away_corners", "INTEGER DEFAULT 0"),
    ("home_shots", "INTEGER DEFAULT 0"),
    ("away_shots", "INTEGER DEFAULT 0"),
    ("home_shots_on_target", "INTEGER DEFAULT 0"),
    ("away_shots_on_target", "INTEGER DEFAULT 0"),
    ("home_possession", "REAL"),       # e.g. 55.0
    ("away_possession", "REAL"),
    ("home_fouls", "INTEGER DEFAULT 0"),
    ("away_fouls", "INTEGER DEFAULT 0"),
]

def migrate():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Get existing columns
    cursor.execute("PRAGMA table_info(matches)")
    existing = {row[1] for row in cursor.fetchall()}
    print(f"Existing columns: {existing}")

    added = 0
    for col_name, col_def in NEW_COLUMNS:
        if col_name not in existing:
            sql = f"ALTER TABLE matches ADD COLUMN {col_name} {col_def}"
            try:
                cursor.execute(sql)
                print(f"  + Added column: {col_name}")
                added += 1
            except Exception as e:
                print(f"  ! Error adding {col_name}: {e}")
        else:
            print(f"  - Column already exists: {col_name}")

    # Create index on event_id for fast lookups
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_event_id ON matches(event_id)")
    # Create index on status for filtering today's matches
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_status ON matches(status)")
    # Create index on kickoff_timestamp for date-range queries
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_kickoff ON matches(kickoff_timestamp)")

    conn.commit()
    conn.close()
    print(f"\nMigration complete. Added {added} new columns.")

if __name__ == "__main__":
    migrate()
