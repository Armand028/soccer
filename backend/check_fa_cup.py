import sqlite3
import os

# Database setup
_here = os.path.dirname(os.path.abspath(__file__))
_parent = os.path.dirname(_here)
DB_PATH = os.path.join(_here, "soccer.db") if os.path.exists(os.path.join(_here, "soccer.db")) else os.path.join(_parent, "soccer.db")

def check_fa_cup():
    """Check if FA Cup matches exist in the database."""
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        # Check how many FA Cup matches exist
        cursor.execute("SELECT COUNT(*) FROM matches WHERE league = ?", ("FA Cup",))
        count = cursor.fetchone()[0]
        print(f"FA Cup matches in DB: {count}")
        
        if count > 0:
            # Show a few examples
            cursor.execute("""
                SELECT id, home_team, away_team, date, round 
                FROM matches 
                WHERE league = ? 
                LIMIT 5
            """, ("FA Cup",))
            
            examples = cursor.fetchall()
            print("\nExample FA Cup matches:")
            for match in examples:
                print(f"  ID: {match[0]}, {match[1]} vs {match[2]}, {match[3]}, {match[4]}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    check_fa_cup()
