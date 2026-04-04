"""
Clean up FA Cup matches from the database
"""
import sqlite3
import os

# Database setup
_here = os.path.dirname(os.path.abspath(__file__))
_parent = os.path.dirname(_here)
DB_PATH = os.path.join(_here, "soccer.db") if os.path.exists(os.path.join(_here, "soccer.db")) else os.path.join(_parent, "soccer.db")

def cleanup_fa_cup():
    """Remove all FA Cup matches from the database."""
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        # First, let's see how many FA Cup matches exist
        cursor.execute("SELECT COUNT(*) FROM matches WHERE league = ?", ("FA Cup",))
        count = cursor.fetchone()[0]
        print(f"Found {count} FA Cup matches in the database")
        
        if count == 0:
            print("No FA Cup matches to remove")
            return
        
        # Show a few examples before deletion
        cursor.execute("""
            SELECT id, home_team, away_team, date, round 
            FROM matches 
            WHERE league = ? 
            LIMIT 5
        """, ("FA Cup",))
        
        examples = cursor.fetchall()
        if examples:
            print("\nExample matches that will be removed:")
            for match in examples:
                print(f"  ID: {match[0]}, {match[1]} vs {match[2]}, {match[3]}, {match[4]}")
        
        # Confirm deletion
        response = input(f"\nAre you sure you want to delete all {count} FA Cup matches? (yes/no): ")
        
        if response.lower() != 'yes':
            print("Deletion cancelled")
            return
        
        # Delete all FA Cup matches
        cursor.execute("DELETE FROM matches WHERE league = ?", ("FA Cup",))
        deleted_count = cursor.rowcount
        
        conn.commit()
        
        print(f"✅ Successfully deleted {deleted_count} FA Cup matches")
        
        # Verify deletion
        cursor.execute("SELECT COUNT(*) FROM matches WHERE league = ?", ("FA Cup",))
        remaining = cursor.fetchone()[0]
        print(f"✅ Verification: {remaining} FA Cup matches remaining")
        
    except Exception as e:
        conn.rollback()
        print(f"❌ Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    cleanup_fa_cup()
