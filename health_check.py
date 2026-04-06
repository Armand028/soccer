"""
Comprehensive Website Health Check
Tests all major components of the soccer analysis website
"""
import sys
import os
import sqlite3
import requests
import json

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'backend'))

# Colors for output
GREEN = '\033[92m'
RED = '\033[91m'
YELLOW = '\033[93m'
RESET = '\033[0m'

def success(msg): print(f"{GREEN}✅ {msg}{RESET}")
def error(msg): print(f"{RED}❌ {msg}{RESET}")
def warning(msg): print(f"{YELLOW}⚠️ {msg}{RESET}")
def info(msg): print(f"  {msg}")

print("=" * 70)
print("SOCCER WEBSITE COMPREHENSIVE HEALTH CHECK")
print("=" * 70)

# ========== SECTION 1: DATABASE CHECK ==========
print("\n📊 SECTION 1: DATABASE INTEGRITY")
print("-" * 70)

try:
    conn = sqlite3.connect('soccer.db')
    cursor = conn.cursor()
    
    # Check total matches
    cursor.execute("SELECT COUNT(*) FROM matches")
    total_matches = cursor.fetchone()[0]
    success(f"Database connected: {total_matches:,} total matches")
    
    # Check leagues
    cursor.execute("SELECT DISTINCT league FROM matches ORDER BY league")
    leagues = [row[0] for row in cursor.fetchall()]
    success(f"Found {len(leagues)} leagues:")
    for league in leagues:
        cursor.execute("SELECT COUNT(*) FROM matches WHERE league = ?", (league,))
        count = cursor.fetchone()[0]
        info(f"  - {league}: {count:,} matches")
    
    # Check for NULL team names
    cursor.execute("SELECT COUNT(*) FROM matches WHERE home_team IS NULL OR away_team IS NULL")
    null_teams = cursor.fetchone()[0]
    if null_teams == 0:
        success("No NULL team names found")
    else:
        error(f"Found {null_teams} matches with NULL team names")
    
    # Check for empty team names
    cursor.execute("SELECT COUNT(*) FROM matches WHERE home_team = '' OR away_team = ''")
    empty_teams = cursor.fetchone()[0]
    if empty_teams == 0:
        success("No empty team names found")
    else:
        error(f"Found {empty_teams} matches with empty team names")
    
    conn.close()
except Exception as e:
    error(f"Database check failed: {e}")

# ========== SECTION 2: TEAM NAME NORMALIZATION ==========
print("\n📝 SECTION 2: TEAM NAME NORMALIZATION")
print("-" * 70)

try:
    from fetch_sportsdb import TEAM_NAME_NORMALIZATION
    from main import normalize_team_name
    
    success(f"Normalization map loaded: {len(TEAM_NAME_NORMALIZATION)} entries")
    
    # Test key German team mappings
    test_cases = [
        ('Freiburg', 'SC Freiburg'),
        ('Bayern Munich', 'Bayern München'),
        ('Hoffenheim', 'TSG Hoffenheim'),
        ('Mainz', '1. FSV Mainz 05'),
        ('Wolfsburg', 'VfL Wolfsburg'),
        ('Dortmund', 'Borussia Dortmund'),
        ('Gladbach', 'Borussia Mönchengladbach'),
        ('Leverkusen', 'Bayer Leverkusen'),
        ('Stuttgart', 'VfB Stuttgart'),
        ('Heidenheim', '1. FC Heidenheim'),
    ]
    
    all_passed = True
    for input_name, expected in test_cases:
        result = normalize_team_name(input_name)
        if result == expected:
            info(f"✓ {input_name} -> {result}")
        else:
            error(f"{input_name} -> {result} (expected: {expected})")
            all_passed = False
    
    if all_passed:
        success("All normalization tests passed")
    
except Exception as e:
    error(f"Normalization check failed: {e}")

# ========== SECTION 3: BACKEND API ENDPOINTS ==========
print("\n🔌 SECTION 3: BACKEND API ENDPOINTS")
print("-" * 70)

BASE_URL = "http://localhost:8000"

endpoints = [
    ("/health", "GET", None),
    ("/api/leagues", "GET", None),
    ("/api/matches/today", "GET", None),
    ("/api/matches/recent?limit=5", "GET", None),
]

for endpoint, method, data in endpoints:
    try:
        url = f"{BASE_URL}{endpoint}"
        if method == "GET":
            response = requests.get(url, timeout=5)
        else:
            response = requests.post(url, json=data, timeout=5)
        
        if response.status_code == 200:
            success(f"{endpoint} - OK (200)")
        else:
            error(f"{endpoint} - Status {response.status_code}")
    except requests.exceptions.ConnectionError:
        error(f"{endpoint} - Cannot connect to backend (is it running?)")
    except Exception as e:
        error(f"{endpoint} - Error: {e}")

# ========== SECTION 4: MATCH DATA QUALITY ==========
print("\n⚽ SECTION 4: MATCH DATA QUALITY")
print("-" * 70)

try:
    conn = sqlite3.connect('soccer.db')
    cursor = conn.cursor()
    
    # Check for duplicate matches (same teams, same date)
    cursor.execute("""
        SELECT home_team, away_team, date, COUNT(*) as cnt
        FROM matches
        WHERE league = 'Germany Bundesliga 1'
        GROUP BY home_team, away_team, date
        HAVING cnt > 1
    """)
    duplicates = cursor.fetchall()
    if duplicates:
        warning(f"Found {len(duplicates)} duplicate match groups in Bundesliga")
        for dup in duplicates[:3]:
            info(f"  {dup[0]} vs {dup[1]} on {dup[2]}: {dup[3]} copies")
    else:
        success("No duplicate matches found in Bundesliga")
    
    # Check score distribution
    cursor.execute("""
        SELECT 
            COUNT(CASE WHEN home_score >= 0 AND away_score >= 0 THEN 1 END) as finished,
            COUNT(CASE WHEN home_score < 0 OR away_score < 0 THEN 1 END) as upcoming
        FROM matches
    """)
    finished, upcoming = cursor.fetchone()
    success(f"Match status: {finished:,} finished, {upcoming:,} upcoming")
    
    # Check date range
    cursor.execute("SELECT MIN(date), MAX(date) FROM matches")
    min_date, max_date = cursor.fetchone()
    success(f"Date range: {min_date} to {max_date}")
    
    conn.close()
except Exception as e:
    error(f"Data quality check failed: {e}")

# ========== SECTION 5: FRONTEND CHECK ==========
print("\n🌐 SECTION 5: FRONTEND BUILD")
print("-" * 70)

# Check if frontend dependencies are installed
frontend_path = os.path.join(os.path.dirname(__file__), 'frontend')
node_modules_path = os.path.join(frontend_path, 'node_modules')

if os.path.exists(node_modules_path):
    success("Frontend node_modules exists")
else:
    warning("Frontend node_modules not found - run 'npm install' in frontend/")

# Check package.json
package_json = os.path.join(frontend_path, 'package.json')
if os.path.exists(package_json):
    with open(package_json) as f:
        pkg = json.load(f)
    success(f"Frontend: {pkg.get('name', 'Unknown')} v{pkg.get('version', 'Unknown')}")
    
    # Check for required dependencies
    deps = pkg.get('dependencies', {})
    required = ['next', 'react', 'react-dom', 'tailwindcss']
    missing = [d for d in required if d not in deps]
    if missing:
        warning(f"Missing dependencies: {', '.join(missing)}")
    else:
        success("All core dependencies present")
else:
    error("package.json not found")

# ========== SECTION 6: THESPORTSDB INTEGRATION ==========
print("\n🏟️ SECTION 6: THESPORTSDB INTEGRATION")
print("-" * 70)

try:
    import fetch_sportsdb as sdb
    
    # Check if API key is configured
    if hasattr(sdb, 'THESPORTSDB_API_KEY'):
        success("TheSportsDB API key configured")
    else:
        warning("TheSportsDB API key not found in fetch_sportsdb")
    
    # Check scheduler status
    if hasattr(sdb, 'get_scheduler_status'):
        status = sdb.get_scheduler_status()
        if status.get('running'):
            success(f"Scheduler is running (interval: {status.get('interval_seconds', 'unknown')}s)")
        else:
            warning("Scheduler is not running")
    else:
        warning("Scheduler status function not found")
        
except Exception as e:
    error(f"TheSportsDB integration check failed: {e}")

# ========== SUMMARY ==========
print("\n" + "=" * 70)
print("CHECK COMPLETE")
print("=" * 70)
