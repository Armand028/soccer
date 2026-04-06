"""
Comprehensive Website Health Check - Detailed Report
"""
import sys
import os
import sqlite3
import json
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'backend'))

print("=" * 80)
print("SOCCER WEBSITE - COMPREHENSIVE HEALTH CHECK REPORT")
print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 80)

issues_found = []
warnings_found = []

# ========== 1. DATABASE INTEGRITY ==========
print("\n📊 1. DATABASE INTEGRITY")
print("-" * 80)

try:
    conn = sqlite3.connect('soccer.db')
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    # Total matches
    cursor.execute("SELECT COUNT(*) FROM matches")
    total = cursor.fetchone()[0]
    print(f"   Total matches: {total:,}")
    
    # Date range
    cursor.execute("SELECT MIN(date), MAX(date) FROM matches")
    min_date, max_date = cursor.fetchone()
    print(f"   Date range: {min_date} to {max_date}")
    
    # Leagues
    cursor.execute("SELECT DISTINCT league, COUNT(*) as cnt FROM matches GROUP BY league ORDER BY cnt DESC")
    leagues = cursor.fetchall()
    print(f"   Leagues ({len(leagues)}):")
    for league in leagues:
        print(f"      • {league[0]}: {league[1]:,} matches")
    
    # Check for data quality issues
    cursor.execute("SELECT COUNT(*) FROM matches WHERE home_team IS NULL OR away_team IS NULL OR home_team = '' OR away_team = ''")
    null_empty = cursor.fetchone()[0]
    if null_empty > 0:
        issues_found.append(f"Found {null_empty} matches with NULL/empty team names")
    else:
        print("   ✓ No NULL or empty team names")
    
    # Check for duplicate matches
    cursor.execute("""
        SELECT home_team, away_team, date, COUNT(*) as cnt 
        FROM matches 
        GROUP BY home_team, away_team, date 
        HAVING cnt > 1
    """)
    dups = cursor.fetchall()
    if dups:
        warnings_found.append(f"Found {len(dups)} duplicate match groups")
        print(f"   ⚠ Found {len(dups)} duplicate match groups")
    else:
        print("   ✓ No duplicate matches found")
    
    conn.close()
    
except Exception as e:
    issues_found.append(f"Database check failed: {e}")
    print(f"   ✗ Database check failed: {e}")

# ========== 2. TEAM NAME NORMALIZATION ==========
print("\n📝 2. TEAM NAME NORMALIZATION")
print("-" * 80)

try:
    from fetch_sportsdb import TEAM_NAME_NORMALIZATION
    from main import normalize_team_name
    
    print(f"   Normalization entries: {len(TEAM_NAME_NORMALIZATION)}")
    
    # Test key variants
    test_cases = [
        ('Gladbach', 'Borussia Mönchengladbach'),
        ('Freiburg', 'SC Freiburg'),
        ('Bayern Munich', 'Bayern München'),
        ('Hoffenheim', 'TSG Hoffenheim'),
        ('Mainz', '1. FSV Mainz 05'),
        ('Wolfsburg', 'VfL Wolfsburg'),
        ('Dortmund', 'Borussia Dortmund'),
        ('Leverkusen', 'Bayer Leverkusen'),
        ('Stuttgart', 'VfB Stuttgart'),
        ('Heidenheim', '1. FC Heidenheim'),
        ('Real Madrid', 'Real Madrid'),
        ('Barcelona', 'Barcelona'),
        ('Man City', 'Manchester City'),
        ('Man United', 'Manchester United'),
    ]
    
    failed = []
    for variant, expected in test_cases:
        result = normalize_team_name(variant)
        if result != expected:
            failed.append(f"{variant} -> {result} (expected: {expected})")
    
    if failed:
        print(f"   ✗ {len(failed)} normalization tests failed:")
        for f in failed:
            print(f"      • {f}")
        issues_found.extend(failed)
    else:
        print(f"   ✓ All {len(test_cases)} normalization tests passed")
    
except Exception as e:
    issues_found.append(f"Normalization check failed: {e}")
    print(f"   ✗ Normalization check failed: {e}")

# ========== 3. BACKEND API STATUS ==========
print("\n🔌 3. BACKEND API STATUS")
print("-" * 80)

try:
    import requests
    
    endpoints = [
        ('GET', '/health', 'Health check'),
        ('GET', '/api/leagues', 'Leagues list'),
        ('GET', '/api/matches/today', "Today's matches"),
        ('GET', '/api/matches/recent?limit=5', 'Recent matches'),
        ('GET', '/api/data/scheduler', 'Scheduler status'),
    ]
    
    backend_ok = True
    for method, endpoint, desc in endpoints:
        try:
            url = f"http://localhost:8000{endpoint}"
            resp = requests.get(url, timeout=3)
            if resp.status_code == 200:
                print(f"   ✓ {desc}: OK")
            else:
                print(f"   ✗ {desc}: Status {resp.status_code}")
                backend_ok = False
        except requests.exceptions.ConnectionError:
            print(f"   ✗ {desc}: Cannot connect")
            backend_ok = False
        except Exception as e:
            print(f"   ✗ {desc}: {e}")
            backend_ok = False
    
    if not backend_ok:
        warnings_found.append("Backend API not fully accessible")
        
except ImportError:
    warnings_found.append("requests module not available, skipping API check")
    print("   ⚠ requests module not available")
except Exception as e:
    issues_found.append(f"API check failed: {e}")
    print(f"   ✗ API check failed: {e}")

# ========== 4. THESPORTSDB INTEGRATION ==========
print("\n🏟️ 4. THESPORTSDB INTEGRATION")
print("-" * 80)

try:
    import fetch_sportsdb as sdb
    
    # Check scheduler
    if hasattr(sdb, 'get_scheduler_status'):
        status = sdb.get_scheduler_status()
        if status.get('running'):
            print(f"   ✓ Scheduler running (interval: {status.get('interval_seconds', '?')}s)")
        else:
            print(f"   ⚠ Scheduler not running")
            warnings_found.append("Scheduler not running")
    else:
        warnings_found.append("Scheduler status unavailable")
        print(f"   ⚠ Scheduler status function unavailable")
    
    # Check API key
    if hasattr(sdb, 'THESPORTSDB_API_KEY'):
        key = sdb.THESPORTSDB_API_KEY
        if key and key != "123":
            print(f"   ✓ API key configured (masked)")
        else:
            print(f"   ⚠ Using default/public API key")
    else:
        warnings_found.append("API key not found")
        print(f"   ⚠ API key not configured")
    
except Exception as e:
    issues_found.append(f"TheSportsDB check failed: {e}")
    print(f"   ✗ TheSportsDB check failed: {e}")

# ========== 5. FRONTEND STATUS ==========
print("\n🌐 5. FRONTEND STATUS")
print("-" * 80)

frontend_path = os.path.join(os.path.dirname(__file__), 'frontend')

# Check package.json
pkg_path = os.path.join(frontend_path, 'package.json')
if os.path.exists(pkg_path):
    with open(pkg_path) as f:
        pkg = json.load(f)
    print(f"   ✓ package.json: {pkg.get('name', 'unknown')} v{pkg.get('version', 'unknown')}")
else:
    issues_found.append("Frontend package.json not found")
    print(f"   ✗ package.json not found")

# Check node_modules
node_mods = os.path.join(frontend_path, 'node_modules')
if os.path.exists(node_mods):
    print(f"   ✓ node_modules exists")
else:
    warnings_found.append("Frontend node_modules missing - run npm install")
    print(f"   ⚠ node_modules missing")

# Check key pages exist
pages_dir = os.path.join(frontend_path, 'src', 'app')
expected_pages = ['today', 'predict', 'h2h', 'match', 'leagues']
for page in expected_pages:
    page_path = os.path.join(pages_dir, page)
    if os.path.exists(page_path):
        print(f"   ✓ Page: /{page}")
    else:
        warnings_found.append(f"Page /{page} not found")
        print(f"   ⚠ Page /{page} not found")

# ========== 6. VERCEL DEPLOYMENT CONFIG ==========
print("\n🚀 6. VERCEL DEPLOYMENT CONFIG")
print("-" * 80)

vercel_files = [
    os.path.join(os.path.dirname(__file__), 'vercel.json'),
    os.path.join(frontend_path, 'vercel.json'),
]

found_vercel = False
for vf in vercel_files:
    if os.path.exists(vf):
        found_vercel = True
        with open(vf) as f:
            config = json.load(f)
        print(f"   ✓ {os.path.basename(os.path.dirname(vf))}/vercel.json found")
        
        # Check for common issues
        if 'buildCommand' in config and 'cd ' in config['buildCommand']:
            warnings_found.append(f"vercel.json has 'cd' in buildCommand - may cause issues")
            print(f"   ⚠ Has 'cd' in buildCommand")
        if 'rootDirectory' in config:
            warnings_found.append(f"vercel.json has deprecated rootDirectory field")
            print(f"   ⚠ Has deprecated rootDirectory field")

if not found_vercel:
    warnings_found.append("No vercel.json found")
    print(f"   ⚠ No vercel.json found")

# ========== SUMMARY ==========
print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)

if issues_found:
    print(f"\n   ❌ {len(issues_found)} CRITICAL ISSUES:")
    for issue in issues_found:
        print(f"      • {issue}")
else:
    print(f"\n   ✅ No critical issues found")

if warnings_found:
    print(f"\n   ⚠️  {len(warnings_found)} WARNINGS:")
    for warning in warnings_found:
        print(f"      • {warning}")
else:
    print(f"\n   ✅ No warnings")

print(f"\n   OVERALL STATUS: ", end="")
if issues_found:
    print("NEEDS ATTENTION")
elif warnings_found:
    print("OK (with warnings)")
else:
    print("HEALTHY")

print("=" * 80)
