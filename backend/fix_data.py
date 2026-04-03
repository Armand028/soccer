"""
Data normalization script — fixes team name variants, league names, and removes duplicates.
Run this ONCE to clean the database. Creates a backup first.

Strategy: To avoid UNIQUE constraint failures, we first resolve conflicts by deleting
the less data-rich duplicate, then rename the remaining record.
"""
import sqlite3
import os
import shutil

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "soccer.db")
BACKUP_PATH = DB_PATH.replace(".db", "_backup_pre_normalize.db")

# ---------------------------------------------------------------------------
# TEAM NAME NORMALIZATION MAP
# Maps variant -> canonical name (using TheSportsDB 2025-2026 names as canonical)
# ---------------------------------------------------------------------------
TEAM_NAME_MAP = {
    # === ENGLISH PREMIER LEAGUE ===
    "Brighton": "Brighton and Hove Albion",
    "Leeds": "Leeds United",
    "Manchester Utd": "Manchester United",
    "Newcastle": "Newcastle United",
    "Nottingham": "Nottingham Forest",
    "Sheffield Utd": "Sheffield United",
    "Tottenham": "Tottenham Hotspur",
    "West Ham": "West Ham United",
    "Wolves": "Wolverhampton Wanderers",
    "West Brom": "West Bromwich Albion",
    "Leicester": "Leicester City",
    "Luton": "Luton Town",
    "Ipswich": "Ipswich Town",
    "Norwich": "Norwich City",

    # === SPANISH LA LIGA ===
    "Ath Bilbao": "Athletic Bilbao",
    "Ath Madrid": "Atlético Madrid",
    "Atl. Madrid": "Atlético Madrid",
    "Atletico Madrid": "Atlético Madrid",
    "Betis": "Real Betis",
    "Alaves": "Deportivo Alavés",
    "Alavés": "Deportivo Alavés",
    "Cadiz": "Cádiz CF",
    "Cadiz CF": "Cádiz CF",
    "Granada CF": "Granada",
    "Sociedad": "Real Sociedad",

    # === GERMAN BUNDESLIGA ===
    "Augsburg": "FC Augsburg",
    "Dortmund": "Borussia Dortmund",
    "B. Monchengladbach": "Borussia Mönchengladbach",
    "Monchengladbach": "Borussia Mönchengladbach",
    "Mönchengladbach": "Borussia Mönchengladbach",
    "Ein Frankfurt": "Eintracht Frankfurt",
    "FC Koln": "FC Köln",
    "Hertha": "Hertha Berlin",
    "Leverkusen": "Bayer Leverkusen",
    "Bielefeld": "Arminia Bielefeld",
    "Schalke": "Schalke 04",
    "RasenBallsport Leipzig": "RB Leipzig",
    "Greuther Furth": "Greuther Fürth",
    "Greuther Fuerth": "Greuther Fürth",

    # === ITALIAN SERIE A ===
    "Inter": "Inter Milan",
    "Verona": "Hellas Verona",

    # === FRENCH LIGUE 1 ===
    "Paris Saint Germain": "Paris Saint-Germain",
    "Paris S-G": "Paris Saint-Germain",
    "PSG": "Paris Saint-Germain",
    "St Etienne": "Saint-Étienne",
    "Saint-Etienne": "Saint-Étienne",
    "Marseille": "Olympique Marseille",
    "Lyon": "Olympique Lyonnais",
    "Olympique Lyon": "Olympique Lyonnais",
    "Clermont": "Clermont Foot",
}

# League name fixes
LEAGUE_NAME_MAP = {
    "Italy_ Serie A": "Italian Serie A",
}


def _data_score(c, mid):
    """Score how much useful data a match record has (higher = keep it)."""
    c.execute("""SELECT
        (CASE WHEN event_id IS NOT NULL AND event_id > 0 THEN 10 ELSE 0 END) +
        (CASE WHEN kickoff_timestamp IS NOT NULL AND kickoff_timestamp > 0 THEN 5 ELSE 0 END) +
        (CASE WHEN home_score_ht IS NOT NULL AND home_score_ht >= 0 THEN 3 ELSE 0 END) +
        (CASE WHEN home_shots IS NOT NULL AND home_shots > 0 THEN 2 ELSE 0 END) +
        (CASE WHEN home_possession IS NOT NULL AND home_possession > 0 THEN 2 ELSE 0 END) +
        (CASE WHEN home_corners IS NOT NULL AND home_corners > 0 THEN 1 ELSE 0 END) +
        id  -- tiebreak: higher id = newer fetch = likely better
        as score
        FROM matches WHERE id = ?""", (mid,))
    row = c.fetchone()
    return row["score"] if row else -1


def _resolve_conflicts_for_rename(c, old_name, new_name, column):
    """Before renaming old_name->new_name in column, find and resolve UNIQUE conflicts."""
    other_col = "away_team" if column == "home_team" else "home_team"
    deleted = 0

    # Find rows that would conflict: old_name rows that have a matching new_name row
    # The UNIQUE is on (league, season, round, home_team, away_team)
    c.execute(f"""
        SELECT a.id as old_id, b.id as new_id, a.league, a.season, a.round,
               a.home_team as a_home, a.away_team as a_away,
               b.home_team as b_home, b.away_team as b_away
        FROM matches a
        JOIN matches b ON a.league = b.league AND a.season = b.season AND a.round = b.round
        WHERE a.{column} = ? AND b.{column} = ?
          AND a.{other_col} = b.{other_col}
    """, (old_name, new_name))

    conflicts = c.fetchall()
    for cf in conflicts:
        old_id = cf["old_id"]
        new_id = cf["new_id"]
        # Keep the one with better data
        score_old = _data_score(c, old_id)
        score_new = _data_score(c, new_id)
        delete_id = old_id if score_new >= score_old else new_id
        c.execute("DELETE FROM matches WHERE id = ?", (delete_id,))
        deleted += 1

    return deleted


def main():
    # 1. Backup
    if not os.path.exists(BACKUP_PATH):
        print(f"Creating backup: {BACKUP_PATH}")
        shutil.copy2(DB_PATH, BACKUP_PATH)
    else:
        print(f"Backup already exists: {BACKUP_PATH}")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    # 2. Fix league names
    print("\n--- FIXING LEAGUE NAMES ---")
    for old_name, new_name in LEAGUE_NAME_MAP.items():
        c.execute("SELECT COUNT(*) as cnt FROM matches WHERE league = ?", (old_name,))
        cnt = c.fetchone()["cnt"]
        if cnt > 0:
            c.execute("UPDATE matches SET league = ? WHERE league = ?", (new_name, old_name))
            print(f"  '{old_name}' -> '{new_name}': {cnt} matches updated")

    # 3. Fix team names (resolve conflicts first, then rename)
    print("\n--- FIXING TEAM NAMES ---")
    total_fixed = 0
    total_conflict_deleted = 0
    for old_name, new_name in TEAM_NAME_MAP.items():
        if old_name == new_name:
            continue

        # Resolve conflicts for home_team column
        del_h = _resolve_conflicts_for_rename(c, old_name, new_name, "home_team")
        total_conflict_deleted += del_h

        # Resolve conflicts for away_team column
        del_a = _resolve_conflicts_for_rename(c, old_name, new_name, "away_team")
        total_conflict_deleted += del_a

        # Now rename — should be safe
        c.execute("SELECT COUNT(*) as cnt FROM matches WHERE home_team = ?", (old_name,))
        cnt_h = c.fetchone()["cnt"]
        if cnt_h > 0:
            c.execute("UPDATE matches SET home_team = ? WHERE home_team = ?", (new_name, old_name))
            total_fixed += cnt_h

        c.execute("SELECT COUNT(*) as cnt FROM matches WHERE away_team = ?", (old_name,))
        cnt_a = c.fetchone()["cnt"]
        if cnt_a > 0:
            c.execute("UPDATE matches SET away_team = ? WHERE away_team = ?", (new_name, old_name))
            total_fixed += cnt_a

        if cnt_h + cnt_a > 0 or del_h + del_a > 0:
            print(f"  '{old_name}' -> '{new_name}': renamed {cnt_h}h+{cnt_a}a, resolved {del_h+del_a} conflicts")

    print(f"  Total renames: {total_fixed}")
    print(f"  Total conflict duplicates deleted: {total_conflict_deleted}")

    # 4. Remove remaining duplicates (same match, same score, different records)
    print("\n--- REMOVING REMAINING DUPLICATES ---")
    c.execute("""
        SELECT league, season, home_team, away_team, home_score, away_score,
               COUNT(*) as cnt, GROUP_CONCAT(id, ',') as ids
        FROM matches WHERE home_score >= 0
        GROUP BY league, season, home_team, away_team, home_score, away_score
        HAVING cnt > 1
    """)
    dupes = c.fetchall()
    removed = 0
    for d in dupes:
        ids = [int(x) for x in d["ids"].split(",")]
        best_id = max(ids, key=lambda mid: _data_score(c, mid))
        to_delete = [x for x in ids if x != best_id]
        if to_delete:
            c.execute(f"DELETE FROM matches WHERE id IN ({','.join('?' * len(to_delete))})", to_delete)
            removed += len(to_delete)
    print(f"  Removed {removed} duplicate completed matches")

    # Upcoming duplicates
    c.execute("""
        SELECT league, season, home_team, away_team,
               COUNT(*) as cnt, GROUP_CONCAT(id, ',') as ids
        FROM matches WHERE home_score < 0
        GROUP BY league, season, home_team, away_team
        HAVING cnt > 1
    """)
    dupes_up = c.fetchall()
    removed_up = 0
    for d in dupes_up:
        ids = [int(x) for x in d["ids"].split(",")]
        best_id = max(ids, key=lambda mid: _data_score(c, mid))
        to_delete = [x for x in ids if x != best_id]
        if to_delete:
            c.execute(f"DELETE FROM matches WHERE id IN ({','.join('?' * len(to_delete))})", to_delete)
            removed_up += len(to_delete)
    print(f"  Removed {removed_up} duplicate upcoming matches")

    conn.commit()

    # 5. Post-fix stats
    print("\n" + "=" * 80)
    print("POST-FIX STATS")
    print("=" * 80)
    c.execute("SELECT COUNT(*) as cnt FROM matches")
    print(f"  Total matches: {c.fetchone()['cnt']}")

    c.execute("""SELECT league, season, COUNT(*) as total,
                 SUM(CASE WHEN home_score >= 0 THEN 1 ELSE 0 END) as completed
                 FROM matches GROUP BY league, season ORDER BY league, season""")
    print(f"\n  {'League':40s} {'Season':12s} {'Total':>6s} {'Done':>6s}")
    print("  " + "-" * 66)
    for r in c.fetchall():
        print(f"  {r['league']:40s} {r['season']:12s} {r['total']:>6d} {r['completed']:>6d}")

    print(f"\n  --- UNIQUE TEAMS PER LEAGUE (after fix) ---")
    c.execute("""
        SELECT league, COUNT(DISTINCT team) as cnt FROM (
            SELECT league, home_team as team FROM matches
            UNION
            SELECT league, away_team as team FROM matches
        ) GROUP BY league ORDER BY league
    """)
    for r in c.fetchall():
        print(f"  {r['league']:40s} {r['cnt']:>4d} teams")

    conn.close()
    print("\nDone!")


if __name__ == "__main__":
    main()
