"""
Enhanced analysis engine v2.
Provides deep match-level analysis with all available statistics:
- Home/away form (separate)
- Goals scored/conceded trends
- First-half vs second-half performance
- Shots, corners, cards frequency
- Clean sheets
- Over/under tendencies
- Late-goal patterns
- Head-to-head history
- League position & momentum
- Anomaly detection
"""
import sqlite3
import math
import os
from collections import defaultdict

import numpy as np

_here = os.path.dirname(os.path.abspath(__file__))
_parent = os.path.dirname(_here)
DB_PATH = os.path.join(_here, "soccer.db") if os.path.exists(os.path.join(_here, "soccer.db")) else os.path.join(_parent, "soccer.db")


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def safe_div(a, b, default=0.0):
    return a / b if b else default


def poisson_prob(lam, k):
    return (math.exp(-lam) * (lam ** k)) / math.factorial(k)


# ---------------------------------------------------------------------------
# FORM ANALYSIS (home-only / away-only / overall)
# ---------------------------------------------------------------------------
def _form_from_rows(rows, team_name):
    """Compute form stats from a list of match rows for a team."""
    points, goals_for, goals_against = 0, 0, 0
    results = []
    clean_sheets = 0
    first_half_goals_for, first_half_goals_against = 0, 0
    second_half_goals_for, second_half_goals_against = 0, 0
    total_cards = 0
    total_corners = 0
    total_shots = 0
    total_shots_on_target = 0
    total_possession = 0.0
    possession_count = 0
    scored_first_count = 0
    conceded_first_count = 0
    over_2_5_count = 0
    btts_count = 0

    for row in rows:
        is_home = row["home_team"] == team_name
        gf = row["home_score"] if is_home else row["away_score"]
        ga = row["away_score"] if is_home else row["home_score"]

        if gf is None or ga is None or gf < 0 or ga < 0:
            continue

        goals_for += gf
        goals_against += ga

        if gf > ga:
            points += 3
            results.append("W")
        elif gf == ga:
            points += 1
            results.append("D")
        else:
            results.append("L")

        if ga == 0:
            clean_sheets += 1

        # Halftime splits — only if HT data actually exists
        has_ht = (row["home_score_ht"] or 0) > 0 or (row["away_score_ht"] or 0) > 0
        if has_ht:
            ht_gf = (row["home_score_ht"] if is_home else row["away_score_ht"]) or 0
            ht_ga = (row["away_score_ht"] if is_home else row["home_score_ht"]) or 0
            first_half_goals_for += ht_gf
            first_half_goals_against += ht_ga
            second_half_goals_for += (gf - ht_gf)
            second_half_goals_against += (ga - ht_ga)

        # Cards
        cards = (row["home_yellow_cards"] if is_home else row["away_yellow_cards"]) or 0
        cards += ((row["home_red_cards"] if is_home else row["away_red_cards"]) or 0)
        total_cards += cards

        # Corners
        corners = (row["home_corners"] if is_home else row["away_corners"]) or 0
        total_corners += corners

        # Shots
        shots = (row["home_shots"] if is_home else row["away_shots"]) or 0
        sot = (row["home_shots_on_target"] if is_home else row["away_shots_on_target"]) or 0
        total_shots += shots
        total_shots_on_target += sot

        # Possession
        poss = (row["home_possession"] if is_home else row["away_possession"])
        if poss and poss > 0:
            total_possession += poss
            possession_count += 1

        # Over 2.5 and BTTS
        if gf + ga > 2.5:
            over_2_5_count += 1
        if gf > 0 and ga > 0:
            btts_count += 1

    n = len([r for r in results])
    results.reverse()  # oldest -> newest

    return {
        "matches_played": n,
        "points": points,
        "max_points": n * 3,
        "percentage": round(safe_div(points, n * 3) * 100, 1),
        "streak": results,
        "wins": results.count("W"),
        "draws": results.count("D"),
        "losses": results.count("L"),
        "goals_for": goals_for,
        "goals_against": goals_against,
        "avg_goals_for": round(safe_div(goals_for, n), 2),
        "avg_goals_against": round(safe_div(goals_against, n), 2),
        "clean_sheets": clean_sheets,
        "clean_sheet_pct": round(safe_div(clean_sheets, n) * 100, 1),
        "first_half_goals_for": first_half_goals_for,
        "first_half_goals_against": first_half_goals_against,
        "second_half_goals_for": second_half_goals_for,
        "second_half_goals_against": second_half_goals_against,
        "avg_cards": round(safe_div(total_cards, n), 2),
        "avg_corners": round(safe_div(total_corners, n), 2),
        "avg_shots": round(safe_div(total_shots, n), 1),
        "avg_shots_on_target": round(safe_div(total_shots_on_target, n), 1),
        "avg_possession": round(safe_div(total_possession, possession_count), 1) if possession_count else None,
        "over_2_5_pct": round(safe_div(over_2_5_count, n) * 100, 1),
        "btts_pct": round(safe_div(btts_count, n) * 100, 1),
    }


def get_team_form(team_name, limit=10, venue=None):
    """
    Get form for a team.
    venue: None=all, 'home'=home only, 'away'=away only
    """
    conn = get_db()
    cursor = conn.cursor()
    if venue == "home":
        cursor.execute("SELECT * FROM matches WHERE home_team = ? AND home_score >= 0 ORDER BY COALESCE(kickoff_timestamp, id) DESC LIMIT ?",
                        (team_name, limit))
    elif venue == "away":
        cursor.execute("SELECT * FROM matches WHERE away_team = ? AND away_score >= 0 ORDER BY COALESCE(kickoff_timestamp, id) DESC LIMIT ?",
                        (team_name, limit))
    else:
        cursor.execute("""SELECT * FROM matches WHERE (home_team = ? OR away_team = ?)
                          AND home_score >= 0 ORDER BY COALESCE(kickoff_timestamp, id) DESC LIMIT ?""",
                        (team_name, team_name, limit))
    rows = cursor.fetchall()
    conn.close()
    return _form_from_rows(rows, team_name)


# ---------------------------------------------------------------------------
# GOAL TRENDS
# ---------------------------------------------------------------------------
def get_goal_trends(team_name, limit=20):
    """Analyze scoring/conceding patterns over recent matches."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""SELECT * FROM matches WHERE (home_team = ? OR away_team = ?)
                      AND home_score >= 0 ORDER BY COALESCE(kickoff_timestamp, id) DESC LIMIT ?""",
                    (team_name, team_name, limit))
    rows = cursor.fetchall()
    conn.close()

    match_goals = []
    for row in rows:
        is_home = row["home_team"] == team_name
        gf = row["home_score"] if is_home else row["away_score"]
        ga = row["away_score"] if is_home else row["home_score"]
        ht_gf = (row["home_score_ht"] if is_home else row["away_score_ht"]) or 0
        ht_ga = (row["away_score_ht"] if is_home else row["home_score_ht"]) or 0
        match_goals.append({
            "opponent": row["away_team"] if is_home else row["home_team"],
            "venue": "H" if is_home else "A",
            "gf": gf, "ga": ga,
            "ht_gf": ht_gf, "ht_ga": ht_ga,
            "total": gf + ga,
        })

    match_goals.reverse()

    # Detect streaks
    scoring_streak = 0
    for mg in reversed(match_goals):
        if mg["gf"] > 0:
            scoring_streak += 1
        else:
            break

    conceding_streak = 0
    for mg in reversed(match_goals):
        if mg["ga"] > 0:
            conceding_streak += 1
        else:
            break

    clean_sheet_streak = 0
    for mg in reversed(match_goals):
        if mg["ga"] == 0:
            clean_sheet_streak += 1
        else:
            break

    return {
        "matches": match_goals,
        "scoring_streak": scoring_streak,
        "conceding_streak": conceding_streak,
        "clean_sheet_streak": clean_sheet_streak,
    }


# ---------------------------------------------------------------------------
# HEAD-TO-HEAD (enhanced)
# ---------------------------------------------------------------------------
def get_h2h(team_a, team_b, limit=10):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT * FROM matches
        WHERE ((home_team = ? AND away_team = ?) OR (home_team = ? AND away_team = ?))
          AND home_score >= 0 AND away_score >= 0
        ORDER BY COALESCE(kickoff_timestamp, id) DESC LIMIT ?
    """, (team_a, team_b, team_b, team_a, limit))
    rows = cursor.fetchall()
    conn.close()

    a_wins, b_wins, draws = 0, 0, 0
    a_goals, b_goals = 0, 0
    over_2_5, btts = 0, 0
    meetings = []

    for row in rows:
        is_a_home = row["home_team"] == team_a
        sa = row["home_score"] if is_a_home else row["away_score"]
        sb = row["away_score"] if is_a_home else row["home_score"]
        if sa is None or sb is None:
            continue

        a_goals += sa
        b_goals += sb
        if sa > sb:
            a_wins += 1
        elif sb > sa:
            b_wins += 1
        else:
            draws += 1

        if sa + sb > 2.5:
            over_2_5 += 1
        if sa > 0 and sb > 0:
            btts += 1

        meetings.append({
            "home": row["home_team"],
            "away": row["away_team"],
            "home_score": row["home_score"],
            "away_score": row["away_score"],
            "league": row["league"],
            "season": row["season"],
        })

    n = len(meetings)
    return {
        "total_matches": n,
        f"{team_a}_wins": a_wins,
        f"{team_b}_wins": b_wins,
        "draws": draws,
        f"{team_a}_goals": a_goals,
        f"{team_b}_goals": b_goals,
        "avg_goals": round(safe_div(a_goals + b_goals, n), 2),
        "over_2_5_pct": round(safe_div(over_2_5, n) * 100, 1),
        "btts_pct": round(safe_div(btts, n) * 100, 1),
        "meetings": meetings,
    }


# ---------------------------------------------------------------------------
# LEAGUE TABLE (approx from DB data for a league+season)
# ---------------------------------------------------------------------------
def get_league_table(league, season=None):
    """Build a simple league table from match results."""
    conn = get_db()
    cursor = conn.cursor()
    if season:
        cursor.execute("SELECT * FROM matches WHERE league = ? AND season = ? AND home_score >= 0",
                        (league, season))
    else:
        # Latest season
        cursor.execute("SELECT DISTINCT season FROM matches WHERE league = ? ORDER BY season DESC LIMIT 1",
                        (league,))
        row = cursor.fetchone()
        if not row:
            conn.close()
            return []
        season = row["season"]
        cursor.execute("SELECT * FROM matches WHERE league = ? AND season = ? AND home_score >= 0",
                        (league, season))

    rows = cursor.fetchall()
    conn.close()

    table = defaultdict(lambda: {"played": 0, "won": 0, "drawn": 0, "lost": 0,
                                  "gf": 0, "ga": 0, "points": 0})
    for row in rows:
        h, a = row["home_team"], row["away_team"]
        hs, as_ = row["home_score"], row["away_score"]
        if hs is None or as_ is None:
            continue
        for team, gf, ga in [(h, hs, as_), (a, as_, hs)]:
            t = table[team]
            t["played"] += 1
            t["gf"] += gf
            t["ga"] += ga
            if gf > ga:
                t["won"] += 1
                t["points"] += 3
            elif gf == ga:
                t["drawn"] += 1
                t["points"] += 1
            else:
                t["lost"] += 1

    sorted_table = sorted(table.items(),
                          key=lambda x: (x[1]["points"], x[1]["gf"] - x[1]["ga"], x[1]["gf"]),
                          reverse=True)
    result = []
    for pos, (team, s) in enumerate(sorted_table, 1):
        s["team"] = team
        s["position"] = pos
        s["gd"] = s["gf"] - s["ga"]
        result.append(s)
    return result


# ---------------------------------------------------------------------------
# PATTERN DETECTION
# ---------------------------------------------------------------------------
def detect_patterns(team_name, limit=15):
    """Auto-detect interesting patterns and anomalies."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""SELECT * FROM matches WHERE (home_team = ? OR away_team = ?)
                      AND home_score >= 0 ORDER BY COALESCE(kickoff_timestamp, id) DESC LIMIT ?""",
                    (team_name, team_name, limit))
    rows = cursor.fetchall()
    conn.close()

    patterns = []
    home_wins, away_wins = 0, 0
    first_half_heavy, second_half_heavy = 0, 0
    high_card_matches = 0
    high_corner_matches = 0
    late_goal_indicator = 0  # 2nd half goals > 1st half goals

    for row in rows:
        is_home = row["home_team"] == team_name
        gf = row["home_score"] if is_home else row["away_score"]
        ga = row["away_score"] if is_home else row["home_score"]
        ht_gf = (row["home_score_ht"] if is_home else row["away_score_ht"]) or 0
        ht_ga = (row["away_score_ht"] if is_home else row["home_score_ht"]) or 0
        sh_gf = gf - ht_gf
        sh_ga = ga - ht_ga

        if gf > ga:
            if is_home:
                home_wins += 1
            else:
                away_wins += 1

        # Only analyze half splits if halftime data actually exists (not all zeros)
        has_ht_data = (row["home_score_ht"] or 0) > 0 or (row["away_score_ht"] or 0) > 0
        if has_ht_data:
            if (ht_gf + ht_ga) > (sh_gf + sh_ga):
                first_half_heavy += 1
            elif (sh_gf + sh_ga) > (ht_gf + ht_ga):
                second_half_heavy += 1
                late_goal_indicator += 1

        cards = ((row["home_yellow_cards"] if is_home else row["away_yellow_cards"]) or 0) + \
                ((row["home_red_cards"] if is_home else row["away_red_cards"]) or 0)
        opp_cards = ((row["away_yellow_cards"] if is_home else row["home_yellow_cards"]) or 0) + \
                    ((row["away_red_cards"] if is_home else row["home_red_cards"]) or 0)
        if cards + opp_cards >= 5:
            high_card_matches += 1

        corners = ((row["home_corners"] if is_home else row["away_corners"]) or 0)
        if corners >= 6:
            high_corner_matches += 1

    n = len(rows)
    if n == 0:
        return patterns

    # Home/away asymmetry
    home_matches = sum(1 for r in rows if r["home_team"] == team_name)
    away_matches = n - home_matches
    if home_wins > 0 and away_wins == 0 and away_matches >= 3:
        patterns.append({"type": "warning", "text": f"No away wins in last {away_matches} away matches"})
    if away_wins > home_wins and home_matches >= 3:
        patterns.append({"type": "info", "text": f"Better away ({away_wins}W) than home ({home_wins}W) recently"})

    # Late goals
    if safe_div(second_half_heavy, n) >= 0.6:
        patterns.append({"type": "trend", "text": f"Second-half heavy: {second_half_heavy}/{n} matches have more goals after HT"})

    # Card-heavy
    if safe_div(high_card_matches, n) >= 0.5:
        patterns.append({"type": "warning", "text": f"Card-heavy matches: {high_card_matches}/{n} had 5+ total cards"})

    # Corner dominant
    if safe_div(high_corner_matches, n) >= 0.5:
        patterns.append({"type": "info", "text": f"Corner dominant: {high_corner_matches}/{n} matches with 6+ corners"})

    # Scoring streak detection
    streak = 0
    for row in rows:
        is_home = row["home_team"] == team_name
        gf = row["home_score"] if is_home else row["away_score"]
        if gf > 0:
            streak += 1
        else:
            break
    if streak >= 5:
        patterns.append({"type": "hot", "text": f"Scored in last {streak} consecutive matches"})

    # Defensive collapse detection
    conceded_3plus = sum(1 for row in rows
                         if ((row["away_score"] if row["home_team"] == team_name else row["home_score"]) or 0) >= 3)
    if conceded_3plus >= 3:
        patterns.append({"type": "warning", "text": f"Defensive concerns: conceded 3+ goals in {conceded_3plus}/{n} recent matches"})

    return patterns


# ---------------------------------------------------------------------------
# POISSON xG + PROBABILITIES
# ---------------------------------------------------------------------------
def calculate_xg_and_probabilities(home_team, away_team, league):
    """Poisson-based expected goals and match outcome probabilities."""
    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM matches WHERE league = ? AND home_score >= 0 ORDER BY COALESCE(kickoff_timestamp, id) DESC LIMIT 300",
                    (league,))
    league_matches = cursor.fetchall()
    if not league_matches:
        conn.close()
        return None

    total_hg = sum(r["home_score"] for r in league_matches)
    total_ag = sum(r["away_score"] for r in league_matches)
    n = len(league_matches)
    avg_hg = total_hg / n
    avg_ag = total_ag / n

    # Home team home matches
    cursor.execute("SELECT * FROM matches WHERE home_team = ? AND home_score >= 0 ORDER BY COALESCE(kickoff_timestamp, id) DESC LIMIT 20",
                    (home_team,))
    hm = cursor.fetchall()
    # Away team away matches
    cursor.execute("SELECT * FROM matches WHERE away_team = ? AND away_score >= 0 ORDER BY COALESCE(kickoff_timestamp, id) DESC LIMIT 20",
                    (away_team,))
    am = cursor.fetchall()
    conn.close()

    if not hm:
        h_att, h_def = 1.0, 1.0
    else:
        h_att = safe_div(sum(r["home_score"] for r in hm) / len(hm), avg_hg, 1.0)
        h_def = safe_div(sum(r["away_score"] for r in hm) / len(hm), avg_ag, 1.0)

    if not am:
        a_att, a_def = 1.0, 1.0
    else:
        a_att = safe_div(sum(r["away_score"] for r in am) / len(am), avg_ag, 1.0)
        a_def = safe_div(sum(r["home_score"] for r in am) / len(am), avg_hg, 1.0)

    home_xg = h_att * a_def * avg_hg
    away_xg = a_att * h_def * avg_ag

    # Poisson matrix
    probs = {"home_win": 0, "draw": 0, "away_win": 0,
             "over_1_5": 0, "over_2_5": 0, "over_3_5": 0, "btts": 0}
    score_matrix = []
    for i in range(7):
        row = []
        for j in range(7):
            p = poisson_prob(home_xg, i) * poisson_prob(away_xg, j)
            row.append(round(p * 100, 2))
            if i > j: probs["home_win"] += p
            elif i == j: probs["draw"] += p
            else: probs["away_win"] += p
            if i + j > 1.5: probs["over_1_5"] += p
            if i + j > 2.5: probs["over_2_5"] += p
            if i + j > 3.5: probs["over_3_5"] += p
            if i > 0 and j > 0: probs["btts"] += p
        score_matrix.append(row)

    for k in probs:
        probs[k] = round(probs[k] * 100, 2)

    return {
        "home_xg": round(home_xg, 2),
        "away_xg": round(away_xg, 2),
        "home_attack_strength": round(h_att, 2),
        "home_defense_strength": round(h_def, 2),
        "away_attack_strength": round(a_att, 2),
        "away_defense_strength": round(a_def, 2),
        "probabilities": probs,
        "score_matrix": score_matrix,
    }


# ---------------------------------------------------------------------------
# FULL MATCH ANALYSIS (the main entry point for deep analysis)
# ---------------------------------------------------------------------------
def generate_full_analysis(home_team, away_team, league):
    """
    Generate a comprehensive match analysis combining all modules.
    This is the main function called by the API for match-level deep analysis.
    """
    # Forms
    home_overall = get_team_form(home_team, limit=10)
    home_home = get_team_form(home_team, limit=10, venue="home")
    away_overall = get_team_form(away_team, limit=10)
    away_away = get_team_form(away_team, limit=10, venue="away")

    # Goal trends
    home_trends = get_goal_trends(home_team, limit=15)
    away_trends = get_goal_trends(away_team, limit=15)

    # H2H
    h2h = get_h2h(home_team, away_team, limit=10)

    # xG + probabilities
    xg = calculate_xg_and_probabilities(home_team, away_team, league)

    # League table position
    table = get_league_table(league)
    home_pos = next((t for t in table if t["team"] == home_team), None)
    away_pos = next((t for t in table if t["team"] == away_team), None)

    # Pattern detection
    home_patterns = detect_patterns(home_team, limit=15)
    away_patterns = detect_patterns(away_team, limit=15)

    return {
        "home_team": home_team,
        "away_team": away_team,
        "league": league,
        "form": {
            "home_overall": home_overall,
            "home_at_home": home_home,
            "away_overall": away_overall,
            "away_at_away": away_away,
        },
        "goal_trends": {
            "home": home_trends,
            "away": away_trends,
        },
        "head_to_head": h2h,
        "expected_goals": xg,
        "league_position": {
            "home": home_pos,
            "away": away_pos,
        },
        "patterns": {
            "home": home_patterns,
            "away": away_patterns,
        },
    }
