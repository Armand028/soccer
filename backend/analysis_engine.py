import sqlite3
import math
from collections import defaultdict
import numpy as np
from datetime import datetime

import os
DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "soccer.db")

def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def convert_date_to_timestamp(date_str):
    """
    Tries to convert a date string (like '23.05. 11:00' or epoch) into a sortable format.
    If it's an epoch string from the API, integerise it.
    If it's 'DD.MM. HH:MM', we guess the year or just leave it since the text files are grouped by season/round anyway.
    For simplicity in standard sorting, we'll return a rough sortable key, or rely on id/round.
    """
    try:
        # Check if it's epoch
        if len(date_str) > 8 and date_str.isdigit():
            return int(date_str)
        # Check if text format
        parts = date_str.split('.')
        if len(parts) >= 2:
            day = parts[0].strip()
            month = parts[1].strip()
            # return a rough MM-DD key for sorting within a season (Note: this is brittle across year boundaries)
            return int(month) * 100 + int(day)
    except:
        pass
    return 0

def calculate_moment_form(team_name, limit=5):
    """
    Calculates the recent form of a team based on the last `limit` matches.
    Form is assigned points: Win = 3, Draw = 1, Loss = 0.
    Returns:
        form_points: Total points in the last N matches
        max_points: Maximum possible points (N * 3)
        form_percent: Percentage form (0.0 to 1.0)
        recent_matches: List of result statuses ['W', 'D', 'L', 'W', 'W']
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # We fetch the last N matches involving the team
    # Ordering by id DESC acts as a proxy for date since they were inserted mostly chronologically
    cursor.execute('''
    SELECT * FROM matches 
    WHERE (home_team = ? OR away_team = ?) AND home_score >= 0 AND away_score >= 0
    ORDER BY COALESCE(kickoff_timestamp, id) DESC LIMIT ?
    ''', (team_name, team_name, limit))
    
    matches = cursor.fetchall()
    
    points = 0
    results = []
    
    for row in matches:
        is_home = row['home_team'] == team_name
        team_score = row['home_score'] if is_home else row['away_score']
        opp_score = row['away_score'] if is_home else row['home_score']
        
        if team_score > opp_score:
            points += 3
            results.append('W')
        elif team_score == opp_score:
            points += 1
            results.append('D')
        else:
            results.append('L')
            
    # The list is newest -> oldest, let's reverse to oldest -> newest for standard reading
    results.reverse()
    
    max_pts = len(matches) * 3
    form_pct = (points / max_pts) if max_pts > 0 else 0.0
    
    conn.close()
    return {
        "points": points,
        "max_points": max_pts,
        "percentage": round(form_pct * 100, 2),
        "streak": results,
        "matches_played": len(matches)
    }

def calculate_h2h(team_a, team_b):
    """
    Calculates Head-to-Head statistics between two specific teams historically.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
    SELECT * FROM matches 
    WHERE ((home_team = ? AND away_team = ?) 
       OR (home_team = ? AND away_team = ?))
       AND home_score >= 0 AND away_score >= 0
    ORDER BY COALESCE(kickoff_timestamp, id) DESC
    ''', (team_a, team_b, team_b, team_a))
    
    matches = cursor.fetchall()
    conn.close()
    
    stats = {
        "total_matches": len(matches),
        f"{team_a}_wins": 0,
        f"{team_b}_wins": 0,
        "draws": 0,
        f"{team_a}_goals": 0,
        f"{team_b}_goals": 0,
        "avg_goals_per_match": 0.0,
        "recent_meetings": []
    }
    
    for row in matches:
        is_a_home = row['home_team'] == team_a
        score_a = row['home_score'] if is_a_home else row['away_score']
        score_b = row['away_score'] if is_a_home else row['home_score']
        
        stats[f"{team_a}_goals"] += score_a
        stats[f"{team_b}_goals"] += score_b
        
        if score_a > score_b:
            stats[f"{team_a}_wins"] += 1
            winner = team_a
        elif score_b > score_a:
            stats[f"{team_b}_wins"] += 1
            winner = team_b
        else:
            stats["draws"] += 1
            winner = "Draw"
            
        # Collect the last 5 results
        if len(stats["recent_meetings"]) < 5:
            stats["recent_meetings"].append(f"{row['home_team']} {row['home_score']} - {row['away_score']} {row['away_team']}")

    if len(matches) > 0:
        stats["avg_goals_per_match"] = round((stats[f"{team_a}_goals"] + stats[f"{team_b}_goals"]) / len(matches), 2)
        
    return stats

def poisson_probability(lambda_val, k):
    """
    Calculates the Poisson probability of exactly k events occurring
    given the expected average lambda_val.
    """
    return (math.exp(-lambda_val) * (lambda_val ** k)) / math.factorial(k)

def calculate_goal_expectancy(home_team, away_team, league_name):
    """
    A foundational model using Poisson distribution logic.
    Calculates the attacking and defensive strengths of both teams 
    relative to the league average, to predict Expected Goals (xG).
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Get all matches in this league to establish the league average
    # We'll limit to the last 300 matches (roughly one season) for recent relevance
    cursor.execute('''
    SELECT * FROM matches WHERE league = ? AND home_score >= 0 AND away_score >= 0 ORDER BY COALESCE(kickoff_timestamp, id) DESC LIMIT 300
    ''', (league_name,))
    league_matches = cursor.fetchall()
    
    if not league_matches:
        conn.close()
        return None
        
    total_home_goals = sum([row['home_score'] for row in league_matches])
    total_away_goals = sum([row['away_score'] for row in league_matches])
    total_games = len(league_matches)
    
    avg_home_goals_scored = total_home_goals / total_games
    avg_away_goals_scored = total_away_goals / total_games
    # Because one team's goals scored equals the other's goals conceded:
    avg_home_goals_conceded = avg_away_goals_scored 
    avg_away_goals_conceded = avg_home_goals_scored
    
    # Calculate Home Team Metrics
    cursor.execute('''SELECT * FROM matches WHERE home_team = ? AND home_score >= 0 ORDER BY COALESCE(kickoff_timestamp, id) DESC LIMIT 20''', (home_team,))
    home_matches = cursor.fetchall()
    if len(home_matches) == 0:
        home_attack_strength = 1.0
        home_defense_strength = 1.0
    else:
        home_goals_scored = sum([row['home_score'] for row in home_matches]) / len(home_matches)
        home_goals_conceded = sum([row['away_score'] for row in home_matches]) / len(home_matches)
        
        home_attack_strength = home_goals_scored / max(avg_home_goals_scored, 0.1)
        home_defense_strength = home_goals_conceded / max(avg_home_goals_conceded, 0.1)

    # Calculate Away Team Metrics
    cursor.execute('''SELECT * FROM matches WHERE away_team = ? AND away_score >= 0 ORDER BY COALESCE(kickoff_timestamp, id) DESC LIMIT 20''', (away_team,))
    away_matches = cursor.fetchall()
    if len(away_matches) == 0:
        away_attack_strength = 1.0
        away_defense_strength = 1.0
    else:
        away_goals_scored = sum([row['away_score'] for row in away_matches]) / len(away_matches)
        away_goals_conceded = sum([row['home_score'] for row in away_matches]) / len(away_matches)
        
        away_attack_strength = away_goals_scored / max(avg_away_goals_scored, 0.1)
        away_defense_strength = away_goals_conceded / max(avg_away_goals_conceded, 0.1)
        
    conn.close()
    
    # Calculate Expected Goals for this match
    home_xg = home_attack_strength * away_defense_strength * avg_home_goals_scored
    away_xg = away_attack_strength * home_defense_strength * avg_away_goals_scored
    
    return {
        "home_xg": round(home_xg, 2),
        "away_xg": round(away_xg, 2),
        "home_attack_strength": round(home_attack_strength, 2),
        "home_defense_strength": round(home_defense_strength, 2),
        "away_attack_strength": round(away_attack_strength, 2),
        "away_defense_strength": round(away_defense_strength, 2)
    }

def generate_match_prediction(home_team, away_team, league_name):
    """
    Combines Moment Form, H2H, and Poisson xG into a single analysis block.
    """
    home_form = calculate_moment_form(home_team)
    away_form = calculate_moment_form(away_team)
    h2h = calculate_h2h(home_team, away_team)
    xg_stats = calculate_goal_expectancy(home_team, away_team, league_name)
    
    prediction = {
        "home_team": home_team,
        "away_team": away_team,
        "league": league_name,
        "form": {
            "home": home_form,
            "away": away_form
        },
        "head_to_head": h2h,
        "expected_goals": xg_stats
    }
    
    # Generate Probabilities using Poisson Matrix
    probabilities = {
        "home_win": 0.0,
        "draw": 0.0,
        "away_win": 0.0,
        "over_1_5": 0.0,
        "over_2_5": 0.0,
        "btts": 0.0 # Both Teams To Score
    }
    
    if xg_stats:
        home_max_goals = 6
        away_max_goals = 6
        
        for i in range(home_max_goals):
            for j in range(away_max_goals):
                prob = poisson_probability(xg_stats['home_xg'], i) * poisson_probability(xg_stats['away_xg'], j)
                
                if i > j:
                    probabilities["home_win"] += prob
                elif i == j:
                    probabilities["draw"] += prob
                else:
                    probabilities["away_win"] += prob
                    
                total_goals = i + j
                if total_goals > 1.5:
                    probabilities["over_1_5"] += prob
                if total_goals > 2.5:
                    probabilities["over_2_5"] += prob
                    
                if i > 0 and j > 0:
                    probabilities["btts"] += prob
                    
    # Format probabilities as percentages
    for key in probabilities:
        probabilities[key] = round(probabilities[key] * 100, 2)
        
    prediction["probabilities"] = probabilities
    
    return prediction

if __name__ == "__main__":
    import json
    # Example Test Run
    test_result = generate_match_prediction("Chelsea", "Arsenal", "English Premier League")
    print(json.dumps(test_result, indent=2))
