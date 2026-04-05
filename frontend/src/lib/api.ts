const API_BASE = process.env.NEXT_PUBLIC_API_URL
  ? `${process.env.NEXT_PUBLIC_API_URL}/api`
  : "/api";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
async function apiGet<T>(path: string): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`);
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(err.detail || `Request failed: ${path}`);
  }
  return res.json();
}

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------
export interface Match {
  id: number;
  league: string;
  season: string;
  round: string;
  date: string;
  home_team: string;
  away_team: string;
  home_score: number;
  away_score: number;
  event_id?: number;
  status?: string;
  kickoff_timestamp?: number;
  home_score_ht?: number | null;
  away_score_ht?: number | null;
  home_yellow_cards?: number;
  away_yellow_cards?: number;
  home_red_cards?: number;
  away_red_cards?: number;
  home_corners?: number;
  away_corners?: number;
  home_shots?: number;
  away_shots?: number;
  home_shots_on_target?: number;
  away_shots_on_target?: number;
  home_possession?: number | null;
  away_possession?: number | null;
  home_fouls?: number;
  away_fouls?: number;
  kickoff_ottawa?: string | null;
}

export interface Stats {
  total_matches: number;
  total_leagues: number;
  total_teams: number;
}

export interface FormData {
  matches_played: number;
  points: number;
  max_points: number;
  percentage: number;
  streak: string[];
  wins: number;
  draws: number;
  losses: number;
  goals_for: number;
  goals_against: number;
  avg_goals_for: number;
  avg_goals_against: number;
  clean_sheets: number;
  clean_sheet_pct: number;
  first_half_goals_for: number;
  first_half_goals_against: number;
  second_half_goals_for: number;
  second_half_goals_against: number;
  avg_cards: number;
  avg_corners: number;
  avg_shots: number;
  avg_shots_on_target: number;
  avg_possession: number | null;
  over_2_5_pct: number;
  btts_pct: number;
}

export interface H2HMeeting {
  home: string;
  away: string;
  home_score: number;
  away_score: number;
  league: string;
  season: string;
}

export interface H2HData {
  total_matches: number;
  draws: number;
  avg_goals: number;
  over_2_5_pct: number;
  btts_pct: number;
  meetings: H2HMeeting[];
  [key: string]: unknown;
}

export interface Probabilities {
  home_win: number;
  draw: number;
  away_win: number;
  over_1_5: number;
  over_2_5: number;
  over_3_5?: number;
  btts: number;
}

export interface XGData {
  home_xg: number;
  away_xg: number;
  home_attack_strength: number;
  home_defense_strength: number;
  away_attack_strength: number;
  away_defense_strength: number;
  probabilities: Probabilities;
  score_matrix: number[][];
}

export interface GoalTrendMatch {
  opponent: string;
  venue: string;
  gf: number;
  ga: number;
  ht_gf: number;
  ht_ga: number;
  total: number;
}

export interface GoalTrends {
  matches: GoalTrendMatch[];
  scoring_streak: number;
  conceding_streak: number;
  clean_sheet_streak: number;
}

export interface Pattern {
  type: string;
  text: string;
}

export interface LeagueTableEntry {
  team: string;
  position: number;
  played: number;
  won: number;
  drawn: number;
  lost: number;
  gf: number;
  ga: number;
  gd: number;
  points: number;
}

export interface FullAnalysis {
  home_team: string;
  away_team: string;
  league: string;
  match?: Match;
  form: {
    home_overall: FormData;
    home_at_home: FormData;
    away_overall: FormData;
    away_at_away: FormData;
  };
  goal_trends: {
    home: GoalTrends;
    away: GoalTrends;
  };
  head_to_head: H2HData;
  expected_goals: XGData | null;
  league_position: {
    home: LeagueTableEntry | null;
    away: LeagueTableEntry | null;
  };
  patterns: {
    home: Pattern[];
    away: Pattern[];
  };
}

export interface Prediction {
  home_team: string;
  away_team: string;
  league: string;
  form: { home: FormData; away: FormData };
  head_to_head: H2HData;
  expected_goals: XGData;
  probabilities: Probabilities;
}

// ---------------------------------------------------------------------------
// API Functions
// ---------------------------------------------------------------------------
export async function fetchLeagues(): Promise<string[]> {
  const data = await apiGet<{ leagues: string[] }>("/leagues");
  return data.leagues;
}

export async function fetchTeams(league?: string): Promise<string[]> {
  const params = league ? `?league=${encodeURIComponent(league)}` : "";
  const data = await apiGet<{ teams: string[] }>(`/teams${params}`);
  return data.teams;
}

export async function fetchStats(): Promise<Stats> {
  return apiGet<Stats>("/stats");
}

export async function fetchRecentMatches(limit = 50, league?: string): Promise<Match[]> {
  const params = new URLSearchParams({ limit: String(limit) });
  if (league) params.set("league", league);
  const data = await apiGet<{ matches: Match[] }>(`/matches/recent?${params}`);
  return data.matches;
}

export async function fetchTodayMatches(date?: string): Promise<{ date: string; matches: Match[] }> {
  const params = date ? `?date=${date}` : "";
  return apiGet<{ date: string; matches: Match[] }>(`/matches/today${params}`);
}

export async function fetchMatchById(id: number): Promise<Match> {
  return apiGet<Match>(`/matches/${id}`);
}

export async function fetchMatchAnalysis(id: number): Promise<FullAnalysis> {
  return apiGet<FullAnalysis>(`/matches/${id}/analysis`);
}

export async function fetchFullAnalysis(homeTeam: string, awayTeam: string, league: string): Promise<FullAnalysis> {
  const params = new URLSearchParams({ home_team: homeTeam, away_team: awayTeam, league });
  return apiGet<FullAnalysis>(`/analysis/full?${params}`);
}

export async function fetchLeagueTable(league: string, season?: string): Promise<LeagueTableEntry[]> {
  const params = new URLSearchParams({ league });
  if (season) params.set("season", season);
  const data = await apiGet<{ league: string; table: LeagueTableEntry[] }>(`/league-table?${params}`);
  return data.table;
}

export async function fetchH2H(teamA: string, teamB: string): Promise<H2HData> {
  const params = new URLSearchParams({ team_a: teamA, team_b: teamB });
  return apiGet<H2HData>(`/analysis/h2h?${params}`);
}

export async function fetchForm(team: string, limit = 5): Promise<FormData> {
  const params = new URLSearchParams({ team, limit: String(limit) });
  return apiGet<FormData>(`/analysis/form?${params}`);
}

export async function fetchPrediction(homeTeam: string, awayTeam: string, league: string): Promise<Prediction> {
  const params = new URLSearchParams({ home_team: homeTeam, away_team: awayTeam, league });
  return apiGet<Prediction>(`/analysis/predict?${params}`);
}

export interface MatchReport {
  report: string;
  sections: {
    h_overview: string;
    a_overview: string;
    h_scoring: string;
    a_scoring: string;
    h_momentum: string;
    a_momentum: string;
    h2h: string;
    scores: string;
    metrics: string;
    patterns: string;
    insights: string;
    verdict: string;
    multi_season: string;
  };
  analysis: FullAnalysis;
  match?: Match;
}

export async function fetchMatchReport(id: number): Promise<MatchReport> {
  return apiGet<MatchReport>(`/matches/${id}/report`);
}

export async function fetchAnalysisReport(homeTeam: string, awayTeam: string, league: string): Promise<MatchReport> {
  const params = new URLSearchParams({ home_team: homeTeam, away_team: awayTeam, league });
  return apiGet<MatchReport>(`/analysis/report?${params}`);
}

export async function triggerDataRefresh(): Promise<{ status: string; message: string }> {
  return apiGet<{ status: string; message: string }>("/data/refresh");
}
