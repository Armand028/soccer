"use client";

import { useEffect, useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Select } from "@/components/ui/select";
import { MobileNav } from "@/components/mobile-nav";
import {
  fetchLeagues,
  fetchTeams,
  fetchAnalysisReport,
  fetchFullAnalysis,
  type MatchReport,
  type FullAnalysis,
  type FormData,
  type Pattern,
  type H2HMeeting,
} from "@/lib/api";
import {
  BarChart3, Shield, Swords, Target, Flag, AlertTriangle,
  TrendingUp, Flame, GitCompareArrows, ArrowLeft, FileText, Loader2,
} from "lucide-react";
import Link from "next/link";

function FormStreak({ streak }: { streak: string[] }) {
  return (
    <div className="flex gap-1 flex-wrap">
      {streak.map((r, i) => (
        <span
          key={i}
          className={`w-7 h-7 flex items-center justify-center rounded text-xs font-bold ${
            r === "W"
              ? "bg-green-500/20 text-green-400"
              : r === "D"
              ? "bg-yellow-500/20 text-yellow-400"
              : "bg-red-500/20 text-red-400"
          }`}
        >
          {r}
        </span>
      ))}
    </div>
  );
}

function FormCard({ title, form, icon }: { title: string; form: FormData; icon: React.ReactNode }) {
  return (
    <Card>
      <CardHeader className="pb-2">
        <CardTitle className="text-sm font-medium flex items-center gap-2">
          {icon}{title}
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-3">
        <FormStreak streak={form.streak} />
        <div className="grid grid-cols-3 gap-2 text-center text-xs">
          <div><span className="text-green-400 font-bold text-base">{form.wins}</span><br/>W</div>
          <div><span className="text-yellow-400 font-bold text-base">{form.draws}</span><br/>D</div>
          <div><span className="text-red-400 font-bold text-base">{form.losses}</span><br/>L</div>
        </div>
        <div className="space-y-1 text-xs text-muted-foreground">
          <div className="flex justify-between"><span>Goals For / Against</span><span className="text-foreground font-medium">{form.goals_for} / {form.goals_against}</span></div>
          <div className="flex justify-between"><span>Avg GF / GA</span><span className="text-foreground font-medium">{form.avg_goals_for} / {form.avg_goals_against}</span></div>
          <div className="flex justify-between"><span>Clean Sheets</span><span className="text-foreground font-medium">{form.clean_sheets} ({form.clean_sheet_pct}%)</span></div>
          {(form.first_half_goals_for > 0 || form.first_half_goals_against > 0) && (
            <div className="flex justify-between"><span>1st Half GF / GA</span><span className="text-foreground font-medium">{form.first_half_goals_for} / {form.first_half_goals_against}</span></div>
          )}
          {(form.second_half_goals_for > 0 || form.second_half_goals_against > 0) && (
            <div className="flex justify-between"><span>2nd Half GF / GA</span><span className="text-foreground font-medium">{form.second_half_goals_for} / {form.second_half_goals_against}</span></div>
          )}
          {form.avg_cards > 0 && (
            <div className="flex justify-between"><span>Avg Cards</span><span className="text-foreground font-medium">{form.avg_cards}</span></div>
          )}
          {form.avg_corners > 0 && (
            <div className="flex justify-between"><span>Avg Corners</span><span className="text-foreground font-medium">{form.avg_corners}</span></div>
          )}
          {form.avg_shots > 0 && (
            <div className="flex justify-between"><span>Avg Shots / On Target</span><span className="text-foreground font-medium">{form.avg_shots} / {form.avg_shots_on_target}</span></div>
          )}
          {form.avg_possession != null && form.avg_possession > 0 && (
            <div className="flex justify-between"><span>Avg Possession</span><span className="text-foreground font-medium">{form.avg_possession}%</span></div>
          )}
          <div className="flex justify-between"><span>Over 2.5</span><span className="text-foreground font-medium">{form.over_2_5_pct}%</span></div>
          <div className="flex justify-between"><span>BTTS</span><span className="text-foreground font-medium">{form.btts_pct}%</span></div>
        </div>
      </CardContent>
    </Card>
  );
}

function PatternList({ patterns, team }: { patterns: Pattern[]; team: string }) {
  if (!patterns.length) return null;
  const iconMap: Record<string, string> = { hot: "🔥", trend: "📈", warning: "⚠️", info: "ℹ️" };
  return (
    <div className="space-y-1.5">
      <p className="text-xs font-medium text-muted-foreground uppercase tracking-wider">{team}</p>
      {patterns.map((p, i) => (
        <div key={i} className="flex items-start gap-2 text-xs bg-secondary/30 px-3 py-2 rounded-md">
          <span>{iconMap[p.type] || "•"}</span>
          <span>{p.text}</span>
        </div>
      ))}
    </div>
  );
}

function StatRow({ label, home, away, suffix, highlight }: {
  label: string; home?: number | string | null; away?: number | string | null; suffix?: string; highlight?: boolean;
}) {
  const h = home ?? "—";
  const a = away ?? "—";
  return (
    <div className={`flex items-center justify-between py-1.5 px-2 rounded text-sm ${highlight ? "bg-secondary/50" : ""}`}>
      <span className="font-mono w-16 text-right">{h}{suffix}</span>
      <span className="text-xs text-muted-foreground flex-1 text-center">{label}</span>
      <span className="font-mono w-16 text-left">{a}{suffix}</span>
    </div>
  );
}

function MarkdownReport({ content }: { content: string }) {
  const lines = content.split("\n");
  const elements: React.ReactNode[] = [];
  let tableRows: string[][] = [];
  let inTable = false;

  const renderInline = (text: string) => {
    const parts: React.ReactNode[] = [];
    let remaining = text;
    let key = 0;
    const regex = /(\*\*(.+?)\*\*|\*(.+?)\*)/g;
    let lastIndex = 0;
    let m;
    while ((m = regex.exec(remaining)) !== null) {
      if (m.index > lastIndex) parts.push(<span key={key++}>{remaining.slice(lastIndex, m.index)}</span>);
      if (m[2]) parts.push(<strong key={key++} className="text-foreground font-semibold">{m[2]}</strong>);
      else if (m[3]) parts.push(<em key={key++} className="text-primary/80">{m[3]}</em>);
      lastIndex = regex.lastIndex;
    }
    if (lastIndex < remaining.length) parts.push(<span key={key++}>{remaining.slice(lastIndex)}</span>);
    return parts;
  };

  const flushTable = () => {
    if (tableRows.length === 0) return;
    const header = tableRows[0];
    const body = tableRows.slice(2);
    elements.push(
      <div key={elements.length} className="overflow-x-auto my-3">
        <table className="w-full text-xs border-collapse">
          <thead>
            <tr className="border-b border-border">
              {header.map((cell, i) => (
                <th key={i} className="px-3 py-2 text-left font-semibold text-muted-foreground">{cell.trim()}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {body.map((row, ri) => (
              <tr key={ri} className={ri % 2 === 0 ? "bg-secondary/20" : ""}>
                {row.map((cell, ci) => (
                  <td key={ci} className="px-3 py-1.5">{renderInline(cell.trim())}</td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    );
    tableRows = [];
    inTable = false;
  };

  lines.forEach((line) => {
    const trimmed = line.trim();
    if (trimmed.startsWith("|")) {
      const cells = trimmed.split("|").filter((c) => c.trim() !== "");
      if (!cells.every((c) => /^[-\s|]+$/.test(c))) {
        tableRows.push(cells);
      }
      inTable = true;
      return;
    } else if (inTable) {
      flushTable();
    }
    if (trimmed.startsWith("# ")) {
      elements.push(<h1 key={elements.length} className="text-xl font-bold mt-6 mb-3">{renderInline(trimmed.slice(2))}</h1>);
    } else if (trimmed.startsWith("## ")) {
      elements.push(<h2 key={elements.length} className="text-lg font-semibold mt-5 mb-2 text-primary">{renderInline(trimmed.slice(3))}</h2>);
    } else if (trimmed.startsWith("### ")) {
      elements.push(<h3 key={elements.length} className="text-sm font-medium mt-4 mb-2 text-muted-foreground uppercase tracking-wider">{renderInline(trimmed.slice(4))}</h3>);
    } else if (trimmed.startsWith("---")) {
      elements.push(<hr key={elements.length} className="my-4 border-border" />);
    } else if (trimmed.startsWith("- ")) {
      elements.push(<li key={elements.length} className="ml-4 text-sm leading-relaxed">{renderInline(trimmed.slice(2))}</li>);
    } else if (trimmed === "") {
      elements.push(<div key={elements.length} className="h-2" />);
    } else {
      elements.push(<p key={elements.length} className="text-sm leading-relaxed">{renderInline(trimmed)}</p>);
    }
  });

  if (inTable) flushTable();
  return <div className="space-y-1">{elements}</div>;
}

export default function PredictPage() {
  const [leagues, setLeagues] = useState<string[]>([]);
  const [teams, setTeams] = useState<string[]>([]);
  const [selectedLeague, setSelectedLeague] = useState("");
  const [homeTeam, setHomeTeam] = useState("");
  const [awayTeam, setAwayTeam] = useState("");
  const [analysis, setAnalysis] = useState<FullAnalysis | null>(null);
  const [report, setReport] = useState<MatchReport | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [tab, setTab] = useState<"data" | "report">("report");

  useEffect(() => {
    fetchLeagues().then(setLeagues).catch(console.error);
  }, []);

  useEffect(() => {
    if (selectedLeague) {
      fetchTeams(selectedLeague).then(setTeams).catch(console.error);
      setHomeTeam("");
      setAwayTeam("");
      setAnalysis(null);
      setReport(null);
    }
  }, [selectedLeague]);

  async function handlePredict() {
    if (!homeTeam || !awayTeam || !selectedLeague) return;
    if (homeTeam === awayTeam) {
      setError("Home and away teams must be different.");
      return;
    }
    setLoading(true);
    setError("");
    setAnalysis(null);
    setReport(null);
    try {
      const [analysisData, reportData] = await Promise.all([
        fetchFullAnalysis(homeTeam, awayTeam, selectedLeague),
        fetchAnalysisReport(homeTeam, awayTeam, selectedLeague),
      ]);
      setAnalysis(analysisData);
      setReport(reportData);
    } catch (e: any) {
      setError(e.message || "Failed to fetch prediction");
    } finally {
      setLoading(false);
    }
  }

  const hasData = analysis && report;
  const home = homeTeam;
  const away = awayTeam;
  const probs = analysis?.expected_goals?.probabilities;
  const form = analysis?.form;
  const h2h = analysis?.head_to_head;
  const goal_trends = analysis?.goal_trends;
  const patterns = analysis?.patterns;
  const league_position = analysis?.league_position;
  const xg = analysis?.expected_goals;

  return (
    <div className="space-y-6 pb-20 lg:pb-0">
      {/* Back link */}
      <Link href="/" className="inline-flex items-center gap-1 text-sm text-muted-foreground hover:text-foreground">
        <ArrowLeft className="h-4 w-4" /> Back to Dashboard
      </Link>

      {/* Header */}
      <div>
        <h1 className="text-3xl font-bold tracking-tight">Match Predictor</h1>
        <p className="text-muted-foreground mt-1">
          Select two teams to generate an AI-powered match analysis
        </p>
      </div>

      {/* Selection Card */}
      <Card>
        <CardHeader className="pb-2">
          <CardTitle className="text-base flex items-center gap-2">
            <Target className="h-4 w-4 text-primary" /> Setup
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <Select
            label="League"
            placeholder="Select a league"
            options={leagues.map((l) => ({ value: l, label: l }))}
            value={selectedLeague}
            onChange={(e) => setSelectedLeague(e.target.value)}
          />
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
            <Select
              label="Home Team"
              placeholder="Select home team"
              options={teams.map((t) => ({ value: t, label: t }))}
              value={homeTeam}
              onChange={(e) => setHomeTeam(e.target.value)}
              disabled={!selectedLeague}
            />
            <Select
              label="Away Team"
              placeholder="Select away team"
              options={teams.map((t) => ({ value: t, label: t }))}
              value={awayTeam}
              onChange={(e) => setAwayTeam(e.target.value)}
              disabled={!selectedLeague}
            />
          </div>
          <button
            onClick={handlePredict}
            disabled={!homeTeam || !awayTeam || loading}
            className="w-full sm:w-auto px-6 py-2.5 bg-primary text-primary-foreground font-medium rounded-md hover:bg-primary/90 disabled:opacity-50 disabled:cursor-not-allowed transition-colors flex items-center justify-center gap-2"
          >
            {loading ? (
              <>
                <Loader2 className="h-4 w-4 animate-spin" />
                Analyzing...
              </>
            ) : (
              <>
                <Swords className="h-4 w-4" />
                Generate Analysis
              </>
            )}
          </button>
          {error && <p className="text-sm text-destructive">{error}</p>}
        </CardContent>
      </Card>

      {/* Match Header (when data loaded) */}
      {hasData && (
        <Card className="border-primary/30">
          <CardContent className="py-6">
            <div className="text-center space-y-2">
              <p className="text-xs text-muted-foreground uppercase tracking-widest">{selectedLeague}</p>
              <div className="flex items-center justify-center gap-4 sm:gap-8">
                <div className="text-right flex-1">
                  <p className="text-lg sm:text-xl font-bold">{home}</p>
                  {league_position?.home && (
                    <p className="text-xs text-muted-foreground">#{league_position.home.position} • {league_position.home.points} pts</p>
                  )}
                </div>
                <div className="text-center flex-shrink-0">
                  <div className="text-2xl font-bold text-muted-foreground">vs</div>
                </div>
                <div className="text-left flex-1">
                  <p className="text-lg sm:text-xl font-bold">{away}</p>
                  {league_position?.away && (
                    <p className="text-xs text-muted-foreground">#{league_position.away.position} • {league_position.away.points} pts</p>
                  )}
                </div>
              </div>
            </div>
          </CardContent>
        </Card>
      )}

      {/* Tab Switcher (when data loaded) */}
      {hasData && (
        <div className="flex gap-1 bg-secondary/30 p-1 rounded-lg">
          <button
            onClick={() => setTab("report")}
            className={`flex-1 flex items-center justify-center gap-2 py-2 px-4 rounded-md text-sm font-medium transition-colors
              ${tab === "report" ? "bg-primary text-primary-foreground" : "text-muted-foreground hover:text-foreground"}`}
          >
            <FileText className="h-4 w-4" /> AI Report
          </button>
          <button
            onClick={() => setTab("data")}
            className={`flex-1 flex items-center justify-center gap-2 py-2 px-4 rounded-md text-sm font-medium transition-colors
              ${tab === "data" ? "bg-primary text-primary-foreground" : "text-muted-foreground hover:text-foreground"}`}
          >
            <BarChart3 className="h-4 w-4" /> Data View
          </button>
        </div>
      )}

      {/* AI Report Tab */}
      {hasData && tab === "report" && report && (
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium flex items-center gap-2">
              <FileText className="h-4 w-4 text-primary" /> Intelligent Match Analysis
            </CardTitle>
          </CardHeader>
          <CardContent>
            <MarkdownReport content={report.report} />
          </CardContent>
        </Card>
      )}

      {/* Data View Tab */}
      {hasData && tab === "data" && (
        <>
          {/* Probabilities Bar */}
          {probs && (
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-sm font-medium flex items-center gap-2">
                  <BarChart3 className="h-4 w-4 text-primary" /> Match Probabilities
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-4">
                {/* 1X2 Bar */}
                <div>
                  <div className="flex text-xs mb-1.5">
                    <span className="flex-1 font-bold text-green-400">{home} {probs.home_win}%</span>
                    <span className="font-bold text-yellow-400">Draw {probs.draw}%</span>
                    <span className="flex-1 text-right font-bold text-blue-400">{away} {probs.away_win}%</span>
                  </div>
                  <div className="h-3 rounded-full overflow-hidden flex">
                    <div className="bg-green-500/60" style={{ width: `${probs.home_win}%` }} />
                    <div className="bg-yellow-500/60" style={{ width: `${probs.draw}%` }} />
                    <div className="bg-blue-500/60" style={{ width: `${probs.away_win}%` }} />
                  </div>
                </div>
                {/* Market probabilities */}
                <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
                  {[
                    { label: "Over 1.5", val: probs.over_1_5 },
                    { label: "Over 2.5", val: probs.over_2_5 },
                    { label: "Over 3.5", val: probs.over_3_5 },
                    { label: "BTTS", val: probs.btts },
                  ].map((item) => item.val != null && (
                    <div key={item.label} className="text-center bg-secondary/30 rounded-lg p-3">
                      <p className="text-xs text-muted-foreground">{item.label}</p>
                      <p className="text-lg font-bold">{item.val}%</p>
                    </div>
                  ))}
                </div>
                {/* xG */}
                {xg && (
                  <div className="flex items-center justify-center gap-6 pt-2 border-t border-border">
                    <div className="text-center">
                      <p className="text-xs text-muted-foreground">Home xG</p>
                      <p className="text-2xl font-bold text-primary">{xg.home_xg}</p>
                    </div>
                    <span className="text-muted-foreground">—</span>
                    <div className="text-center">
                      <p className="text-xs text-muted-foreground">Away xG</p>
                      <p className="text-2xl font-bold">{xg.away_xg}</p>
                    </div>
                  </div>
                )}
              </CardContent>
            </Card>
          )}

          {/* Form: 4 cards in 2x2 grid */}
          {form && (
            <div className="grid gap-4 md:grid-cols-2">
              <FormCard title={`${home} — Overall (10)`} form={form.home_overall} icon={<Flame className="h-4 w-4 text-green-400" />} />
              <FormCard title={`${away} — Overall (10)`} form={form.away_overall} icon={<Flame className="h-4 w-4 text-blue-400" />} />
              <FormCard title={`${home} — Home Only`} form={form.home_at_home} icon={<Shield className="h-4 w-4 text-green-400" />} />
              <FormCard title={`${away} — Away Only`} form={form.away_at_away} icon={<Swords className="h-4 w-4 text-blue-400" />} />
            </div>
          )}

          {/* Head-to-Head */}
          {h2h && (
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-sm font-medium flex items-center gap-2">
                  <GitCompareArrows className="h-4 w-4 text-primary" /> Head to Head ({h2h.total_matches} meetings)
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="grid grid-cols-3 text-center">
                  <div>
                    <p className="text-2xl font-bold text-green-400">{(h2h[`${home}_wins`] as number) ?? 0}</p>
                    <p className="text-xs text-muted-foreground">{home} wins</p>
                  </div>
                  <div>
                    <p className="text-2xl font-bold text-yellow-400">{h2h.draws}</p>
                    <p className="text-xs text-muted-foreground">Draws</p>
                  </div>
                  <div>
                    <p className="text-2xl font-bold text-blue-400">{(h2h[`${away}_wins`] as number) ?? 0}</p>
                    <p className="text-xs text-muted-foreground">{away} wins</p>
                  </div>
                </div>
                <div className="grid grid-cols-3 gap-2 text-center text-xs">
                  <div className="bg-secondary/30 rounded-lg p-2">
                    <p className="text-muted-foreground">Avg Goals</p>
                    <p className="font-bold text-sm">{h2h.avg_goals}</p>
                  </div>
                  <div className="bg-secondary/30 rounded-lg p-2">
                    <p className="text-muted-foreground">Over 2.5</p>
                    <p className="font-bold text-sm">{h2h.over_2_5_pct}%</p>
                  </div>
                  <div className="bg-secondary/30 rounded-lg p-2">
                    <p className="text-muted-foreground">BTTS</p>
                    <p className="font-bold text-sm">{h2h.btts_pct}%</p>
                  </div>
                </div>
                {h2h.meetings && h2h.meetings.length > 0 && (
                  <div className="space-y-1 pt-2 border-t border-border">
                    <p className="text-xs text-muted-foreground font-medium mb-1">Recent Meetings</p>
                    {h2h.meetings.slice(0, 5).map((m, i) => (
                      <div key={i} className="flex items-center justify-between text-xs bg-secondary/30 px-3 py-1.5 rounded">
                        <span className={m.home_score > m.away_score ? "font-bold" : ""}>{m.home}</span>
                        <span className="font-mono font-bold">{m.home_score} - {m.away_score}</span>
                        <span className={m.away_score > m.home_score ? "font-bold" : ""}>{m.away}</span>
                      </div>
                    ))}
                  </div>
                )}
              </CardContent>
            </Card>
          )}

          {/* Goal Trends */}
          {goal_trends && (
            <div className="grid gap-4 md:grid-cols-2">
              {[{ team: home, trends: goal_trends.home }, { team: away, trends: goal_trends.away }].map(({ team, trends }) => (
                <Card key={team}>
                  <CardHeader className="pb-2">
                    <CardTitle className="text-sm font-medium flex items-center gap-2">
                      <TrendingUp className="h-4 w-4 text-primary" /> {team} Goal Trends
                    </CardTitle>
                  </CardHeader>
                  <CardContent className="space-y-3">
                    <div className="grid grid-cols-3 gap-2 text-center text-xs">
                      <div className="bg-secondary/30 rounded-lg p-2">
                        <p className="text-muted-foreground">Scoring Streak</p>
                        <p className="font-bold text-sm text-green-400">{trends.scoring_streak}</p>
                      </div>
                      <div className="bg-secondary/30 rounded-lg p-2">
                        <p className="text-muted-foreground">Conceding Streak</p>
                        <p className="font-bold text-sm text-red-400">{trends.conceding_streak}</p>
                      </div>
                      <div className="bg-secondary/30 rounded-lg p-2">
                        <p className="text-muted-foreground">Clean Sheet Run</p>
                        <p className="font-bold text-sm text-blue-400">{trends.clean_sheet_streak}</p>
                      </div>
                    </div>
                    <div className="overflow-x-auto">
                      <div className="flex gap-1 min-w-max">
                        {trends.matches.map((mg, i) => (
                          <div key={i} className="text-center text-[10px] w-10 flex-shrink-0">
                            <div className={`font-bold ${mg.gf > mg.ga ? "text-green-400" : mg.gf < mg.ga ? "text-red-400" : "text-yellow-400"}`}>
                              {mg.gf}-{mg.ga}
                            </div>
                            <div className="text-muted-foreground truncate">{mg.venue}</div>
                          </div>
                        ))}
                      </div>
                    </div>
                  </CardContent>
                </Card>
              ))}
            </div>
          )}

          {/* Patterns & Anomalies */}
          {patterns && (patterns.home.length > 0 || patterns.away.length > 0) && (
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-sm font-medium flex items-center gap-2">
                  <AlertTriangle className="h-4 w-4 text-primary" /> Detected Patterns & Anomalies
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-4">
                <PatternList patterns={patterns.home} team={home} />
                <PatternList patterns={patterns.away} team={away} />
              </CardContent>
            </Card>
          )}
        </>
      )}

      <MobileNav />
    </div>
  );
}
