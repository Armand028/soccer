"use client";

import { useEffect, useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { MobileNav } from "@/components/mobile-nav";
import { fetchTodayMatches, type Match } from "@/lib/api";
import { Calendar, Clock, ChevronRight, RefreshCw } from "lucide-react";
import Link from "next/link";

function formatKickoff(m: Match) {
  // Prefer backend-provided Ottawa time, fallback to client-side conversion
  if (m.kickoff_ottawa) {
    const parts = m.kickoff_ottawa.split(" ");
    return parts[1] ?? parts[0];  // "HH:MM" portion
  }
  if (!m.kickoff_timestamp) return "TBD";
  const d = new Date(m.kickoff_timestamp * 1000);
  return d.toLocaleTimeString("en-CA", { hour: "2-digit", minute: "2-digit", timeZone: "America/Toronto" });
}

function statusBadge(status?: string) {
  switch (status) {
    case "finished":
      return <span className="text-[10px] font-bold uppercase px-2 py-0.5 rounded-full bg-green-500/15 text-green-400">FT</span>;
    case "inprogress":
      return <span className="text-[10px] font-bold uppercase px-2 py-0.5 rounded-full bg-yellow-500/15 text-yellow-400 animate-pulse">LIVE</span>;
    case "notstarted":
      return <span className="text-[10px] font-bold uppercase px-2 py-0.5 rounded-full bg-secondary text-muted-foreground">NS</span>;
    default:
      return <span className="text-[10px] font-bold uppercase px-2 py-0.5 rounded-full bg-secondary text-muted-foreground">{status || "—"}</span>;
  }
}

export default function TodayPage() {
  const [date, setDate] = useState(() => new Date().toISOString().slice(0, 10));
  const [matches, setMatches] = useState<Match[]>([]);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);

  function load(d: string) {
    setLoading(true);
    fetchTodayMatches(d)
      .then((data) => {
        setMatches(data.matches);
      })
      .catch(console.error)
      .finally(() => setLoading(false));
  }

  useEffect(() => {
    load(date);
  }, [date]);

  function shiftDate(days: number) {
    const d = new Date(date);
    d.setDate(d.getDate() + days);
    setDate(d.toISOString().slice(0, 10));
  }

  async function handleRefresh() {
    setRefreshing(true);
    try {
      await fetch("/api/data/refresh");
      load(date);
    } catch (e) {
      console.error(e);
    } finally {
      setRefreshing(false);
    }
  }

  // Group matches by league
  const grouped = matches.reduce<Record<string, Match[]>>((acc, m) => {
    const key = m.league;
    if (!acc[key]) acc[key] = [];
    acc[key].push(m);
    return acc;
  }, {});

  const isToday = date === new Date().toISOString().slice(0, 10);

  return (
    <div className="space-y-6 pb-20 lg:pb-0">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-end sm:justify-between gap-4">
        <div>
          <h1 className="text-3xl font-bold tracking-tight">
            {isToday ? "Today's Matches" : "Matches"}
          </h1>
          <p className="text-muted-foreground mt-1">
            {new Date(date + "T12:00:00").toLocaleDateString("en-US", {
              weekday: "long", year: "numeric", month: "long", day: "numeric",
            })}
          </p>
        </div>
        <div className="flex items-center gap-2">
          <button onClick={() => shiftDate(-1)}
            className="px-3 py-2 text-sm rounded-md bg-secondary hover:bg-secondary/80 transition-colors">
            ← Prev
          </button>
          <button onClick={() => setDate(new Date().toISOString().slice(0, 10))}
            className="px-3 py-2 text-sm rounded-md bg-primary/10 text-primary hover:bg-primary/20 transition-colors font-medium">
            Today
          </button>
          <button onClick={() => shiftDate(1)}
            className="px-3 py-2 text-sm rounded-md bg-secondary hover:bg-secondary/80 transition-colors">
            Next →
          </button>
          <button onClick={handleRefresh} disabled={refreshing}
            className="ml-2 p-2 rounded-md bg-secondary hover:bg-secondary/80 transition-colors disabled:opacity-50"
            title="Refresh data from API">
            <RefreshCw className={`h-4 w-4 ${refreshing ? "animate-spin" : ""}`} />
          </button>
        </div>
      </div>

      {/* Content */}
      {loading ? (
        <div className="text-center py-16 text-muted-foreground animate-pulse">
          Loading matches...
        </div>
      ) : matches.length === 0 ? (
        <Card>
          <CardContent className="py-16 text-center">
            <Calendar className="h-10 w-10 mx-auto text-muted-foreground/50 mb-3" />
            <p className="text-muted-foreground">No matches found for this date.</p>
            <p className="text-xs text-muted-foreground mt-1">
              Try clicking &quot;Refresh&quot; to fetch data from the API, or browse another date.
            </p>
          </CardContent>
        </Card>
      ) : (
        <div className="space-y-6">
          {Object.entries(grouped).map(([league, leagueMatches]) => (
            <Card key={league}>
              <CardHeader className="pb-3">
                <CardTitle className="text-sm font-medium flex items-center gap-2">
                  <span className="w-1.5 h-1.5 rounded-full bg-primary" />
                  {league}
                  <span className="text-muted-foreground font-normal">({leagueMatches.length})</span>
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-1">
                {leagueMatches.map((m) => (
                  <Link key={m.id} href={`/match/${m.id}`}>
                    <div className="flex items-center gap-3 px-3 py-3 rounded-lg hover:bg-secondary/50 transition-colors cursor-pointer group">
                      {/* Time */}
                      <div className="w-14 flex-shrink-0 text-center">
                        {m.status === "finished" ? (
                          statusBadge(m.status)
                        ) : m.status === "inprogress" ? (
                          statusBadge(m.status)
                        ) : (
                          <span className="text-xs text-muted-foreground flex items-center justify-center gap-1">
                            <Clock className="h-3 w-3" />
                            {formatKickoff(m)}
                          </span>
                        )}
                      </div>

                      {/* Teams */}
                      <div className="flex-1 min-w-0">
                        <div className={`text-sm truncate ${m.status === "finished" && m.home_score > m.away_score ? "font-bold" : ""}`}>
                          {m.home_team}
                        </div>
                        <div className={`text-sm truncate ${m.status === "finished" && m.away_score > m.home_score ? "font-bold" : ""}`}>
                          {m.away_team}
                        </div>
                      </div>

                      {/* Score */}
                      {m.status === "finished" || m.status === "inprogress" ? (
                        <div className="w-12 text-center flex-shrink-0">
                          <div className={`text-sm font-mono ${m.home_score > m.away_score ? "font-bold text-green-400" : ""}`}>
                            {m.home_score}
                          </div>
                          <div className={`text-sm font-mono ${m.away_score > m.home_score ? "font-bold text-green-400" : ""}`}>
                            {m.away_score}
                          </div>
                        </div>
                      ) : (
                        <div className="w-12 text-center text-xs text-muted-foreground">vs</div>
                      )}

                      {/* Arrow */}
                      <ChevronRight className="h-4 w-4 text-muted-foreground/50 group-hover:text-foreground transition-colors flex-shrink-0" />
                    </div>
                  </Link>
                ))}
              </CardContent>
            </Card>
          ))}
        </div>
      )}

      <MobileNav />
    </div>
  );
}
