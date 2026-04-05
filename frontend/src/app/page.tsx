"use client";

import { useEffect, useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { fetchRecentMatches, fetchStats, type Match, type Stats } from "@/lib/api";
import { Trophy, Activity, TrendingUp, Swords, Users, Database } from "lucide-react";
import Link from "next/link";
import { MobileNav } from "@/components/mobile-nav";

export default function DashboardPage() {
  const [stats, setStats] = useState<Stats | null>(null);
  const [recentMatches, setRecentMatches] = useState<Match[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    async function load() {
      try {
        const [s, m] = await Promise.all([
          fetchStats(),
          fetchRecentMatches(15),
        ]);
        setStats(s);
        setRecentMatches(m);
      } catch (e) {
        console.error(e);
      } finally {
        setLoading(false);
      }
    }
    load();
  }, []);

  if (loading) {
    return (
      <div className="flex items-center justify-center h-full">
        <div className="animate-pulse text-muted-foreground">Loading dashboard...</div>
      </div>
    );
  }

  return (
    <div className="space-y-8 pb-20 lg:pb-0">
      <div>
        <h1 className="text-3xl font-bold tracking-tight">Dashboard</h1>
        <p className="text-muted-foreground mt-1">
          Soccer match analytics and Poisson-based predictions
        </p>
      </div>

      {/* Stats Cards */}
      <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-5">
        <Card>
          <CardHeader className="flex flex-row items-center justify-between pb-2">
            <CardTitle className="text-sm font-medium text-muted-foreground">Leagues</CardTitle>
            <Trophy className="h-4 w-4 text-primary" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">{stats?.total_leagues ?? "—"}</div>
          </CardContent>
        </Card>
        <Card>
          <CardHeader className="flex flex-row items-center justify-between pb-2">
            <CardTitle className="text-sm font-medium text-muted-foreground">Teams</CardTitle>
            <Users className="h-4 w-4 text-primary" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">{stats?.total_teams ?? "—"}</div>
          </CardContent>
        </Card>
        <Card>
          <CardHeader className="flex flex-row items-center justify-between pb-2">
            <CardTitle className="text-sm font-medium text-muted-foreground">Total Matches</CardTitle>
            <Database className="h-4 w-4 text-primary" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">{stats?.total_matches?.toLocaleString() ?? "—"}</div>
          </CardContent>
        </Card>
        <Link href="/predict">
          <Card className="hover:border-primary/50 transition-colors cursor-pointer h-full">
            <CardHeader className="flex flex-row items-center justify-between pb-2">
              <CardTitle className="text-sm font-medium text-muted-foreground">Predict Match</CardTitle>
              <Swords className="h-4 w-4 text-primary" />
            </CardHeader>
            <CardContent>
              <div className="text-sm text-muted-foreground">Run a prediction →</div>
            </CardContent>
          </Card>
        </Link>
        <Card>
          <CardHeader className="flex flex-row items-center justify-between pb-2">
            <CardTitle className="text-sm font-medium text-muted-foreground">Model</CardTitle>
            <TrendingUp className="h-4 w-4 text-primary" />
          </CardHeader>
          <CardContent>
            <div className="text-sm font-medium">Poisson xG</div>
          </CardContent>
        </Card>
      </div>

      {/* Recent Matches Table */}
      <Card>
        <CardHeader>
          <CardTitle>Recent Matches</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-border text-left">
                  <th className="pb-3 font-medium text-muted-foreground">League</th>
                  <th className="pb-3 font-medium text-muted-foreground">Home</th>
                  <th className="pb-3 font-medium text-muted-foreground text-center">Score</th>
                  <th className="pb-3 font-medium text-muted-foreground">Away</th>
                  <th className="pb-3 font-medium text-muted-foreground">Round</th>
                </tr>
              </thead>
              <tbody>
                {recentMatches.map((m) => {
                  const homeWin = m.home_score > m.away_score;
                  const awayWin = m.away_score > m.home_score;
                  return (
                    <tr key={m.id} className="border-b border-border/50 hover:bg-secondary/30 transition-colors">
                      <td className="py-3 pr-4">
                        <span className="text-xs bg-secondary px-2 py-1 rounded-full">{m.league}</span>
                      </td>
                      <td className={`py-3 pr-4 font-medium ${homeWin ? "text-green-400" : ""}`}>{m.home_team}</td>
                      <td className="py-3 text-center">
                        <span className="font-mono font-bold bg-secondary px-3 py-1 rounded">
                          {m.home_score} - {m.away_score}
                        </span>
                      </td>
                      <td className={`py-3 pl-4 font-medium ${awayWin ? "text-green-400" : ""}`}>{m.away_team}</td>
                      <td className="py-3 pl-4 text-muted-foreground text-xs">{m.round}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
          <div className="mt-4 text-center">
            <Link href="/matches" className="text-sm text-primary hover:underline">
              View all matches →
            </Link>
          </div>
        </CardContent>
      </Card>

      <MobileNav />
    </div>
  );
}
