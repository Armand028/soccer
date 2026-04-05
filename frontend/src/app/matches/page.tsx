"use client";

import { useEffect, useState, Suspense } from "react";
import { useSearchParams } from "next/navigation";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Select } from "@/components/ui/select";
import { MobileNav } from "@/components/mobile-nav";
import { fetchLeagues, fetchRecentMatches, type Match } from "@/lib/api";
import Link from "next/link";

function MatchesContent() {
  const searchParams = useSearchParams();
  const [leagues, setLeagues] = useState<string[]>([]);
  const [selectedLeague, setSelectedLeague] = useState(searchParams.get("league") || "");
  const [matches, setMatches] = useState<Match[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetchLeagues().then(setLeagues).catch(console.error);
  }, []);

  useEffect(() => {
    const leagueParam = searchParams.get("league");
    if (leagueParam && leagueParam !== selectedLeague) {
      setSelectedLeague(leagueParam);
    }
  }, [searchParams]);

  useEffect(() => {
    setLoading(true);
    fetchRecentMatches(100, selectedLeague || undefined)
      .then(setMatches)
      .catch(console.error)
      .finally(() => setLoading(false));
  }, [selectedLeague]);

  return (
    <div className="space-y-6 pb-20 lg:pb-0">
      <div className="flex flex-col sm:flex-row sm:items-end sm:justify-between gap-4">
        <div>
          <h1 className="text-3xl font-bold tracking-tight">Recent Matches</h1>
          <p className="text-muted-foreground mt-1">Browse historical results</p>
        </div>
        <div className="w-full sm:w-64">
          <Select
            placeholder="All Leagues"
            options={[
              { value: "", label: "All Leagues" },
              ...leagues.map((l) => ({ value: l, label: l })),
            ]}
            value={selectedLeague}
            onChange={(e) => setSelectedLeague(e.target.value)}
          />
        </div>
      </div>

      <Card>
        <CardHeader>
          <CardTitle className="text-base">
            {selectedLeague || "All Leagues"} — {matches.length} matches
          </CardTitle>
        </CardHeader>
        <CardContent>
          {loading ? (
            <div className="text-center py-8 text-muted-foreground animate-pulse">
              Loading matches...
            </div>
          ) : matches.length === 0 ? (
            <div className="text-center py-8 text-muted-foreground">
              No matches found.
            </div>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-border text-left">
                    <th className="pb-3 font-medium text-muted-foreground">League</th>
                    <th className="pb-3 font-medium text-muted-foreground">Season</th>
                    <th className="pb-3 font-medium text-muted-foreground">Home</th>
                    <th className="pb-3 font-medium text-muted-foreground text-center">Score</th>
                    <th className="pb-3 font-medium text-muted-foreground">Away</th>
                    <th className="pb-3 font-medium text-muted-foreground">Round</th>
                  </tr>
                </thead>
                <tbody>
                  {matches.map((m) => {
                    const homeWin = m.home_score > m.away_score;
                    const awayWin = m.away_score > m.home_score;
                    return (
                      <tr key={m.id} className="border-b border-border/50 hover:bg-secondary/30 transition-colors cursor-pointer"
                          onClick={() => window.location.href = `/match/${m.id}`}>
                        <td className="py-2.5 pr-3">
                          <span className="text-xs bg-secondary px-2 py-0.5 rounded-full whitespace-nowrap">
                            {m.league}
                          </span>
                        </td>
                        <td className="py-2.5 pr-3 text-muted-foreground text-xs">{m.season}</td>
                        <td className={`py-2.5 pr-3 ${homeWin ? "font-bold text-green-400" : ""}`}>
                          {m.home_team}
                        </td>
                        <td className="py-2.5 text-center">
                          <span className="font-mono font-bold bg-secondary px-2.5 py-0.5 rounded text-xs">
                            {m.home_score} - {m.away_score}
                          </span>
                        </td>
                        <td className={`py-2.5 pl-3 ${awayWin ? "font-bold text-green-400" : ""}`}>
                          {m.away_team}
                        </td>
                        <td className="py-2.5 pl-3 text-muted-foreground text-xs">{m.round}</td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          )}
        </CardContent>
      </Card>

      <MobileNav />
    </div>
  );
}

export default function MatchesPage() {
  return (
    <Suspense fallback={
      <div className="flex items-center justify-center min-h-screen">
        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-primary"></div>
      </div>
    }>
      <MatchesContent />
    </Suspense>
  );
}
