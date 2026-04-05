"use client";

import { useEffect, useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { MobileNav } from "@/components/mobile-nav";
import { fetchLeagues, fetchTeams } from "@/lib/api";
import { Trophy, Users, ChevronRight } from "lucide-react";
import Link from "next/link";

interface LeagueInfo {
  name: string;
  teamCount: number;
}

export default function LeaguesPage() {
  const [leagueInfos, setLeagueInfos] = useState<LeagueInfo[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    async function load() {
      try {
        const leagues = await fetchLeagues();
        const infos: LeagueInfo[] = await Promise.all(
          leagues.map(async (l) => {
            const teams = await fetchTeams(l);
            return { name: l, teamCount: teams.length };
          })
        );
        setLeagueInfos(infos);
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
        <div className="animate-pulse text-muted-foreground">Loading leagues...</div>
      </div>
    );
  }

  return (
    <div className="space-y-6 pb-20 lg:pb-0">
      <div>
        <h1 className="text-3xl font-bold tracking-tight">Leagues</h1>
        <p className="text-muted-foreground mt-1">
          {leagueInfos.length} leagues in the database
        </p>
      </div>

      <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
        {leagueInfos.map((league) => (
          <Link key={league.name} href={`/matches?league=${encodeURIComponent(league.name)}`}>
            <Card className="hover:border-primary/50 transition-colors cursor-pointer h-full">
              <CardHeader className="flex flex-row items-center justify-between pb-2">
                <div className="flex items-center gap-3">
                  <div className="p-2 bg-primary/10 rounded-lg">
                    <Trophy className="h-5 w-5 text-primary" />
                  </div>
                  <CardTitle className="text-base">{league.name}</CardTitle>
                </div>
                <ChevronRight className="h-4 w-4 text-muted-foreground" />
              </CardHeader>
              <CardContent>
                <div className="flex items-center gap-2 text-sm text-muted-foreground">
                  <Users className="h-3.5 w-3.5" />
                  {league.teamCount} teams
                </div>
              </CardContent>
            </Card>
          </Link>
        ))}
      </div>

      <MobileNav />
    </div>
  );
}
