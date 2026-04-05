"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { BarChart3, Trophy, Swords, Activity, Home, GitCompareArrows, CalendarDays } from "lucide-react";
import { cn } from "@/lib/utils";

const navItems = [
  { href: "/", label: "Dashboard", icon: Home },
  { href: "/today", label: "Today's Matches", icon: CalendarDays },
  { href: "/predict", label: "Match Predictor", icon: Swords },
  { href: "/h2h", label: "Head to Head", icon: GitCompareArrows },
  { href: "/matches", label: "Recent Matches", icon: Activity },
  { href: "/leagues", label: "Leagues", icon: Trophy },
];

export function Sidebar() {
  const pathname = usePathname();

  return (
    <aside className="hidden lg:flex lg:flex-col w-64 border-r border-border bg-card">
      <div className="flex items-center gap-3 px-6 py-5 border-b border-border">
        <BarChart3 className="h-7 w-7 text-primary" />
        <span className="text-lg font-bold tracking-tight">Soccer Analyzer</span>
      </div>
      <nav className="flex-1 px-3 py-4 space-y-1">
        {navItems.map((item) => {
          const Icon = item.icon;
          const isActive = pathname === item.href;
          return (
            <Link
              key={item.href}
              href={item.href}
              className={cn(
                "flex items-center gap-3 px-3 py-2.5 rounded-md text-sm font-medium transition-colors",
                isActive
                  ? "bg-primary/10 text-primary"
                  : "text-muted-foreground hover:text-foreground hover:bg-secondary"
              )}
            >
              <Icon className="h-4 w-4" />
              {item.label}
            </Link>
          );
        })}
      </nav>
      <div className="px-6 py-4 border-t border-border">
        <p className="text-xs text-muted-foreground">
          Poisson-based predictions
        </p>
      </div>
    </aside>
  );
}
