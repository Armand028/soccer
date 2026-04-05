"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { BarChart3, Trophy, Swords, Activity, Home, GitCompareArrows, CalendarDays } from "lucide-react";
import { cn } from "@/lib/utils";

const navItems = [
  { href: "/", label: "Home", icon: Home },
  { href: "/today", label: "Today", icon: CalendarDays },
  { href: "/predict", label: "Predict", icon: Swords },
  { href: "/h2h", label: "H2H", icon: GitCompareArrows },
  { href: "/matches", label: "Matches", icon: Activity },
];

export function MobileNav() {
  const pathname = usePathname();

  return (
    <nav className="lg:hidden fixed bottom-0 left-0 right-0 z-50 bg-card border-t border-border">
      <div className="flex justify-around py-2">
        {navItems.map((item) => {
          const Icon = item.icon;
          const isActive = pathname === item.href;
          return (
            <Link
              key={item.href}
              href={item.href}
              className={cn(
                "flex flex-col items-center gap-1 px-3 py-1.5 text-xs font-medium transition-colors",
                isActive ? "text-primary" : "text-muted-foreground"
              )}
            >
              <Icon className="h-5 w-5" />
              {item.label}
            </Link>
          );
        })}
      </div>
    </nav>
  );
}
