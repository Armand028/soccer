"""
Intelligent Match Analyzer v2 — comprehensive data-driven match reports.

Performs deep statistical analysis using:
- Poisson score-matrix decomposition (exact score probabilities, clean sheet %)
- Weighted multi-factor model (xG, form, venue, H2H, league position, momentum)
- Strength of schedule analysis (quality of recent opponents)
- Attack-vs-defense matchup quality
- Season trajectory analysis (points-per-game trend)
- Goal distribution profiling (over/under at all thresholds)

All outputs use probabilistic language — no deterministic predictions.
"""

import sqlite3
import os
import math
from collections import defaultdict
import analysis_engine_v2 as v2

_here = os.path.dirname(os.path.abspath(__file__))
_parent = os.path.dirname(_here)
DB_PATH = os.path.join(_here, "soccer.db") if os.path.exists(os.path.join(_here, "soccer.db")) else os.path.join(_parent, "soccer.db")


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _sd(a, b, d=0.0):
    """Safe divide."""
    return a / b if b else d


def _poisson(lam, k):
    if lam <= 0:
        return 1.0 if k == 0 else 0.0
    return (math.exp(-lam) * (lam ** k)) / math.factorial(k)


def _pct_label(pct):
    if pct >= 75: return "very strong tendency"
    if pct >= 60: return "strong tendency"
    if pct >= 50: return "moderate lean"
    if pct >= 40: return "slight lean"
    if pct >= 30: return "marginal lean"
    return "unlikely"


def _form_label(pct):
    if pct >= 80: return "Excellent"
    if pct >= 65: return "Good"
    if pct >= 50: return "Average"
    if pct >= 35: return "Below average"
    return "Poor"


def _trend_arrow(old_ppg, new_ppg):
    diff = new_ppg - old_ppg
    if diff > 0.4: return "sharply improving"
    if diff > 0.15: return "improving"
    if diff < -0.4: return "sharply declining"
    if diff < -0.15: return "declining"
    return "stable"


# ===== DATA EXTRACTION =====

def _get_multi_season_record(team, league):
    conn = get_db()
    c = conn.cursor()
    c.execute("""
        SELECT season, COUNT(*) as played,
               SUM(CASE WHEN (home_team=? AND home_score>away_score) OR (away_team=? AND away_score>home_score) THEN 1 ELSE 0 END) as wins,
               SUM(CASE WHEN home_score=away_score THEN 1 ELSE 0 END) as draws,
               SUM(CASE WHEN (home_team=? AND home_score<away_score) OR (away_team=? AND away_score<home_score) THEN 1 ELSE 0 END) as losses,
               SUM(CASE WHEN home_team=? THEN home_score ELSE away_score END) as gf,
               SUM(CASE WHEN home_team=? THEN away_score ELSE home_score END) as ga
        FROM matches WHERE (home_team=? OR away_team=?) AND league=? AND home_score>=0
        GROUP BY season ORDER BY season
    """, (team,team,team,team,team,team,team,team,league))
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return rows


def _get_home_away_split(team, limit=20):
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT home_score,away_score FROM matches WHERE home_team=? AND home_score>=0 ORDER BY COALESCE(kickoff_timestamp,id) DESC LIMIT ?", (team,limit))
    hr = c.fetchall()
    c.execute("SELECT home_score,away_score FROM matches WHERE away_team=? AND away_score>=0 ORDER BY COALESCE(kickoff_timestamp,id) DESC LIMIT ?", (team,limit))
    ar = c.fetchall()
    conn.close()
    def _rec(rows, is_home):
        w = sum(1 for r in rows if (r["home_score"]>r["away_score"]) == is_home)
        d = sum(1 for r in rows if r["home_score"]==r["away_score"])
        gf = sum(r["home_score"] if is_home else r["away_score"] for r in rows)
        ga = sum(r["away_score"] if is_home else r["home_score"] for r in rows)
        return {"played":len(rows),"w":w,"d":d,"l":len(rows)-w-d,"gf":gf,"ga":ga}
    return {"home": _rec(hr, True), "away": _rec(ar, False)}


def _get_strength_of_schedule(team, table):
    if not table: return None
    pos_map = {t["team"]:t["position"] for t in table}
    n_teams = len(table)
    conn = get_db()
    c = conn.cursor()
    c.execute("""SELECT home_team,away_team FROM matches WHERE (home_team=? OR away_team=?) AND home_score>=0
                 ORDER BY COALESCE(kickoff_timestamp,id) DESC LIMIT 10""", (team,team))
    rows = c.fetchall()
    conn.close()
    opp_pos = [pos_map.get(r["away_team"] if r["home_team"]==team else r["home_team"]) for r in rows]
    opp_pos = [p for p in opp_pos if p]
    if not opp_pos: return None
    avg = sum(opp_pos)/len(opp_pos)
    top_pct = round(sum(1 for p in opp_pos if p<=n_teams/2)/len(opp_pos)*100,1)
    diff = "hard" if avg < n_teams*0.35 else "medium" if avg < n_teams*0.65 else "easy"
    return {"avg_opp_position":round(avg,1),"top_half_pct":top_pct,"difficulty":diff}


def _compute_score_matrix(home_xg, away_xg, mx=8):
    probs = defaultdict(float)
    exact = []
    for i in range(mx):
        for j in range(mx):
            p = _poisson(home_xg,i)*_poisson(away_xg,j)
            exact.append((i,j,p))
            if i>j: probs["home_win"]+=p
            elif i==j: probs["draw"]+=p
            else: probs["away_win"]+=p
            t=i+j
            if t>=1: probs["over_0_5"]+=p
            if t>=2: probs["over_1_5"]+=p
            if t>=3: probs["over_2_5"]+=p
            if t>=4: probs["over_3_5"]+=p
            if t>=5: probs["over_4_5"]+=p
            if i>0 and j>0: probs["btts_yes"]+=p
            if i==0: probs["away_cs"]+=p
            if j==0: probs["home_cs"]+=p
    exact.sort(key=lambda x:x[2], reverse=True)
    return {
        "probs":{k:round(v*100,2) for k,v in probs.items()},
        "top_scores":[(s[0],s[1],round(s[2]*100,2)) for s in exact[:10]],
    }


def _season_trajectory(records):
    if len(records)<2: return None
    cur,prev = records[-1],records[-2]
    c_ppg = _sd(cur["wins"]*3+cur["draws"],cur["played"])
    p_ppg = _sd(prev["wins"]*3+prev["draws"],prev["played"])
    return {"current_ppg":round(c_ppg,2),"prev_ppg":round(p_ppg,2),"trend":_trend_arrow(p_ppg,c_ppg)}


# ===== SECTION GENERATORS =====

def _sec_overview(team, f_all, f_venue, lp, split, venue, traj):
    L = []
    if lp:
        gd = lp["gf"]-lp["ga"]
        gds = f"+{gd}" if gd>0 else str(gd)
        ppg = round(_sd(lp["points"],lp["played"]),2)
        L.append(f"**{team}** — #{lp['position']} ({lp['points']} pts, {lp['won']}W {lp['drawn']}D {lp['lost']}L, GD {gds}, {ppg} ppg)")
    else:
        L.append(f"**{team}** — League position unavailable")
    L.append(f"- Overall form (last {f_all['matches_played']}): {_form_label(f_all['percentage'])} ({f_all['percentage']}%) — {f_all['wins']}W {f_all['draws']}D {f_all['losses']}L")
    if f_venue["matches_played"]>0:
        L.append(f"- {venue} form (last {f_venue['matches_played']}): {_form_label(f_venue['percentage'])} ({f_venue['percentage']}%) — {f_venue['wins']}W {f_venue['draws']}D {f_venue['losses']}L")
    s = split["home"] if venue=="Home" else split["away"]
    if s["played"]>0:
        L.append(f"- Last {s['played']} {venue.lower()}: {s['w']}W {s['d']}D {s['l']}L (avg {_sd(s['gf'],s['played']):.2f} GF, {_sd(s['ga'],s['played']):.2f} GA)")
    if traj:
        L.append(f"- Season trajectory: {traj['trend']} ({traj['prev_ppg']} -> {traj['current_ppg']} ppg vs last season)")
    return "\n".join(L)


def _sec_scoring(team, f, trends):
    n = f["matches_played"]
    if n==0: return f"**{team}**: Insufficient data"
    L = [f"**{team}** — Scoring Profile (last {n}):"]
    L.append(f"- Scoring: {f['goals_for']} goals ({f['avg_goals_for']}/game) | Conceding: {f['goals_against']} ({f['avg_goals_against']}/game)")
    L.append(f"- Clean sheets: {f['clean_sheets']} ({f['clean_sheet_pct']}%) | Over 2.5: {f['over_2_5_pct']}% | BTTS: {f['btts_pct']}%")
    mg = trends.get("matches",[])
    if mg:
        totals = [m["gf"]+m["ga"] for m in mg]
        nt = len(totals)
        L.append(f"- Goal distribution: 0g={sum(1 for t in totals if t==0)}, 1g={sum(1 for t in totals if t==1)}, 2g={sum(1 for t in totals if t==2)}, 3+g={sum(1 for t in totals if t>=3)} (of {nt})")
        fts = sum(1 for m in mg if m["gf"]==0)
        if fts>0: L.append(f"- Failed to score: {fts}/{nt} ({_sd(fts,nt)*100:.0f}%)")
        bw = sum(1 for m in mg if m["gf"]-m["ga"]>=3)
        bl = sum(1 for m in mg if m["ga"]-m["gf"]>=3)
        if bw: L.append(f"- Dominant wins (3+ margin): {bw}/{nt}")
        if bl: L.append(f"- Heavy defeats (3+ margin): {bl}/{nt}")
    return "\n".join(L)


def _sec_momentum(team, f, trends, sos):
    L = [f"**{team}** — Form & Momentum:"]
    if trends["scoring_streak"]>=5:
        L.append(f"- Scored in {trends['scoring_streak']} consecutive matches — *strong offensive run*")
    elif trends["scoring_streak"]>=3:
        L.append(f"- Scored in {trends['scoring_streak']} consecutive matches")
    if trends["clean_sheet_streak"]>=3:
        L.append(f"- {trends['clean_sheet_streak']} consecutive clean sheets — *defensive solidity*")
    if trends["conceding_streak"]>=5:
        L.append(f"- Conceded in {trends['conceding_streak']} straight — *defensive vulnerability*")
    mg = trends.get("matches",[])
    if len(mg)>=5:
        r5 = mg[-5:]
        w=sum(1 for m in r5 if m["gf"]>m["ga"]); d=sum(1 for m in r5 if m["gf"]==m["ga"]); l=5-w-d
        gf=sum(m["gf"] for m in r5); ga=sum(m["ga"] for m in r5)
        ppg5=round((w*3+d)/5,2)
        L.append(f"- Last 5: {w}W {d}D {l}L ({gf} GF, {ga} GA, {ppg5} ppg)")
        if len(mg)>=10:
            o5 = mg[-10:-5]
            wo=sum(1 for m in o5 if m["gf"]>m["ga"]); do=sum(1 for m in o5 if m["gf"]==m["ga"])
            ppgo=round((wo*3+do)/5,2)
            L.append(f"- Momentum: {_trend_arrow(ppgo,ppg5)} ({ppgo} -> {ppg5} ppg)")
    if sos:
        L.append(f"- Schedule difficulty: {sos['difficulty']} (avg opp rank #{sos['avg_opp_position']}, {sos['top_half_pct']}% top-half)")
    return "\n".join(L)


def _sec_h2h(h2h, home, away):
    L = ["**Head-to-Head History:**"]
    n = h2h["total_matches"]
    if n==0:
        L.append("- No previous meetings found in database.")
        return "\n".join(L)
    hw=h2h.get(f"{home}_wins",0); aw=h2h.get(f"{away}_wins",0); dr=h2h["draws"]
    hg=h2h.get(f"{home}_goals",0); ag=h2h.get(f"{away}_goals",0)
    L.append(f"- {n} meetings: {home} {hw}W, Draws {dr}, {away} {aw}W")
    L.append(f"- Goals: {home} {hg} — {away} {ag} (avg {h2h['avg_goals']}/match)")
    if n>=3:
        if hw>=n*0.6: L.append(f"  -> *{home} dominates this fixture ({hw}/{n} wins)*")
        elif aw>=n*0.6: L.append(f"  -> *{away} dominates this fixture ({aw}/{n} wins)*")
        elif dr>=n*0.4: L.append(f"  -> *Tends toward draws ({dr}/{n})*")
    L.append(f"- Over 2.5: {h2h['over_2_5_pct']}% | BTTS: {h2h['btts_pct']}%")
    meetings = h2h.get("meetings",[])
    hh = [m for m in meetings if m["home"]==home]
    if hh:
        hw2=sum(1 for m in hh if m["home_score"]>m["away_score"])
        hd2=sum(1 for m in hh if m["home_score"]==m["away_score"])
        L.append(f"- When {home} hosts: {hw2}W {hd2}D {len(hh)-hw2-hd2}L in {len(hh)} meetings")
    if meetings:
        L.append("- Recent:")
        for m in meetings[:5]:
            L.append(f"  - {m['home']} {m['home_score']}-{m['away_score']} {m['away']} ({m['season']})")
    return "\n".join(L)


def _sec_scores(home, away, sm):
    L = ["**Most Likely Exact Scores (Poisson):**",""]
    L.append("| Score | Prob |")
    L.append("|-------|------|")
    for h,a,pct in sm["top_scores"][:8]:
        L.append(f"| {home} {h}-{a} {away} | {pct}% |")
    p = sm["probs"]
    L += ["","**Market Probabilities:**","","| Market | Prob |","|----|------|"]
    L.append(f"| {home} Win | {p['home_win']}% |")
    L.append(f"| Draw | {p['draw']}% |")
    L.append(f"| {away} Win | {p['away_win']}% |")
    for k in ["over_0_5","over_1_5","over_2_5","over_3_5","over_4_5"]:
        label = k.replace("_"," ").replace("over","Over")
        L.append(f"| {label} | {p[k]}% |")
    L.append(f"| BTTS Yes | {p['btts_yes']}% |")
    L.append(f"| BTTS No | {round(100-p['btts_yes'],2)}% |")
    L.append(f"| {home} CS | {p['home_cs']}% |")
    L.append(f"| {away} CS | {p['away_cs']}% |")
    return "\n".join(L)


def _sec_metrics(home, away, xg, fh, fa, fhv, fav):
    if not xg: return "**Key Metrics:** Insufficient data."
    L = ["**Key Metrics:**",""]
    L.append(f"| Metric | {home} (H) | {away} (A) |")
    L.append("|--------|-----------|-----------|")
    L.append(f"| xG | {xg['home_xg']} | {xg['away_xg']} |")
    L.append(f"| Attack Str | {xg['home_attack_strength']} | {xg['away_attack_strength']} |")
    L.append(f"| Defense Str | {xg['home_defense_strength']} | {xg['away_defense_strength']} |")
    L.append(f"| Overall Form | {fh['percentage']}% | {fa['percentage']}% |")
    L.append(f"| Venue Form | {fhv['percentage']}% | {fav['percentage']}% |")
    L.append(f"| Avg GF | {fh['avg_goals_for']} | {fa['avg_goals_for']} |")
    L.append(f"| Avg GA | {fh['avg_goals_against']} | {fa['avg_goals_against']} |")
    L.append(f"| Clean Sheet % | {fh['clean_sheet_pct']}% | {fa['clean_sheet_pct']}% |")
    L.append(f"| Over 2.5 % | {fh['over_2_5_pct']}% | {fa['over_2_5_pct']}% |")
    L.append(f"| BTTS % | {fh['btts_pct']}% | {fa['btts_pct']}% |")
    return "\n".join(L)


def _sec_patterns(ph, pa, home, away):
    L = ["**Detected Patterns:**"]
    any_p = False
    for p in (ph or []):
        icon = {"hot":"🔥","trend":"📈","warning":"⚠️","info":"ℹ️"}.get(p["type"],"•")
        L.append(f"- {icon} [{home}] {p['text']}"); any_p=True
    for p in (pa or []):
        icon = {"hot":"🔥","trend":"📈","warning":"⚠️","info":"ℹ️"}.get(p["type"],"•")
        L.append(f"- {icon} [{away}] {p['text']}"); any_p=True
    if not any_p: L.append("- No significant anomalies detected.")
    return "\n".join(L)


# ===== CORE INTELLIGENCE =====

def _sec_insights(home, away, xg, sm, fh, fa, fhv, fav, h2h, th, ta, ph, pa, lph, lpa, sosh, sosa, hsplit, asplit):
    """Deep probabilistic analysis combining all data sources."""
    I = []
    if not xg or not sm:
        return "**Analysis:** Insufficient data."
    p = sm["probs"]
    ts = sm["top_scores"]

    # 1. Match outcome
    outs = [(p["home_win"],f"{home} Win","hw"),(p["draw"],"Draw","d"),(p["away_win"],f"{away} Win","aw")]
    outs.sort(key=lambda x:x[0], reverse=True)
    m1,m2 = outs[0],outs[1]
    I.append(f"**Match Outcome:** Poisson model projects **{m1[1]}** as most probable at **{m1[0]}%**, "
             f"followed by {m2[1]} at {m2[0]}%. Distribution: {home} {p['home_win']}% / Draw {p['draw']}% / {away} {p['away_win']}%.")
    margin = m1[0]-m2[0]
    if margin<8:
        I.append(f"**Competitive Balance:** Only {margin:.1f}pp separates the top two outcomes — *genuinely open match*.")
    elif margin>25:
        favt = home if m1[2]=="hw" else away
        I.append(f"**Clear Lean:** {margin:.1f}pp gap signals a *meaningful statistical advantage* for {favt}.")

    # 2. Most likely scores
    if ts:
        sc = ", ".join(f"{s[0]}-{s[1]} ({s[2]}%)" for s in ts[:3])
        I.append(f"**Likely Scores:** {sc}")

    # 3. xG and attack-defense matchup
    h_att=xg["home_attack_strength"]; h_def=xg["home_defense_strength"]
    a_att=xg["away_attack_strength"]; a_def=xg["away_defense_strength"]
    xdiff = xg["home_xg"]-xg["away_xg"]
    if abs(xdiff)>1.0:
        dom = home if xdiff>0 else away
        I.append(f"**xG Dominance:** {dom} holds significant advantage ({xg['home_xg']} vs {xg['away_xg']}) — *clear offensive superiority*.")
    elif abs(xdiff)>0.4:
        lean = home if xdiff>0 else away
        I.append(f"**xG Edge:** Slight lean to {lean} ({xg['home_xg']} vs {xg['away_xg']}).")
    else:
        I.append(f"**xG Balance:** Near-identical output ({xg['home_xg']} vs {xg['away_xg']}) — *tightly contested*.")

    h_mu = h_att/_sd(a_def,1,1); a_mu = a_att/_sd(h_def,1,1)
    if h_mu>1.5 and a_mu>1.5:
        I.append(f"**Open Match Profile:** Both attacks rate well vs opposing defense ({home}: {h_mu:.2f}, {away}: {a_mu:.2f}). *Expect goals at both ends.*")
    elif h_mu>1.5 and a_mu<0.8:
        I.append(f"**Asymmetric:** {home}'s attack dominates this matchup ({h_mu:.2f}) while {away} faces a wall ({a_mu:.2f}). *One-sided goal expectation.*")
    elif a_mu>1.5 and h_mu<0.8:
        I.append(f"**Asymmetric:** {away}'s attack dominates ({a_mu:.2f}) vs {home}'s struggle ({h_mu:.2f}). *{away} favored for goals.*")

    # 4. Goals analysis — blended model+history
    txg = xg["home_xg"]+xg["away_xg"]
    mo25=p["over_2_5"]; ho25=(fh["over_2_5_pct"]+fa["over_2_5_pct"])/2
    bo25=round(mo25*0.6+ho25*0.4,1)
    mbtts=p["btts_yes"]; hbtts=(fh["btts_pct"]+fa["btts_pct"])/2
    bbtts=round(mbtts*0.6+hbtts*0.4,1)

    if bo25>=65:
        I.append(f"**Over 2.5 Goals:** {_pct_label(bo25).capitalize()} at ~{bo25}% (model {mo25}%, history {ho25:.0f}%). Combined xG {txg:.2f} supports high scoring.")
    elif bo25<=38:
        I.append(f"**Under 2.5:** Lean toward low scoring (~{bo25}% over). Combined xG only {txg:.2f}.")
    else:
        I.append(f"**Goals Total:** Balanced at ~{bo25}% over 2.5 (model {mo25}%, history {ho25:.0f}%). No strong lean.")

    if bbtts>=60:
        I.append(f"**BTTS Yes:** {_pct_label(bbtts).capitalize()} at ~{bbtts}%. Both attacks productive enough for goals from each side.")
    elif bbtts<=35:
        weak = home if fh["avg_goals_for"]<fa["avg_goals_for"] else away
        wavg = min(fh["avg_goals_for"],fa["avg_goals_for"])
        I.append(f"**BTTS No:** ~{bbtts}% blended. {weak}'s low rate ({wavg}/game) is the factor.")

    # Clean sheet
    if p["home_cs"]>=35:
        I.append(f"**{home} Clean Sheet:** {p['home_cs']}% (keeps CS in {fh['clean_sheet_pct']}% of recent).")
    if p["away_cs"]>=35:
        I.append(f"**{away} Clean Sheet:** {p['away_cs']}% (keeps CS in {fa['clean_sheet_pct']}% of recent).")

    # 5. Venue analysis
    fhp = fhv["percentage"] if fhv["matches_played"]>0 else 0
    fap = fav["percentage"] if fav["matches_played"]>0 else 0
    if fhp>0 and fap>0:
        vgap = fhp-fap
        if vgap>30:
            I.append(f"**Venue Mismatch:** {home} home {fhp}% vs {away} away {fap}% — {vgap:.0f}pp gap *strongly favors {home}*.")
        elif vgap<-20:
            I.append(f"**Strong Traveler:** {away} away form ({fap}%) exceeds {home} home ({fhp}%). *{away} comfortable on the road.*")
    hr = hsplit["home"]
    if hr["played"]>=5:
        hgpg = _sd(hr["gf"],hr["played"])
        if hgpg > fh["avg_goals_for"]*1.2:
            I.append(f"**Home Boost [{home}]:** {hgpg:.2f} goals/game at home vs {fh['avg_goals_for']} overall — *home amplifies attack*.")

    # 6. H2H
    hn = h2h["total_matches"]
    if hn>=3:
        hw2=h2h.get(f"{home}_wins",0); aw2=h2h.get(f"{away}_wins",0)
        havg=h2h["avg_goals"]
        if hw2>aw2*2 and hw2>=3:
            I.append(f"**H2H Dominance:** {home} won {hw2}/{hn} — *strong historical pattern*.")
        elif aw2>hw2*2 and aw2>=3:
            I.append(f"**H2H Dominance:** {away} won {aw2}/{hn} — *historical pattern favors {away}*.")
        if havg>3.0:
            I.append(f"**High-Scoring Fixture:** Averages {havg} goals. O2.5 in {h2h['over_2_5_pct']}% of meetings.")
        elif havg<1.8 and hn>=4:
            I.append(f"**Tight Fixture:** Only {havg} goals avg — *historically cagey*.")

    # 7. Strength of schedule
    if sosh and sosa:
        if sosh["difficulty"]=="easy" and sosa["difficulty"]=="hard":
            I.append(f"**Schedule Context:** {home}'s form vs easier opponents (#{sosh['avg_opp_position']}), {away} vs tougher (#{sosa['avg_opp_position']}). *{away}'s form may be underrated.*")
        elif sosa["difficulty"]=="easy" and sosh["difficulty"]=="hard":
            I.append(f"**Schedule Context:** {away}'s form vs weaker opponents (#{sosa['avg_opp_position']}), {home} vs stronger (#{sosh['avg_opp_position']}). *{home}'s form more credible.*")

    # 8. Defensive analysis
    if fh["avg_goals_against"]>=1.8 and fa["avg_goals_against"]>=1.8:
        I.append(f"**Leaky Defenses:** Both concede heavily ({home}: {fh['avg_goals_against']}/g, {away}: {fa['avg_goals_against']}/g). *Strong O2.5 and BTTS indicator.*")
    elif fh["avg_goals_against"]>=2.0:
        I.append(f"**Defensive Concern [{home}]:** {fh['avg_goals_against']}/game conceded. {away} attack ({a_att:.2f}) should find chances.")
    elif fa["avg_goals_against"]>=2.0:
        I.append(f"**Defensive Concern [{away}]:** {fa['avg_goals_against']}/game conceded. {home} + home advantage = *good attacking platform*.")

    # 9. League position
    if lph and lpa:
        gap = abs(lph["position"]-lpa["position"])
        if gap>=10:
            higher = home if lph["position"]<lpa["position"] else away
            I.append(f"**Table Gap:** {gap} places ({home} #{lph['position']}, {away} #{lpa['position']}). *Favors {higher}.*")
        n_teams = max(lph["position"],lpa["position"],20)
        for t,pos in [(home,lph["position"]),(away,lpa["position"])]:
            if pos<=3: I.append(f"**Title Race [{t}]:** #{pos} — *high motivation*.")
            elif pos>=n_teams-3 and n_teams>=18: I.append(f"**Relegation [{t}]:** #{pos} — *desperation factor*.")

    # 10. Pattern insights
    for pp in (ph or []):
        if pp["type"]=="hot": I.append(f"**Hot [{home}]:** {pp['text']} — *momentum factor*.")
        elif pp["type"]=="warning" and "defensive" in pp["text"].lower(): I.append(f"**Warning [{home}]:** {pp['text']}")
    for pp in (pa or []):
        if pp["type"]=="hot": I.append(f"**Hot [{away}]:** {pp['text']} — *momentum factor*.")
        elif pp["type"]=="warning" and "defensive" in pp["text"].lower(): I.append(f"**Warning [{away}]:** {pp['text']} — *exploitable by {home}*.")

    return "**Analysis & Insights:**\n\n" + "\n\n".join(I)


# ===== WEIGHTED VERDICT =====

def _soft(x, scale=10.0):
    """Sigmoid-ish mapping: converts a signed gap into a 0-1 proportion favouring positive side.
    Returns (home_share, draw_share, away_share) that sum to 1.0.
    Positive x → favours home; negative → favours away; near-zero → draw-ish."""
    import math
    t = max(-1.0, min(1.0, x / scale))  # clamp to [-1,1]
    if t >= 0:
        h = 0.33 + 0.47 * t   # max ~0.80
        a = 0.33 - 0.23 * t   # min ~0.10
    else:
        h = 0.33 + 0.23 * t   # min ~0.10
        a = 0.33 - 0.47 * t   # max ~0.80
    d = 1.0 - h - a
    return (h, d, a)


def _sec_verdict(home, away, xg, sm, fh, fa, fhv, fav, h2h, lph, lpa, sosh, sosa):
    if not xg or not sm:
        return "**Verdict:** Insufficient data."
    p = sm["probs"]
    W = {home: 0.0, away: 0.0, "draw": 0.0}

    def _add(weight, h_share, d_share, a_share):
        W[home] += weight * h_share
        W["draw"] += weight * d_share
        W[away] += weight * a_share

    # Signal 1: Poisson model (weight 3.0) — use actual model probabilities directly
    ptot = p["home_win"] + p["draw"] + p["away_win"]
    if ptot > 0:
        _add(3.0, p["home_win"]/ptot, p["draw"]/ptot, p["away_win"]/ptot)
    else:
        _add(3.0, 0.33, 0.34, 0.33)

    # Signal 2: Form (weight 2.0) — proportional to form gap
    fg = fh["percentage"] - fa["percentage"]  # positive = home better
    sh, sd, sa = _soft(fg, scale=30.0)
    _add(2.0, sh, sd, sa)

    # Signal 3: Venue form (weight 1.5) — home-at-home vs away-at-away
    fhp = fhv["percentage"] if fhv["matches_played"] > 0 else 50
    fap = fav["percentage"] if fav["matches_played"] > 0 else 50
    vg = fhp - fap  # positive = home venue advantage
    sh, sd, sa = _soft(vg, scale=25.0)
    _add(1.5, sh, sd, sa)

    # Signal 4: League position (weight 1.5) — position gap
    if lph and lpa:
        pg = lpa["position"] - lph["position"]  # positive = home higher in table
        sh, sd, sa = _soft(pg, scale=8.0)
        _add(1.5, sh, sd, sa)
    else:
        _add(1.5, 0.33, 0.34, 0.33)

    # Signal 5: H2H (weight 1.0) — historical win ratio
    hn = h2h["total_matches"]
    if hn >= 3:
        hw = h2h.get(f"{home}_wins", 0)
        aw = h2h.get(f"{away}_wins", 0)
        dr = h2h.get("draws", 0)
        htot = hw + aw + dr
        if htot > 0:
            _add(1.0, hw/htot, dr/htot, aw/htot)
        else:
            _add(1.0, 0.33, 0.34, 0.33)
    else:
        _add(1.0, 0.33, 0.34, 0.33)

    # Signal 6: Strength of schedule (weight 1.0)
    if sosh and sosa:
        sos_map = {"hard": 1.0, "moderate": 0.0, "easy": -1.0}
        sg = sos_map.get(sosh["difficulty"], 0) - sos_map.get(sosa["difficulty"], 0)
        sh, sd, sa = _soft(sg, scale=2.0)
        _add(1.0, sh, sd, sa)
    else:
        _add(1.0, 0.33, 0.34, 0.33)

    # Build verdict
    total = W[home] + W[away] + W["draw"]
    hpct = round(W[home] / total * 100, 1) if total else 33.3
    apct = round(W[away] / total * 100, 1) if total else 33.3
    dpct = round(100.0 - hpct - apct, 1)  # ensure they sum to 100

    L = ["**Verdict:**\n"]
    L.append(f"Multi-factor signal weights: {home} {hpct}% | Draw {dpct}% | {away} {apct}%\n")

    gap = abs(hpct - apct)
    if hpct > apct and gap > 25:
        L.append(f"Multiple signals converge on **{home}**. Model probability ({p['home_win']}%), form, venue, "
                 f"and position all point their way. A {home} win is the **most supported outcome**.")
    elif apct > hpct and gap > 25:
        L.append(f"Despite playing away, **{away}** commands the statistical edge across multiple metrics. "
                 f"Model gives them {p['away_win']}% — the data leans their way.")
    elif dpct > max(hpct, apct):
        L.append(f"The signals point to a **closely contested match**. Draw probability is elevated "
                 f"at {p['draw']}%. Neither side has enough of an edge to be confident about a winner.")
    elif gap <= 8:
        L.append(f"This is a **coin-flip match**. The signals are nearly evenly split. "
                 f"A draw or one-goal margin is the most plausible shape.")
    else:
        fav_team = home if hpct > apct else away
        fp = p["home_win"] if fav_team == home else p["away_win"]
        L.append(f"A slight lean toward **{fav_team}** ({fp}%), but the margins are thin and upset potential is real.")

    # Goal verdict
    txg = xg["home_xg"]+xg["away_xg"]
    if txg>3.0:
        L.append(f"\n**Goals:** High expectation ({txg:.2f} combined xG). Over 2.5 at {p['over_2_5']}%.")
    elif txg<2.0:
        L.append(f"\n**Goals:** Low expectation ({txg:.2f} combined xG). Under 2.5 at {round(100-p['over_2_5'],1)}%.")
    else:
        L.append(f"\n**Goals:** Moderate ({txg:.2f} combined xG). Over 2.5 at {p['over_2_5']}%.")

    return "\n".join(L)


# ===== MAIN REPORT GENERATOR =====

def generate_match_report(home_team, away_team, league):
    """Generate a comprehensive match analysis report."""
    # Get v2 analysis
    analysis = v2.generate_full_analysis(home_team, away_team, league)
    form = analysis["form"]
    h2h = analysis["head_to_head"]
    xg = analysis["expected_goals"]
    trends = analysis["goal_trends"]
    lp = analysis["league_position"]
    pats = analysis["patterns"]

    # Extra data
    h_split = _get_home_away_split(home_team)
    a_split = _get_home_away_split(away_team)
    h_seasons = _get_multi_season_record(home_team, league)
    a_seasons = _get_multi_season_record(away_team, league)
    h_traj = _season_trajectory(h_seasons)
    a_traj = _season_trajectory(a_seasons)

    # League table for SOS
    table = v2.get_league_table(league)
    h_sos = _get_strength_of_schedule(home_team, table)
    a_sos = _get_strength_of_schedule(away_team, table)

    # Score matrix
    sm = None
    if xg:
        sm = _compute_score_matrix(xg["home_xg"], xg["away_xg"])

    fh = form["home_overall"]; fa = form["away_overall"]
    fhv = form["home_at_home"]; fav = form["away_at_away"]

    # Build sections
    sec = {}
    sec["h_overview"] = _sec_overview(home_team, fh, fhv, lp["home"], h_split, "Home", h_traj)
    sec["a_overview"] = _sec_overview(away_team, fa, fav, lp["away"], a_split, "Away", a_traj)
    sec["h_scoring"] = _sec_scoring(home_team, fh, trends["home"])
    sec["a_scoring"] = _sec_scoring(away_team, fa, trends["away"])
    sec["h_momentum"] = _sec_momentum(home_team, fh, trends["home"], h_sos)
    sec["a_momentum"] = _sec_momentum(away_team, fa, trends["away"], a_sos)
    sec["h2h"] = _sec_h2h(h2h, home_team, away_team)
    sec["scores"] = _sec_scores(home_team, away_team, sm) if sm else "Score matrix unavailable."
    sec["metrics"] = _sec_metrics(home_team, away_team, xg, fh, fa, fhv, fav)
    sec["patterns"] = _sec_patterns(pats["home"], pats["away"], home_team, away_team)
    sec["insights"] = _sec_insights(
        home_team, away_team, xg, sm, fh, fa, fhv, fav,
        h2h, trends["home"], trends["away"],
        pats["home"], pats["away"], lp["home"], lp["away"],
        h_sos, a_sos, h_split, a_split)
    sec["verdict"] = _sec_verdict(
        home_team, away_team, xg, sm, fh, fa, fhv, fav,
        h2h, lp["home"], lp["away"], h_sos, a_sos)

    # Multi-season summary
    ms_lines = []
    for name, recs in [(home_team, h_seasons), (away_team, a_seasons)]:
        if recs:
            tw=sum(r["wins"] for r in recs); td=sum(r["draws"] for r in recs)
            tl=sum(r["losses"] for r in recs); tgf=sum(r["gf"] for r in recs)
            tga=sum(r["ga"] for r in recs); tp=sum(r["played"] for r in recs)
            ms_lines.append(f"**{name}** across {len(recs)} seasons: {tp} matches — {tw}W {td}D {tl}L, {tgf}GF/{tga}GA")
    sec["multi_season"] = "\n".join(ms_lines) if ms_lines else ""

    # Compose report
    report = f"""# Match Analysis Report
## {home_team} vs {away_team}
### {league}

---

## Team Overview

{sec["h_overview"]}

{sec["a_overview"]}

---

## Scoring Profile

{sec["h_scoring"]}

{sec["a_scoring"]}

---

## Form & Momentum

{sec["h_momentum"]}

{sec["a_momentum"]}

---

## Head-to-Head

{sec["h2h"]}

---

## Score Probabilities

{sec["scores"]}

---

## Key Metrics

{sec["metrics"]}

---

## Detected Patterns

{sec["patterns"]}

---

## Multi-Season Context

{sec["multi_season"]}

---

## Analysis & Insights

{sec["insights"]}

---

{sec["verdict"]}

---
*Report generated from {len(h_seasons)+len(a_seasons)} seasons of data using Poisson modeling, weighted multi-factor analysis, and pattern detection. All outputs are probability-based — not deterministic predictions.*
"""

    return {"report": report, "sections": sec, "analysis": analysis}


# ===== CLI =====
if __name__ == "__main__":
    import sys
    if len(sys.argv)>=4:
        h,a,l = sys.argv[1], sys.argv[2], " ".join(sys.argv[3:])
    else:
        h,a,l = "Arsenal", "Chelsea", "English Premier League"
    r = generate_match_report(h,a,l)
    print(r["report"])
