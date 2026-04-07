"""
Single source of truth for team name normalization, date validation, and timezone handling.
All fetchers and main.py import from here — never duplicate this map.
"""
import re
import datetime
from zoneinfo import ZoneInfo

OTTAWA_TZ = ZoneInfo("America/Toronto")

# ---------------------------------------------------------------------------
# TEAM NAME NORMALIZATION MAP
# Variants → Canonical names. Add new variants HERE, nowhere else.
# ---------------------------------------------------------------------------
TEAM_NAME_NORMALIZATION = {
    # === SPANISH LA LIGA ===
    "Club Atlético de Madrid": "Atlético Madrid",
    "Atletico Madrid": "Atlético Madrid",
    "Real Betis Balompié": "Real Betis",
    "Betis": "Real Betis",
    "Real Sociedad de Fútbol": "Real Sociedad",
    "Sociedad": "Real Sociedad",
    "Levante UD": "Levante",
    "RCD Mallorca": "Mallorca",
    "Real Madrid CF": "Real Madrid",
    "RCD Espanyol de Barcelona": "Espanyol",
    "Espanyol Barcelona": "Espanyol",
    "Espanol": "Espanyol",
    "FC Barcelona": "Barcelona",
    "Athletic Bilbao": "Athletic Bilbao",
    "Ath Bilbao": "Athletic Bilbao",
    "Ath Madrid": "Atlético Madrid",
    "Atl. Madrid": "Atlético Madrid",
    "Alaves": "Deportivo Alavés",
    "Alavés": "Deportivo Alavés",
    "Cadiz": "Cádiz CF",
    "Cadiz CF": "Cádiz CF",
    "Granada CF": "Granada",
    "Vallecano": "Rayo Vallecano",
    "Celta": "Celta Vigo",
    "Vigo": "Celta Vigo",
    "Sevilla FC": "Sevilla",
    "Valencia CF": "Valencia",
    "Villarreal CF": "Villarreal",
    "Getafe CF": "Getafe",
    "Girona FC": "Girona",
    "Osasuna": "CA Osasuna",
    "Las Palmas": "UD Las Palmas",
    "Leganes": "Leganés",
    "Elche CF": "Elche",
    "Almeria": "UD Almería",
    "Almería": "UD Almería",
    "Huesca": "SD Huesca",
    "Eibar": "SD Eibar",
    "Valladolid": "Real Valladolid",
    "Oviedo": "Real Oviedo",

    # === ENGLISH PREMIER LEAGUE ===
    "Brighton": "Brighton and Hove Albion",
    "Leeds": "Leeds United",
    "Manchester Utd": "Manchester United",
    "Newcastle": "Newcastle United",
    "Nottingham": "Nottingham Forest",
    "Sheffield Utd": "Sheffield United",
    "Tottenham": "Tottenham Hotspur",
    "West Ham": "West Ham United",
    "Wolves": "Wolverhampton Wanderers",
    "West Brom": "West Bromwich Albion",
    "Leicester": "Leicester City",
    "Luton": "Luton Town",
    "Ipswich": "Ipswich Town",
    "Norwich": "Norwich City",

    # === GERMAN BUNDESLIGA ===
    "Augsburg": "FC Augsburg",
    "Dortmund": "Borussia Dortmund",
    "B. Monchengladbach": "Borussia Mönchengladbach",
    "Monchengladbach": "Borussia Mönchengladbach",
    "Mönchengladbach": "Borussia Mönchengladbach",
    "Gladbach": "Borussia Mönchengladbach",
    "Ein Frankfurt": "Eintracht Frankfurt",
    "FC Koln": "FC Köln",
    "Hertha": "Hertha Berlin",
    "Hertha BSC": "Hertha Berlin",
    "Leverkusen": "Bayer Leverkusen",
    "Bayer 04 Leverkusen": "Bayer Leverkusen",
    "Bielefeld": "Arminia Bielefeld",
    "Schalke": "Schalke 04",
    "Schalke 04": "Schalke 04",
    "FC Schalke 04": "Schalke 04",
    "RasenBallsport Leipzig": "RB Leipzig",
    "Greuther Furth": "Greuther Fürth",
    "Greuther Fuerth": "Greuther Fürth",
    "SPVGG Greuther Fürth": "Greuther Fürth",
    "Bayern Munich": "Bayern München",
    "Bayern": "Bayern München",
    "FC Bayern Munich": "Bayern München",
    "FC Bayern München": "Bayern München",
    "Wolfsburg": "VfL Wolfsburg",
    "VfL Wolfsburg": "VfL Wolfsburg",
    "Stuttgart": "VfB Stuttgart",
    "Hoffenheim": "TSG Hoffenheim",
    "TSG 1899 Hoffenheim": "TSG Hoffenheim",
    "Freiburg": "SC Freiburg",
    "Union Berlin": "1. FC Union Berlin",
    "Werder Bremen": "SV Werder Bremen",
    "Bremen": "SV Werder Bremen",
    "Mainz": "1. FSV Mainz 05",
    "Mainz 05": "1. FSV Mainz 05",
    "1. FSV Mainz": "1. FSV Mainz 05",
    "Bochum": "VfL Bochum",
    "Darmstadt": "SV Darmstadt 98",
    "Heidenheim": "1. FC Heidenheim",
    "Heidenheim 1846": "1. FC Heidenheim",
    "1. FC Heidenheim 1846": "1. FC Heidenheim",
    "FC Heidenheim": "1. FC Heidenheim",
    "Hamburg": "Hamburger SV",
    "Dusseldorf": "Fortuna Düsseldorf",
    "Fortuna Dusseldorf": "Fortuna Düsseldorf",
    "St Pauli": "FC St. Pauli",
    "St. Pauli": "FC St. Pauli",
    "Hannover": "Hannover 96",
    "Hannover 96": "Hannover 96",
    "Kiel": "Holstein Kiel",
    "Holstein Kiel": "Holstein Kiel",
    "SV Elversberg": "Elversberg",

    # === ITALIAN SERIE A ===
    "Inter": "Inter Milan",
    "FC Internazionale Milano": "Inter Milan",
    "Internazionale": "Inter Milan",
    "AC Milan": "Milan",
    "Milan": "Milan",
    "Verona": "Hellas Verona",
    "Hellas": "Hellas Verona",
    "AS Roma": "Roma",
    "Roma": "Roma",
    "Parma": "Parma",
    "Parma Calcio 1913": "Parma",
    "Lazio": "Lazio",
    "SS Lazio": "Lazio",
    "Napoli": "Napoli",
    "SSC Napoli": "Napoli",
    "Juventus": "Juventus",
    "Juventus FC": "Juventus",
    "Juve": "Juventus",
    "Atalanta": "Atalanta",
    "Atalanta BC": "Atalanta",
    "Fiorentina": "Fiorentina",
    "ACF Fiorentina": "Fiorentina",
    "Torino": "Torino",
    "Torino FC": "Torino",
    "Udinese": "Udinese",
    "Udinese Calcio": "Udinese",
    "Bologna": "Bologna",
    "Bologna FC": "Bologna",
    "Sassuolo": "Sassuolo",
    "Sampdoria": "Sampdoria",
    "Cagliari": "Cagliari",
    "Cagliari Calcio": "Cagliari",
    "Genoa": "Genoa",
    "Genoa CFC": "Genoa",
    "Empoli": "Empoli",
    "Empoli FC": "Empoli",
    "Monza": "Monza",
    "AC Monza": "Monza",
    "Salernitana": "Salernitana",
    "US Salernitana": "Salernitana",
    "Lecce": "Lecce",
    "US Lecce": "Lecce",
    "Spezia": "Spezia",
    "Spezia Calcio": "Spezia",
    "Frosinone": "Frosinone",
    "Frosinone Calcio": "Frosinone",
    "Cremonese": "Cremonese",
    "US Cremonese": "Cremonese",
    "Crotone": "Crotone",
    "FC Crotone": "Crotone",
    "Benevento": "Benevento",
    "Benevento Calcio": "Benevento",
    "Venezia": "Venezia",
    "Venezia FC": "Venezia",
    "Pisa": "Pisa",
    "Pisa SC": "Pisa",
    "Como": "Como",
    "Como 1907": "Como",

    # === FRENCH LIGUE 1 ===
    "Paris Saint Germain": "Paris Saint-Germain",
    "Paris S-G": "Paris Saint-Germain",
    "PSG": "Paris Saint-Germain",
    "Paris SG": "Paris Saint-Germain",
    "Paris": "Paris Saint-Germain",
    "St Etienne": "Saint-Étienne",
    "Saint-Etienne": "Saint-Étienne",
    "AS Saint-Étienne": "Saint-Étienne",
    "Marseille": "Olympique Marseille",
    "OM": "Olympique Marseille",
    "Lyon": "Olympique Lyonnais",
    "Olympique Lyon": "Olympique Lyonnais",
    "OL": "Olympique Lyonnais",
    "Clermont": "Clermont Foot",
    "Clermont Foot 63": "Clermont Foot",
    "Lens": "Lens",
    "RC Lens": "Lens",
    "Lille": "Lille",
    "LOSC Lille": "Lille",
    "Monaco": "Monaco",
    "AS Monaco": "Monaco",
    "Rennes": "Rennes",
    "Stade Rennais": "Rennes",
    "Stade Rennais FC": "Rennes",
    "Nice": "Nice",
    "OGC Nice": "Nice",
    "Strasbourg": "Strasbourg",
    "RC Strasbourg": "Strasbourg",
    "RC Strasbourg Alsace": "Strasbourg",
    "Nantes": "Nantes",
    "FC Nantes": "Nantes",
    "Reims": "Reims",
    "Stade de Reims": "Reims",
    "Montpellier": "Montpellier",
    "Montpellier Hérault": "Montpellier",
    "Montpellier HSC": "Montpellier",
    "Angers": "Angers",
    "Angers SCO": "Angers",
    "Troyes": "Troyes",
    "ESTAC Troyes": "Troyes",
    "Lorient": "Lorient",
    "FC Lorient": "Lorient",
    "Brest": "Brest",
    "Stade Brestois": "Brest",
    "Stade Brestois 29": "Brest",
    "Metz": "Metz",
    "FC Metz": "Metz",
    "Auxerre": "Auxerre",
    "AJ Auxerre": "Auxerre",
    "Ajaccio": "AC Ajaccio",
    "AC Ajaccio": "AC Ajaccio",
    "Le Havre": "Le Havre",
    "Havre AC": "Le Havre",
    "Nimes": "Nîmes",
    "Nîmes": "Nîmes",
    "Nîmes Olympique": "Nîmes",
    "Dijon": "Dijon",
    "Dijon FCO": "Dijon",
    "Bordeaux": "Bordeaux",
    "FC Girondins de Bordeaux": "Bordeaux",
    "Girondins de Bordeaux": "Bordeaux",
    "Sochaux": "Sochaux",
    "FC Sochaux-Montbéliard": "Sochaux",
    "Grenoble": "Grenoble",
    "Grenoble Foot": "Grenoble",
    "Rodez": "Rodez",
    "Rodez Aveyron": "Rodez",

    # === PORTUGUESE ===
    "Sporting Clube de Portugal": "Sporting CP",
    "Sporting Club Portugal": "Sporting CP",
    "Sporting Lisbon": "Sporting CP",
    "Sporting": "Sporting CP",

    # === UEFA / CROSS-LEAGUE VARIANTS (found by DB scan) ===
    "Arsenal FC": "Arsenal",
    "FC Astana": "Astana",
    "Bologna FC 1909": "Bologna",
    "Bournemouth FC": "Bournemouth",
    "RC Celta de Vigo": "Celta Vigo",
    "Chelsea FC": "Chelsea",
    "Cádiz": "Cádiz CF",
    "Hellas Verona FC": "Hellas Verona",
    "Hibernians FC": "Hibernians",
    "Le Havre AC": "Le Havre",
    "FC Levadia Tallinn": "Levadia Tallinn",
    "Liverpool FC": "Liverpool",
    "Manchester City FC": "Manchester City",
    "Olympique de Marseille": "Olympique Marseille",
    "AS Monaco FC": "Monaco",
    "Newcastle United FC": "Newcastle United",
    "Paris Saint-Germain FC": "Paris Saint-Germain",
    "AC Pisa 1909": "Pisa",
    "FC Prishtina": "Prishtina",
    "US Sassuolo Calcio": "Sassuolo",
    "FC St. Pauli 1910": "FC St. Pauli",
    "Tottenham Hotspur FC": "Tottenham Hotspur",
    "1. FC Köln": "FC Köln",
    "Lille OSC": "Lille",
    "Racing Club de Lens": "Lens",
    "AFC Ajax": "Ajax",
    "FK Bodø/Glimt": "Bodø/Glimt",
    "Bodoe/Glimt": "Bodø/Glimt",
    "Qarabağ Ağdam FK": "Qarabağ",
    "PAE Olympiakos SFP": "Olympiakos",
    "Zrinjski": "Zrinjski Mostar",
    "KuPS Kuopio": "KuPS",
}

# Valid date pattern
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def normalize_team(name):
    """Normalize a team name to its canonical form."""
    if not name:
        return name
    return TEAM_NAME_NORMALIZATION.get(name, name)


def validate_date(date_str):
    """Return True if date_str is valid YYYY-MM-DD, False otherwise."""
    return bool(date_str and _DATE_RE.match(date_str))


def fix_date(date_str, season=None, kickoff_epoch=None):
    """Convert a date string to YYYY-MM-DD. Tries multiple strategies:
    1. Already valid → return as-is
    2. Has a kickoff_epoch → derive date from that (Ottawa timezone)
    3. Common bad formats (DD.MM.YYYY, DD/MM/YYYY, DD.MM. HH:MM, etc.) → parse + infer year from season
    Returns a valid YYYY-MM-DD string, or '' if truly unparseable.
    """
    if not date_str:
        # If no date string but we have epoch, derive from that
        if kickoff_epoch:
            return datetime.datetime.fromtimestamp(kickoff_epoch, tz=OTTAWA_TZ).strftime("%Y-%m-%d")
        return ""

    # 1. Already good
    if _DATE_RE.match(date_str):
        return date_str

    # 2. Derive from kickoff epoch if available (most reliable)
    if kickoff_epoch:
        return datetime.datetime.fromtimestamp(kickoff_epoch, tz=OTTAWA_TZ).strftime("%Y-%m-%d")

    # 3. Try common bad formats
    stripped = date_str.strip()

    # DD.MM.YYYY or DD.MM.YYYY HH:MM
    m = re.match(r"(\d{1,2})\.(\d{1,2})\.(\d{4})", stripped)
    if m:
        day, month, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
        try:
            return datetime.date(year, month, day).isoformat()
        except ValueError:
            pass

    # DD/MM/YYYY
    m = re.match(r"(\d{1,2})/(\d{1,2})/(\d{4})", stripped)
    if m:
        day, month, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
        try:
            return datetime.date(year, month, day).isoformat()
        except ValueError:
            pass

    # DD.MM. HH:MM (no year — infer from season like "2024-2025" or "2024-25")
    m = re.match(r"(\d{1,2})\.(\d{1,2})\.\s*(\d{1,2}:\d{2})?", stripped)
    if m:
        day, month = int(m.group(1)), int(m.group(2))
        year = _infer_year_from_season(season, month)
        if year:
            try:
                return datetime.date(year, month, day).isoformat()
            except ValueError:
                pass

    # MM-DD-YYYY (American)
    m = re.match(r"(\d{1,2})-(\d{1,2})-(\d{4})$", stripped)
    if m:
        month, day, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
        try:
            return datetime.date(year, month, day).isoformat()
        except ValueError:
            pass

    return ""


def _infer_year_from_season(season, month):
    """Given a season string like '2024-2025' or '2024-25' and a month, return the likely year."""
    if not season:
        return None
    m = re.match(r"(\d{4})[-/](\d{2,4})", str(season))
    if not m:
        # Try plain year like "2024"
        m2 = re.match(r"(\d{4})$", str(season))
        if m2:
            return int(m2.group(1))
        return None
    start_year = int(m.group(1))
    end_str = m.group(2)
    end_year = int(end_str) if len(end_str) == 4 else int(str(start_year)[:2] + end_str)
    # Aug-Dec = start year, Jan-Jul = end year (typical European season)
    if month >= 8:
        return start_year
    else:
        return end_year


def utc_to_ottawa(utc_str):
    """Parse a UTC datetime string and return (epoch_int, ottawa_date YYYY-MM-DD).
    Handles ISO 8601 formats like '2026-04-06T20:00:00Z' or '2026-04-06T20:00:00+00:00'.
    Returns (None, None) on failure."""
    if not utc_str:
        return None, None
    try:
        dt = datetime.datetime.fromisoformat(utc_str.replace("Z", "+00:00"))
        epoch = int(dt.timestamp())
        ottawa_dt = dt.astimezone(OTTAWA_TZ)
        return epoch, ottawa_dt.strftime("%Y-%m-%d")
    except Exception:
        try:
            dt = datetime.datetime.strptime(utc_str, "%Y-%m-%dT%H:%M:%S")
            epoch = int(dt.timestamp())
            ottawa_dt = dt.replace(tzinfo=datetime.timezone.utc).astimezone(OTTAWA_TZ)
            return epoch, ottawa_dt.strftime("%Y-%m-%d")
        except Exception:
            return None, None


def ottawa_now():
    """Return current datetime in Ottawa timezone."""
    return datetime.datetime.now(tz=OTTAWA_TZ)


def ottawa_today():
    """Return today's date string (YYYY-MM-DD) in Ottawa timezone."""
    return ottawa_now().strftime("%Y-%m-%d")
