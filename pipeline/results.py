"""
pipeline/results.py — Auto-fetch confirmed match results from football-data.org
and feed them into the DB, triggering Elo recalculation.

football-data.org free tier:
  - 10 calls/minute
  - Full FIFA World Cup coverage (competition ID 2000)
  - Header: X-Auth-Token: <your token>

Called automatically by run_pipeline.py on every GitHub Actions run.
Also callable standalone:
  python pipeline/results.py          # fetch and record all finished matches
  python pipeline/results.py --dry    # print what would be recorded, don't write
"""

import json
import logging
import sys
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

# Ensure project root is on the path regardless of working directory
sys.path.insert(0, str(Path(__file__).parent.parent))

import duckdb

from db.schema import get_conn
from pipeline.config import FOOTBALL_DATA_API_KEY, FOOTBALL_DATA_BASE, FOOTBALL_DATA_WC_ID
from pipeline.collector import record_result

log = logging.getLogger(__name__)

# football-data.org team name → our team ID mapping
# Their API uses full country names; we store 3-letter codes
TEAM_NAME_MAP = {
    "Mexico":                  "MEX",
    "South Africa":            "RSA",
    "Korea Republic":          "KOR",
    "Czechia":                 "CZE",
    "Czech Republic":          "CZE",
    "Canada":                  "CAN",
    # Bosnia — every variant seen or plausible from football-data.org
    "Bosnia and Herzegovina":  "BIH",
    "Bosnia & Herzegovina":    "BIH",
    "Bosnia-Herzegovina":      "BIH",
    "Bosnia":                  "BIH",
    "Bosnia Herzegowina":      "BIH",
    "Bosnia i Hercegovina":    "BIH",
    "Bosna i Hercegovina":     "BIH",
    "BIH":                     "BIH",
    "Qatar":                   "QAT",
    "Switzerland":             "SUI",
    "Haiti":                   "HAI",
    "Scotland":                "SCO",
    "Brazil":                  "BRA",
    "Morocco":                 "MAR",
    "United States":           "USA",
    "USA":                     "USA",
    "Paraguay":                "PAR",
    "Australia":               "AUS",
    "Türkiye":                 "TUR",
    "Turkey":                  "TUR",
    "Côte d'Ivoire":           "CIV",
    "Ivory Coast":             "CIV",
    "Ecuador":                 "ECU",
    "Germany":                 "GER",
    "Curaçao":                 "CUW",
    "Curacao":                 "CUW",
    "Netherlands":             "NED",
    "Japan":                   "JPN",
    "Sweden":                  "SWE",
    "Tunisia":                 "TUN",
    "Iran":                    "IRN",
    "IR Iran":                 "IRN",
    "New Zealand":             "NZL",
    "Belgium":                 "BEL",
    "Egypt":                   "EGY",
    "Saudi Arabia":            "SAU",
    "Uruguay":                 "URU",
    "Spain":                   "ESP",
    "Cabo Verde":              "CPV",
    "Cape Verde":              "CPV",
    "France":                  "FRA",
    "Senegal":                 "SEN",
    "Iraq":                    "IRQ",
    "Norway":                  "NOR",
    "Argentina":               "ARG",
    "Algeria":                 "ALG",
    "Austria":                 "AUT",
    "Jordan":                  "JOR",
    "Portugal":                "POR",
    "DR Congo":                "COD",
    "Congo DR":                "COD",
    "Congo":                   "COD",
    "Uzbekistan":              "UZB",
    "Colombia":                "COL",
    "Ghana":                   "GHA",
    "Panama":                  "PAN",
    "England":                 "ENG",
    "Croatia":                 "CRO",
}


def _api_request(path: str) -> dict | list | None:
    """Make an authenticated request to football-data.org."""
    if FOOTBALL_DATA_API_KEY == "YOUR_KEY_HERE":
        log.info("football-data.org: API key not set — skipping results fetch")
        return None

    url = f"{FOOTBALL_DATA_BASE}{path}"
    try:
        req = urllib.request.Request(url, headers={
            "X-Auth-Token": FOOTBALL_DATA_API_KEY,
            "User-Agent": "WC2026Predictor/2.0"
        })
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        log.warning("football-data.org HTTP %d for %s", e.code, url)
        return None
    except Exception as e:
        log.warning("football-data.org request failed: %s", e)
        return None


def _resolve_team_id(api_name: str) -> str | None:
    """Map an API team name to our 3-letter team ID."""
    # Direct lookup
    if api_name in TEAM_NAME_MAP:
        return TEAM_NAME_MAP[api_name]
    # Case-insensitive fallback
    lower = api_name.lower()
    for k, v in TEAM_NAME_MAP.items():
        if k.lower() == lower:
            return v
    log.warning("No team ID mapping for API name: '%s'", api_name)
    return None


def fetch_finished_matches() -> list[dict]:
    """
    Fetch all FINISHED matches from football-data.org for the 2026 WC.
    Returns list of dicts: {home_id, away_id, home_score, away_score, utc_date, api_id}
    """
    data = _api_request(f"/competitions/{FOOTBALL_DATA_WC_ID}/matches?status=FINISHED")
    if not data:
        return []

    matches = data.get("matches", [])
    log.info("football-data.org: raw API returned %d finished match(es)", len(matches))
    for m in matches:
        h = m.get("homeTeam", {}).get("name", "?")
        a = m.get("awayTeam", {}).get("name", "?")
        sc = m.get("score", {}).get("fullTime", {})
        log.info("  API match: '%s' %s–%s '%s'  [id=%s]",
                 h, sc.get("home", "?"), sc.get("away", "?"), a, m.get("id"))

    results = []
    for m in matches:
        home_name = m.get("homeTeam", {}).get("name", "")
        away_name = m.get("awayTeam", {}).get("name", "")
        score = m.get("score", {})
        full = score.get("fullTime", {})
        home_score = full.get("home")
        away_score = full.get("away")

        if home_score is None or away_score is None:
            continue

        home_id = _resolve_team_id(home_name)
        away_id = _resolve_team_id(away_name)
        if not home_id or not away_id:
            log.warning("Unrecognised team name(s): home='%s' (→%s)  away='%s' (→%s)",
                        home_name, home_id, away_name, away_id)
            continue

        results.append({
            "api_id":     m.get("id"),
            "home_id":    home_id,
            "away_id":    away_id,
            "home_score": int(home_score),
            "away_score": int(away_score),
            "utc_date":   m.get("utcDate"),
        })

    log.info("football-data.org: found %d finished matches", len(results))
    return results


def sync_results(conn: duckdb.DuckDBPyConnection, dry_run: bool = False) -> int:
    """
    Fetch finished matches and record any that aren't already in our DB.
    Returns the number of new results recorded.
    """
    finished = fetch_finished_matches()
    if not finished:
        return 0

    # Which fixtures already have a recorded result?
    already_done = set(
        row[0] for row in
        conn.execute("SELECT fixture_id FROM match_results").fetchall()
    )

    # Our fixture index: (home_id, away_id) → fixture_id
    fixture_index = {}
    rows = conn.execute("""
        SELECT fixture_id, home_team, away_team FROM fixtures WHERE stage = 'group'
    """).fetchall()
    for fid, home, away in rows:
        fixture_index[(home, away)] = fid

    new_count = 0
    for m in finished:
        fid = fixture_index.get((m["home_id"], m["away_id"]))
        reversed_lookup = False
        if not fid:
            # Try reversed home/away — API may assign neutral-site matches differently
            fid = fixture_index.get((m["away_id"], m["home_id"]))
            if fid:
                reversed_lookup = True
                log.warning("Fixture found with reversed home/away: API has %s(home) vs %s(away) "
                            "but our DB has them swapped — using fixture %d",
                            m["home_id"], m["away_id"], fid)
        if not fid:
            log.warning("No fixture found for %s vs %s (checked both orderings)",
                        m["home_id"], m["away_id"])
            continue
        if fid in already_done:
            log.debug("Result already recorded for fixture %d", fid)
            continue

        log.info(
            "Recording: fixture %d  %s %d–%d %s",
            fid, m["home_id"], m["home_score"], m["away_score"], m["away_id"]
        )
        if not dry_run:
            record_result(conn, fid, m["home_score"], m["away_score"])
        new_count += 1

    if dry_run:
        log.info("Dry run — %d results would be recorded", new_count)
    else:
        log.info("Synced %d new results", new_count)

    return new_count


def print_live_scores() -> None:
    """Print currently live or today's scheduled matches (useful for debugging)."""
    data = _api_request(f"/competitions/{FOOTBALL_DATA_WC_ID}/matches?status=LIVE")
    if not data:
        print("No live matches (or API key not set)")
        return
    matches = data.get("matches", [])
    if not matches:
        print("No matches currently live")
        return
    for m in matches:
        h = m.get("homeTeam", {}).get("name", "?")
        a = m.get("awayTeam", {}).get("name", "?")
        sc = m.get("score", {}).get("fullTime", {})
        print(f"  {h} {sc.get('home','?')} – {sc.get('away','?')} {a}")


if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                        datefmt="%H:%M:%S")
    parser = argparse.ArgumentParser(description="Sync WC match results from football-data.org")
    parser.add_argument("--dry", action="store_true", help="Print what would be recorded, don't write")
    parser.add_argument("--live", action="store_true", help="Show live scores and exit")
    args = parser.parse_args()

    if args.live:
        print_live_scores()
    else:
        conn = get_conn()
        n = sync_results(conn, dry_run=args.dry)
        conn.close()
        print(f"\n{'Would record' if args.dry else 'Recorded'} {n} new result(s)")
