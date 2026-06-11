"""
pipeline/collector.py — Scheduled data ingestion. Appends to DB; never overwrites.

Elo-only mode (current):
  - Seeds Elo ratings from teams.json on first run
  - Polls eloratings.net for updates on subsequent runs (gracefully falls back)
  - Records match results when games finish

Odds mode (slot in when ODDS_API_KEY is set):
  - Polls The-Odds-API for live 3-way moneyline odds
  - Appends a new odds_snapshot row for every bookmaker/fixture pair

Run:
  python pipeline/collector.py
"""

import json
import logging
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from db.schema import get_conn, init_schema, seed_static_data
from pipeline.config import (ODDS_API_KEY, ODDS_API_URL, ODDS_MARKETS, ODDS_REGIONS, ROOT,
                             FOOTBALL_DATA_API_KEY, FOOTBALL_DATA_BASE, FOOTBALL_DATA_WC_ID)

log = logging.getLogger(__name__)


# ── Elo collection ────────────────────────────────────────────────────────────

def collect_elo(conn: duckdb.DuckDBPyConnection) -> int:
    """
    Fetch current Elo ratings and append a snapshot row for each team.
    Falls back to seed values if network is unavailable.
    Returns number of rows inserted.
    """
    now = datetime.now(timezone.utc)
    teams = conn.execute("SELECT team_id, name, seed_elo FROM teams").fetchall()
    if not teams:
        log.warning("No teams in DB — run setup_db.py first")
        return 0

    # Try live fetch
    elo_map = _fetch_live_elo()
    source = "eloratings" if elo_map else "seed"

    rows = []
    for team_id, name, seed_elo in teams:
        elo = _resolve_elo(team_id, name, elo_map, seed_elo)
        rows.append((team_id, now, elo, source))

    conn.executemany("""
        INSERT INTO elo_snapshots (team_id, collected_at, elo_value, source)
        VALUES (?, ?, ?, ?)
    """, rows)
    conn.commit()

    log.info("Elo: inserted %d snapshots (source=%s)", len(rows), source)
    return len(rows)


def _fetch_live_elo() -> dict:
    """
    Fetch current national team Elo ratings from clubelo.com.
    Uses the date endpoint which is more reliable than /Nationals.
    Returns dict of {club_name: elo_value}.
    """
    from datetime import date
    today = date.today().strftime("%Y-%m-%d")
    urls = [
        f"http://api.clubelo.com/{today}",
        "http://api.clubelo.com/Nationals",
    ]
    for url in urls:
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "WC2026Predictor/2.0"})
            with urllib.request.urlopen(req, timeout=12) as resp:
                lines = resp.read().decode("utf-8").splitlines()
            ratings = {}
            for line in lines[1:]:
                parts = line.strip().split(",")
                if len(parts) >= 4:
                    try:
                        # Format: Rank,Club,Country,Level,Elo,...
                        club = parts[1].strip()
                        elo  = float(parts[4].strip())
                        ratings[club] = elo
                    except (ValueError, IndexError):
                        pass
            if ratings:
                log.info("Live Elo: fetched %d ratings from %s", len(ratings), url)
                return ratings
        except Exception as e:
            log.debug("Live Elo fetch failed (%s): %s", url, e)
    log.warning("Live Elo: all endpoints failed — falling back to seed values")
    return {}


def _resolve_elo(team_id: str, name: str, elo_map: dict, seed_elo: float) -> float:
    """Match a team to live Elo data using multiple name formats."""
    for key in [name, team_id, name.upper(), name.lower(),
                name.replace(" ", "_"), name.split()[0] if " " in name else name]:
        if key in elo_map:
            return elo_map[key]
    return float(seed_elo or 1700)


# ── Odds collection ───────────────────────────────────────────────────────────

def collect_odds(conn: duckdb.DuckDBPyConnection) -> int:
    """
    Fetch live 3-way odds from The-Odds-API and append snapshot rows.
    No-op (returns 0) if ODDS_API_KEY is not configured.
    """
    if ODDS_API_KEY == "YOUR_KEY_HERE":
        log.info("Odds: ODDS_API_KEY not set — skipping")
        return 0

    now = datetime.now(timezone.utc)
    url = (f"{ODDS_API_URL}?apiKey={ODDS_API_KEY}"
           f"&regions={ODDS_REGIONS}&markets={ODDS_MARKETS}&oddsFormat=decimal")

    try:
        req = urllib.request.Request(url, headers={"User-Agent": "WC2026Predictor/2.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            events = json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        log.warning("Odds fetch failed: %s", e)
        return 0

    # Load fixture schedule for fuzzy matching
    fixtures = conn.execute("""
        SELECT f.fixture_id, t1.name AS home_name, t2.name AS away_name
        FROM fixtures f
        JOIN teams t1 ON f.home_team = t1.team_id
        JOIN teams t2 ON f.away_team = t2.team_id
        WHERE f.status = 'scheduled'
    """).fetchall()

    rows_inserted = 0
    for event in events:
        fixture_id = _match_fixture(event, fixtures)
        if not fixture_id:
            continue

        for bm in event.get("bookmakers", []):
            for market in bm.get("markets", []):
                if market["key"] != "h2h":
                    continue
                outcomes = {o["name"]: o["price"] for o in market["outcomes"]}
                h_odds = outcomes.get(event["home_team"])
                d_odds = outcomes.get("Draw")
                a_odds = outcomes.get(event["away_team"])
                if not (h_odds and d_odds and a_odds):
                    continue

                h_imp = 1 / h_odds
                d_imp = 1 / d_odds
                a_imp = 1 / a_odds

                conn.execute("""
                    INSERT INTO odds_snapshots
                      (fixture_id, bookmaker, collected_at,
                       home_odds, draw_odds, away_odds,
                       home_implied, draw_implied, away_implied)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (fixture_id, bm["key"], now,
                      h_odds, d_odds, a_odds,
                      h_imp, d_imp, a_imp))
                rows_inserted += 1

    conn.commit()
    log.info("Odds: inserted %d snapshots", rows_inserted)
    return rows_inserted


# Map of common Odds-API / bookmaker spellings → canonical DB name fragment.
# Keys are lowercased; values must match a substring of the DB team name (lowercased).
_NAME_ALIASES: dict[str, str] = {
    # The Odds API common-English name  →  FIFA/DB name fragment
    "south korea":        "korea republic",
    "korea":              "korea republic",
    "turkey":             "türkiye",
    "turkiye":            "türkiye",
    "ivory coast":        "côte d'ivoire",
    "cote d'ivoire":      "côte d'ivoire",
    "cote divoire":       "côte d'ivoire",
    "cape verde":         "cabo verde",
    "cape verde islands": "cabo verde",
    "dr congo":           "congo dr",
    "democratic republic of congo": "congo dr",
    "congo":              "congo dr",
    "czech republic":     "czechia",
    "curacao":            "curaçao",
    "usa":                "usa",
    "united states":      "usa",
    "trinidad & tobago":  "trinidad and tobago",
}


def _normalise(name: str) -> str:
    """Lower-case + apply alias map so bookmaker names match DB names."""
    n = name.lower().strip()
    return _NAME_ALIASES.get(n, n)


def _match_fixture(event: dict, fixtures: list) -> int | None:
    """Fuzzy-match an API event to a fixture_id by team names."""
    h = _normalise(event.get("home_team", ""))
    a = _normalise(event.get("away_team", ""))
    for fid, hname, aname in fixtures:
        hn = hname.lower()
        an = aname.lower()
        if (hn in h or h in hn) and (an in a or a in an):
            return fid
    return None


# ── Results collection ────────────────────────────────────────────────────────

def record_result(conn: duckdb.DuckDBPyConnection,
                  fixture_id: int, home_score: int, away_score: int) -> None:
    """
    Record a confirmed match result and update fixture status.
    Also updates the seed Elo in teams table based on the actual outcome.
    Called manually or via a future results-API integration.
    """
    now = datetime.now(timezone.utc)
    result = ("home_win" if home_score > away_score
              else "away_win" if away_score > home_score
              else "draw")

    conn.execute("""
        INSERT OR REPLACE INTO match_results
          (fixture_id, home_score, away_score, result, confirmed_at)
        VALUES (?, ?, ?, ?, ?)
    """, (fixture_id, home_score, away_score, result, now))

    conn.execute("""
        UPDATE fixtures SET status = 'finished',
               home_score = ?, away_score = ?
        WHERE fixture_id = ?
    """, (home_score, away_score, fixture_id))

    conn.commit()
    log.info("Result recorded: fixture %d  %d–%d (%s)", fixture_id, home_score, away_score, result)

    # Update Elo ratings based on result (closes the feedback loop)
    _update_elo_from_result(conn, fixture_id, result)


def _update_elo_from_result(conn: duckdb.DuckDBPyConnection,
                            fixture_id: int, result: str) -> None:
    """
    Recalculate Elo ratings using the actual match outcome and append new snapshots.
    Uses standard K=32 Elo update formula.
    """
    row = conn.execute("""
        SELECT f.home_team, f.away_team,
               e1.elo_value AS elo_h, e2.elo_value AS elo_a
        FROM fixtures f
        JOIN (SELECT team_id, elo_value FROM elo_snapshots
              WHERE team_id IN (SELECT home_team FROM fixtures WHERE fixture_id = ?)
              ORDER BY collected_at DESC LIMIT 1) e1 ON f.home_team = e1.team_id
        JOIN (SELECT team_id, elo_value FROM elo_snapshots
              WHERE team_id IN (SELECT away_team FROM fixtures WHERE fixture_id = ?)
              ORDER BY collected_at DESC LIMIT 1) e2 ON f.away_team = e2.team_id
        WHERE f.fixture_id = ?
    """, (fixture_id, fixture_id, fixture_id)).fetchone()

    if not row:
        return

    home_id, away_id, elo_h, elo_a = row
    expected_h = 1 / (1 + 10 ** ((elo_a - elo_h) / 400))
    expected_a = 1 - expected_h
    actual_h   = 1.0 if result == "home_win" else 0.5 if result == "draw" else 0.0
    actual_a   = 1.0 - actual_h

    K = 32
    new_elo_h = elo_h + K * (actual_h - expected_h)
    new_elo_a = elo_a + K * (actual_a - expected_a)

    now = datetime.now(timezone.utc)
    conn.executemany("""
        INSERT INTO elo_snapshots (team_id, collected_at, elo_value, source)
        VALUES (?, ?, ?, 'calculated')
    """, [(home_id, now, new_elo_h), (away_id, now, new_elo_a)])
    conn.commit()
    log.info("Elo updated: %s %.0f→%.0f  %s %.0f→%.0f",
             home_id, elo_h, new_elo_h, away_id, elo_a, new_elo_a)


# ── Auto results ingestion ────────────────────────────────────────────────────

def collect_results(conn: duckdb.DuckDBPyConnection) -> int:
    """
    Fetch completed WC 2026 match scores from football-data.org and record
    any results not yet in the DB. Each new result triggers an Elo update.
    No-op if FOOTBALL_DATA_API_KEY is not set.
    """
    if FOOTBALL_DATA_API_KEY == "YOUR_KEY_HERE":
        log.info("Results: FOOTBALL_DATA_API_KEY not set — skipping")
        return 0

    url = f"{FOOTBALL_DATA_BASE}/competitions/{FOOTBALL_DATA_WC_ID}/matches?status=FINISHED"
    req = urllib.request.Request(url, headers={
        "X-Auth-Token": FOOTBALL_DATA_API_KEY,
        "User-Agent": "WC2026Predictor/2.0"
    })
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        log.warning("Results fetch failed: %s", e)
        return 0

    matches = data.get("matches", [])
    log.info("Results: %d finished matches from API", len(matches))

    # Load fixtures for name matching
    fixtures = conn.execute("""
        SELECT f.fixture_id, t1.name AS home_name, t2.name AS away_name
        FROM fixtures f
        JOIN teams t1 ON f.home_team = t1.team_id
        JOIN teams t2 ON f.away_team = t2.team_id
    """).fetchall()

    # Already-recorded fixture IDs
    recorded = {r[0] for r in conn.execute(
        "SELECT DISTINCT fixture_id FROM match_results"
    ).fetchall()}

    new_results = 0
    for m in matches:
        score = m.get("score", {})
        full  = score.get("fullTime", {})
        hs, as_ = full.get("home"), full.get("away")
        if hs is None or as_ is None:
            continue

        # Match by team names using same alias logic as odds
        home_api = m.get("homeTeam", {}).get("name", "")
        away_api = m.get("awayTeam", {}).get("name", "")
        fake_event = {"home_team": home_api, "away_team": away_api}
        fid = _match_fixture(fake_event, fixtures)
        if not fid:
            log.warning("Results: no fixture match for %s vs %s", home_api, away_api)
            continue
        if fid in recorded:
            continue

        record_result(conn, fid, int(hs), int(as_))
        recorded.add(fid)
        new_results += 1
        log.info("Auto-recorded result: fixture %d  %s %d–%d %s",
                 fid, home_api, hs, as_, away_api)

    return new_results


# ── Entry point ───────────────────────────────────────────────────────────────

def run_collection() -> dict:
    """Full collection pass — called by GitHub Actions on every scheduled run."""
    conn = get_conn()
    init_schema(conn)
    seed_static_data(conn)

    elo_rows    = collect_elo(conn)
    odds_rows   = collect_odds(conn)
    result_rows = collect_results(conn)

    conn.close()
    return {"elo_snapshots": elo_rows, "odds_snapshots": odds_rows, "results": result_rows}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                        datefmt="%H:%M:%S")
    result = run_collection()
    print(f"\nCollection complete: {result}")
