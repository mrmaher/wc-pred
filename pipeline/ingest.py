"""
ingest.py — Pull fixtures, Elo ratings, and betting odds from external sources.

Sources:
  1. fixtures.json      — openfootball-style seed (bundled, refreshed manually)
  2. eloratings.net     — live national team Elo scores via CSV
  3. the-odds-api.com   — 3-way moneyline odds (free tier: 500 req/month)

Run standalone:
    python pipeline/ingest.py
"""

import json
import logging
import time
import urllib.request
import urllib.error
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from pipeline.config import (
    FIXTURES, TEAMS, ELO_CACHE, ELO_URL,
    ODDS_API_KEY, ODDS_API_URL, ODDS_MARKETS, ODDS_REGIONS, LOOKAHEAD_DAYS
)

log = logging.getLogger(__name__)


# ── Fixtures ───────────────────────────────────────────────────────────────────

def load_fixtures() -> list[dict]:
    """Load group-stage fixture schedule from seed file."""
    with open(FIXTURES) as f:
        return json.load(f)["fixtures"]


def load_teams() -> dict[str, dict]:
    """Load team metadata (name, group, seed Elo). Keyed by team ID."""
    with open(TEAMS) as f:
        raw = json.load(f)["teams"]
    return {t["id"]: t for t in raw}


# ── Elo Ratings ───────────────────────────────────────────────────────────────

def fetch_elo_ratings(use_cache: bool = True) -> dict[str, float]:
    """
    Fetch current national team Elo ratings.

    Strategy:
      1. Try eloratings.net API (club-elo.com nationals endpoint)
      2. Fall back to ELO_CACHE if network unavailable
      3. Fall back to seed Elo values in teams.json
    """
    if ELO_CACHE.exists() and use_cache:
        cache = json.loads(ELO_CACHE.read_text())
        age_hours = (time.time() - cache.get("fetched_at", 0)) / 3600
        if age_hours < 12:
            log.info("Elo: using cache (%.1fh old)", age_hours)
            return cache["ratings"]

    log.info("Elo: fetching from %s", ELO_URL)
    try:
        req = urllib.request.Request(ELO_URL, headers={"User-Agent": "WCPredictor/1.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            lines = resp.read().decode("utf-8").splitlines()

        ratings = {}
        for line in lines[1:]:  # skip header
            parts = line.strip().split(",")
            if len(parts) >= 3:
                club = parts[1].strip()
                try:
                    elo = float(parts[2].strip())
                    ratings[club] = elo
                except ValueError:
                    pass

        if ratings:
            cache = {"fetched_at": time.time(), "ratings": ratings}
            ELO_CACHE.write_text(json.dumps(cache, indent=2))
            log.info("Elo: fetched %d teams", len(ratings))
            return ratings

    except Exception as e:
        log.warning("Elo: fetch failed (%s), falling back to seed data", e)

    # Fallback: seed Elo from teams.json
    teams = load_teams()
    return {tid: t["elo"] for tid, t in teams.items()}


def resolve_elo(team_id: str, elo_data: dict[str, float], teams: dict[str, dict]) -> float:
    """
    Find Elo for a team_id, trying multiple name formats.
    Falls back to seed Elo in teams.json.
    """
    team = teams.get(team_id, {})
    name = team.get("name", team_id)

    # Try exact name match, then common aliases
    for key in [name, team_id, name.lower(), name.upper()]:
        if key in elo_data:
            return elo_data[key]

    return float(team.get("elo", 1700))


# ── Betting Odds ───────────────────────────────────────────────────────────────

def fetch_odds(api_key: str = ODDS_API_KEY) -> list[dict]:
    """
    Fetch live 3-way moneyline odds from The-Odds-API.

    Returns list of events with structured home/draw/away odds.
    Falls back to [] if API key not set or network unavailable.

    Docs: https://the-odds-api.com/liveapi/guides/v4/
    """
    if api_key == "YOUR_KEY_HERE":
        log.warning("Odds: ODDS_API_KEY not set — skipping live odds")
        return []

    today = datetime.utcnow()
    cutoff = (today + timedelta(days=LOOKAHEAD_DAYS)).strftime("%Y-%m-%dT%H:%M:%SZ")

    url = (
        f"{ODDS_API_URL}"
        f"?apiKey={api_key}"
        f"&regions={ODDS_REGIONS}"
        f"&markets={ODDS_MARKETS}"
        f"&oddsFormat=decimal"
        f"&commenceTimeTo={cutoff}"
    )

    log.info("Odds: fetching from the-odds-api.com")
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "WCPredictor/1.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            raw = json.loads(resp.read().decode("utf-8"))

        events = []
        for game in raw:
            event = {
                "home_team": game.get("home_team", ""),
                "away_team": game.get("away_team", ""),
                "commence_time": game.get("commence_time", ""),
                "bookmakers": []
            }
            for bm in game.get("bookmakers", []):
                for market in bm.get("markets", []):
                    if market["key"] == "h2h":
                        outcomes = {o["name"]: o["price"] for o in market["outcomes"]}
                        event["bookmakers"].append({
                            "name": bm["key"],
                            "home": outcomes.get(game["home_team"], None),
                            "away": outcomes.get(game["away_team"], None),
                            "draw": outcomes.get("Draw", None)
                        })
            events.append(event)

        log.info("Odds: received %d events", len(events))
        return events

    except Exception as e:
        log.warning("Odds: fetch failed (%s)", e)
        return []


def match_odds_to_fixture(fixture: dict, odds_events: list[dict], teams: dict[str, dict]) -> Optional[dict]:
    """
    Find the odds event that matches a fixture (fuzzy team name match).
    Returns best-consensus odds dict or None.
    """
    home_name = teams.get(fixture["home"], {}).get("name", fixture["home"]).lower()
    away_name = teams.get(fixture["away"], {}).get("name", fixture["away"]).lower()

    for event in odds_events:
        h = event["home_team"].lower()
        a = event["away_team"].lower()
        if (home_name in h or h in home_name) and (away_name in a or a in away_name):
            return _consensus_odds(event["bookmakers"])

    return None


def _consensus_odds(bookmakers: list[dict]) -> Optional[dict]:
    """Average decimal odds across all available bookmakers (consensus line)."""
    h_odds, d_odds, a_odds = [], [], []
    for bm in bookmakers:
        if bm.get("home"): h_odds.append(bm["home"])
        if bm.get("draw"): d_odds.append(bm["draw"])
        if bm.get("away"): a_odds.append(bm["away"])

    if not (h_odds and d_odds and a_odds):
        return None

    return {
        "home": sum(h_odds) / len(h_odds),
        "draw": sum(d_odds) / len(d_odds),
        "away": sum(a_odds) / len(a_odds),
        "bookmaker_count": len(bookmakers)
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    teams = load_teams()
    elo   = fetch_elo_ratings()
    odds  = fetch_odds()
    fixtures = load_fixtures()

    print(f"Teams loaded: {len(teams)}")
    print(f"Elo ratings: {len(elo)}")
    print(f"Odds events: {len(odds)}")
    print(f"Fixtures:    {len(fixtures)}")
