"""
Pipeline configuration — edit ODDS_API_KEY before running.
Get a free key at https://the-odds-api.com (500 req/month free tier).
"""

import os
from pathlib import Path

# ── Paths ─────────────────────────────────────────────────────────────────────
ROOT      = Path(__file__).parent.parent
DATA_DIR  = ROOT / "data"
FIXTURES  = DATA_DIR / "fixtures.json"
TEAMS     = DATA_DIR / "teams.json"
OUTPUT    = DATA_DIR / "predictions.json"
ELO_CACHE = DATA_DIR / "elo_cache.json"

# ── API Keys ──────────────────────────────────────────────────────────────────
# The-Odds-API free tier: 500 requests/month
# Docs: https://the-odds-api.com/liveapi/guides/v4/
ODDS_API_KEY = os.getenv("ODDS_API_KEY", "YOUR_KEY_HERE")
ODDS_API_URL = "https://api.the-odds-api.com/v4/sports/soccer_fifa_world_cup/odds"

# football-data.org free tier — live scores, fixtures, results for FIFA WC
# Register at https://www.football-data.org/client/register
# Set env var FOOTBALL_DATA_API_KEY or add your token below
FOOTBALL_DATA_API_KEY = os.getenv("FOOTBALL_DATA_API_KEY", "YOUR_KEY_HERE")
FOOTBALL_DATA_BASE    = "https://api.football-data.org/v4"
FOOTBALL_DATA_WC_ID   = 2000  # FIFA World Cup competition ID

# ── Elo source ─────────────────────────────────────────────────────────────────
# eloratings.net provides CSV of national team Elo scores
ELO_URL = "http://api.clubelo.com/Nationals"

# ── Model parameters ──────────────────────────────────────────────────────────
# Home advantage (neutral venue at WC — use 0 or small bump for "host region")
HOME_ADVANTAGE_ELO = 0

# Value flag threshold: flag when |model_prob - market_prob| > this
VALUE_THRESHOLD = 0.05   # 5%

# Draw adjustment — Elo doesn't natively produce draw probability.
# We use a simplified Dixon-Coles style split: P(draw) ≈ base × f(Δelo)
DRAW_BASE = 0.265        # historical WC draw rate ~26-28%

# Markets to pull from the-odds-api (h2h = 3-way moneyline)
ODDS_MARKETS = "h2h"
ODDS_REGIONS = "us,uk,eu"

# ── Schedule ──────────────────────────────────────────────────────────────────
# How many days ahead to fetch odds for
LOOKAHEAD_DAYS = 7
