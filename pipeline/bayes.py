"""
pipeline/bayes.py — Bayesian time-weighted probability engine.

Core idea:
  The Elo model gives us a prior for each match at the time of the draw.
  Every odds snapshot is a market observation that updates that prior.
  More recent observations get higher weight via exponential decay.
  The half-life of that decay shrinks as kickoff approaches —
  so the model becomes more market-driven closer to the match.

Weighting scheme:
  weight(obs) = exp(−ln(2) × age_hours / half_life)

  Adaptive half-life based on hours-to-kickoff:
    > 168h (7+ days)   → 48h   slow decay, history matters
    24–168h (1–7 days) → 12h   medium decay
    < 24h (match day)  →  2h   fast decay, very recent odds dominate

  Elo prior has a fixed weight equivalent to PRIOR_WEIGHT observations,
  so the model is never fully swamped by a single market spike.

  Final probability = weighted average of (Elo prior + all market observations)
  Confidence score  = market_weight_sum / (prior_weight + market_weight_sum)
                    → 0 means pure Elo, 1 means fully market-informed
"""

import math
import logging
from datetime import datetime, timezone
from typing import NamedTuple

import duckdb

from pipeline.model import elo_probabilities
from pipeline.config import HOME_ADVANTAGE_ELO, DRAW_BASE

log = logging.getLogger(__name__)

# Fixed weight assigned to the Elo prior (equivalent to N market observations)
PRIOR_WEIGHT = 5.0


class MatchProb(NamedTuple):
    home_win:        float
    draw:            float
    away_win:        float
    elo_weight:      float
    market_weight:   float
    n_observations:  int
    half_life_hours: float
    confidence:      float


def adaptive_half_life(hours_to_kickoff: float) -> float:
    """Return the decay half-life in hours based on time remaining."""
    if hours_to_kickoff > 168:
        return 48.0
    elif hours_to_kickoff > 24:
        return 12.0
    else:
        return 2.0


def obs_weight(age_hours: float, half_life: float) -> float:
    """Exponential decay weight for an observation that is `age_hours` old."""
    if half_life <= 0:
        return 1.0
    return math.exp(-math.log(2) * age_hours / half_life)


def compute_bayesian_prob(
    fixture_id: int,
    conn: duckdb.DuckDBPyConnection,
    now: datetime | None = None
) -> MatchProb:
    """
    Compute Bayesian time-weighted probabilities for a single fixture.

    Steps:
      1. Get current Elo ratings for both teams (latest snapshot)
      2. Compute Elo prior probabilities
      3. Load all odds snapshots for this fixture
      4. Compute exponential decay weights based on kickoff proximity
      5. Blend prior + observations into a weighted average
      6. Return normalized MatchProb with metadata
    """
    if now is None:
        now = datetime.now(timezone.utc)

    # ── 1. Elo prior ─────────────────────────────────────────────────────────
    row = conn.execute("""
        SELECT f.home_team, f.away_team,
               COALESCE(e_h.elo_value, t_h.seed_elo, 1700) AS elo_h,
               COALESCE(e_a.elo_value, t_a.seed_elo, 1700) AS elo_a,
               f.match_date
        FROM fixtures f
        JOIN teams t_h ON f.home_team = t_h.team_id
        JOIN teams t_a ON f.away_team = t_a.team_id
        LEFT JOIN LATERAL (
            SELECT elo_value FROM elo_snapshots
            WHERE team_id = f.home_team ORDER BY collected_at DESC LIMIT 1
        ) e_h ON TRUE
        LEFT JOIN LATERAL (
            SELECT elo_value FROM elo_snapshots
            WHERE team_id = f.away_team ORDER BY collected_at DESC LIMIT 1
        ) e_a ON TRUE
        WHERE f.fixture_id = ?
    """, (fixture_id,)).fetchone()

    if not row:
        raise ValueError(f"Fixture {fixture_id} not found in DB")

    home_id, away_id, elo_h, elo_a, match_date = row
    prior = elo_probabilities(elo_h, elo_a, HOME_ADVANTAGE_ELO, DRAW_BASE)

    # ── 2. Time to kickoff ────────────────────────────────────────────────────
    if match_date:
        # match_date is a date; assume 15:00 UTC kickoff for decay calc
        kickoff = datetime(match_date.year, match_date.month, match_date.day,
                           15, 0, 0, tzinfo=timezone.utc)
        hours_to_kickoff = max(0.0, (kickoff - now).total_seconds() / 3600)
    else:
        hours_to_kickoff = 999.0   # unknown future match

    half_life = adaptive_half_life(hours_to_kickoff)

    # ── 3. Odds snapshots ─────────────────────────────────────────────────────
    snapshots = conn.execute("""
        SELECT collected_at, home_implied, draw_implied, away_implied
        FROM odds_snapshots
        WHERE fixture_id = ?
        ORDER BY collected_at ASC
    """, (fixture_id,)).fetchall()

    # ── 4. Weighted blend ─────────────────────────────────────────────────────
    # Accumulate weighted sums: start with Elo prior at fixed weight
    w_sum      = PRIOR_WEIGHT
    h_sum      = PRIOR_WEIGHT * prior["home_win"]
    d_sum      = PRIOR_WEIGHT * prior["draw"]
    a_sum      = PRIOR_WEIGHT * prior["away_win"]
    market_w   = 0.0

    for collected_at, h_imp, d_imp, a_imp in snapshots:
        if None in (h_imp, d_imp, a_imp):
            continue

        # Remove vig from this snapshot
        total = h_imp + d_imp + a_imp
        if total <= 0:
            continue
        h_clean = h_imp / total
        d_clean = d_imp / total
        a_clean = a_imp / total

        # Age of this observation in hours
        if hasattr(collected_at, "tzinfo") and collected_at.tzinfo:
            age_hours = max(0.0, (now - collected_at).total_seconds() / 3600)
        else:
            age_hours = 0.0

        w = obs_weight(age_hours, half_life)
        w_sum    += w
        h_sum    += w * h_clean
        d_sum    += w * d_clean
        a_sum    += w * a_clean
        market_w += w

    # ── 5. Normalize and compute metadata ────────────────────────────────────
    h_final = h_sum / w_sum
    d_final = d_sum / w_sum
    a_final = a_sum / w_sum

    # Re-normalize to guarantee sum = 1.0
    total = h_final + d_final + a_final
    h_final /= total
    d_final /= total
    a_final /= total

    confidence = market_w / w_sum  # 0=pure Elo, approaches 1 as data accumulates

    return MatchProb(
        home_win       = round(h_final, 4),
        draw           = round(d_final, 4),
        away_win       = round(a_final, 4),
        elo_weight     = round(PRIOR_WEIGHT / w_sum, 4),
        market_weight  = round(market_w / w_sum, 4),
        n_observations = len(snapshots),
        half_life_hours= half_life,
        confidence     = round(confidence, 4)
    )


def compute_all_bayesian_probs(
    conn: duckdb.DuckDBPyConnection,
    now: datetime | None = None
) -> list[dict]:
    """
    Compute Bayesian probabilities for every scheduled fixture.
    Returns list of dicts ready for INSERT into bayesian_probs table.
    """
    if now is None:
        now = datetime.now(timezone.utc)

    # Only recompute fixtures that are genuinely upcoming or live.
    # Past-date fixtures still marked 'scheduled' (result not yet ingested)
    # must NOT be recomputed — their odds are too old to decay correctly and
    # confidence collapses to 0. Their last pre-match probs remain valid.
    today = now.date()
    fixtures = conn.execute("""
        SELECT fixture_id FROM fixtures
        WHERE status IN ('scheduled', 'live')
          AND (match_date >= ? OR status = 'live')
        ORDER BY match_date ASC
    """, (today,)).fetchall()

    results = []
    for (fid,) in fixtures:
        try:
            prob = compute_bayesian_prob(fid, conn, now)
            results.append({
                "fixture_id":      fid,
                "computed_at":     now,
                "home_win":        prob.home_win,
                "draw":            prob.draw,
                "away_win":        prob.away_win,
                "elo_weight":      prob.elo_weight,
                "market_weight":   prob.market_weight,
                "n_observations":  prob.n_observations,
                "half_life_hours": prob.half_life_hours,
                "confidence":      prob.confidence,
            })
        except Exception as e:
            log.warning("Skipping fixture %d: %s", fid, e)

    return results


def write_bayesian_probs(
    conn: duckdb.DuckDBPyConnection,
    probs: list[dict]
) -> None:
    """Insert computed Bayesian probabilities into the DB."""
    conn.executemany("""
        INSERT INTO bayesian_probs
          (fixture_id, computed_at, home_win, draw, away_win,
           elo_weight, market_weight, n_observations, half_life_hours, confidence)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [(r["fixture_id"], r["computed_at"], r["home_win"], r["draw"], r["away_win"],
           r["elo_weight"], r["market_weight"], r["n_observations"],
           r["half_life_hours"], r["confidence"]) for r in probs])
    conn.commit()
    log.info("Bayesian probs: wrote %d rows", len(probs))
