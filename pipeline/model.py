"""
model.py — Prediction math for the 2026 World Cup predictor.

Core functions:
  elo_probabilities()    — Win/Draw/Loss from Elo delta
  implied_probabilities()— Convert decimal odds → cleaned implied %
  value_gap()            — Detect model vs market discrepancy
  simulate_group()       — Monte Carlo group-stage standings
  simulate_tournament()  — Full knockout bracket simulation
"""

import math
import random
from typing import Optional


# ── Elo Probability Model ─────────────────────────────────────────────────────

def elo_win_prob(elo_a: float, elo_b: float, home_advantage: float = 0.0) -> float:
    """
    Expected probability that team A beats team B using the Elo formula.
    P(Win) = 1 / (1 + 10^(-(Δelo + home_adv) / 400))
    """
    delta = (elo_a + home_advantage) - elo_b
    return 1.0 / (1.0 + 10.0 ** (-delta / 400.0))


def draw_probability(elo_a: float, elo_b: float, base_draw: float = 0.265) -> float:
    """
    Estimate draw probability using a logistic decay around even matchups.
    Draws are more likely when teams are evenly matched (Δelo ≈ 0).
    Derived from historical World Cup draw rate (~26-28%).
    """
    delta = abs(elo_a - elo_b)
    # Decay: draw prob shrinks as mismatch grows
    # At Δelo=0: ~base_draw; at Δelo=400: ~half base_draw
    decay = math.exp(-delta / 550.0)
    return base_draw * decay


def elo_probabilities(
    elo_home: float,
    elo_away: float,
    home_advantage: float = 0.0,
    base_draw: float = 0.265
) -> dict:
    """
    Return normalized Win/Draw/Loss probabilities from Elo ratings.

    Steps:
      1. Calculate raw P(home wins) via Elo formula
      2. Estimate P(draw) via logistic decay
      3. Assign remainder to P(away wins)
      4. Renormalize so all three sum to 1.0
    """
    p_home_raw = elo_win_prob(elo_home, elo_away, home_advantage)
    p_draw_raw = draw_probability(elo_home, elo_away, base_draw)

    # Raw away prob: complement of home win, scaled down by draw
    p_away_raw = 1.0 - p_home_raw

    # Scale home and away to leave room for draw
    scale = 1.0 - p_draw_raw
    p_home = p_home_raw * scale
    p_away = p_away_raw * scale
    p_draw = p_draw_raw

    total = p_home + p_draw + p_away
    return {
        "home_win": round(p_home / total, 4),
        "draw":     round(p_draw / total, 4),
        "away_win": round(p_away / total, 4),
        "elo_delta": round(elo_home - elo_away, 1)
    }


# ── Market Implied Probability ─────────────────────────────────────────────────

def decimal_to_implied(odds: float) -> float:
    """Raw implied probability from decimal odds (includes vig)."""
    if odds <= 1.0:
        return 1.0
    return 1.0 / odds


def american_to_decimal(american: int) -> float:
    """Convert American odds to decimal format."""
    if american > 0:
        return (american / 100.0) + 1.0
    else:
        return (100.0 / abs(american)) + 1.0


def remove_vig(raw_home: float, raw_draw: float, raw_away: float) -> dict:
    """
    Remove bookmaker margin (vig/overround) to get clean market probabilities.

    Method: divide each implied probability by the sum of all three.
    The sum is typically 1.04–1.07 (the bookmaker's edge).
    """
    total = raw_home + raw_draw + raw_away
    if total <= 0:
        return {"home_win": None, "draw": None, "away_win": None, "vig": None}

    vig = round(total - 1.0, 4)
    return {
        "home_win": round(raw_home / total, 4),
        "draw":     round(raw_draw / total, 4),
        "away_win": round(raw_away / total, 4),
        "vig":      vig,
        "overround": round(total, 4)
    }


def implied_probabilities(consensus_odds: Optional[dict]) -> Optional[dict]:
    """
    Convert consensus decimal odds → vig-removed implied probabilities.
    Returns None if odds unavailable.
    """
    if not consensus_odds:
        return None

    h = decimal_to_implied(consensus_odds.get("home") or 0)
    d = decimal_to_implied(consensus_odds.get("draw") or 0)
    a = decimal_to_implied(consensus_odds.get("away") or 0)

    if not (h and d and a):
        return None

    return remove_vig(h, d, a)


# ── Value Detection ────────────────────────────────────────────────────────────

def value_gap(model: dict, market: Optional[dict], threshold: float = 0.05) -> dict:
    """
    Compare model probabilities against market-implied probabilities.
    Flag outcomes where |model - market| > threshold as value plays.

    Returns dict with gaps and any flagged value opportunities.
    """
    if not market:
        return {"available": False}

    gaps = {}
    flags = []

    for outcome in ["home_win", "draw", "away_win"]:
        m_prob = model.get(outcome, 0)
        mkt_prob = market.get(outcome, 0)
        if m_prob is None or mkt_prob is None:
            continue

        gap = round(m_prob - mkt_prob, 4)
        gaps[outcome] = {
            "model": m_prob,
            "market": mkt_prob,
            "gap": gap,
            "pct_diff": round(abs(gap) * 100, 1)
        }

        if abs(gap) >= threshold:
            direction = "overvalued by model" if gap > 0 else "undervalued by model"
            flags.append({
                "outcome": outcome,
                "gap": gap,
                "direction": direction,
                "strength": "strong" if abs(gap) >= threshold * 2 else "mild"
            })

    return {
        "available": True,
        "gaps": gaps,
        "value_flags": flags,
        "has_value": len(flags) > 0
    }


# ── Group Stage Simulation ────────────────────────────────────────────────────

def simulate_match(prob_home: float, prob_draw: float, prob_away: float) -> str:
    """Return 'home', 'draw', or 'away' based on probabilities."""
    r = random.random()
    if r < prob_home:
        return "home"
    elif r < prob_home + prob_draw:
        return "draw"
    else:
        return "away"


def simulate_group(
    teams: list[str],
    elo_ratings: dict[str, float],
    n_simulations: int = 10000,
    home_advantage: float = 0.0
) -> dict:
    """
    Monte Carlo simulation of a group stage round-robin.

    Returns advancement probabilities and average expected points for each team.
    """
    # Track how often each team finishes in each position (handle any group size)
    n_teams = len(teams)
    position_counts = {t: {p: 0 for p in range(1, n_teams + 1)} for t in teams}
    advance_counts  = {t: 0 for t in teams}
    total_points    = {t: 0.0 for t in teams}

    # All round-robin pairs
    pairs = [(teams[i], teams[j]) for i in range(len(teams)) for j in range(i+1, len(teams))]

    for _ in range(n_simulations):
        points = {t: 0 for t in teams}
        gd     = {t: 0 for t in teams}  # goal difference proxy for tiebreaking

        for home, away in pairs:
            e_home = elo_ratings.get(home, 1700)
            e_away = elo_ratings.get(away, 1700)
            probs  = elo_probabilities(e_home, e_away, home_advantage)

            result = simulate_match(probs["home_win"], probs["draw"], probs["away_win"])
            if result == "home":
                points[home] += 3
                gd[home] += 1; gd[away] -= 1
            elif result == "draw":
                points[home] += 1; points[away] += 1
            else:
                points[away] += 3
                gd[away] += 1; gd[home] -= 1

        # Rank teams: sort by pts desc, then GD desc, then random tiebreak
        ranked = sorted(teams, key=lambda t: (points[t], gd[t], random.random()), reverse=True)
        for pos, team in enumerate(ranked, 1):
            position_counts[team][pos] += 1
            total_points[team] += points[team]
        # Top 2 advance (8 best 3rd-place teams also advance, handled in tournament sim)
        for team in ranked[:2]:
            advance_counts[team] += 1

    results = {}
    for t in teams:
        results[t] = {
            "advance_prob": round(advance_counts[t] / n_simulations, 3),
            "avg_points":   round(total_points[t] / n_simulations, 2),
            "finish_1st":   round(position_counts[t].get(1, 0) / n_simulations, 3),
            "finish_2nd":   round(position_counts[t].get(2, 0) / n_simulations, 3),
            "finish_3rd":   round(position_counts[t].get(3, 0) / n_simulations, 3),
            "finish_4th":   round(position_counts[t].get(4, 0) / n_simulations, 3),
        }
    return results


# ── Knockout Stage Simulation ─────────────────────────────────────────────────

def simulate_knockout_match(team_a: str, team_b: str, elo_ratings: dict[str, float]) -> str:
    """No draws in knockout — run extra time if needed (ignore draw outcome)."""
    e_a = elo_ratings.get(team_a, 1700)
    e_b = elo_ratings.get(team_b, 1700)
    p_a = elo_win_prob(e_a, e_b)
    return team_a if random.random() < p_a else team_b


def simulate_tournament(
    bracket: list[tuple[str, str]],
    elo_ratings: dict[str, float],
    n_simulations: int = 10000
) -> dict:
    """
    Simulate a knockout bracket and return per-team advancement probabilities.

    Expects 16 matchups (32 teams) for the 2026 WC format — a power of 2
    ensures no team is silently dropped between rounds.

    Round progression: R32 → R16 → QF → SF → Final → Champion
    """
    ROUND_NAMES = ["r32", "r16", "qf", "sf", "final"]
    reach_counts = {r: {} for r in ROUND_NAMES + ["champion"]}

    all_teams = [t for pair in bracket for t in pair]
    for t in all_teams:
        for r in reach_counts:
            reach_counts[r][t] = 0

    for _ in range(n_simulations):
        current_round = [list(pair) for pair in bracket]
        r_idx = 0

        while current_round:
            is_final = (len(current_round) == 1)
            rname = ROUND_NAMES[r_idx] if r_idx < len(ROUND_NAMES) else "final"
            winners = []

            for matchup in current_round:
                if len(matchup) == 2:
                    # Both teams in the final get "final" credit; only winner gets "champion"
                    if is_final:
                        for t in matchup:
                            reach_counts["final"][t] = reach_counts["final"].get(t, 0) + 1
                    w = simulate_knockout_match(matchup[0], matchup[1], elo_ratings)
                    if not is_final:
                        reach_counts[rname][w] = reach_counts[rname].get(w, 0) + 1
                    else:
                        reach_counts["champion"][w] = reach_counts["champion"].get(w, 0) + 1
                    winners.append(w)

            next_round = []
            for i in range(0, len(winners), 2):
                if i + 1 < len(winners):
                    next_round.append([winners[i], winners[i + 1]])

            r_idx += 1
            current_round = next_round

    results = {}
    for t in all_teams:
        results[t] = {
            "champion":      round(reach_counts["champion"].get(t, 0) / n_simulations, 4),
            "final":         round(reach_counts["final"].get(t, 0) / n_simulations, 4),
            "semi_final":    round(reach_counts["sf"].get(t, 0) / n_simulations, 4),
            "quarter_final": round(reach_counts["qf"].get(t, 0) / n_simulations, 4),
            "round_of_16":   round(reach_counts["r16"].get(t, 0) / n_simulations, 4),
            "round_of_32":   round(reach_counts["r32"].get(t, 0) / n_simulations, 4),
        }
    return results
