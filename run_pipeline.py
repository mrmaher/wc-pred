"""
run_pipeline.py — DB-backed pipeline orchestrator.

What it does every run:
  1. Ensures DB schema exists and static data is seeded
  2. Runs data collection (Elo + odds if key is set)
  3. Computes Bayesian time-weighted probabilities for all scheduled fixtures
  4. Simulates all 12 group stages via Monte Carlo
  5. Simulates knockout bracket from expected qualifiers
  6. Writes all derived results back to DB
  7. Exports Parquet files for the duckdb-wasm dashboard
  8. Exports predictions.json as a fallback

Called by:
  python run_pipeline.py                 # full pipeline
  python run_pipeline.py --no-collect    # skip data collection (compute only)
  python run_pipeline.py --sims 50000    # higher precision Monte Carlo
  GitHub Actions → .github/workflows/collect.yml
"""

import argparse
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from db.schema import get_conn, init_schema, seed_static_data, export_parquet
from pipeline.collector import run_collection
from pipeline.results import sync_results
from pipeline.bayes import compute_all_bayesian_probs, write_bayesian_probs
from pipeline.model import (
    elo_probabilities, simulate_group, simulate_tournament
)
from pipeline.config import HOME_ADVANTAGE_ELO, DRAW_BASE, OUTPUT, VALUE_THRESHOLD

log = logging.getLogger(__name__)


# ── Helpers ───────────────────────────────────────────────────────────────────

def latest_elo(conn: duckdb.DuckDBPyConnection) -> dict[str, float]:
    """
    Return best available Elo per team, prioritising source quality:
      1. 'calculated'  — updated after actual match results (most accurate)
      2. 'eloratings'  — live national-team ratings from eloratings.net
      3. 'seed'        — static seed values (fallback only)
    Within each tier, take the most recent snapshot.
    Seed rows inserted by a failed network call must never beat valid
    eloratings data, which is why we rank source before collected_at.
    """
    rows = conn.execute("""
        SELECT DISTINCT ON (team_id) team_id, elo_value
        FROM elo_snapshots
        ORDER BY team_id,
                 CASE source
                     WHEN 'calculated'  THEN 1
                     WHEN 'eloratings'  THEN 2
                     ELSE 3
                 END ASC,
                 collected_at DESC
    """).fetchall()
    if not rows:
        rows = conn.execute("SELECT team_id, seed_elo FROM teams").fetchall()
    return {r[0]: float(r[1] or 1700) for r in rows}


def group_map(conn: duckdb.DuckDBPyConnection) -> dict[str, list[str]]:
    """Return {group_letter: [team_id, ...]} sorted by Elo desc."""
    rows = conn.execute("""
        SELECT t.team_id, t.group_letter,
               COALESCE(e.elo_value, t.seed_elo, 1700) AS elo
        FROM teams t
        LEFT JOIN LATERAL (
            SELECT elo_value FROM elo_snapshots
            WHERE team_id = t.team_id ORDER BY collected_at DESC LIMIT 1
        ) e ON TRUE
        WHERE t.group_letter IS NOT NULL
        ORDER BY t.group_letter, elo DESC
    """).fetchall()
    groups: dict[str, list[str]] = {}
    for tid, g, _ in rows:
        groups.setdefault(g, []).append(tid)
    return dict(sorted(groups.items()))


def build_actual_ko_bracket(conn: duckdb.DuckDBPyConnection) -> list[tuple[str, str]]:
    """
    Load the current-round knockout bracket from actual DB fixtures.

    Finds the most advanced KO stage that still has unplayed matches and
    returns those matchups. As rounds finish and new fixtures are added,
    this naturally advances to the next round.
    """
    priority = {"qf": 1, "sf": 2, "final": 3, "r16": 4, "r32": 5}

    scheduled = conn.execute("""
        SELECT stage, home_team, away_team
        FROM fixtures
        WHERE stage IN ('r32','r16','qf','sf','final')
          AND status IN ('scheduled','live')
        ORDER BY match_date ASC
    """).fetchall()

    if not scheduled:
        log.info("No scheduled KO fixtures — KO simulation skipped")
        return []

    current_stage = min(scheduled, key=lambda r: priority.get(r[0], 9))[0]
    bracket = [(row[1], row[2]) for row in scheduled if row[0] == current_stage]
    log.info("KO bracket: %d '%s' fixture(s) to simulate", len(bracket), current_stage)
    return bracket


def build_knockout_bracket(
    group_standings: dict[str, list[dict]],
    groups: list[str]
) -> list[tuple[str, str]]:
    """
    Fallback: derive R32 bracket from group simulation results.
    Used only if no KO fixtures exist in the DB yet.
    """
    firsts, seconds, thirds = [], [], []
    for g in groups:
        s = group_standings.get(g, [])
        if len(s) > 0: firsts.append(s[0])
        if len(s) > 1: seconds.append(s[1])
        if len(s) > 2: thirds.append(s[2])

    best_thirds = sorted(
        thirds,
        key=lambda t: (t.get("avg_points", 0), t.get("advance_prob", 0)),
        reverse=True
    )[:8]

    seeded       = [t["team_id"] for t in firsts]
    unseeded_2nd = [t["team_id"] for t in seconds]
    wild_cards   = [t["team_id"] for t in best_thirds]

    bracket = []
    for i in range(0, len(seeded) - 1, 2):
        ga_winner = seeded[i]
        gb_runner = unseeded_2nd[i + 1] if i + 1 < len(unseeded_2nd) else None
        gb_winner = seeded[i + 1]
        ga_runner = unseeded_2nd[i] if i < len(unseeded_2nd) else None
        if ga_winner and gb_runner: bracket.append((ga_winner, gb_runner))
        if gb_winner and ga_runner: bracket.append((gb_winner, ga_runner))
    for i in range(0, len(wild_cards) - 1, 2):
        bracket.append((wild_cards[i], wild_cards[i + 1]))
    return bracket


# ── Main pipeline ─────────────────────────────────────────────────────────────

def run(skip_collect: bool = False, n_sims: int = 10000) -> dict:
    start = time.time()
    now   = datetime.now(timezone.utc)
    log.info("=== 2026 WC Pipeline  [%s] ===", now.strftime("%Y-%m-%d %H:%M UTC"))

    # ── Step 1: DB init + collection ─────────────────────────────────────────
    conn = get_conn()
    init_schema(conn)
    seed_static_data(conn)

    if not skip_collect:
        log.info("Step 1: Collecting data...")
        result = run_collection()  # uses its own conn internally
        # Reopen after collection
        conn.close()
        conn = get_conn()
        log.info("  Elo snapshots: %d  Odds snapshots: %d",
                 result["elo_snapshots"], result["odds_snapshots"])

        log.info("Step 1b: Syncing match results from football-data.org...")
        new_results = sync_results(conn)
        if new_results:
            log.info("  %d new result(s) recorded — Elo ratings updated", new_results)
        else:
            log.info("  No new results to record")
    else:
        log.info("Step 1: Skipping collection (--no-collect)")

    # ── Step 2: Bayesian probabilities ───────────────────────────────────────
    log.info("Step 2: Computing Bayesian probabilities...")
    bayes_probs = compute_all_bayesian_probs(conn, now)
    write_bayesian_probs(conn, bayes_probs)
    log.info("  Computed %d fixture probabilities", len(bayes_probs))

    # Index bayesian probs by fixture_id for fast lookup
    bayes_by_fid = {r["fixture_id"]: r for r in bayes_probs}

    # ── Step 3: Group stage simulations ──────────────────────────────────────
    log.info("Step 3: Simulating group stages (%d iters)...", n_sims)
    elo_data = latest_elo(conn)
    groups   = group_map(conn)

    # Lock in confirmed results so simulations start from the actual scoreboard
    confirmed = conn.execute("""
        SELECT f.home_team, f.away_team, mr.result, f.group_letter
        FROM match_results mr
        JOIN fixtures f ON mr.fixture_id = f.fixture_id
        WHERE f.stage = 'group'
    """).fetchall()
    locked_by_group: dict[str, list[tuple[str, str, str]]] = {}
    for home, away, result, grp in confirmed:
        if grp:
            locked_by_group.setdefault(grp, []).append((home, away, result))
    if confirmed:
        log.info("  Locking %d confirmed group result(s) before simulation", len(confirmed))

    group_standings: dict[str, list[dict]] = {}
    sim_rows = []

    for g, team_ids in groups.items():
        elo_for_group = {tid: elo_data.get(tid, 1700) for tid in team_ids}
        sim = simulate_group(team_ids, elo_for_group, n_sims, HOME_ADVANTAGE_ELO,
                             locked_results=locked_by_group.get(g) or None)

        ranked = sorted(team_ids, key=lambda t: sim[t]["advance_prob"], reverse=True)
        group_standings[g] = []

        for tid in ranked:
            team_row = conn.execute(
                "SELECT name, confederation, seed_elo FROM teams WHERE team_id = ?",
                (tid,)
            ).fetchone()
            group_standings[g].append({
                "team_id":       tid,
                "team_name":     team_row[0] if team_row else tid,
                "elo":           elo_data.get(tid, 1700),
                "confederation": team_row[1] if team_row else "",
                **sim[tid]
            })

            sim_rows.append((g, tid, now, n_sims,
                             sim[tid]["advance_prob"], sim[tid]["finish_1st"],
                             sim[tid]["finish_2nd"],   sim[tid]["finish_3rd"],
                             sim[tid]["finish_4th"],   sim[tid]["avg_points"]))

    conn.executemany("""
        INSERT INTO group_sim_results
          (group_letter, team_id, computed_at, n_simulations,
           advance_prob, finish_1st, finish_2nd, finish_3rd, finish_4th, avg_points)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, sim_rows)
    conn.commit()

    # ── Step 4: Knockout simulation ───────────────────────────────────────────
    log.info("Step 4: Simulating knockout bracket...")
    # Prefer actual KO fixtures from DB (populated once group stage ends).
    # Fall back to group-sim-derived bracket only if DB has no KO fixtures yet.
    bracket = build_actual_ko_bracket(conn)
    if not bracket:
        log.info("  No KO fixtures in DB — falling back to group-sim bracket")
        bracket = build_knockout_bracket(group_standings, list(groups.keys()))
    all_ko_teams = {t for pair in bracket for t in pair if t}
    ko_elo = {t: elo_data.get(t, 1700) for t in all_ko_teams}

    ko_sim = simulate_tournament(bracket, ko_elo, n_sims) if bracket else {}

    ko_rows = []
    for tid, s in ko_sim.items():
        ko_rows.append((tid, now,
                        s.get("round_of_32",   0),
                        s.get("round_of_16",   0),
                        s.get("quarter_final", 0),
                        s.get("semi_final",    0),
                        s.get("final",         0),
                        s.get("champion",      0)))

    if ko_rows:
        conn.executemany("""
            INSERT INTO ko_sim_results
              (team_id, computed_at, round_of_32, round_of_16,
               quarter_final, semi_final, final, champion)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, ko_rows)
        conn.commit()

    # ── Step 5: Export Parquet files ─────────────────────────────────────────
    log.info("Step 5: Exporting Parquet files for dashboard...")
    export_parquet(conn)

    # ── Step 6: Export predictions.json (fallback) ───────────────────────────
    log.info("Step 6: Writing predictions.json fallback...")

    # Pull enriched group fixtures
    group_fixtures_out = {}
    fixtures_rows = conn.execute("""
        SELECT f.fixture_id, f.group_letter, f.round, f.match_date,
               f.home_team, f.away_team, t1.name, t2.name,
               f.status, f.home_score, f.away_score
        FROM fixtures f
        JOIN teams t1 ON f.home_team = t1.team_id
        JOIN teams t2 ON f.away_team = t2.team_id
        WHERE f.stage = 'group'
        ORDER BY f.match_date, f.fixture_id
    """).fetchall()

    for row in fixtures_rows:
        fid, grp, rnd, mdate, home, away, hname, aname, status, hs, as_ = row
        bp = bayes_by_fid.get(fid, {})
        entry = {
            "id": fid, "group": grp, "round": rnd,
            "date": str(mdate) if mdate else None,
            "home": home, "away": away,
            "home_name": hname, "away_name": aname,
            "status": status, "home_score": hs, "away_score": as_,
            "elo_home": elo_data.get(home, 1700),
            "elo_away": elo_data.get(away, 1700),
            "model": {
                "home_win": bp.get("home_win"),
                "draw":     bp.get("draw"),
                "away_win": bp.get("away_win"),
            } if bp else None,
            "confidence": bp.get("confidence", 0),
            "n_observations": bp.get("n_observations", 0),
        }
        group_fixtures_out.setdefault(grp, []).append(entry)

    output = {
        "meta": {
            "generated_at":   now.isoformat(),
            "n_simulations":  n_sims,
            "pipeline_sec":   round(time.time() - start, 2),
            "odds_available": any(r.get("n_observations", 0) > 0 for r in bayes_probs),
            "value_threshold": VALUE_THRESHOLD
        },
        "group_standings":  group_standings,
        "group_fixtures":   group_fixtures_out,
        "knockout_sims":    ko_sim,
        "value_board":      []   # populated when odds data is present
    }

    OUTPUT.parent.mkdir(exist_ok=True)
    with open(OUTPUT, "w") as f:
        json.dump(output, f, indent=2)

    conn.close()
    elapsed = time.time() - start
    log.info("✓ Done in %.1fs — DB updated, Parquet exported, predictions.json written", elapsed)
    return output


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s",
                        datefmt="%H:%M:%S")
    parser = argparse.ArgumentParser(description="2026 WC Pipeline")
    parser.add_argument("--no-collect", action="store_true")
    parser.add_argument("--sims", type=int, default=10000)
    args = parser.parse_args()
    run(skip_collect=args.no_collect, n_sims=args.sims)
