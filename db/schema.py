"""
db/schema.py — DuckDB schema for the 2026 World Cup Predictor.

Design principles:
  - Raw tables (elo_snapshots, odds_snapshots) are APPEND-ONLY. Never delete rows.
  - Derived tables (bayesian_probs, group_sim_results, ko_sim_results) are
    fully rebuilt on every pipeline run.
  - Ground truth (match_results) is written after each game finishes.

Usage:
  from db.schema import get_conn, init_schema, export_parquet
  conn = get_conn()           # opens data/wc2026.db
  init_schema(conn)           # creates tables if they don't exist
  export_parquet(conn)        # writes data/parquet/*.parquet for the dashboard
"""

import json
from pathlib import Path
import duckdb

ROOT       = Path(__file__).parent.parent
DB_PATH    = ROOT / "data" / "wc2026.db"
PARQUET_DIR = ROOT / "data" / "parquet"


def get_conn(read_only: bool = False) -> duckdb.DuckDBPyConnection:
    """Open (or create) the database. Thread-safe for single-process use."""
    DB_PATH.parent.mkdir(exist_ok=True)
    return duckdb.connect(str(DB_PATH), read_only=read_only)


def init_schema(conn: duckdb.DuckDBPyConnection) -> None:
    """Create all tables if they don't already exist."""

    conn.executemany("", [])   # no-op to ensure connection is live

    conn.execute("""
    -- ── Static reference ──────────────────────────────────────────────────────

    CREATE TABLE IF NOT EXISTS teams (
        team_id         VARCHAR PRIMARY KEY,
        name            VARCHAR NOT NULL,
        group_letter    VARCHAR,          -- 'A' through 'L'
        confederation   VARCHAR,
        seed_elo        FLOAT,
        notes           VARCHAR
    );

    CREATE TABLE IF NOT EXISTS fixtures (
        fixture_id      INTEGER PRIMARY KEY,
        stage           VARCHAR,          -- 'group' | 'r32' | 'qf' | 'sf' | 'final'
        group_letter    VARCHAR,
        round           INTEGER,
        match_date      DATE,
        kickoff_utc     TIMESTAMPTZ,
        home_team       VARCHAR REFERENCES teams(team_id),
        away_team       VARCHAR REFERENCES teams(team_id),
        status          VARCHAR DEFAULT 'scheduled',  -- 'scheduled'|'live'|'finished'
        home_score      INTEGER,
        away_score      INTEGER
    );

    -- ── Append-only time series ───────────────────────────────────────────────

    CREATE SEQUENCE IF NOT EXISTS elo_snap_seq;
    CREATE TABLE IF NOT EXISTS elo_snapshots (
        id              BIGINT DEFAULT nextval('elo_snap_seq') PRIMARY KEY,
        team_id         VARCHAR NOT NULL REFERENCES teams(team_id),
        collected_at    TIMESTAMPTZ NOT NULL,
        elo_value       FLOAT NOT NULL,
        source          VARCHAR DEFAULT 'seed'  -- 'seed' | 'eloratings' | 'calculated'
    );

    CREATE SEQUENCE IF NOT EXISTS odds_snap_seq;
    CREATE TABLE IF NOT EXISTS odds_snapshots (
        id              BIGINT DEFAULT nextval('odds_snap_seq') PRIMARY KEY,
        fixture_id      INTEGER NOT NULL REFERENCES fixtures(fixture_id),
        bookmaker       VARCHAR NOT NULL,
        collected_at    TIMESTAMPTZ NOT NULL,
        home_odds       FLOAT,   -- decimal odds
        draw_odds       FLOAT,
        away_odds       FLOAT,
        home_implied    FLOAT,   -- raw implied prob (with vig)
        draw_implied    FLOAT,
        away_implied    FLOAT
    );

    -- ── Ground truth ──────────────────────────────────────────────────────────

    CREATE TABLE IF NOT EXISTS match_results (
        fixture_id      INTEGER PRIMARY KEY REFERENCES fixtures(fixture_id),
        home_score      INTEGER NOT NULL,
        away_score      INTEGER NOT NULL,
        result          VARCHAR NOT NULL,  -- 'home_win' | 'draw' | 'away_win'
        confirmed_at    TIMESTAMPTZ NOT NULL
    );

    -- ── Derived (rebuilt each pipeline run) ───────────────────────────────────

    CREATE TABLE IF NOT EXISTS bayesian_probs (
        fixture_id          INTEGER NOT NULL REFERENCES fixtures(fixture_id),
        computed_at         TIMESTAMPTZ NOT NULL,
        home_win            FLOAT NOT NULL,
        draw                FLOAT NOT NULL,
        away_win            FLOAT NOT NULL,
        elo_weight          FLOAT,   -- fraction of weight from Elo prior
        market_weight       FLOAT,   -- fraction of weight from market observations
        n_observations      INTEGER, -- number of odds snapshots used
        half_life_hours     FLOAT,   -- adaptive half-life used in this computation
        confidence          FLOAT,   -- 0=pure Elo, 1=fully market-informed
        PRIMARY KEY (fixture_id, computed_at)
    );

    CREATE TABLE IF NOT EXISTS group_sim_results (
        group_letter        VARCHAR NOT NULL,
        team_id             VARCHAR NOT NULL REFERENCES teams(team_id),
        computed_at         TIMESTAMPTZ NOT NULL,
        n_simulations       INTEGER,
        advance_prob        FLOAT,
        finish_1st          FLOAT,
        finish_2nd          FLOAT,
        finish_3rd          FLOAT,
        finish_4th          FLOAT,
        avg_points          FLOAT,
        PRIMARY KEY (group_letter, team_id, computed_at)
    );

    CREATE TABLE IF NOT EXISTS ko_sim_results (
        team_id             VARCHAR NOT NULL REFERENCES teams(team_id),
        computed_at         TIMESTAMPTZ NOT NULL,
        round_of_32         FLOAT,
        round_of_16         FLOAT,
        quarter_final       FLOAT,
        semi_final          FLOAT,
        final               FLOAT,
        champion            FLOAT,
        PRIMARY KEY (team_id, computed_at)
    );
    """)

    # ── Migrations (safe to re-run) ───────────────────────────────────────────
    try:
        conn.execute("ALTER TABLE fixtures ADD COLUMN kickoff_utc TIMESTAMPTZ")
        log.info("Migration: added fixtures.kickoff_utc")
    except Exception:
        pass  # column already exists

    conn.commit()


def seed_static_data(conn: duckdb.DuckDBPyConnection) -> None:
    """Load teams and fixtures from JSON seed files into the DB (idempotent)."""
    teams_file   = ROOT / "data" / "teams.json"
    fixtures_file = ROOT / "data" / "fixtures.json"

    if teams_file.exists():
        teams = json.loads(teams_file.read_text())["teams"]
        conn.executemany("""
            INSERT OR IGNORE INTO teams (team_id, name, group_letter, confederation, seed_elo)
            VALUES (?, ?, ?, ?, ?)
        """, [(t["id"], t["name"], t["group"], t["confederation"], t["elo"]) for t in teams])

    if fixtures_file.exists():
        fixtures = json.loads(fixtures_file.read_text())["fixtures"]
        conn.executemany("""
            INSERT OR IGNORE INTO fixtures
              (fixture_id, stage, group_letter, round, match_date, home_team, away_team, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, [(f["id"], f["stage"], f.get("group"), f.get("round"), f.get("date"),
               f["home"], f["away"], f.get("status","scheduled")) for f in fixtures])

    conn.commit()


def export_parquet(conn: duckdb.DuckDBPyConnection) -> None:
    """
    Export key tables as Parquet files for duckdb-wasm dashboard queries.
    Each file is small enough to fetch over HTTPS from GitHub Pages.
    """
    PARQUET_DIR.mkdir(parents=True, exist_ok=True)

    exports = {
        "fixtures": """
            SELECT f.*, t1.name AS home_name, t2.name AS away_name
            FROM fixtures f
            LEFT JOIN teams t1 ON f.home_team = t1.team_id
            LEFT JOIN teams t2 ON f.away_team = t2.team_id
        """,
        "teams": "SELECT * FROM teams",
        "bayesian_probs": """
            SELECT bp.*, t1.name AS home_name, t2.name AS away_name,
                   f.match_date, f.kickoff_utc, f.group_letter, f.round,
                   f.home_team, f.away_team, f.status,
                   mr.home_score, mr.away_score
            FROM bayesian_probs bp
            JOIN fixtures f ON bp.fixture_id = f.fixture_id
            JOIN teams t1 ON f.home_team = t1.team_id
            JOIN teams t2 ON f.away_team = t2.team_id
            LEFT JOIN match_results mr ON bp.fixture_id = mr.fixture_id
            WHERE bp.computed_at = (
                SELECT MAX(computed_at) FROM bayesian_probs bp2
                WHERE bp2.fixture_id = bp.fixture_id
            )
        """,
        "group_sim_results": """
            SELECT gs.*, t.name AS team_name, t.confederation, t.seed_elo AS elo
            FROM group_sim_results gs
            JOIN teams t ON gs.team_id = t.team_id
            WHERE gs.computed_at = (SELECT MAX(computed_at) FROM group_sim_results)
        """,
        "ko_sim_results": """
            SELECT ks.*, t.name AS team_name, t.confederation, t.group_letter
            FROM ko_sim_results ks
            JOIN teams t ON ks.team_id = t.team_id
            WHERE ks.computed_at = (SELECT MAX(computed_at) FROM ko_sim_results)
        """,
        "ko_sim_history": """
            SELECT ks.team_id, ks.computed_at, ks.champion, ks.final,
                   ks.semi_final, ks.quarter_final, ks.round_of_16, ks.round_of_32,
                   t.name AS team_name, t.group_letter
            FROM ko_sim_results ks
            JOIN teams t ON ks.team_id = t.team_id
            ORDER BY ks.team_id, ks.computed_at ASC
        """,
        "group_sim_history": """
            SELECT gs.team_id, gs.computed_at, gs.advance_prob,
                   gs.finish_1st, gs.finish_2nd, gs.avg_points,
                   gs.group_letter, t.name AS team_name
            FROM group_sim_results gs
            JOIN teams t ON gs.team_id = t.team_id
            ORDER BY gs.team_id, gs.computed_at ASC
        """,
        "elo_snapshots": """
            SELECT es.*, t.name AS team_name
            FROM elo_snapshots es
            JOIN teams t ON es.team_id = t.team_id
            ORDER BY collected_at DESC
        """,
        "match_results": """
            SELECT mr.*, f.home_team, f.away_team, t1.name AS home_name, t2.name AS away_name,
                   f.match_date, f.group_letter
            FROM match_results mr
            JOIN fixtures f ON mr.fixture_id = f.fixture_id
            JOIN teams t1 ON f.home_team = t1.team_id
            JOIN teams t2 ON f.away_team = t2.team_id
        """,
        "odds_snapshots": """
            SELECT os.fixture_id, os.bookmaker, os.collected_at,
                   os.home_odds, os.draw_odds, os.away_odds,
                   os.home_implied, os.draw_implied, os.away_implied,
                   f.home_team, f.away_team
            FROM odds_snapshots os
            JOIN fixtures f ON os.fixture_id = f.fixture_id
            ORDER BY os.collected_at DESC
        """
    }

    for name, query in exports.items():
        out = PARQUET_DIR / f"{name}.parquet"
        try:
            conn.execute(f"COPY ({query}) TO '{out}' (FORMAT PARQUET)")
            print(f"  ✓ {name}.parquet")
        except Exception as e:
            print(f"  ✗ {name}.parquet — {e}")
