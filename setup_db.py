"""
setup_db.py — One-time setup script. Run this once before anything else.

What it does:
  1. Creates data/wc2026.db with the full schema
  2. Loads all 48 teams from data/teams.json
  3. Loads all 72 group-stage fixtures from data/fixtures.json
  4. Seeds the first Elo snapshot for every team (from seed values)
  5. Runs the full pipeline to generate initial predictions + Parquet files

Run:
  python setup_db.py

After this you can:
  - Open dashboard.html in a browser (serve with: python -m http.server 8000)
  - Run python run_pipeline.py to refresh predictions any time
  - Push to GitHub and enable Actions + Pages (see SETUP.md for those steps)
"""

import logging
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

# ── Preflight checks ──────────────────────────────────────────────────────────
def check_files():
    missing = []
    for p in ["data/teams.json", "data/fixtures.json"]:
        if not Path(p).exists():
            missing.append(p)
    if missing:
        log.error("Missing seed files: %s", missing)
        sys.exit(1)

def check_duckdb():
    try:
        import duckdb
        log.info("duckdb %s ✓", duckdb.__version__)
    except ImportError:
        log.error("duckdb not installed. Run: pip install duckdb")
        sys.exit(1)

# ── Setup ─────────────────────────────────────────────────────────────────────
def main():
    log.info("=== 2026 WC Predictor — First-Time Setup ===")

    check_files()
    check_duckdb()

    from db.schema import get_conn, init_schema, seed_static_data
    from pipeline.collector import collect_elo

    # 1. Create DB + schema
    log.info("Creating database at data/wc2026.db...")
    conn = get_conn()
    init_schema(conn)
    log.info("  Schema created ✓")

    # 2. Load static data
    log.info("Loading teams and fixtures...")
    seed_static_data(conn)
    n_teams    = conn.execute("SELECT COUNT(*) FROM teams").fetchone()[0]
    n_fixtures = conn.execute("SELECT COUNT(*) FROM fixtures").fetchone()[0]
    log.info("  %d teams, %d fixtures loaded ✓", n_teams, n_fixtures)

    # 3. Seed initial Elo snapshots
    log.info("Seeding Elo ratings...")
    n_elo = collect_elo(conn)
    log.info("  %d Elo snapshots written ✓", n_elo)
    conn.close()

    # 4. Run full pipeline (generates Parquet + predictions.json)
    log.info("Running initial pipeline...")
    from run_pipeline import run
    run(skip_collect=True, n_sims=10000)

    # ── Summary ───────────────────────────────────────────────────────────────
    print("\n" + "═"*55)
    print("  Setup complete! Here's what to do next:\n")
    print("  📊 Open the dashboard locally:")
    print("     cd '2026 World Cup Predictor'")
    print("     python -m http.server 8000")
    print("     → Open http://localhost:8000 in your browser\n")
    print("  🔄 Refresh predictions any time:")
    print("     python run_pipeline.py\n")
    print("  🚀 Deploy to GitHub (automatic updates):")
    print("     git init && git add . && git commit -m 'init'")
    print("     git remote add origin https://github.com/YOUR/REPO.git")
    print("     git push -u origin main")
    print("     → Enable Pages: Settings → Pages → Branch: main → / (root)")
    print("     → GitHub Actions runs automatically every 6 hours")
    print("═"*55 + "\n")

if __name__ == "__main__":
    main()
