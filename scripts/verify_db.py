"""Quick DB verification — called by GitHub Actions after each pipeline run."""
import duckdb

conn = duckdb.connect('data/wc2026.db', read_only=True)

row = conn.execute('SELECT MAX(computed_at), COUNT(*) FROM group_sim_results').fetchone()
print(f'group_sim_results: latest={row[0]}  total_rows={row[1]}')

row2 = conn.execute('SELECT MAX(computed_at), COUNT(*) FROM ko_sim_results').fetchone()
print(f'ko_sim_results:    latest={row2[0]}  total_rows={row2[1]}')

row3 = conn.execute('SELECT MAX(collected_at), COUNT(*) FROM elo_snapshots').fetchone()
print(f'elo_snapshots:     latest={row3[0]}  total_rows={row3[1]}')

conn.close()
