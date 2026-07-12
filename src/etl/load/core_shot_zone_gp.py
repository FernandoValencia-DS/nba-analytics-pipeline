from src.db.connection import get_conn

def refresh_fct_shot_zone_gp():
    conn = get_conn()
    conn.autocommit = True  # REFRESH CONCURRENTLY can't run inside a transaction block
    cur = conn.cursor()
    try:
        cur.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY core.fct_shot_zone_gp;")
    finally:
        cur.close()
        conn.close()
