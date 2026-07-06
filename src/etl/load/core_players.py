from src.db.connection import get_conn
from psycopg2.extras import execute_batch

def upsert_core_players(df):
    sql = """
        insert into core.dim_players
        (id, full_name, first_name, last_name, is_active, player_headshot)
        values (%s, %s, %s, %s, %s, %s)
        on conflict (id) do update
        set
          full_name = excluded.full_name,
          first_name = excluded.first_name,
          last_name = excluded.last_name,
          is_active = excluded.is_active,
          player_headshot = excluded.player_headshot;
    """

    data = [
        (
            int(row["id"]),
            row["full_name"],
            row["first_name"],
            row["last_name"],
            bool(row["is_active"]),
            row["player_headshot"],
        )
        for _, row in df.iterrows()
    ]

    conn = get_conn()
    cur = conn.cursor()

    execute_batch(cur, sql, data, page_size=500)

    conn.commit()
    cur.close()
    conn.close()