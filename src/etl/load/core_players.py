from src.db.connection import get_conn

def upsert_core_players(df):
    sql = """
        insert into core.dim_players
        (id, full_name, first_name, last_name, is_active)
        values (%s, %s, %s, %s, %s)
        on conflict (id) do update
        set
          full_name = excluded.full_name,
          first_name = excluded.first_name,
          last_name = excluded.last_name,
          is_active = excluded.is_active;
    """

    conn = get_conn()
    cur = conn.cursor()

    for _, row in df.iterrows():
        cur.execute(sql, (
            int(row["id"]),
            row["full_name"],
            row["first_name"],
            row["last_name"],
            bool(row["is_active"])
        ))

    conn.commit()
    cur.close()
    conn.close()