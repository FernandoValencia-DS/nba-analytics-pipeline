from src.db.connection import get_conn

def upsert_core_teams(df):
    sql = """
        insert into core.dim_teams
        (team_id, full_name, abbreviation, nickname, city, state, year_founded, team_logo)
        values (%s, %s, %s, %s, %s, %s, %s, %s)
        on conflict (team_id) do update
        set
          full_name = excluded.full_name,
          abbreviation = excluded.abbreviation,
          nickname = excluded.nickname,
          city = excluded.city,
          state = excluded.state,
          year_founded = excluded.year_founded,
          team_logo = excluded.team_logo;
    """

    conn = get_conn()
    cur = conn.cursor()

    for _, row in df.iterrows():
        cur.execute(sql, (
            int(row["id"]),
            row["full_name"],
            row["abbreviation"],
            row["nickname"],
            row["city"],
            row["state"],
            int(row["year_founded"]) if row["year_founded"] is not None else None,
            row["team_logo"]
        ))

    conn.commit()
    cur.close()
    conn.close()