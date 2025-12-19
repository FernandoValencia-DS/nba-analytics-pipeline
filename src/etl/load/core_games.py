from src.db.connection import get_conn

def upsert_fct_games(df):
    sql = """
    insert into core.fct_games (
      game_id, team_id,
      season_id, season_num,          -- 👈 NUEVO
      game_date, matchup, ha, wl,
      min, pts, fgm, fga, fg_pct,
      fg3m, fg3a, fg3_pct,
      ftm, fta, ft_pct,
      oreb, dreb, reb, ast, stl, blk, tov, pf,
      plus_minus,
      team_abbreviation, team_name
    )
    values (
      %s,%s,
      %s,%s,
      %s,%s,%s,%s,
      %s,%s,%s,%s,%s,
      %s,%s,%s,
      %s,%s,%s,
      %s,%s,%s,%s,%s,%s,%s,%s,
      %s,
      %s,%s
    )
    on conflict (game_id, team_id) do update set
      season_id   = excluded.season_id,
      season_num  = excluded.season_num,   -- 👈 NUEVO
      game_date   = excluded.game_date,
      matchup     = excluded.matchup,
      ha          = excluded.ha,
      wl          = excluded.wl,
      min         = excluded.min,
      pts         = excluded.pts,
      fgm         = excluded.fgm,
      fga         = excluded.fga,
      fg_pct      = excluded.fg_pct,
      fg3m        = excluded.fg3m,
      fg3a        = excluded.fg3a,
      fg3_pct     = excluded.fg3_pct,
      ftm         = excluded.ftm,
      fta         = excluded.fta,
      ft_pct      = excluded.ft_pct,
      oreb        = excluded.oreb,
      dreb        = excluded.dreb,
      reb         = excluded.reb,
      ast         = excluded.ast,
      stl         = excluded.stl,
      blk         = excluded.blk,
      tov         = excluded.tov,
      pf          = excluded.pf,
      plus_minus  = excluded.plus_minus,
      team_abbreviation = excluded.team_abbreviation,
      team_name   = excluded.team_name;
    """

    conn = get_conn()
    cur = conn.cursor()

    for _, r in df.iterrows():
        cur.execute(sql, (
            r["GAME_ID"], int(r["TEAM_ID"]),
            int(r["SEASON_ID"]), r["SEASON_NUM"],     # 👈 NUEVO
            r["GAME_DATE"], r["MATCHUP"], r["HA"], r["WL"],
            int(r["MIN"]), int(r["PTS"]), int(r["FGM"]), int(r["FGA"]), r["FG_PCT"],
            int(r["FG3M"]), int(r["FG3A"]), r["FG3_PCT"],
            int(r["FTM"]), int(r["FTA"]), r["FT_PCT"],
            int(r["OREB"]), int(r["DREB"]), int(r["REB"]),
            int(r["AST"]), int(r["STL"]), int(r["BLK"]),
            int(r["TOV"]), int(r["PF"]),
            r["PLUS_MINUS"],
            r["TEAM_ABBREVIATION"], r["TEAM_NAME"]
        ))

    conn.commit()
    cur.close()
    conn.close()
