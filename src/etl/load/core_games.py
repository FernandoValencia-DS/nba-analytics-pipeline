from src.db.connection import get_conn
import pandas as pd

def upsert_fct_games(df: pd.DataFrame):
   
    sql = """
    INSERT INTO core.fct_games (
        game_team_id, game_id, team_id,
        season_id, season_num, season_team_id,
        game_date, matchup, ha, wl,
        min, pts, fgm, fga, fg_pct,
        fg3m, fg3a, fg3_pct,
        ftm, fta, ft_pct,
        oreb, dreb, reb, ast, stl, blk, tov, pf,
        plus_minus,
        team_abbreviation, team_name
    )
    VALUES (
        %s, %s, %s,
        %s, %s, %s,
        %s, %s, %s, %s,
        %s, %s, %s, %s, %s,
        %s, %s, %s,
        %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s,
        %s,
        %s, %s
    )
    ON CONFLICT (game_team_id) DO UPDATE SET
        game_id = excluded.game_id,
        team_id = excluded.team_id,
        season_id = excluded.season_id,
        season_num = excluded.season_num,
        season_team_id = excluded.season_team_id,
        game_date = excluded.game_date,
        matchup = excluded.matchup,
        ha = excluded.ha,
        wl = excluded.wl,
        min = excluded.min,
        pts = excluded.pts,
        fgm = excluded.fgm,
        fga = excluded.fga,
        fg_pct = excluded.fg_pct,
        fg3m = excluded.fg3m,
        fg3a = excluded.fg3a,
        fg3_pct = excluded.fg3_pct,
        ftm = excluded.ftm,
        fta = excluded.fta,
        ft_pct = excluded.ft_pct,
        oreb = excluded.oreb,
        dreb = excluded.dreb,
        reb = excluded.reb,
        ast = excluded.ast,
        stl = excluded.stl,
        blk = excluded.blk,
        tov = excluded.tov,
        pf = excluded.pf,
        plus_minus = excluded.plus_minus,
        team_abbreviation = excluded.team_abbreviation,
        team_name = excluded.team_name;
    """

    conn = get_conn()
    cur = conn.cursor()

    for _, r in df.iterrows():
        cur.execute(sql, (
            r["GAME_TEAM_ID"], r["GAME_ID"], int(r["TEAM_ID"]),
            int(r["SEASON_ID"]), r["SEASON_NUM"], r["SEASON_TEAM_ID"],
            r["GAME_DATE"], r["MATCHUP"], r["HA"], r["WL"],
            r["MIN"], r["PTS"], r["FGM"], r["FGA"], r["FG_PCT"],
            r["FG3M"], r["FG3A"], r["FG3_PCT"],
            r["FTM"], r["FTA"], r["FT_PCT"],
            r["OREB"], r["DREB"], r["REB"], r["AST"], r["STL"], r["BLK"],
            r["TOV"], r["PF"],
            r["PLUS_MINUS"],
            r["TEAM_ABBREVIATION"], r["TEAM_NAME"]
        ))

    conn.commit()
    cur.close()
    conn.close()