from src.db.connection import get_conn
import pandas as pd

def upsert_fct_standings(df: pd.DataFrame):

    sql = """
    INSERT INTO core.dim_standings (
        season_team_id,
        season_id,
        team_id,
        team_city,
        team_name,
        conference,
        conference_record,
        playoff_rank,
        clinch_indicator,
        division,
        division_record,
        division_rank,
        wins,
        losses,
        win_pct,
        record,
        home,
        road,
        l10
    )
    VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    )
    ON CONFLICT (season_team_id)
    DO UPDATE SET
        team_city = EXCLUDED.team_city,
        team_name = EXCLUDED.team_name,
        conference = EXCLUDED.conference,
        conference_record = EXCLUDED.conference_record,
        playoff_rank = EXCLUDED.playoff_rank,
        clinch_indicator = EXCLUDED.clinch_indicator,
        division = EXCLUDED.division,
        division_record = EXCLUDED.division_record,
        division_rank = EXCLUDED.division_rank,
        wins = EXCLUDED.wins,
        losses = EXCLUDED.losses,
        win_pct = EXCLUDED.win_pct,
        record = EXCLUDED.record,
        home = EXCLUDED.home,
        road = EXCLUDED.road,
        l10 = EXCLUDED.l10;
    """

    conn = get_conn()
    cur = conn.cursor()

    for _, r in df.iterrows():
        cur.execute(sql, (
            r["SEASON_TEAM_ID"],                # PK
            int(r["SeasonID"]),
            int(r["TeamID"]),
            r["TeamCity"],
            r["TeamName"],
            r["Conference"],
            r["ConferenceRecord"],
            int(r["PlayoffRank"]) if pd.notna(r["PlayoffRank"]) else None,
            r["ClinchIndicator"],
            r["Division"],
            r["DivisionRecord"],
            int(r["DivisionRank"]) if pd.notna(r["DivisionRank"]) else None,
            int(r["WINS"]),
            int(r["LOSSES"]),
            float(r["WinPCT"]),
            r["Record"],
            r["HOME"],
            r["ROAD"],
            r["L10"]
        ))

    conn.commit()
    cur.close()
    conn.close()
