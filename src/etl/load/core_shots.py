from src.db.connection import get_conn
from psycopg2.extras import execute_batch
import pandas as pd

def upsert_fct_shots(df):
    sql = """
    INSERT INTO core.fct_shots (
        game_gevent_id, game_team_id, game_id, game_event_id, season_num, game_date,
        team_id, team_name, player_id, player_name, htm, vtm,
        period, minutes_remaining, seconds_remaining,
        grid_type, event_type, action_type, shot_type,
        shot_zone_basic, shot_zone_area, shot_zone_range,
        shot_distance, loc_x, loc_y, shot_attempted_flag, shot_made_flag
    )
    VALUES (
        %s,%s,%s,%s,%s,%s,
        %s,%s,%s,%s,%s,%s,
        %s,%s,%s,
        %s,%s,%s,%s,
        %s,%s,%s,
        %s,%s,%s,%s,%s
    )
    ON CONFLICT (game_gevent_id) DO UPDATE SET
        game_team_id = excluded.game_team_id,
        game_id = excluded.game_id,
        game_event_id = excluded.game_event_id,
        season_num = excluded.season_num,
        game_date = excluded.game_date,
        team_id = excluded.team_id,
        team_name = excluded.team_name,
        player_id = excluded.player_id,
        player_name = excluded.player_name,
        htm = excluded.htm,
        vtm = excluded.vtm,
        period = excluded.period,
        minutes_remaining = excluded.minutes_remaining,
        seconds_remaining = excluded.seconds_remaining,
        grid_type = excluded.grid_type,
        event_type = excluded.event_type,
        action_type = excluded.action_type,
        shot_type = excluded.shot_type,
        shot_zone_basic = excluded.shot_zone_basic,
        shot_zone_area = excluded.shot_zone_area,
        shot_zone_range = excluded.shot_zone_range,
        shot_distance = excluded.shot_distance,
        loc_x = excluded.loc_x,
        loc_y = excluded.loc_y,
        shot_attempted_flag = excluded.shot_attempted_flag,
        shot_made_flag = excluded.shot_made_flag;
    """

    rows = [
        (
            r["GAME_GEVENT_ID"], r["GAME_TEAM_ID"], r["GAME_ID"], int(r["GAME_EVENT_ID"]),
            r["SEASON_NUM"], r["GAME_DATE"],
            int(r["TEAM_ID"]), r["TEAM_NAME"], int(r["PLAYER_ID"]), r["PLAYER_NAME"],
            r["HTM"], r["VTM"],
            int(r["PERIOD"]), int(r["MINUTES_REMAINING"]), int(r["SECONDS_REMAINING"]),
            r["GRID_TYPE"], r["EVENT_TYPE"], r["ACTION_TYPE"], r["SHOT_TYPE"],
            r["SHOT_ZONE_BASIC"], r["SHOT_ZONE_AREA"], r["SHOT_ZONE_RANGE"],
            int(r["SHOT_DISTANCE"]), int(r["LOC_X"]), int(r["LOC_Y"]),
            int(r["SHOT_ATTEMPTED_FLAG"]), int(r["SHOT_MADE_FLAG"])
        )
        for _, r in df.iterrows()
    ]

    conn = get_conn()
    cur = conn.cursor()
    try:
        execute_batch(cur, sql, rows, page_size=1000)  # prueba 500–2000
        conn.commit()
    finally:
        cur.close()
        conn.close()