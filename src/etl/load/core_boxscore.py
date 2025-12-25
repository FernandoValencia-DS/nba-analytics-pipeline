from src.db.connection import get_conn
import pandas as pd
from psycopg2.extras import execute_batch

def upsert_fct_boxscore(df: pd.DataFrame, page_size: int = 1000):

    sql = """
    INSERT INTO core.fct_boxscore (
        game_player_id,
        game_id, team_id, person_id, game_team_id,
        team_city, team_name, team_tricode, team_slug,
        first_name, family_name, name_i, player_slug,
        position, comment, jersey_num, minutes,
        field_goals_made, field_goals_attempted, field_goals_percentage,
        three_pointers_made, three_pointers_attempted, three_pointers_percentage,
        free_throws_made, free_throws_attempted, free_throws_percentage,
        rebounds_offensive, rebounds_defensive, rebounds_total,
        assists, steals, blocks, turnovers, fouls_personal,
        points, plus_minus_points
    )
    VALUES (
        %s,
        %s, %s, %s, %s,
        %s, %s, %s, %s,
        %s, %s, %s, %s,
        %s, %s, %s, %s,
        %s, %s, %s,
        %s, %s, %s,
        %s, %s, %s,
        %s, %s, %s,
        %s, %s, %s, %s, %s,
        %s, %s
    )
    ON CONFLICT (game_player_id) DO UPDATE SET
        game_id = excluded.game_id,
        team_id = excluded.team_id,
        person_id = excluded.person_id,
        game_team_id = excluded.game_team_id,
        team_city = excluded.team_city,
        team_name = excluded.team_name,
        team_tricode = excluded.team_tricode,
        team_slug = excluded.team_slug,
        first_name = excluded.first_name,
        family_name = excluded.family_name,
        name_i = excluded.name_i,
        player_slug = excluded.player_slug,
        position = excluded.position,
        comment = excluded.comment,
        jersey_num = excluded.jersey_num,
        minutes = excluded.minutes,
        field_goals_made = excluded.field_goals_made,
        field_goals_attempted = excluded.field_goals_attempted,
        field_goals_percentage = excluded.field_goals_percentage,
        three_pointers_made = excluded.three_pointers_made,
        three_pointers_attempted = excluded.three_pointers_attempted,
        three_pointers_percentage = excluded.three_pointers_percentage,
        free_throws_made = excluded.free_throws_made,
        free_throws_attempted = excluded.free_throws_attempted,
        free_throws_percentage = excluded.free_throws_percentage,
        rebounds_offensive = excluded.rebounds_offensive,
        rebounds_defensive = excluded.rebounds_defensive,
        rebounds_total = excluded.rebounds_total,
        assists = excluded.assists,
        steals = excluded.steals,
        blocks = excluded.blocks,
        turnovers = excluded.turnovers,
        fouls_personal = excluded.fouls_personal,
        points = excluded.points,
        plus_minus_points = excluded.plus_minus_points;
    """

    rows = []
    for _, r in df.iterrows():
        rows.append((
            r["game_player_id"],
            r["gameId"], int(r["teamId"]), int(r["personId"]), r["game_team_id"],
            r["teamCity"], r["teamName"], r["teamTricode"], r["teamSlug"],
            r["firstName"], r["familyName"], r["nameI"], r["playerSlug"],
            r["position"], r["comment"], r["jerseyNum"], r["minutes"],
            int(r["fieldGoalsMade"]), int(r["fieldGoalsAttempted"]), float(r["fieldGoalsPercentage"]),
            int(r["threePointersMade"]), int(r["threePointersAttempted"]), float(r["threePointersPercentage"]),
            int(r["freeThrowsMade"]), int(r["freeThrowsAttempted"]), float(r["freeThrowsPercentage"]),
            int(r["reboundsOffensive"]), int(r["reboundsDefensive"]), int(r["reboundsTotal"]),
            int(r["assists"]), int(r["steals"]), int(r["blocks"]), int(r["turnovers"]), int(r["foulsPersonal"]),
            int(r["points"]), float(r["plusMinusPoints"])
        ))

    conn = get_conn()
    cur = conn.cursor()
    try:
        execute_batch(cur, sql, rows, page_size=page_size)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()
