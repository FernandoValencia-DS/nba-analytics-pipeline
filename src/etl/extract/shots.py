import pandas as pd
from nba_api.stats.endpoints import shotchartdetail
import numpy as np
from src.etl.extract._nba_http import nba_request_kwargs
from src.etl.transform.transform_shots import tr_shots
from src.db.connection import get_conn

def fetch_team_ids_for_shots() -> list[str]:
    conn = get_conn()
    cur = conn.cursor()

    consulta = """
    select distinct team_id
    from core.dim_teams
    """ 
    cur.execute(consulta)

    team_ids = [r[0] for r in cur.fetchall()]
    cur.close(); conn.close()
    return team_ids

team_ids = fetch_team_ids_for_shots()


def fetch_shots(seasons: list[str]):

    dfs = []
    
    for team_id in team_ids:
        for season in seasons:
            for season_type in ['Regular Season', 'Playoffs']:
                shots = shotchartdetail.ShotChartDetail(
                    team_id=team_id,
                    player_id=0,
                    season_type_all_star=season_type,
                    season_nullable=season,
                    context_measure_simple="FGA",
                    **nba_request_kwargs()
                )
                df_shots = shots.get_data_frames()[0]
                df_shots['SEASON_NUM'] = season
                dfs.append(df_shots)
                print(f"Tiros cargados para team_id: {team_id}, season: {season}")
    
    df_contac = pd.concat(dfs, axis=0)
    
    df_transformado = tr_shots(df_contac)

    return df_transformado