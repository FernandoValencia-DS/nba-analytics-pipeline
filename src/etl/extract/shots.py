import pandas as pd
from nba_api.stats.endpoints import shotchartdetail
import numpy as np
from src.etl.transform.trasform_shots import tr_shots

def fetch_shots(seasons: list[str]):

    dfs = []

    for season in seasons:
        shots = shotchartdetail.ShotChartDetail(
            team_id=0,
            player_id=0,
            season_type_all_star='Regular Season',
            season_nullable=season,
            context_measure_simple="FGA"
        )
        df_shots = shots.get_data_frames()[0]
        df_shots['SEASON_NUM'] = season
        dfs.append(df_shots)
    
    df_contac = pd.concat(dfs, axis=0)
    
    df_transformado = tr_shots(df_contac)

    return df_transformado