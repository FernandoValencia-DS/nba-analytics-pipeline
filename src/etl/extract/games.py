import pandas as pd
from nba_api.stats.endpoints import leaguegamefinder
import numpy as np
from src.etl.transform.trasform_games import transform_games



def fecth_games(seasons: list[str]):

    dfs = []

    for season in seasons:
        partidos = leaguegamefinder.LeagueGameFinder(season_nullable= season)
        df_partidos = partidos.get_data_frames()[0]
        df_partidos['SEASON_NUM'] = season
        dfs.append(df_partidos)

    df_concat = pd.concat(dfs, axis=0)

    df_temporadas = transform_games(df_concat)

    return df_temporadas

