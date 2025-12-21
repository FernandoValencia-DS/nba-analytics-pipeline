import pandas as pd
from nba_api.stats.endpoints import leaguegamefinder
from src.etl.transform.trasform_games import transform_games

def fetch_games(seasons: list[str]) -> pd.DataFrame:
    dfs = []

    for season in seasons:
        partidos = leaguegamefinder.LeagueGameFinder(season_nullable=season)
        df_partidos = partidos.get_data_frames()[0]
        df_partidos['SEASON_NUM'] = season
        dfs.append(df_partidos)

    # Concatenar todos los DataFrames de temporadas
    df_concat = pd.concat(dfs, axis=0)

    # Aplicar transformaciones
    df_temporadas = transform_games(df_concat)

    # Asegurar índice limpio
    df_temporadas = df_temporadas.reset_index(drop=True)

    return df_temporadas