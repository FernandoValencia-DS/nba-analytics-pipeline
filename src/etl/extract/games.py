import time
import pandas as pd
import requests
from nba_api.stats.endpoints import leaguegamefinder
from src.etl.transform.transform_games import transform_games


def _fetch_with_retry(season: str, season_type: str, timeout: int = 90, attempts: int = 4, base_backoff: float = 2.0):
    last_err = None
    for a in range(1, attempts + 1):
        try:
            partidos = leaguegamefinder.LeagueGameFinder(
                season_nullable=season,
                season_type_nullable=season_type,
                timeout=timeout)
            return partidos.get_data_frames()[0]
        except requests.exceptions.RequestException as e:
            last_err = e
            if a < attempts:
                time.sleep(base_backoff * a)  # 2s, 4s, 6s...
    raise last_err

def fetch_games(seasons: list[str]) -> pd.DataFrame:
    dfs = []

    for season in seasons:
        for season_type in ['Regular Season', 'Playoffs']:
            df_partidos = _fetch_with_retry(season, season_type)
            df_partidos['SEASON_NUM'] = season
            df_partidos['SEASON_TYPE'] = season_type
            dfs.append(df_partidos)
            time.sleep(0.6)

    # Concatenar todos los DataFrames de temporadas
    df_concat = pd.concat(dfs, axis=0)

    # Aplicar transformaciones
    df_temporadas = transform_games(df_concat)

    # Asegurar índice limpio
    df_temporadas = df_temporadas.reset_index(drop=True)

    return df_temporadas