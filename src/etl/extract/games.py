import time
import random
import pandas as pd
import requests
from nba_api.stats.endpoints import leaguegamefinder
from nba_api.stats.library.http import STATS_HEADERS
from src.etl.transform.transform_games import transform_games


# Headers de navegador real + headers legacy que a veces evitan bloqueos de stats.nba.com
NBA_HEADERS = {
    **STATS_HEADERS,
    'Referer': 'https://www.nba.com/',
    'Origin': 'https://www.nba.com',
    'x-nba-stats-origin': 'stats',
    'x-nba-stats-token': 'true',
}


def _fetch_with_retry(
    season: str,
    season_type: str,
    timeout: int = 90,
    attempts: int = 4,
    base_backoff: float = 2.0,
) -> pd.DataFrame:
    last_err = None

    for a in range(1, attempts + 1):
        try:
            partidos = leaguegamefinder.LeagueGameFinder(
                season_nullable=season,
                season_type_nullable=season_type,
                timeout=timeout,
                headers=NBA_HEADERS,
            )
            return partidos.get_data_frames()[0]

        except requests.exceptions.RequestException as e:
            last_err = e
            status = getattr(e.response, "status_code", None)

            if status in (403, 429):
                # Bloqueo activo: esperar mucho más, no tiene sentido reintentar rápido
                wait = base_backoff * a * 3
            else:
                # Timeout u otro error de red: backoff exponencial + jitter
                wait = base_backoff * a + random.uniform(0, 1.5)

            if a < attempts:
                print(
                    f"  Intento {a}/{attempts} falló para {season} {season_type} "
                    f"({status or type(e).__name__}). Esperando {wait:.1f}s..."
                )
                time.sleep(wait)

    raise last_err


def fetch_games(seasons: list[str]) -> pd.DataFrame:
    dfs = []
    fallidas = []

    for season in seasons:
        for season_type in ['Regular Season', 'Playoffs']:
            try:
                df_partidos = _fetch_with_retry(season, season_type)
                df_partidos['SEASON_NUM'] = season
                df_partidos['SEASON_TYPE'] = season_type
                dfs.append(df_partidos)
                print(f"OK: {season} {season_type} ({len(df_partidos)} filas)")

            except requests.exceptions.RequestException as e:
                print(f"ERROR: no se pudo obtener {season} {season_type}: {e}")
                fallidas.append((season, season_type))

            # Pausa entre requests para no saturar el servidor (con jitter)
            time.sleep(0.6 + random.uniform(0, 0.4))

    if fallidas:
        raise RuntimeError(
            f"No se pudo obtener datos de NBA stats para: {fallidas}"
        )

    # Concatenar todos los DataFrames obtenidos
    df_concat = pd.concat(dfs, axis=0)

    # Aplicar transformaciones
    df_temporadas = transform_games(df_concat)

    # Asegurar índice limpio
    df_temporadas = df_temporadas.reset_index(drop=True)

    return df_temporadas
