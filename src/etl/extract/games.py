import time
import random
import pandas as pd
import requests
from nba_api.stats.endpoints import leaguegamelog
from src.etl.transform.transform_games import transform_games


# Headers "de navegador real" para reducir bloqueos de stats.nba.com
NBA_HEADERS = {
    'Host': 'stats.nba.com',
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
        '(KHTML, like Gecko) Chrome/124.0 Safari/537.36'
    ),
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.9',
    'Referer': 'https://www.nba.com/',
    'Origin': 'https://www.nba.com',
    'x-nba-stats-origin': 'stats',
    'x-nba-stats-token': 'true',
    'Connection': 'keep-alive',
}


def _fetch_with_retry(
    season: str,
    season_type: str,
    timeout: int = 90,
    attempts: int = 4,
    base_backoff: float = 2.0,
) -> pd.DataFrame:
    """
    Intenta obtener los datos de una temporada/tipo de temporada con reintentos.
    Distingue entre bloqueos (403/429) y timeouts/errores de red para
    aplicar una espera distinta en cada caso.
    """
    last_err = None

    for a in range(1, attempts + 1):
        try:
            partidos = leaguegamelog.LeagueGameLog(
                season=season,
                season_type_all_star=season_type,
                timeout=timeout,
                headers=NBA_HEADERS,
            )
            df = partidos.get_data_frames()[0]

            # LeagueGameLog trae una columna extra (VIDEO_AVAILABLE) que
            # LeagueGameFinder no tenía. La descartamos para mantener la
            # misma estructura que el resto del pipeline espera.
            df = df.drop(columns=['VIDEO_AVAILABLE'], errors='ignore')
            return df

        except requests.exceptions.RequestException as e:
            last_err = e
            status = getattr(e.response, "status_code", None) if hasattr(e, "response") else None

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
    """
    Descarga partidos de NBA para las temporadas indicadas (Regular Season y Playoffs).
    Si una temporada/tipo falla tras todos los reintentos, se registra y se continúa
    con las demás en vez de abortar todo el pipeline.
    """
    dfs = []
    fallidas = []

    for season in seasons:
        for season_type in ['Regular Season', 'Playoffs']:
            try:
                df_partidos = _fetch_with_retry(season, season_type)

                if df_partidos.empty:
                    # Temporada/tipo sin partidos aún (ej. temporada futura o
                    # playoffs que no han empezado): no es un error, simplemente
                    # no hay datos todavía.
                    print(f"VACÍO: {season} {season_type} no tiene partidos registrados aún.")
                    continue

                df_partidos['SEASON_NUM'] = season
                df_partidos['SEASON_TYPE'] = season_type
                dfs.append(df_partidos)
                print(f"OK: {season} {season_type} ({len(df_partidos)} filas)")

            except requests.exceptions.RequestException as e:
                print(f"ERROR: no se pudo obtener {season} {season_type}: {e}")
                fallidas.append((season, season_type))

            # Pausa entre requests para no saturar el servidor (con jitter)
            time.sleep(0.6 + random.uniform(0, 0.4))

    if not dfs:
        raise RuntimeError("No se pudo obtener ningún dato de NBA stats tras todos los intentos.")

    if fallidas:
        print(f"ADVERTENCIA: las siguientes combinaciones fallaron y se omitieron: {fallidas}")

    # Concatenar todos los DataFrames obtenidos
    df_concat = pd.concat(dfs, axis=0)

    # Aplicar transformaciones
    df_temporadas = transform_games(df_concat)

    # Asegurar índice limpio
    df_temporadas = df_temporadas.reset_index(drop=True)

    return df_temporadas
