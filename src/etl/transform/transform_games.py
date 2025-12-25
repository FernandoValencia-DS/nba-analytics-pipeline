import pandas as pd
import numpy as np
from src.etl.extract.teams import fetch_teams

def extraer_sim(matchup):
    resultado = matchup[4]
    return resultado

def extraer_primero(matchup):
    resultado = matchup[0:3]
    return resultado

def transform_games(df_temporadas: pd.DataFrame) -> pd.DataFrame:
    df_temporadas['matchup_sim'] = df_temporadas['MATCHUP'].loc[:].apply(extraer_sim)
    df_temporadas['matchup_pri'] = df_temporadas['MATCHUP'].loc[:].apply(extraer_primero)
    df_temporadas['HA'] = np.where((df_temporadas['TEAM_ABBREVIATION'] == df_temporadas['matchup_pri'])&
        (df_temporadas['matchup_sim'] == '@'), 'AWAY', np.where(
            (df_temporadas['TEAM_ABBREVIATION'] != df_temporadas['matchup_pri']) &
                (df_temporadas['matchup_sim'] == 'v'),'AWAY', 'HOME'
        )
    )   
    df_temporadas.drop(columns=['matchup_sim', 'matchup_pri'], inplace=True)
    df_temporadas['GAME_TEAM_ID'] = df_temporadas['GAME_ID'].astype(str)+'-'+df_temporadas['TEAM_ID'].astype(str)
    df_temporadas = df_temporadas.drop_duplicates(subset=['GAME_TEAM_ID'])

    equipos = list(fetch_teams()['id'].astype('str'))
    df_temporadas = df_temporadas.loc[
        df_temporadas["TEAM_ID"].astype(str).isin(equipos)
    ]

    return df_temporadas
