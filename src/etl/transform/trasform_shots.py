import pandas as pd
import numpy as np
from src.etl.extract.teams import fetch_teams

def tr_shots(df: pd.DataFrame) -> pd.DataFrame:
    df['GAME_TEAM_ID'] = df['GAME_ID'].astype(str)+'-'+df['TEAM_ID'].astype(str)
    df['GAME_GEVENT_ID'] = df['GAME_ID'].astype(str)+'-'+df['GAME_EVENT_ID'].astype(str)
    df = df.drop_duplicates(subset=['GAME_GEVENT_ID'])

    #Quitar equipos no NBA
    equipos = list(fetch_teams()['id'].astype('str'))
    df = df.loc[
        df["TEAM_ID"].astype(str).isin(equipos)\
    ]

    #modificar orientación de los tiros
    df['LOC_X'] = df['LOC_X']*-1
    df['LOC_Y'] = df['LOC_Y']*-1

    return df

