import pandas as pd
from src.etl.extract.teams import fetch_teams
from src.etl.transform.shot_zones import load_zones, assign_zones_to_df

ZONES_GEOJSON = "src/data/zones/zones_flipped.geojson"

def tr_shots(df: pd.DataFrame) -> pd.DataFrame:
    df['GAME_TEAM_ID'] = df['GAME_ID'].astype(str) + '-' + df['TEAM_ID'].astype(str)
    df['GAME_GEVENT_ID'] = df['GAME_ID'].astype(str) + '-' + df['GAME_EVENT_ID'].astype(str)
    df = df.drop_duplicates(subset=['GAME_GEVENT_ID'])

    df['GAME_PLAYER_ID'] = df['GAME_ID'].astype(str) + '-' + df['PLAYER_ID'].astype(str)

    equipos = list(fetch_teams()['id'].astype(str))
    df = df.loc[df["TEAM_ID"].astype(str).isin(equipos)].copy()

    # flip
    df['LOC_X'] = df['LOC_X'] * -1
    df['LOC_Y'] = df['LOC_Y'] * -1

    # zonas
    zones = load_zones(ZONES_GEOJSON)
    df = assign_zones_to_df(df, zones, shot_type_col="SHOT_TYPE", max_dist=12.0)

    return df


