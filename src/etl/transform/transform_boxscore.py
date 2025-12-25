import pandas as pd
import numpy as np

def tr_boxscore(df: pd.DataFrame) -> pd.DataFrame:
    df['game_player_id'] = df['gameId'].astype(str)+'-'+df['personId'].astype(str)
    df['game_team_id'] = df['gameId'].astype(str)+'-'+df['teamId'].astype(str)

    return df