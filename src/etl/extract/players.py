import pandas as pd 
from nba_api.stats.static import players

def fetch_players() -> pd.DataFrame:
    nba_players = players.get_players()
    df = pd.DataFrame(nba_players)
    return df