import pandas as pd 
from nba_api.stats.static import teams

def fetch_teams() -> pd.DataFrame:
    nba_teams = teams.get_teams()
    df = pd.DataFrame(nba_teams)
    return df
