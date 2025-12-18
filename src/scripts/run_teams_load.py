from nba_api.stats.static import teams
import pandas as pd
from src.etl.load.core_teams import upsert_core_teams

def run():
    df = pd.DataFrame(teams.get_teams())
    # columnas vienen como: id, full_name, abbreviation, nickname, city, state, year_founded
    upsert_core_teams(df)

if __name__ == "__main__":
    run()
