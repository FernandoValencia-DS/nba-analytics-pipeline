from src.etl.load.core_players import upsert_core_players
from src.etl.extract.players import fetch_players
def run():
    df = fetch_players()
    # columnas vienen como: id, full_name, abbreviation, nickname, city, state, year_founded
    upsert_core_players(df)

if __name__ == "__main__":
    run()