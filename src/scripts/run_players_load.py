from src.etl.extract.players import fetch_players
from src.etl.load.core_players import upsert_core_players

def run():
    df = fetch_players()
    upsert_core_players(df)

if __name__ == "__main__":
    run()