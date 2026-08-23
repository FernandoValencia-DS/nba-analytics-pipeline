from src.etl.load.core_games import upsert_fct_games
from src.etl.extract.games import fetch_games

def run(seasons = ['2024-25','2025-26']):
    df = fetch_games(seasons)
    upsert_fct_games(df)

if __name__ == "__main__":
    run()