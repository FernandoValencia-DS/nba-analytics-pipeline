from src.etl.load.core_games import upsert_fct_games
from src.etl.extract.games import fecth_games

seasons = ['2024-25','2025-26']

def run():
    df = fecth_games(seasons)

    upsert_fct_games(df)

if __name__ == "__main__":
    run()
