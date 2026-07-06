from src.etl.extract.boxscore import fetch_gamebox, fetch_pending_game_ids_for_boxscore
from src.etl.load.core_boxscore import upsert_fct_boxscore

def run():
    seasons = ["2024-25","2025-26"]
    pending = fetch_pending_game_ids_for_boxscore(seasons)

    # toma solo 400 esta corrida
    block = pending[:400]

    df = fetch_gamebox(block)
    if not df.empty:
        upsert_fct_boxscore(df, page_size=1000)

if __name__ == "__main__":
    run()


