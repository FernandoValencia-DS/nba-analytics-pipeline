from src.etl.load.core_players import upsert_core_players

def run():
    df = pd.DataFrame(players.get_players())
    # columnas vienen como: id, full_name, first_name, last_name, is_active
    upsert_core_players(df)

if __name__ == "__main__":
    run()