from src.etl.extract.standings import fetch_standings
from src.etl.transform.transform_standings import transform_standings
from src.etl.load.core_standings import upsert_fct_standings


def run(seasons = ["2024-25", "2025-26"]):
    df_raw = fetch_standings(seasons)
    df = transform_standings(df_raw)
    upsert_fct_standings(df)

if __name__ == "__main__":
    run()