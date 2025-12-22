from src.etl.load.core_shots import upsert_fct_shots
from src.etl.extract.shots import fetch_shots

seasons = ['2024-25','2025-26']

def run():
    df = fetch_shots(seasons)
    upsert_fct_shots(df)

if __name__ == "__main__":
    run()