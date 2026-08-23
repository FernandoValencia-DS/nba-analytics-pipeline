from src.etl.load.core_shots import upsert_fct_shots
from src.etl.load.core_shot_zone_gp import refresh_fct_shot_zone_gp
from src.etl.extract.shots import fetch_shots


def run(seasons = ['2024-25','2025-26']):
    df = fetch_shots(seasons)
    upsert_fct_shots(df)
    refresh_fct_shot_zone_gp()

if __name__ == "__main__":
    run()