from prefect import flow, get_run_logger, task

from src.scripts.run_teams_load import run as run_teams
from src.scripts.run_players_load import run as run_players
from src.scripts.run_games_load import run as run_games
from src.scripts.run_standings_load import run as run_standings
from src.scripts.run_boxscore_load import run as run_boxscore
from src.scripts.run_shots_load import run as run_shots
from src.scripts.run_archetypes_load import run as run_archetypes

seasons = ["2025-26", "2026-27"]

@task(name="load-teams", retries=2, retry_delay_seconds=30)
def load_teams():
    run_teams()


@task(name="load-players", retries=2, retry_delay_seconds=30)
def load_players():
    run_players()


@task(name="load-games", retries=2, retry_delay_seconds=30)
def load_games():
    run_games(seasons=seasons)


@task(name="load-standings", retries=2, retry_delay_seconds=30)
def load_standings():
    run_standings(seasons=seasons)


@task(name="load-boxscore", retries=2, retry_delay_seconds=30)
def load_boxscore():
    run_boxscore(seasons=seasons)


@task(name="load-shots", retries=2, retry_delay_seconds=30)
def load_shots():
    run_shots(seasons=seasons)

@task(name="load-archetypes", retries=1, retry_delay_seconds=60)
def load_archetypes():
    summary = run_archetypes(seasons=seasons)
    get_run_logger().info("archetypes: %s", summary)


@flow(name="nba-data-pipeline")
def nba_pipeline():
    load_teams()
    load_players()
    load_games()
    load_standings()
    load_boxscore()
    load_shots()
    load_archetypes()


if __name__ == "__main__":
    nba_pipeline()
