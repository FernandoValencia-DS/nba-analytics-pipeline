from src.etl.load.core_teams import upsert_core_teams
from src.etl.extract.teams import fetch_teams

TEAM_LOGO_BASE_URL = "https://cdn.nba.com/logos/nba"

def run():
    df = fetch_teams()

    # --- TEAM LOGO URL (usando team id) ---
    df["team_logo"] = df["id"].apply(
        lambda team_id: f"{TEAM_LOGO_BASE_URL}/{int(team_id)}/primary/L/logo.svg"
    )

    upsert_core_teams(df)

if __name__ == "__main__":
    run()