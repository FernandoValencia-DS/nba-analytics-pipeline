import pandas as pd
from nba_api.stats.static import teams

LOGO_BASE_URL = "https://cdn.nba.com/logos/nba"

def fetch_teams() -> pd.DataFrame:
    nba_teams = teams.get_teams()
    df = pd.DataFrame(nba_teams)

    # --- TEAM LOGO URL (usando team id) ---
    df["team_logo"] = df["id"].apply(
        lambda team_id: f"{LOGO_BASE_URL}/{int(team_id)}/primary/L/logo.svg"
    )

    return df