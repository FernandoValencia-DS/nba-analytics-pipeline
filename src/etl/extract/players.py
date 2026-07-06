from nba_api.stats.static import players
import pandas as pd

HEADSHOT_BASE_URL = "https://cdn.nba.com/headshots/nba/latest/1040x760"

def fetch_players():
    df = pd.DataFrame(players.get_players())

    # --- LIMPIEZA ---
    df["first_name"] = df["first_name"].fillna("").str.strip()
    df["last_name"] = df["last_name"].fillna("").str.strip()
    df["full_name"] = df["full_name"].fillna("").str.strip()

    df.loc[df["first_name"] == "", "first_name"] = df["full_name"]
    df.loc[df["last_name"] == "", "last_name"] = df["full_name"]

    # --- PLAYER HEADSHOT ---
    df["player_headshot"] = df["id"].apply(
        lambda player_id: f"{HEADSHOT_BASE_URL}/{int(player_id)}.png"
    )

    return df