from nba_api.stats.static import players
import pandas as pd
from src.etl.load.core_players import upsert_core_players

def run():
    df = pd.DataFrame(players.get_players())

    # --- LIMPIEZA OBLIGATORIA (casos tipo "Nene") ---
    df["first_name"] = df["first_name"].fillna("").str.strip()
    df["last_name"] = df["last_name"].fillna("").str.strip()
    df["full_name"] = df["full_name"].fillna("").str.strip()

    df.loc[df["first_name"] == "", "first_name"] = df["full_name"]
    df.loc[df["last_name"] == "", "last_name"] = df["full_name"]

    # Seguridad extra: eliminar cualquier fila aún inválida
    df = df[
        (df["first_name"] != "") &
        (df["last_name"] != "") &
        (df["full_name"] != "")
    ]

    upsert_core_players(df)

if __name__ == "__main__":
    run()