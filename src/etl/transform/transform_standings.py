import pandas as pd

def transform_standings(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform para standings.
    - Crea PK SEASON_TEAM_ID
    """

    # Crear PK season-team
    df["SEASON_TEAM_ID"] = (
        df["SeasonID"].astype(str)
        + "-"
        + df["TeamID"].astype(str)
    )

    df = df.sort_values(
        ["SeasonID", "Conference", "PlayoffRank"]
    )

    return df.reset_index(drop=True)