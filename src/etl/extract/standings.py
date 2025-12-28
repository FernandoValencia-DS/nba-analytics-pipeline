import pandas as pd
from nba_api.stats.endpoints import leaguestandingsv3

def fetch_standings(seasons: list[str]) -> pd.DataFrame:
    dfs = []

    for season in seasons:
        standings = leaguestandingsv3.LeagueStandingsV3(
            league_id="00",
            season=season,
            season_type="Regular Season"
        )

        df = standings.get_data_frames()[0]

        MAIN_COLUMNS = [
            "SeasonID",
            "TeamID",
            "TeamCity",
            "TeamName",
            "Conference",
            "ConferenceRecord",
            "PlayoffRank",
            "ClinchIndicator",
            "Division",
            "DivisionRecord",
            "DivisionRank",
            "WINS",
            "LOSSES",
            "WinPCT",
            "Record",
            "HOME",
            "ROAD",
            "L10"
        ]

        df_standings = df[MAIN_COLUMNS].copy()

        dfs.append(df_standings)

    # Concatenar temporadas
    df_concat = pd.concat(dfs, axis=0)

    df_concat = df_concat.sort_values(
        ["SeasonID", "Conference", "PlayoffRank"]
    )

    df_concat = df_concat.reset_index(drop=True)

    return df_concat
