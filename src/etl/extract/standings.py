import pandas as pd
from nba_api.stats.endpoints import leaguestandingsv3
from src.etl.extract._nba_http import nba_request_kwargs

def fetch_standings(seasons: list[str]) -> pd.DataFrame:
    dfs = []

    for season in seasons:
        standings = leaguestandingsv3.LeagueStandingsV3(
            league_id="00",
            season=season,
            season_type="Regular Season",
            **nba_request_kwargs()
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
