"""Feature matrix for the player archetype model.

One row per player-season with:
- shot profile: share of shot attempts from each zone in ZONE_ORDER (sums to 1);
- role: assists, offensive rebounds, field goal attempts, free throw attempts and turnovers
  per 36 minutes;
- control columns that do not enter the model: fga, minutes and games.

Training and the daily application must call `build_features`, never their own copy of
these steps, so that both see exactly the same variables.
"""

import logging
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

from src.features.config import (
    CLR_COLS,
    CORNER_ZONES,
    FEATURE_COLS,
    JEFFREYS_PRIOR,
    MIN_FGA,
    PER_MINUTES,
    ROLE_COLS,
    SEASON_TYPES,
    THREE_PT_ZONES,
    ZONE_ORDER,
)

logger = logging.getLogger(__name__)

KEYS = ["player_id", "season_num"]
CONTROL_COLS = ["fga", "minutes", "games"]

SHOT_COLS = ["game_id", "player_id", "season_num", "shot_type", "shot_zone_basic", "shot_attempted_flag"]
GAME_COLS = ["game_id", "season_num", "season_type"]
BOX_COLS = ["game_id", "person_id", "first_name", "family_name", "minutes"] + ROLE_COLS


# =============================================================================
# Inputs
# =============================================================================
def load_inputs(seasons: list[str], parquet_dir: str | Path | None = None,
                ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Shots, box score and games of the given seasons.

    Reads from the database by default, querying only the needed columns and seasons.
    With `parquet_dir`, reads local copies of the tables instead
    (fct_shots.parquet, fct_boxscore.parquet, fct_games.parquet).
    """
    seasons = list(seasons)
    if parquet_dir is not None:
        return _load_from_parquet(Path(parquet_dir), seasons)

    from src.db.connection import get_conn

    queries = {
        "shots": f"SELECT {', '.join(SHOT_COLS)} FROM core.fct_shots "
                 "WHERE season_num = ANY(%(seasons)s)",
        "games": f"SELECT DISTINCT {', '.join(GAME_COLS)} FROM core.fct_games "
                 "WHERE season_num = ANY(%(seasons)s)",
        "boxscore": f"SELECT {', '.join('b.' + c for c in BOX_COLS)} FROM core.fct_boxscore b "
                    "WHERE b.game_id IN (SELECT game_id FROM core.fct_games "
                    "WHERE season_num = ANY(%(seasons)s))",
    }
    frames = {}
    conn = get_conn()
    try:
        with warnings.catch_warnings():
            # pandas warns about non-SQLAlchemy connections; psycopg2 works fine here
            warnings.filterwarnings("ignore", message=".*SQLAlchemy.*", category=UserWarning)
            for name, sql in queries.items():
                frames[name] = pd.read_sql(sql, conn, params={"seasons": seasons})
    finally:
        conn.close()

    for name, df in frames.items():
        logger.info("loaded %s: %s rows", name, f"{len(df):,}")
    return frames["shots"], frames["boxscore"], frames["games"]


def _load_from_parquet(parquet_dir: Path, seasons: list[str]):
    shots = pd.read_parquet(parquet_dir / "fct_shots.parquet", columns=SHOT_COLS)
    games = pd.read_parquet(parquet_dir / "fct_games.parquet", columns=GAME_COLS).drop_duplicates()
    box = pd.read_parquet(parquet_dir / "fct_boxscore.parquet", columns=BOX_COLS)

    shots = shots[shots["season_num"].isin(seasons)]
    games = games[games["season_num"].isin(seasons)]
    box = box[box["game_id"].isin(games["game_id"])]
    for name, df in {"shots": shots, "boxscore": box, "games": games}.items():
        logger.info("loaded %s from parquet: %s rows", name, f"{len(df):,}")
    return shots, box, games


# =============================================================================
# Steps
# =============================================================================
def clean_shots(df_shots: pd.DataFrame, seasons: list[str]) -> pd.DataFrame:
    """Keep the given seasons and remove shots that do not describe playing style."""
    df = df_shots[df_shots["season_num"].isin(seasons)].copy()
    logger.info("shots in %s: %s", seasons, f"{len(df):,}")

    # 1. inconsistent shots: shot type does not match the zone
    is_three = df["shot_type"] == "3PT Field Goal"
    in_three_zone = df["shot_zone_basic"].isin(THREE_PT_ZONES)
    inconsistent = is_three != in_three_zone
    logger.info("inconsistent shots removed: %d", inconsistent.sum())
    df = df[~inconsistent]

    # 2. backcourt heaves
    is_backcourt = df["shot_zone_basic"] == "Backcourt"
    logger.info("backcourt shots removed: %d", is_backcourt.sum())
    df = df[~is_backcourt].copy()

    # 3. merge left and right corners
    df["shot_zone_basic"] = df["shot_zone_basic"].replace(CORNER_ZONES)

    unexpected = set(df["shot_zone_basic"].unique()) - set(ZONE_ORDER)
    if unexpected:
        raise ValueError(f"unexpected shot zones: {sorted(unexpected)}")
    return df


def filter_season_types(df: pd.DataFrame, df_games: pd.DataFrame,
                        season_types: list[str] | None) -> pd.DataFrame:
    """Keep the rows whose game belongs to `season_types` (None keeps every game)."""
    if season_types is None:
        return df
    allowed = df_games.loc[df_games["season_type"].isin(season_types), "game_id"].unique()
    kept = df[df["game_id"].isin(allowed)]
    logger.info("rows outside %s removed: %d", season_types, len(df) - len(kept))
    return kept


def filter_min_shots(df_shots: pd.DataFrame, min_fga: int) -> pd.DataFrame:
    """Keep the player-seasons with at least `min_fga` shot attempts."""
    fga = df_shots.groupby(KEYS)["shot_attempted_flag"].sum()
    included = fga[fga >= min_fga].index
    keys = pd.MultiIndex.from_frame(df_shots[KEYS])
    df = df_shots[keys.isin(included)].copy()

    logger.info("player-seasons before the %d-shot filter: %d", min_fga, len(fga))
    logger.info("player-seasons after the %d-shot filter:  %d", min_fga, len(included))
    return df


def shot_profile(df_shots: pd.DataFrame) -> pd.DataFrame:
    """Share of each player's shot attempts from each zone, with Jeffreys smoothing.

    Columns always follow ZONE_ORDER, even if a zone has no shots yet.
    """
    counts = (
        df_shots.groupby(KEYS + ["shot_zone_basic"])["shot_attempted_flag"].sum()
        .unstack(fill_value=0)
        .reindex(columns=ZONE_ORDER, fill_value=0)
    )
    smoothed = counts + JEFFREYS_PRIOR
    profile = smoothed.div(smoothed.sum(axis=1), axis=0)
    profile.columns.name = None
    profile["fga"] = counts.sum(axis=1)
    return profile


def role_per36(df_boxscore: pd.DataFrame, df_games: pd.DataFrame, seasons: list[str]) -> pd.DataFrame:
    """Role rates per 36 minutes, plus total minutes and games played."""
    df_box = df_boxscore[["game_id", "person_id", "minutes"] + ROLE_COLS].copy()

    # minutes come as 'mm:ss'; games not played (DNP) come empty or null
    parts = df_box["minutes"].astype("string").str.extract(r"^\s*(\d+):(\d{1,2})")
    played = parts.notna().all(axis=1)
    logger.info("box score rows without minutes (DNP) removed: %s", f"{(~played).sum():,}")
    df_box = df_box[played].copy()
    df_box["minutes"] = parts.loc[played, 0].astype(int) + parts.loc[played, 1].astype(int) / 60

    # season of each game
    games = df_games[["game_id", "season_num"]].drop_duplicates()
    if not games["game_id"].is_unique:
        raise ValueError("some game_id values belong to more than one season")
    df_box = df_box.merge(games, how="left", on="game_id", validate="many_to_one")
    logger.info("box score rows without season: %s", f"{df_box['season_num'].isna().sum():,}")
    df_box = df_box[df_box["season_num"].isin(seasons)]

    df_role = (
        df_box.rename(columns={"person_id": "player_id"})
        .groupby(KEYS)
        .agg(minutes=("minutes", "sum"),
             games=("game_id", "nunique"),
             **{col: (col, "sum") for col in ROLE_COLS})
    )
    df_role = df_role[df_role["minutes"] > 0]
    df_role[ROLE_COLS] = df_role[ROLE_COLS].div(df_role["minutes"], axis=0) * PER_MINUTES
    return df_role


# =============================================================================
# Entry points
# =============================================================================
def build_features(df_shots: pd.DataFrame, df_boxscore: pd.DataFrame, df_games: pd.DataFrame,
                   seasons: list[str], min_fga: int = MIN_FGA,
                   season_types: list[str] | None = SEASON_TYPES) -> pd.DataFrame:
    """Feature matrix: FEATURE_COLS (model input, fixed order) + CONTROL_COLS.

    Indexed by (player_id, season_num).
    """
    seasons = list(seasons)
    shots = clean_shots(df_shots, seasons)
    shots = filter_season_types(shots, df_games, season_types)
    shots = filter_min_shots(shots, min_fga)
    profile = shot_profile(shots)

    box = filter_season_types(df_boxscore, df_games, season_types)
    role = role_per36(box, df_games, seasons)

    df = profile.join(role, how="left")

    # the daily pipeline loads shots and box scores in separate tasks, so a player can have
    # shots before his box score arrives: drop him for now instead of failing the run
    missing = df[ROLE_COLS].isna().any(axis=1)
    if missing.any():
        logger.warning("player-seasons with shots but no box score, dropped: %d", missing.sum())
        df = df[~missing]

    if not np.allclose(df[ZONE_ORDER].sum(axis=1), 1):
        raise ValueError("shot profile rows do not sum to 1")
    if df[FEATURE_COLS].isna().any().any():
        raise ValueError("feature matrix has missing values")

    df["games"] = df["games"].astype(int)
    df["fga"] = df["fga"].astype(int)
    logger.info("feature matrix: %d player-seasons x %d features", len(df), len(FEATURE_COLS))
    return df[FEATURE_COLS + CONTROL_COLS]


def clr_matrix(features: pd.DataFrame) -> pd.DataFrame:
    """Model input: centered log-ratio of the shot profile + the role rates (MODEL_COLS order)."""
    shares = features[ZONE_ORDER].to_numpy(dtype=float)
    log_shares = np.log(shares)
    clr = log_shares - log_shares.mean(axis=1, keepdims=True)
    out = pd.DataFrame(clr, columns=CLR_COLS, index=features.index)
    out[ROLE_COLS] = features[ROLE_COLS]
    return out


def player_names(df_boxscore: pd.DataFrame) -> pd.Series:
    """Player name by player_id, taken from the box score."""
    names = df_boxscore["first_name"].fillna("").str.cat(df_boxscore["family_name"].fillna(""), sep=" ")
    return names.str.strip().groupby(df_boxscore["person_id"]).first().rename_axis("player_id")

