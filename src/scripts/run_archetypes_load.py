"""Apply the trained archetype model to the given seasons and load the result.

Runs every day inside the Prefect flow, after shots and box scores are loaded. The model is
never refitted here: it is only applied, so archetypes stay comparable between seasons.

A player-season enters with MIN_FGA_PROVISIONAL shots and MIN_MINUTES_PROVISIONAL minutes, and
is marked as provisional until he reaches the shots the model was trained with (150).

Run it by hand from the repository root:
    python -m src.scripts.run_archetypes_load --seasons 2026-27
    python -m src.scripts.run_archetypes_load --seasons 2026-27 --dry-run
"""

import argparse
import logging
from datetime import datetime, timezone

import pandas as pd

from src.etl.load.core_player_archetypes import (
    FCT_COLS,
    upsert_dim_archetypes,
    upsert_fct_player_archetypes,
)
from src.features.config import (
    ACTIVE_MODEL_VERSION,
    ARCHETYPES,
    FEATURE_COLS,
    MIN_FGA_PROVISIONAL,
    MIN_MINUTES_PROVISIONAL,
)
from src.features.features_clustering import build_features, load_inputs
from src.models.archetypes import load_artifact, predict
from src.models.train_archetypes import artifact_path

logger = logging.getLogger(__name__)

# feature columns -> table columns
FEATURE_TO_COLUMN = {
    "Restricted Area": "share_restricted_area",
    "In The Paint (Non-RA)": "share_paint",
    "Mid-Range": "share_mid_range",
    "Corner 3": "share_corner_3",
    "Above the Break 3": "share_above_break_3",
    "assists": "ast_per36",
    "rebounds_offensive": "oreb_per36",
    "field_goals_attempted": "fga_per36",
    "free_throws_attempted": "fta_per36",
    "turnovers": "tov_per36",
}


def score_seasons(seasons: list[str], version: str = ACTIVE_MODEL_VERSION,
                  parquet_dir: str | None = None) -> pd.DataFrame:
    """Archetype of every player-season with enough data, in the layout of the table."""
    seasons = list(seasons)
    artifact = load_artifact(artifact_path(version))
    if artifact["feature_cols"] != FEATURE_COLS:
        raise ValueError("the features in config.py changed after training: retrain the model")

    shots, boxscore, games = load_inputs(seasons, parquet_dir)
    features = build_features(
        shots, boxscore, games, seasons,
        min_fga=MIN_FGA_PROVISIONAL,
        season_types=artifact["season_types"],   # same games as in training
    )
    features = features[features["minutes"] >= MIN_MINUTES_PROVISIONAL]
    if features.empty:
        logger.warning("no player-season reaches %d shots and %d minutes yet",
                       MIN_FGA_PROVISIONAL, MIN_MINUTES_PROVISIONAL)
        return pd.DataFrame(columns=FCT_COLS)

    pred = predict(artifact, features)
    df = features.rename(columns=FEATURE_TO_COLUMN).join(pred).reset_index()

    df["player_season_id"] = df["player_id"].astype(str) + "_" + df["season_num"]
    df["is_provisional"] = df["fga"] < artifact["min_fga"]
    df["model_version"] = artifact["version"]
    df["computed_at"] = datetime.now(timezone.utc)

    decimals = {c: 4 for c in df.columns if c.startswith(("affinity", "share_", "pc"))}
    decimals.update({c: 3 for c in df.columns if c.endswith("_per36")})
    decimals.update({"minutes": 1, "log_likelihood": 3})
    df = df.round(decimals)

    summary = df.groupby("season_num").agg(
        players=("player_id", "size"),
        provisional=("is_provisional", "sum"),
        borderline=("is_borderline", "sum"),
    )
    logger.info("scored player-seasons:\n%s", summary.to_string())
    logger.info("players by archetype and season:\n%s",
                pd.crosstab(df["season_num"], df["archetype_name"]).to_string())
    return df[FCT_COLS]


def run(seasons: list[str] | None = None, version: str = ACTIVE_MODEL_VERSION,
        dry_run: bool = False) -> dict:
    """Score the seasons and load them. Returns a short summary for the flow logs."""
    df = score_seasons(seasons or ["2026-27"], version)
    if dry_run:
        return {"rows": len(df), "loaded": False}

    upsert_dim_archetypes(ARCHETYPES)
    n = upsert_fct_player_archetypes(df)
    summary = {
        "rows": n,
        "loaded": True,
        "model_version": version,
        "by_season": df["season_num"].value_counts().to_dict(),
        "provisional": int(df["is_provisional"].sum()),
    }
    logger.info("archetypes loaded: %s", summary)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Score and load player archetypes.")
    parser.add_argument("--seasons", nargs="+", default=["2026-27"])
    parser.add_argument("--version", default=ACTIVE_MODEL_VERSION)
    parser.add_argument("--dry-run", action="store_true", help="score only, do not write")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    print(run(seasons=args.seasons, version=args.version, dry_run=args.dry_run))


if __name__ == "__main__":
    main()
