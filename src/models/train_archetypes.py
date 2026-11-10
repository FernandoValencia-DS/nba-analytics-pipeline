"""Train the player archetype model and save it as a joblib artifact.

Run it by hand from the repository root, not in the daily pipeline:

    python -m src.models.train_archetypes                      # read from the database
    python -m src.models.train_archetypes --parquet-dir src/data/parquet
    python -m src.models.train_archetypes --dry-run            # report only, save nothing

The choice of k and of the number of PCA components is made in the clustering notebook;
this script only fits the final model with the values in src/features/config.py.
"""

import argparse
import logging
from pathlib import Path

import pandas as pd

from src.features.config import ARCHETYPES, MIN_FGA, SEASON_TYPES, TRAIN_SEASONS
from src.features.features_clustering import build_features, load_inputs, player_names
from src.models.archetypes import ARTIFACT_DIR, fit_model, predict, project, save_artifact

logger = logging.getLogger(__name__)

DEFAULT_VERSION = "v1"
CONTROL_PLAYERS = {
    201939: "Stephen Curry",
    201942: "DeMar DeRozan",
    203083: "Andre Drummond",
    1627734: "Domantas Sabonis",
}


def artifact_path(version: str) -> Path:
    return ARTIFACT_DIR / f"archetypes_{version}.joblib"


def train(seasons: list[str] = TRAIN_SEASONS, min_fga: int = MIN_FGA,
          season_types: list[str] | None = SEASON_TYPES, parquet_dir: str | Path | None = None,
          version: str = DEFAULT_VERSION, save: bool = True, overwrite: bool = False):
    """Build the features, fit the model, log a report and (optionally) save the artifact.

    Returns (artifact, features, predictions) so the notebook can inspect the result.
    """
    seasons = list(seasons)
    output = artifact_path(version)
    if save and output.exists() and not overwrite:
        raise FileExistsError(f"{output} already exists: use a new --version or --overwrite")

    shots, boxscore, games = load_inputs(seasons, parquet_dir)
    features = build_features(shots, boxscore, games, seasons, min_fga, season_types)

    metadata = {
        "version": version,
        "train_seasons": seasons,
        "season_types": season_types,
        "min_fga": min_fga,
    }
    artifact = fit_model(features, metadata=metadata)
    predictions = predict(artifact, features)
    predictions["player_name"] = (player_names(boxscore)
                                  .reindex(predictions.index.get_level_values("player_id")).values)

    report(artifact, features, predictions)
    if save:
        save_artifact(artifact, output, overwrite=overwrite)
    return artifact, features, predictions


def report(artifact: dict, features: pd.DataFrame, predictions: pd.DataFrame) -> None:
    """Log what a person should check before using the model."""
    metrics = artifact["metrics"]
    variance = metrics["explained_variance"]
    logger.info("player-seasons: %d", metrics["n_player_seasons"])
    logger.info("explained variance by component: %s (total %.0f%%)",
                ", ".join(f"{v:.0%}" for v in variance), 100 * sum(variance))
    logger.info("silhouette: %.2f", metrics["silhouette"])
    logger.info("borderline players (affinity < %.2f): %.0f%%",
                artifact["borderline_threshold"], 100 * metrics["borderline_share"])

    names = {code: info["name"] for code, info in ARCHETYPES.items()}
    profile = (features.join(predictions["archetype"])
               .groupby("archetype")[artifact["feature_cols"]].mean().T
               .rename(columns=names).round(2))
    logger.info("players by archetype: %s",
                predictions["archetype_name"].value_counts().to_dict())
    logger.info("archetype profile (mean of each feature):\n%s", profile.to_string())

    bins = pd.cut(predictions["affinity"], [0, 0.6, 0.7, 0.8, 0.9, 0.99, 1.0]).value_counts().sort_index()
    logger.info("players by affinity with their main archetype:\n%s", bins.to_string())

    # most representative players: closest to the center of their archetype
    coords = project(artifact, features).to_numpy()
    centers = artifact["gmm"].means_[predictions["cluster"].to_numpy()]
    dist = pd.Series(((coords - centers) ** 2).sum(axis=1) ** 0.5, index=predictions.index)
    top = (predictions.assign(dist=dist).sort_values("dist")
           .groupby("archetype_name").head(3)[["archetype_name", "player_name", "affinity"]]
           .sort_values("archetype_name"))
    logger.info("representative players:\n%s", top.round(2).to_string())

    ids = predictions.index.get_level_values("player_id")
    control = predictions[ids.isin(list(CONTROL_PLAYERS))]
    if len(control):
        cols = ["archetype_name"] + [c for c in predictions.columns if c.startswith("affinity_")]
        logger.info("control players:\n%s", control[cols].round(2).to_string())


def main() -> None:
    parser = argparse.ArgumentParser(description="Train the player archetype model.")
    parser.add_argument("--seasons", nargs="+", default=TRAIN_SEASONS)
    parser.add_argument("--min-fga", type=int, default=MIN_FGA)
    parser.add_argument("--season-types", nargs="+", default=SEASON_TYPES,
                        help='e.g. "Regular Season" (default: every game in the tables)')
    parser.add_argument("--parquet-dir", default=None,
                        help="read local parquet copies of the tables instead of the database")
    parser.add_argument("--version", default=DEFAULT_VERSION)
    parser.add_argument("--dry-run", action="store_true", help="report only, do not save")
    parser.add_argument("--overwrite", action="store_true", help="replace an existing artifact")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    pd.set_option("display.width", 160)
    try:
        train(seasons=args.seasons, min_fga=args.min_fga, season_types=args.season_types,
              parquet_dir=args.parquet_dir, version=args.version,
              save=not args.dry_run, overwrite=args.overwrite)
    except FileExistsError as err:
        logger.error("%s", err)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
