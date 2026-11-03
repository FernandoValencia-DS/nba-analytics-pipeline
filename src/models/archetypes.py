"""Player archetype model: fit, project, predict, save and load.

The model is a chain fitted once on the training seasons and then only applied:

    features -> CLR -> StandardScaler -> PCA (+ anchor signs) -> GaussianMixture

Everything needed to apply it later (fitted objects, column order, archetype names) is kept
in a single dictionary, the "artifact", saved with joblib.
"""

import logging
from datetime import datetime, timezone
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import sklearn
from sklearn.decomposition import PCA
from sklearn.metrics import silhouette_score
from sklearn.mixture import GaussianMixture
from sklearn.preprocessing import StandardScaler

from src.features.config import (
    ARCHETYPES,
    BORDERLINE_THRESHOLD,
    FEATURE_COLS,
    GMM_N_INIT,
    MODEL_COLS,
    N_ARCHETYPES,
    N_COMPONENTS,
    PCA_ANCHORS,
    SEED,
)
from src.features.features_clustering import clr_matrix

logger = logging.getLogger(__name__)

ARTIFACT_DIR = Path(__file__).resolve().parent / "artifacts"


# =============================================================================
# Fit
# =============================================================================
def fit_model(features: pd.DataFrame, *, n_components: int = N_COMPONENTS,
              n_archetypes: int = N_ARCHETYPES, seed: int = SEED,
              metadata: dict | None = None) -> dict:
    """Fit scaler, PCA and GMM on the training features and return the artifact."""
    X = clr_matrix(features)
    scaler = StandardScaler().fit(X)
    X_scaled = scaler.transform(X)

    pca = PCA(n_components=n_components, random_state=seed).fit(X_scaled)
    signs = np.ones(n_components)
    for i, anchor in PCA_ANCHORS.items():
        if i < n_components:
            signs[i] = 1.0 if pca.components_[i, MODEL_COLS.index(anchor)] >= 0 else -1.0
    Z = pca.transform(X_scaled) * signs

    gmm = GaussianMixture(n_components=n_archetypes, covariance_type="full",
                          n_init=GMM_N_INIT, random_state=seed).fit(Z)
    clusters = gmm.predict(Z)
    cluster_names = _name_clusters(features, clusters, n_archetypes)

    sizes = pd.Series(clusters).map(cluster_names).value_counts()
    affinity = gmm.predict_proba(Z).max(axis=1)
    artifact = {
        "version": (metadata or {}).get("version", "v1"),
        "trained_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "feature_cols": list(FEATURE_COLS),
        "model_cols": list(MODEL_COLS),
        "scaler": scaler,
        "pca": pca,
        "signs": signs,
        "gmm": gmm,
        "cluster_names": cluster_names,
        "borderline_threshold": BORDERLINE_THRESHOLD,
        "metrics": {
            "n_player_seasons": int(len(features)),
            "explained_variance": [round(float(v), 4) for v in pca.explained_variance_ratio_],
            "silhouette": round(float(silhouette_score(Z, clusters)), 4),
            "sizes": {k: int(v) for k, v in sizes.items()},
            "borderline_share": round(float((affinity < BORDERLINE_THRESHOLD).mean()), 4),
        },
        "versions": {
            "scikit-learn": sklearn.__version__,
            "pandas": pd.__version__,
            "numpy": np.__version__,
        },
        **{k: v for k, v in (metadata or {}).items() if k != "version"},
    }
    return artifact


def _name_clusters(features: pd.DataFrame, clusters: np.ndarray, n_archetypes: int) -> dict[int, str]:
    """Name each GMM component by rule, because component numbers change between fits.

    The component with the most shots at the rim is the interior finisher; of the rest,
    the one with the most assists is the creator; the remaining one is the role shooter.
    """
    if n_archetypes != 3:
        raise ValueError("the naming rule is defined for 3 archetypes")
    means = features[FEATURE_COLS].groupby(clusters).mean()
    if len(means) != n_archetypes:
        raise ValueError(f"only {len(means)} of {n_archetypes} archetypes received players")

    interior = means["Restricted Area"].idxmax()
    creator = means.drop(index=interior)["assists"].idxmax()
    names = {int(c): "role_shooter" for c in means.index}
    names[int(interior)] = "interior_finisher"
    names[int(creator)] = "creator"
    return names


# =============================================================================
# Apply
# =============================================================================
def project(artifact: dict, features: pd.DataFrame) -> pd.DataFrame:
    """Coordinates of each player on the model's PCA axes (PC1, PC2, ...)."""
    missing = set(artifact["feature_cols"]) - set(features.columns)
    if missing:
        raise ValueError(f"features are missing columns: {sorted(missing)}")

    X = clr_matrix(features[artifact["feature_cols"]])
    if list(X.columns) != artifact["model_cols"]:
        raise ValueError("model columns do not match the ones used in training")

    Z = artifact["pca"].transform(artifact["scaler"].transform(X)) * artifact["signs"]
    return pd.DataFrame(Z, index=features.index, columns=[f"PC{i + 1}" for i in range(Z.shape[1])])


def predict(artifact: dict, features: pd.DataFrame) -> pd.DataFrame:
    """Archetype, affinity with every archetype and borderline flag for each player."""
    coords = project(artifact, features)
    gmm = artifact["gmm"]
    prob = gmm.predict_proba(coords.to_numpy())
    names = artifact["cluster_names"]

    order = np.argsort(prob, axis=1)
    out = pd.DataFrame(index=features.index)
    out["cluster"] = order[:, -1]
    out["archetype"] = out["cluster"].map(names)
    out["archetype_name"] = out["archetype"].map(lambda a: ARCHETYPES[a]["name"])
    for c, code in names.items():
        out[f"affinity_{code}"] = prob[:, c]
    out["affinity"] = prob.max(axis=1)
    out["secondary_archetype"] = pd.Series(order[:, -2], index=out.index).map(names)
    out["is_borderline"] = out["affinity"] < artifact["borderline_threshold"]
    out["log_likelihood"] = gmm.score_samples(coords.to_numpy())   # low = atypical profile
    out["pc1"] = coords["PC1"]
    out["pc2"] = coords["PC2"]
    return out


# =============================================================================
# Persist
# =============================================================================
def save_artifact(artifact: dict, path: str | Path, overwrite: bool = False) -> Path:
    path = Path(path)
    if path.exists() and not overwrite:
        raise FileExistsError(f"{path} already exists; use a new version or overwrite=True")
    path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(artifact, path, compress=3)
    logger.info("model saved -> %s", path)
    return path


def load_artifact(path: str | Path) -> dict:
    artifact = joblib.load(Path(path))
    trained_with = artifact["versions"]["scikit-learn"]
    if trained_with != sklearn.__version__:
        logger.warning("model trained with scikit-learn %s, running %s: pin the training version",
                       trained_with, sklearn.__version__)
    return artifact
