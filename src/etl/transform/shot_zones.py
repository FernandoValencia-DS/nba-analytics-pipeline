import json
from typing import List, Dict, Tuple, Optional

from shapely.geometry import shape, Point

PRIORITY = {
    "RA": 1,
    "P_LOW": 2, "P_HIGH": 2,
    "C3_L": 3, "C3_R": 3,
    "ATB3_L": 4, "ATB3_C": 4, "ATB3_R": 4,
    "LMR_L": 5, "LMR_C": 5, "LMR_R": 5,
    "SMR_L": 6, "SMR_C": 6, "SMR_R": 6,
    "BACKCOURT": 7,
}

THREE_ZONE_IDS = {"C3_L", "C3_R", "ATB3_L", "ATB3_C", "ATB3_R", "BACKCOURT"}
TWO_ZONE_IDS   = {"RA", "P_LOW", "P_HIGH", "SMR_L", "SMR_C", "SMR_R", "LMR_L", "LMR_C", "LMR_R"}

def load_zones(geojson_path: str) -> List[Dict]:
    with open(geojson_path, "r") as f:
        gj = json.load(f)

    zones = []
    for feat in gj["features"]:
        zones.append({
            "zone_id": feat["properties"]["zone_id"],
            "geometry": shape(feat["geometry"]),
        })

    # Orden determinístico (prioridad + area como desempate)
    zones.sort(key=lambda z: (PRIORITY.get(z["zone_id"], 999), z["geometry"].area))
    return zones

def _candidate_zones(zones: List[Dict], shot_type: str) -> List[Dict]:
    if shot_type == "3PT Field Goal":
        return [z for z in zones if z["zone_id"] in THREE_ZONE_IDS]
    if shot_type == "2PT Field Goal":
        return [z for z in zones if z["zone_id"] in TWO_ZONE_IDS]
    return zones

def assign_zone_id_hybrid_nearest(
    x: float,
    y: float,
    zones: List[Dict],
    shot_type: str,
    max_dist: float = 12.0,
) -> Tuple[str, str, float]:
    """
    Retorna (zone_id, method, dist)
    method: "covers" | "nearest" | "other"
    dist: distancia al polígono asignado (0 si covers)
    """
    pt = Point(float(x), float(y))
    candidates = _candidate_zones(zones, shot_type)

    # (A) covers (incluye bordes)
    for z in candidates:
        if z["geometry"].covers(pt):
            return z["zone_id"], "covers", 0.0

    # (B) nearest válido dentro del grupo
    best_zone = None
    best_dist = None
    for z in candidates:
        d = z["geometry"].distance(pt)
        if best_dist is None or d < best_dist:
            best_dist = d
            best_zone = z["zone_id"]

    if best_zone is not None and best_dist is not None and best_dist <= max_dist:
        return best_zone, "nearest", float(best_dist)

    return "OTHER_RARE", "other", float(best_dist) if best_dist is not None else float("inf")

def assign_zones_to_df(df, zones, shot_type_col: str = "SHOT_TYPE", max_dist: float = 12.0):
    """
    Agrega columnas:
      - zone_id
      - zone_method
      - zone_dist
    """
    zone_ids = []
    methods = []
    dists = []

    for x, y, st in zip(df["LOC_X"], df["LOC_Y"], df[shot_type_col]):
        zid, m, d = assign_zone_id_hybrid_nearest(x, y, zones, st, max_dist=max_dist)
        zone_ids.append(zid)
        methods.append(m)
        dists.append(d)

    df["zone_id"] = zone_ids
    df["zone_method"] = methods
    df["zone_dist"] = dists
    return df
