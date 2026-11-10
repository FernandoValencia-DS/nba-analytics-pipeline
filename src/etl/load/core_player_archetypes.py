"""Load the player archetypes into core.fct_player_archetypes and core.dim_archetypes."""

import pandas as pd
from psycopg2.extras import execute_batch

from src.db.connection import get_conn

FCT_COLS = [
    "player_season_id",
    "player_id",
    "season_num",
    "archetype",
    "archetype_name",
    "affinity_creator",
    "affinity_role_shooter",
    "affinity_interior_finisher",
    "affinity",
    "secondary_archetype",
    "is_borderline",
    "is_provisional",
    "fga",
    "minutes",
    "games",
    "share_restricted_area",
    "share_paint",
    "share_mid_range",
    "share_corner_3",
    "share_above_break_3",
    "ast_per36",
    "oreb_per36",
    "fga_per36",
    "fta_per36",
    "tov_per36",
    "pc1",
    "pc2",
    "pc3",
    "pc4",
    "log_likelihood",
    "model_version",
    "computed_at",
]

DIM_COLS = ["archetype", "archetype_name", "description", "color", "sort_order"]


def _upsert(table: str, key: str, cols: list[str], df: pd.DataFrame, page_size: int = 1000) -> int:
    """Insert or update rows of `table` by `key`. Returns the number of rows sent."""
    if df.empty:
        return 0

    sql = (
        f"INSERT INTO core.{table} ({', '.join(cols)}) "
        f"VALUES ({', '.join(['%s'] * len(cols))}) "
        f"ON CONFLICT ({key}) DO UPDATE SET "
        + ", ".join(f"{c} = excluded.{c}" for c in cols if c != key)
    )
    # psycopg2 does not adapt numpy scalars: convert to Python objects and NaN to NULL
    values = df[cols].astype(object)
    values = values.where(df[cols].notna(), None)
    rows = list(values.itertuples(index=False, name=None))

    conn = get_conn()
    cur = conn.cursor()
    try:
        execute_batch(cur, sql, rows, page_size=page_size)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()
    return len(rows)


def upsert_fct_player_archetypes(df: pd.DataFrame, page_size: int = 1000) -> int:
    """One row per player-season; a new run of the same season overwrites the previous one."""
    return _upsert("fct_player_archetypes", "player_season_id", FCT_COLS, df, page_size)


def upsert_dim_archetypes(archetypes: dict) -> int:
    """Name, description and color of each archetype, taken from src/features/config.py."""
    df = pd.DataFrame(
        [
            (code, info["name"], info["description"], info["color"], order)
            for order, (code, info) in enumerate(archetypes.items(), start=1)
        ],
        columns=DIM_COLS,
    )
    return _upsert("dim_archetypes", "archetype", DIM_COLS, df)
