"""Shared constants for the player archetype features and model.

Training (src/models/train_archetypes.py) and the daily application read these values, so
both build the features in exactly the same way. Changing any of them requires retraining
the model.
"""

# --- data scope
TRAIN_SEASONS = ["2025-26"]
SEASON_TYPES = None          # None = every game in the tables (Regular Season + Playoffs).
                             # Use ["Regular Season"] to train on the regular season only.
MIN_FGA = 150                # minimum shot attempts per player-season to enter training

# --- shot zones (column shot_zone_basic of core.fct_shots)
THREE_PT_ZONES = {"Above the Break 3", "Left Corner 3", "Right Corner 3", "Backcourt"}
CORNER_ZONES = {"Left Corner 3": "Corner 3", "Right Corner 3": "Corner 3"}
ZONE_ORDER = [
    "Restricted Area",
    "In The Paint (Non-RA)",
    "Mid-Range",
    "Corner 3",
    "Above the Break 3",
]

# --- role block: rates per 36 minutes (columns of core.fct_boxscore)
ROLE_COLS = [
    "assists",
    "rebounds_offensive",
    "field_goals_attempted",
    "free_throws_attempted",
    "turnovers",
]
PER_MINUTES = 36

# --- feature matrix: the order is part of the model, never change it without retraining
FEATURE_COLS = ZONE_ORDER + ROLE_COLS
CLR_COLS = [f"clr_{zone}" for zone in ZONE_ORDER]
MODEL_COLS = CLR_COLS + ROLE_COLS
JEFFREYS_PRIOR = 0.5         # half a shot added to every zone before computing proportions

# --- model
N_COMPONENTS = 4             # PCA components that enter the GMM
N_ARCHETYPES = 3
GMM_N_INIT = 10
SEED = 0
BORDERLINE_THRESHOLD = 0.70  # affinity below this value = borderline player

# Each PCA component is oriented so that its anchor variable has a positive loading;
# this keeps the reading of the axes fixed between trainings.
PCA_ANCHORS = {
    0: "clr_Corner 3",         # PC1 +  perimeter shooter      -  interior / high usage
    1: "clr_Restricted Area",  # PC2 +  interior finisher      -  ball-dominant creator
    2: "clr_Mid-Range",        # PC3 +  mid-range specialist
    3: "assists",              # PC4 +  pure passer
}

# --- archetypes: internal code -> label shown in the dashboard, description and color
ARCHETYPES = {
    "creator": {
        "name": "Creador con balón",
        "description": "Genera su tiro y el de otros: más tiros, asistencias y tiros libres.",
        "color": "#d64541",
    },
    "role_shooter": {
        "name": "Tirador de rol",
        "description": "Juega sin balón: espera el pase en el perímetro y pierde pocos balones.",
        "color": "#2a78d6",
    },
    "interior_finisher": {
        "name": "Interior finalizador",
        "description": "Termina cerca del aro y genera segundas oportunidades con el rebote ofensivo.",
        "color": "#8e5fc2",
    },
}
