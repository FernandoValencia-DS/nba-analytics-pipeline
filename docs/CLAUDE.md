# NBA Analytics Pipeline — Contexto del Proyecto

## Descripción general

Pipeline ETL en Python que consume datos de la NBA API pública, los transforma y los persiste en PostgreSQL (Supabase). El objetivo final es construir una base de datos analítica lista para dashboards y modelos de predicción.

**Branch activo:** `zonas_cancha`  
**Temporadas configuradas:** `2024-25`, `2025-26`

---

## Stack tecnológico

| Componente | Tecnología |
|---|---|
| Lenguaje | Python 3.11 |
| Entorno | conda + python-dotenv |
| Fuente de datos | `nba_api` (stats.nba.com) |
| Transformación | `pandas`, `numpy` |
| Geometría de zonas | `shapely` + GeoJSON |
| Base de datos | PostgreSQL en Supabase (schema: `core`) |
| Driver DB | `psycopg2` (SSL, sin pool de conexiones) |

---

## Estructura de directorios

```
nba-analytics-pipeline/
├── .env                          # Credenciales (NO versionado)
├── requirements.txt
├── CLAUDE.md                     # Este archivo
├── src/
│   ├── db/connection.py          # get_conn() → psycopg2.connection
│   ├── data/zones/               # GeoJSON de zonas de cancha
│   │   ├── zones_flipped.geojson              # Producción
│   │   ├── zones_flipped_visual.geojson
│   │   └── zones_flipped_visual_labeled.geojson
│   ├── etl/
│   │   ├── extract/              # teams, players, games, shots, boxscore, standings
│   │   ├── transform/            # transform_games, transform_shots, transform_boxscore,
│   │   │                         # transform_standings, shot_zones
│   │   └── load/                 # core_teams, core_players, core_games,
│   │                             # core_shots, core_boxscore, core_standings
│   ├── scripts/                  # Scripts de ejecución autónomos
│   ├── features/                 # (reservado — feature engineering)
│   ├── models/                   # (reservado — modelos ML)
│   └── visualization/            # (reservado — visualizaciones)
├── notebooks/
│   └── poligonos.ipynb           # Construcción de polígonos de zonas
└── tests/
    └── partido_unico.ipynb       # Análisis exploratorio
```

---

## Base de datos — Schema `core`

### Tablas de dimensión
- `dim_teams` — PK: `team_id`
- `dim_players` — PK: `id`
- `dim_standings` — PK: `season_team_id` (`{season_id}-{team_id}`)

### Tablas de hechos
- `fct_games` — PK: `game_team_id` (`{game_id}-{team_id}`)
- `fct_shots` — PK: `game_gevent_id` (`{game_id}-{game_event_id}`)
- `fct_boxscore` — PK: `game_player_id` (`{game_id}-{person_id}`)

### Claves compuestas (patrón de idempotencia)
```
GAME_TEAM_ID   = game_id + "-" + team_id
GAME_GEVENT_ID = game_id + "-" + game_event_id   ← PK de fct_shots
GAME_PLAYER_ID = game_id + "-" + player_id
SEASON_TEAM_ID = season_id + "-" + team_id
```

---

## Convenciones del código

### Patrón de carga (upsert)
Todos los módulos de load usan:
```sql
INSERT INTO core.<tabla> (...) VALUES (...)
ON CONFLICT (<pk>) DO UPDATE SET <col> = EXCLUDED.<col>
```
Tablas de alto volumen (`fct_shots`, `fct_boxscore`) usan `execute_batch(page_size=1000)`.

### Patrón de conexión
Cada función de carga crea y cierra su propia conexión. No hay pool de conexiones.
```python
from src.db.connection import get_conn
conn = get_conn()
# ... operaciones ...
conn.close()
```

### Ejecución de scripts
```bash
# Siempre desde la raíz del proyecto
python -m src.scripts.run_teams_load
python -m src.scripts.run_players_load
python -m src.scripts.run_games_load
python -m src.scripts.run_standings_load
python -m src.scripts.run_shots_load
python -m src.scripts.run_boxscore_load   # Ejecutar múltiples veces (400 game_ids por vez)
```

### Orden de dependencias
```
1. dim_teams     (sin dependencias)
2. dim_players   (sin dependencias)
3. fct_games     (requiere dim_teams)
4. dim_standings (independiente)
5. fct_shots     (requiere dim_teams)
6. fct_boxscore  (requiere fct_games — consulta game_ids pendientes)
```

---

## Comportamientos críticos a preservar

### Flip de coordenadas de tiros
La NBA API devuelve coordenadas invertidas. El flip es obligatorio:
```python
LOC_X *= -1
LOC_Y *= -1
```
Este flip se aplica en `transform_shots.py` ANTES de asignar zonas.

### Sistema de zonas de cancha
- 15 zonas geométricas definidas con Shapely sobre medidas reales NBA
- GeoJSON de producción: `src/data/zones/zones_flipped.geojson`
- Motor: `shot_zones.py` → `assign_zone_id_hybrid_nearest(x, y, zones, shot_type, max_dist=12.0)`
- Algoritmo: primero `covers` (punto dentro del polígono), luego `nearest` (más cercano ≤ 12.0 unidades)
- Filtrado por tipo de tiro: 2PT → zonas paint/midrange; 3PT → zonas corner/above-break
- Si se modifican las zonas, re-ejecutar `notebooks/poligonos.ipynb`

### Rate limiting de la NBA API
- Sleep base: `0.6s` entre requests exitosos
- Backoff en boxscore: `1.5s × intento` (1.5s, 3.0s, 4.5s)
- Cooldown: `60s` tras 3 fallos consecutivos
- Fallback boxscore: `BoxScoreTraditionalV3` → `BoxScoreTraditionalV2`
- Boxscore procesa bloques de **400 game_ids** por ejecución; los pendientes se detectan automáticamente

### Deduplicación
Siempre aplicar `drop_duplicates(subset=['<pk>'])` en transform ANTES del load.

### Columnas camelCase en boxscore
El endpoint V3 de la NBA API devuelve columnas en camelCase (`gameId`, `personId`, `teamId`). El resto del pipeline usa UPPER_SNAKE_CASE.

---

## Variables de entorno requeridas

```dotenv
SUPABASE_DB_HOST=<host>
SUPABASE_DB_PORT=5432
SUPABASE_DB_NAME=postgres
SUPABASE_DB_USER=postgres
SUPABASE_DB_PASSWORD=<password>
SUPABASE_DB_SSLMODE=require
```

---

## Áreas reservadas para desarrollo futuro

- `src/features/` → Feature engineering para modelos predictivos
- `src/models/` → Modelos de ML (predicción de tiros, rendimiento, etc.)
- `src/visualization/` → Generación de visualizaciones y mapas de calor
- Automatización del pipeline (scheduling, orquestación, alertas)
