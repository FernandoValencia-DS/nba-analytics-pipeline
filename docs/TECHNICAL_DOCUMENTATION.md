# NBA Analytics Pipeline — Documentación Técnica

> Última actualización: 2026-03-21
> Branch actual: `zonas_cancha`

---

## Tabla de Contenidos

1. [Visión General](#1-visión-general)
2. [Arquitectura del Sistema](#2-arquitectura-del-sistema)
3. [Estructura de Directorios](#3-estructura-de-directorios)
4. [Configuración y Dependencias](#4-configuración-y-dependencias)
5. [Capa de Base de Datos](#5-capa-de-base-de-datos)
6. [Módulos ETL — Extract](#6-módulos-etl--extract)
7. [Módulos ETL — Transform](#7-módulos-etl--transform)
8. [Módulos ETL — Load](#8-módulos-etl--load)
9. [Scripts de Ejecución](#9-scripts-de-ejecución)
10. [Sistema de Zonas de Cancha](#10-sistema-de-zonas-de-cancha)
11. [Flujo de Datos Completo](#11-flujo-de-datos-completo)
12. [Esquema de Base de Datos](#12-esquema-de-base-de-datos)
13. [Claves Compuestas y Deduplicación](#13-claves-compuestas-y-deduplicación)
14. [Manejo de Errores y Rate Limiting](#14-manejo-de-errores-y-rate-limiting)
15. [Notebooks](#15-notebooks)

---

## 1. Visión General

El **NBA Analytics Pipeline** es un sistema ETL (Extract → Transform → Load) que consume datos de la API pública de la NBA, los transforma y los persiste en una base de datos PostgreSQL alojada en Supabase.

**Objetivos principales:**
- Ingesta automatizada de datos de temporadas regulares de la NBA
- Enriquecimiento de datos de tiros con zonas geométricas de cancha
- Persistencia idempotente mediante operaciones upsert
- Base de datos analítica lista para dashboards y modelos de predicción

**Temporadas configuradas:** `2024-25`, `2025-26`

---

## 2. Arquitectura del Sistema

```
┌─────────────────────────────────────────────────────────────┐
│                        NBA API                              │
│  (nba_api Python library — stats.nba.com)                   │
└────────────┬────────────────────────────────────────────────┘
             │  HTTP (rate-limited)
             ▼
┌─────────────────────────────────────────────────────────────┐
│                    EXTRACT Layer                            │
│  teams / players / games / shots / boxscore / standings     │
└────────────┬────────────────────────────────────────────────┘
             │  pandas DataFrames
             ▼
┌─────────────────────────────────────────────────────────────┐
│                   TRANSFORM Layer                           │
│  - Claves compuestas (PKs)                                  │
│  - Flag HOME/AWAY                                           │
│  - Flip de coordenadas de tiros                             │
│  - Asignación de zonas geométricas (GeoJSON + Shapely)      │
│  - Deduplicación                                            │
│  - Filtrado por equipos NBA activos                         │
└────────────┬────────────────────────────────────────────────┘
             │  pandas DataFrames transformados
             ▼
┌─────────────────────────────────────────────────────────────┐
│                      LOAD Layer                             │
│  INSERT ... ON CONFLICT DO UPDATE (upsert)                  │
│  execute_batch para volumen alto                            │
└────────────┬────────────────────────────────────────────────┘
             │  psycopg2 (SSL)
             ▼
┌─────────────────────────────────────────────────────────────┐
│              PostgreSQL en Supabase                         │
│  Schema: core                                               │
│  dim_teams / dim_players / dim_standings                    │
│  fct_games / fct_shots / fct_boxscore                       │
└─────────────────────────────────────────────────────────────┘
```

**Tecnologías clave:**
| Componente | Tecnología |
|---|---|
| Lenguaje | Python 3.11 |
| Fuente de datos | `nba_api` |
| Transformación | `pandas`, `numpy` |
| Geometría de zonas | `shapely` |
| Base de datos | PostgreSQL (Supabase) |
| Driver DB | `psycopg2` |
| Entorno | `python-dotenv` + conda |

---

## 3. Estructura de Directorios

```
nba-analytics-pipeline/
│
├── .env                          # Variables de entorno (no versionado)
├── requirements.txt              # Dependencias Python
│
├── src/
│   ├── __init__.py
│   ├── db/
│   │   └── connection.py         # Fábrica de conexiones PostgreSQL
│   │
│   ├── data/
│   │   └── zones/
│   │       ├── zones_flipped.geojson              # Polígonos de zonas (coordenadas flip)
│   │       ├── zones_flipped_visual.geojson        # Versión visual con backcourt limitado
│   │       └── zones_flipped_visual_labeled.geojson # Con puntos de etiqueta (label_x, label_y)
│   │
│   ├── etl/
│   │   ├── extract/
│   │   │   ├── teams.py          # Extracción de equipos (estático)
│   │   │   ├── players.py        # Extracción de jugadores (estático)
│   │   │   ├── games.py          # Extracción de partidos por temporada
│   │   │   ├── shots.py          # Extracción de tiros (shot chart)
│   │   │   ├── boxscore.py       # Extracción de boxscore por partido
│   │   │   └── standings.py      # Extracción de posiciones de conferencia
│   │   │
│   │   ├── transform/
│   │   │   ├── transform_games.py    # FLAG HA, claves compuestas, filtrado
│   │   │   ├── transform_shots.py    # Flip coords, zonas, claves compuestas
│   │   │   ├── transform_boxscore.py # Claves compuestas
│   │   │   ├── transform_standings.py# PK SEASON_TEAM_ID, ordenamiento
│   │   │   └── shot_zones.py         # Motor de asignación de zonas (GeoJSON + Shapely)
│   │   │
│   │   └── load/
│   │       ├── core_teams.py         # Upsert → dim_teams
│   │       ├── core_players.py       # Upsert → dim_players
│   │       ├── core_games.py         # Upsert → fct_games
│   │       ├── core_shots.py         # Upsert batch → fct_shots
│   │       ├── core_boxscore.py      # Upsert batch → fct_boxscore
│   │       └── core_standings.py     # Upsert → dim_standings
│   │
│   ├── scripts/
│   │   ├── run_teams_load.py         # Ejecutar pipeline de equipos
│   │   ├── run_players_load.py       # Ejecutar pipeline de jugadores
│   │   ├── run_games_load.py         # Ejecutar pipeline de partidos
│   │   ├── run_shots_load.py         # Ejecutar pipeline de tiros
│   │   ├── run_boxscore_load.py      # Ejecutar pipeline de boxscore (bloques de 400)
│   │   └── run_standings_load.py     # Ejecutar pipeline de standings
│   │
│   ├── features/                     # (reservado)
│   ├── models/                       # (reservado)
│   └── visualization/                # (reservado)
│
├── notebooks/
│   └── poligonos.ipynb               # Construcción de polígonos de zonas de cancha
│
└── tests/
    └── partido_unico.ipynb           # Análisis exploratorio de partido único
```

---

## 4. Configuración y Dependencias

### Variables de Entorno (`.env`)

```dotenv
SUPABASE_DB_HOST=<host>
SUPABASE_DB_PORT=5432           # default: 5432
SUPABASE_DB_NAME=postgres       # default: postgres
SUPABASE_DB_USER=postgres       # default: postgres
SUPABASE_DB_PASSWORD=<password>
SUPABASE_DB_SSLMODE=require     # default: require
```

El archivo `.env` está excluido del repositorio por `.gitignore`.

### Dependencias principales

| Librería | Uso |
|---|---|
| `nba_api` | Cliente para la API de estadísticas de la NBA |
| `pandas` | Manipulación de DataFrames en todas las capas |
| `numpy` | Operaciones vectoriales (coordenadas, flags) |
| `psycopg2` | Driver PostgreSQL para Python |
| `python-dotenv` | Carga de variables de entorno desde `.env` |
| `shapely` | Geometría computacional para zonas de cancha |
| `requests` | Manejo de errores HTTP en boxscore |

### Conexión a Base de Datos

**Archivo:** `src/db/connection.py`

```python
def get_conn() -> psycopg2.connection
```

Devuelve una nueva conexión psycopg2 con las credenciales del `.env`. Cada función de carga crea y cierra su propia conexión (no hay pool de conexiones).

---

## 6. Módulos ETL — Extract

### 6.1 `extract/teams.py`

**Función:** `fetch_teams() → pd.DataFrame`

- Fuente: `nba_api.stats.static.teams.get_teams()` (lista estática, sin llamada HTTP por season)
- Construye URL de logo SVG: `https://cdn.nba.com/logos/nba/{team_id}/primary/L/logo.svg`
- Devuelve columnas: `id, full_name, abbreviation, nickname, city, state, year_founded, team_logo`

### 6.2 `extract/players.py`

**Función:** `fetch_players() → pd.DataFrame`

- Fuente: `nba_api.stats.static.players.get_players()` (lista estática completa histórica)
- Limpieza: `strip()`, relleno de `first_name`/`last_name` vacíos con `full_name`
- Construye URL de headshot PNG: `https://cdn.nba.com/headshots/nba/latest/1040x760/{player_id}.png`
- Devuelve columnas: `id, full_name, first_name, last_name, is_active, player_headshot`

### 6.3 `extract/games.py`

**Función:** `fetch_games(seasons: list[str]) → pd.DataFrame`

- Fuente: `nba_api.stats.endpoints.LeagueGameFinder` (Regular Season)
- Itera por temporada, agrega columna `SEASON_NUM`
- Concatena todos los DataFrames y aplica `transform_games()`
- Devuelve DataFrame transformado con índice limpio

### 6.4 `extract/shots.py`

**Función:** `fetch_shots(seasons: list[str]) → pd.DataFrame`

- Fuente: `nba_api.stats.endpoints.ShotChartDetail`
- Parámetros: `team_id=0, player_id=0` (toda la liga), `context_measure_simple="FGA"`
- Itera por temporada, agrega `SEASON_NUM`
- Aplica `tr_shots()` que incluye flip de coordenadas y asignación de zonas

### 6.5 `extract/boxscore.py`

Contiene dos funciones:

#### `fetch_pending_game_ids_for_boxscore(seasons=None) → list[str]`

Consulta la DB para obtener `game_id`s presentes en `fct_games` pero ausentes en `fct_boxscore`. Permite retomar ejecuciones parciales sin duplicados.

```sql
SELECT DISTINCT g.game_id
FROM core.fct_games g
LEFT JOIN core.fct_boxscore b ON b.game_id = g.game_id
WHERE b.game_id IS NULL
  [AND g.season_num = ANY(%s)]
ORDER BY g.game_id
```

#### `fetch_gamebox(game_ids, sleep_sec=0.6, ...) → pd.DataFrame`

Extrae boxscore para una lista de `game_id`s con manejo robusto de errores:

- **Endpoint primario:** `BoxScoreTraditionalV3` (3 intentos)
- **Fallback:** `BoxScoreTraditionalV2` (2 intentos)
- **Backoff:** 1.5s × intento (1.5s, 3s, 4.5s)
- **Cooldown:** 60s al acumular 3 fallos consecutivos
- **Rate limiting:** `sleep_sec=0.6s` entre requests exitosos
- Agrega columna `SOURCE_ENDPOINT` para trazabilidad
- Aplica `tr_boxscore()` al DataFrame final

### 6.6 `extract/standings.py`

**Función:** `fetch_standings(seasons: list[str]) → pd.DataFrame`

- Fuente: `nba_api.stats.endpoints.LeagueStandingsV3` (Regular Season, league_id="00")
- Selecciona 18 columnas relevantes: posición, récord, conferencia, división
- Concatena temporadas y ordena por `(SeasonID, Conference, PlayoffRank)`

---

## 7. Módulos ETL — Transform

### 7.1 `transform/transform_games.py`

**Función:** `transform_games(df) → pd.DataFrame`

Transformaciones aplicadas:

| Transformación | Lógica |
|---|---|
| Flag `HA` (Home/Away) | Parsea el campo `MATCHUP`: si el equipo es el primero y hay `@` es AWAY; si hay `vs.` es HOME |
| Clave `GAME_TEAM_ID` | `game_id + "-" + team_id` |
| Deduplicación | `drop_duplicates(subset=['GAME_TEAM_ID'])` |
| Clave `SEASON_TEAM_ID` | `season_id + "-" + team_id` |
| Filtrado | Solo equipos con ID en el listado estático de `fetch_teams()` |

**Lógica del flag HOME/AWAY:**
```
MATCHUP tiene formato: "BOS vs. LAL" o "BOS @ LAL"
- matchup_sim = MATCHUP[4]  → 'v' o '@'
- matchup_pri = MATCHUP[0:3] → abreviatura del primer equipo
Si TEAM_ABBREVIATION == matchup_pri y matchup_sim == '@' → AWAY
Si TEAM_ABBREVIATION != matchup_pri y matchup_sim == 'v' → AWAY
En caso contrario → HOME
```

### 7.2 `transform/transform_shots.py`

**Función:** `tr_shots(df) → pd.DataFrame`

| Transformación | Detalle |
|---|---|
| `GAME_TEAM_ID` | `game_id + "-" + team_id` |
| `GAME_GEVENT_ID` | `game_id + "-" + game_event_id` (PK) |
| `GAME_PLAYER_ID` | `game_id + "-" + player_id` |
| Deduplicación | `drop_duplicates(subset=['GAME_GEVENT_ID'])` |
| Filtrado | Solo equipos NBA activos |
| **Flip de coordenadas** | `LOC_X *= -1`, `LOC_Y *= -1` |
| **Asignación de zonas** | `load_zones()` + `assign_zones_to_df()` con `max_dist=12.0` |

El flip de coordenadas es necesario porque la NBA API devuelve los tiros con orientación invertida respecto al sistema de coordenadas interno del proyecto.

### 7.3 `transform/transform_boxscore.py`

**Función:** `tr_boxscore(df) → pd.DataFrame`

| Transformación | Detalle |
|---|---|
| `game_player_id` | `gameId + "-" + personId` (PK) |
| `game_team_id` | `gameId + "-" + teamId` |

Nota: los nombres de columna del boxscore usan camelCase (por el endpoint V3 de la NBA API).

### 7.4 `transform/transform_standings.py`

**Función:** `transform_standings(df) → pd.DataFrame`

| Transformación | Detalle |
|---|---|
| `SEASON_TEAM_ID` | `SeasonID + "-" + TeamID` (PK) |
| Ordenamiento | `(SeasonID, Conference, PlayoffRank)` |

### 7.5 `transform/shot_zones.py`

Motor de asignación de zonas de cancha. Ver sección completa en [Sistema de Zonas de Cancha](#10-sistema-de-zonas-de-cancha).

---

## 8. Módulos ETL — Load

Todos los módulos de carga siguen el patrón **upsert**:
```sql
INSERT INTO core.<tabla> (...) VALUES (...)
ON CONFLICT (<pk>) DO UPDATE SET <col> = EXCLUDED.<col>, ...
```

Para tablas de alto volumen (`fct_shots`, `fct_boxscore`) se usa `psycopg2.extras.execute_batch` con `page_size=1000`.

### 8.1 `load/core_teams.py` → `dim_teams`

- PK: `team_id`
- Columnas: `team_id, full_name, abbreviation, nickname, city, state, year_founded, team_logo`
- Método: `execute()` en loop por fila

### 8.2 `load/core_players.py` → `dim_players`

- PK: `id`
- Columnas: `id, full_name, first_name, last_name, is_active, player_headshot`
- Método: `execute_batch(page_size=500)`

### 8.3 `load/core_games.py` → `fct_games`

- PK: `game_team_id`
- 32 columnas incluyendo: estadísticas de equipo por partido (pts, fgm, fga, fg3m, reb, ast, stl, blk, tov, etc.)
- Método: `execute()` en loop por fila

### 8.4 `load/core_shots.py` → `fct_shots`

- PK: `game_gevent_id`
- 29 columnas incluyendo: coordenadas (loc_x, loc_y), zona (`zone_id`), tipo de tiro, resultado
- Método: `execute_batch(page_size=1000)`

### 8.5 `load/core_boxscore.py` → `fct_boxscore`

- PK: `game_player_id`
- 36 columnas: estadísticas individuales completas (pts, reb, ast, stl, blk, fg, 3p, ft, +/-)
- Método: `execute_batch(page_size=1000)` con rollback en excepción

### 8.6 `load/core_standings.py` → `dim_standings`

- PK: `season_team_id`
- 19 columnas: récord, posición por conferencia y división, últimos 10 partidos
- Método: `execute()` en loop por fila

---

## 9. Scripts de Ejecución

Cada script en `src/scripts/` es autónomo y ejecutable directamente con Python.

### Orden recomendado de ejecución

```
1. run_teams_load.py       # Dimension base (sin dependencias)
2. run_players_load.py     # Dimension base (sin dependencias)
3. run_games_load.py       # Requiere equipos válidos en fetch_teams()
4. run_standings_load.py   # Independiente
5. run_shots_load.py       # Requiere equipos válidos en fetch_teams()
6. run_boxscore_load.py    # Requiere fct_games cargado (consulta game_ids pendientes)
```

### Detalle de scripts

| Script | Temporadas | Comportamiento especial |
|---|---|---|
| `run_teams_load.py` | N/A (estático) | Reconstruye URL de logo |
| `run_players_load.py` | N/A (estático) | — |
| `run_games_load.py` | `2024-25`, `2025-26` | — |
| `run_shots_load.py` | `2024-25`, `2025-26` | — |
| `run_boxscore_load.py` | `2024-25`, `2025-26` | Procesa solo 400 game_ids por ejecución; consulta pendientes |
| `run_standings_load.py` | `2024-25`, `2025-26` | Llama transform por separado |

### Ejecución

```bash
# Desde la raíz del proyecto
python -m src.scripts.run_teams_load
python -m src.scripts.run_players_load
python -m src.scripts.run_games_load
python -m src.scripts.run_shots_load
python -m src.scripts.run_standings_load

# Boxscore: ejecutar múltiples veces hasta completar todos los partidos pendientes
python -m src.scripts.run_boxscore_load
```

---

## 10. Sistema de Zonas de Cancha

Una de las características más complejas del proyecto. Divide la media cancha de la NBA en 15 zonas geométricas nominadas.

### Zonas definidas

| Zone ID | Nombre completo | Tipo de tiro |
|---|---|---|
| `RA` | Restricted Area | 2PT |
| `P_LOW` | Paint Low (sin RA) | 2PT |
| `P_HIGH` | Paint High (sin RA) | 2PT |
| `C3_L` | Corner 3 Izquierda | 3PT |
| `C3_R` | Corner 3 Derecha | 3PT |
| `ATB3_L` | Above Break 3 Izquierda | 3PT |
| `ATB3_C` | Above Break 3 Centro | 3PT |
| `ATB3_R` | Above Break 3 Derecha | 3PT |
| `SMR_L` | Short Midrange Izquierda | 2PT |
| `SMR_C` | Short Midrange Centro | 2PT |
| `SMR_R` | Short Midrange Derecha | 2PT |
| `LMR_L` | Long Midrange Izquierda | 2PT |
| `LMR_C` | Long Midrange Centro | 2PT |
| `LMR_R` | Long Midrange Derecha | 2PT |
| `BACKCOURT` | Backcourt | 3PT |

Tiros que no caen en ninguna zona reciben `zone_id = "OTHER_RARE"`.

### Construcción de polígonos (`notebooks/poligonos.ipynb`)

Los polígonos fueron construidos programáticamente usando **Shapely** con las medidas reales de la cancha NBA (en décimas de pie / unidades de la API):

| Parámetro | Valor |
|---|---|
| Cancha media: X | [-250, 250] |
| Cancha media: Y | [-52, 422.5] |
| Radio Restricted Area | 40 |
| Radio arco de 3 | 237.5 |
| X de corner 3 | ±219 |
| Radio Short Midrange (interno) | 80 |
| Radio frontera Short/Long MR | 165 |
| División L/C/R: rayos desde origen | 119° y 61° |

**Procedimiento:**
1. Se construyen polígonos en sistema normal (Y positivo hacia canasta)
2. Se aplica flip `(x, y) → (-x, -y)` para alinear con coordenadas de la NBA API (post flip interno)
3. Se exportan a GeoJSON

### Archivos GeoJSON generados

| Archivo | Uso |
|---|---|
| `zones_flipped.geojson` | Producción — usado en `transform_shots.py` |
| `zones_flipped_visual.geojson` | Visualización con backcourt limitado a Y=500 |
| `zones_flipped_visual_labeled.geojson` | Igual + `label_x`, `label_y` para etiquetas en mapas |

### Algoritmo de asignación (`shot_zones.py`)

**Función principal:** `assign_zone_id_hybrid_nearest(x, y, zones, shot_type, max_dist=12.0)`

**Algoritmo híbrido en dos pasos:**

1. **Paso A — covers:** Itera las zonas candidatas en orden de prioridad. Si el punto está dentro o en el borde del polígono (`geometry.covers(point)`), asigna esa zona. Método: `"covers"`, distancia: `0.0`.

2. **Paso B — nearest:** Si ningún polígono cubre el punto, busca el más cercano entre los candidatos. Si la distancia es ≤ `max_dist`, asigna esa zona. Método: `"nearest"`. Si supera `max_dist`, devuelve `"OTHER_RARE"`.

**Prioridad de zonas** (para resolver superposiciones en bordes):
```python
PRIORITY = {
    "RA": 1,
    "P_LOW": 2, "P_HIGH": 2,
    "C3_L": 3, "C3_R": 3,
    "ATB3_L": 4, "ATB3_C": 4, "ATB3_R": 4,
    "LMR_L": 5, "LMR_C": 5, "LMR_R": 5,
    "SMR_L": 6, "SMR_C": 6, "SMR_R": 6,
    "BACKCOURT": 7,
}
```

**Filtrado por tipo de tiro** (`_candidate_zones()`):
- `"2PT Field Goal"` → solo zonas 2PT (`RA`, `P_LOW`, `P_HIGH`, `SMR_*`, `LMR_*`)
- `"3PT Field Goal"` → solo zonas 3PT (`C3_*`, `ATB3_*`, `BACKCOURT`)

**Columnas agregadas al DataFrame:**
- `zone_id`: identificador de zona (e.g., `"RA"`, `"ATB3_C"`)
- `zone_method`: `"covers"` | `"nearest"` | `"other"`
- `zone_dist`: distancia al polígono asignado (`0.0` si `covers`)

---

## 11. Flujo de Datos Completo

### Pipeline de Equipos
```
nba_api.stats.static.teams
    → fetch_teams()
    → [agrega team_logo URL]
    → upsert_core_teams()
    → core.dim_teams
```

### Pipeline de Jugadores
```
nba_api.stats.static.players
    → fetch_players()
    → [limpieza names + headshot URL]
    → upsert_core_players()
    → core.dim_players
```

### Pipeline de Partidos
```
LeagueGameFinder(season) × N temporadas
    → fetch_games(seasons)
    → transform_games()
        ├─ Flag HA (Home/Away)
        ├─ GAME_TEAM_ID, SEASON_TEAM_ID
        ├─ Deduplicación
        └─ Filtrado por equipos NBA
    → upsert_fct_games()
    → core.fct_games
```

### Pipeline de Tiros
```
ShotChartDetail(all teams, season) × N temporadas
    → fetch_shots(seasons)
    → tr_shots()
        ├─ GAME_TEAM_ID, GAME_GEVENT_ID, GAME_PLAYER_ID
        ├─ Deduplicación
        ├─ Filtrado por equipos NBA
        ├─ Flip coordenadas (LOC_X, LOC_Y × -1)
        └─ assign_zones_to_df() → zone_id, zone_method, zone_dist
    → upsert_fct_shots()  [execute_batch]
    → core.fct_shots
```

### Pipeline de Boxscore
```
DB Query: game_ids pendientes (fct_games - fct_boxscore)
    → fetch_pending_game_ids_for_boxscore(seasons)
    → [toma bloque de 400]
    → fetch_gamebox(game_ids)
        ├─ Por cada game_id:
        │   ├─ BoxScoreTraditionalV3 (3 intentos + backoff)
        │   ├─ Fallback: BoxScoreTraditionalV2 (2 intentos)
        │   ├─ Rate limit: sleep 0.6s
        │   └─ Cooldown 60s tras 3 fallos consecutivos
        └─ tr_boxscore() → game_player_id, game_team_id
    → upsert_fct_boxscore()  [execute_batch]
    → core.fct_boxscore
```

### Pipeline de Standings
```
LeagueStandingsV3(season) × N temporadas
    → fetch_standings(seasons)
    → transform_standings()
        ├─ SEASON_TEAM_ID
        └─ Ordenamiento por conferencia/rank
    → upsert_fct_standings()
    → core.dim_standings
```

---

## 12. Esquema de Base de Datos

**Schema:** `core`

### Tablas de Dimensión

#### `core.dim_teams`
| Columna | Tipo | Descripción |
|---|---|---|
| `team_id` | INT | **PK** — ID NBA del equipo |
| `full_name` | TEXT | Nombre completo (e.g., "Los Angeles Lakers") |
| `abbreviation` | TEXT | Abreviatura (e.g., "LAL") |
| `nickname` | TEXT | Apodo (e.g., "Lakers") |
| `city` | TEXT | Ciudad |
| `state` | TEXT | Estado |
| `year_founded` | INT | Año de fundación |
| `team_logo` | TEXT | URL del logo SVG en cdn.nba.com |

#### `core.dim_players`
| Columna | Tipo | Descripción |
|---|---|---|
| `id` | INT | **PK** — ID NBA del jugador |
| `full_name` | TEXT | Nombre completo |
| `first_name` | TEXT | Nombre |
| `last_name` | TEXT | Apellido |
| `is_active` | BOOL | ¿Activo actualmente? |
| `player_headshot` | TEXT | URL headshot PNG en cdn.nba.com |

#### `core.dim_standings`
| Columna | Tipo | Descripción |
|---|---|---|
| `season_team_id` | TEXT | **PK** — `{season_id}-{team_id}` |
| `season_id` | INT | ID de temporada |
| `team_id` | INT | FK → dim_teams |
| `team_city` | TEXT | Ciudad del equipo |
| `team_name` | TEXT | Nombre del equipo |
| `conference` | TEXT | Conferencia (East / West) |
| `conference_record` | TEXT | Récord en conferencia |
| `playoff_rank` | INT | Posición para playoffs |
| `clinch_indicator` | TEXT | Indicador de clasificación |
| `division` | TEXT | División |
| `division_record` | TEXT | Récord en división |
| `division_rank` | INT | Posición en división |
| `wins` | INT | Victorias |
| `losses` | INT | Derrotas |
| `win_pct` | FLOAT | Porcentaje de victorias |
| `record` | TEXT | Récord total (e.g., "45-20") |
| `home` | TEXT | Récord de local |
| `road` | TEXT | Récord de visitante |
| `l10` | TEXT | Récord últimos 10 partidos |

### Tablas de Hechos

#### `core.fct_games`
| Columna | Tipo | Descripción |
|---|---|---|
| `game_team_id` | TEXT | **PK** — `{game_id}-{team_id}` |
| `game_id` | TEXT | ID del partido |
| `team_id` | INT | FK → dim_teams |
| `season_id` | INT | ID de temporada |
| `season_num` | TEXT | Temporada (e.g., "2024-25") |
| `season_team_id` | TEXT | FK → dim_standings |
| `game_date` | DATE | Fecha del partido |
| `matchup` | TEXT | Descripción del enfrentamiento |
| `ha` | TEXT | HOME o AWAY |
| `wl` | TEXT | W o L |
| `min` | TEXT | Minutos jugados |
| `pts` | INT | Puntos |
| `fgm / fga / fg_pct` | INT/FLOAT | Tiros de campo |
| `fg3m / fg3a / fg3_pct` | INT/FLOAT | Triples |
| `ftm / fta / ft_pct` | INT/FLOAT | Tiros libres |
| `oreb / dreb / reb` | INT | Rebotes |
| `ast / stl / blk / tov / pf` | INT | Asistencias, robos, tapones, pérdidas, faltas |
| `plus_minus` | INT | +/- del equipo |
| `team_abbreviation` | TEXT | Abreviatura del equipo |
| `team_name` | TEXT | Nombre del equipo |

#### `core.fct_shots`
| Columna | Tipo | Descripción |
|---|---|---|
| `game_gevent_id` | TEXT | **PK** — `{game_id}-{game_event_id}` |
| `game_team_id` | TEXT | FK → fct_games |
| `game_player_id` | TEXT | FK → fct_boxscore |
| `game_id` | TEXT | ID del partido |
| `game_event_id` | INT | ID del evento en el partido |
| `season_num` | TEXT | Temporada |
| `game_date` | DATE | Fecha |
| `team_id / team_name` | INT/TEXT | Equipo |
| `player_id / player_name` | INT/TEXT | Jugador |
| `htm / vtm` | TEXT | Equipo local / visitante |
| `period` | INT | Cuarto |
| `minutes_remaining / seconds_remaining` | INT | Tiempo restante |
| `grid_type / event_type / action_type` | TEXT | Tipo de evento |
| `shot_type` | TEXT | "2PT Field Goal" o "3PT Field Goal" |
| `shot_zone_basic / shot_zone_area / shot_zone_range` | TEXT | Zonas originales NBA |
| `shot_distance` | INT | Distancia en pies |
| `loc_x / loc_y` | INT | Coordenadas (post-flip) |
| `shot_attempted_flag` | INT | 1 = intentado |
| `shot_made_flag` | INT | 1 = anotado |
| `zone_id` | TEXT | Zona personalizada (e.g., "RA", "ATB3_C") |

#### `core.fct_boxscore`
| Columna | Tipo | Descripción |
|---|---|---|
| `game_player_id` | TEXT | **PK** — `{game_id}-{person_id}` |
| `game_id` | TEXT | FK → fct_games |
| `team_id / person_id` | INT | Equipo y jugador |
| `game_team_id` | TEXT | FK → fct_games (team level) |
| `team_city / team_name / team_tricode / team_slug` | TEXT | Datos del equipo |
| `first_name / family_name / name_i / player_slug` | TEXT | Datos del jugador |
| `position / comment / jersey_num` | TEXT | Posición y número |
| `minutes` | TEXT | Minutos jugados |
| `field_goals_made/attempted/percentage` | INT/FLOAT | Tiros de campo |
| `three_pointers_made/attempted/percentage` | INT/FLOAT | Triples |
| `free_throws_made/attempted/percentage` | INT/FLOAT | Tiros libres |
| `rebounds_offensive/defensive/total` | INT | Rebotes |
| `assists / steals / blocks / turnovers / fouls_personal` | INT | Stats defensivos |
| `points` | INT | Puntos |
| `plus_minus_points` | FLOAT | +/- del jugador |

---

## 13. Claves Compuestas y Deduplicación

El proyecto usa claves compuestas como PKs para garantizar idempotencia:

| Clave | Construcción | Tabla |
|---|---|---|
| `GAME_TEAM_ID` | `game_id + "-" + team_id` | `fct_games` (PK), `fct_shots` (FK), `fct_boxscore` (FK) |
| `GAME_GEVENT_ID` | `game_id + "-" + game_event_id` | `fct_shots` (PK) |
| `GAME_PLAYER_ID` | `game_id + "-" + player_id` | `fct_shots` (FK), `fct_boxscore` (PK) |
| `SEASON_TEAM_ID` | `season_id + "-" + team_id` | `fct_games` (FK), `dim_standings` (PK) |

**Estrategia de deduplicación:**
- En Extract/Transform: `drop_duplicates(subset=['<pk>'])` antes de cargar
- En Load: `ON CONFLICT (<pk>) DO UPDATE SET ...` para idempotencia total

---

## 14. Manejo de Errores y Rate Limiting

### Boxscore (el más crítico)

```
Por cada game_id:
  Intento V3 #1 → espera 1.5s si falla
  Intento V3 #2 → espera 3.0s si falla
  Intento V3 #3 → espera 4.5s si falla
  Si V3 falla → Fallback V2
    Intento V2 #1 → espera 1.5s si falla
    Intento V2 #2 → espera 3.0s si falla
  Si V2 falla → registra error, continúa

  Tras cada request exitoso: sleep 0.6s
  Si 3 fallos consecutivos: cooldown 60s
```

Los errores capturados son `requests.exceptions.RequestException` y sus subclases. Cualquier otra excepción no HTTP detiene el procesamiento del game_id pero no el pipeline completo.

### Estrategia de ejecución parcial (Boxscore)

El script `run_boxscore_load.py` procesa solo **400 game_ids por ejecución**. Al re-ejecutar, `fetch_pending_game_ids_for_boxscore()` detecta automáticamente cuáles ya fueron cargados y continúa donde quedó.

---

## 15. Notebooks

### `notebooks/poligonos.ipynb`

Notebook de construcción y validación del sistema de zonas de cancha. Contiene:
1. Helpers geométricos (rect, semicircle, circle, flip_geom, split_lr_center)
2. Construcción de todos los polígonos de cancha NBA con medidas reales
3. Exportación a los tres archivos GeoJSON en `src/data/zones/`
4. Visualización de validación con matplotlib

**Este notebook debe re-ejecutarse si se modifican las definiciones de zonas.**

### `tests/partido_unico.ipynb`

Notebook de análisis exploratorio que trabaja con datos de un partido específico. Incluye:
- Consulta a equipos NBA via `nba_api.stats.static.teams`
- Análisis y exploración interactiva de datos del partido

---

*Documentación generada a partir del análisis completo del código fuente en la rama `zonas_cancha`.*
