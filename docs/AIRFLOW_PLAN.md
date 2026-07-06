# Plan de Automatización con Apache Airflow

> Fecha: 2026-03-23
> Branch activo: `zonas_cancha`

A lo largo de este documento cada paso está etiquetado con el equipo donde se ejecuta:

- **`[DEV]`** → PC de desarrollo (equipo principal, donde vive el código ETL)
- **`[AIR]`** → Portátil secundario (el único que ejecuta Airflow)
- **`[AMBOS]`** → Acción necesaria en los dos equipos

---

## Índice

1. [Conceptos clave de Airflow](#1-conceptos-clave-de-airflow)
2. [Roles de cada equipo](#2-roles-de-cada-equipo)
3. [Setup inicial — PC de desarrollo](#3-setup-inicial--pc-de-desarrollo)
4. [Setup inicial — Portátil Airflow](#4-setup-inicial--portátil-airflow)
5. [Estructura de DAGs](#5-estructura-de-dags)
6. [Caso especial: Boxscore en bloques de 400](#6-caso-especial-boxscore-en-bloques-de-400)
7. [Logging y alertas](#7-logging-y-alertas)
8. [Flujo de trabajo Git entre equipos](#8-flujo-de-trabajo-git-entre-equipos)
9. [Cambios concretos al código](#9-cambios-concretos-al-código)
10. [Orden de implementación paso a paso](#10-orden-de-implementación-paso-a-paso)

---

## 1. Conceptos clave de Airflow

Estos son los conceptos de Airflow que se usan directamente en esta implementación:

**DAG** *(Directed Acyclic Graph)*: Es el "pipeline" en sí. Un fichero Python que define qué tareas se ejecutan, en qué orden, y con qué frecuencia. El Scheduler de Airflow lee la carpeta `dags/` periódicamente y programa las ejecuciones.

**Operator**: La plantilla de una tarea. El más usado es `PythonOperator` (ejecuta una función Python). También se usan `ShortCircuitOperator` (cortocircuita el pipeline si una condición no se cumple) y `TriggerDagRunOperator` (dispara otro DAG desde dentro de un DAG).

**Task**: Una instancia concreta de un Operator dentro de un DAG. La sintaxis `task_a >> task_b` declara que `task_b` no puede empezar hasta que `task_a` termine con éxito.

**XCom** *(Cross-Communication)*: Mecanismo para que una tarea pase datos a otra. Una tarea hace `return valor` o `ti.xcom_push(key, valor)`, y la siguiente lo lee con `ti.xcom_pull(task_ids='task_a')`. Se usa para pasar el conteo de boxscores pendientes entre tareas.

**Variables de Airflow**: Pares clave-valor globales almacenados en la metadata DB de Airflow, editables desde la UI sin tocar código. Se usan para la lista de temporadas y el límite de loops del boxscore.

---

## 2. Roles de cada equipo

```
┌──────────────────────────────────┐     ┌──────────────────────────────────┐
│       PC DE DESARROLLO           │     │       PORTÁTIL AIRFLOW           │
│                                  │     │                                  │
│  • Escribe código ETL (src/)     │     │  • Clona el repo (solo lectura)  │
│  • Escribe DAGs (dags/)          │     │  • Ejecuta Airflow 24/7*         │
│  • Hace git push                 │     │  • Hace git pull para sync       │
│  • NO ejecuta Airflow            │     │  • NO modifica código            │
│  • Entorno conda: etl-env        │     │  • Entorno conda: airflow-env    │
│                                  │     │  (* puede estar apagado)         │
└──────────────────────┬───────────┘     └─────────────────┬────────────────┘
                       │                                   │
                       └──────────── GitHub ───────────────┘
                                  (repositorio compartido)
```

El portátil puede estar apagado: cuando se enciende, el pipeline retoma
desde donde quedó porque `fetch_pending_game_ids_for_boxscore()` siempre
consulta qué falta en Supabase antes de procesar.

---

## 3. Setup inicial — PC de desarrollo

`[DEV]` Estos pasos se hacen **una sola vez** en el PC de desarrollo.

### 3.1 Crear la carpeta `dags/` en el repositorio

```bash
mkdir dags
touch dags/.gitkeep   # para que Git versione la carpeta vacía
```

La estructura final del repo quedará:

```
nba-analytics-pipeline/
├── dags/                        ← NUEVO
│   ├── nba_main_pipeline.py
│   ├── nba_boxscore_pipeline.py
│   └── git_sync.py
├── src/
├── docs/
└── pyproject.toml               ← NUEVO
```

### 3.2 Añadir `pyproject.toml`

Necesario para que el portátil Airflow pueda instalar el proyecto en modo
editable (`pip install -e .`) y los imports `from src.etl...` funcionen
sin manipular PYTHONPATH manualmente.

```toml
[build-system]
requires = ["setuptools"]
build-backend = "setuptools.backends.legacy:build"

[project]
name = "nba-analytics-pipeline"
version = "0.1.0"
requires-python = ">=3.11"

[tool.setuptools.packages.find]
where = ["."]
include = ["src*"]
```

### 3.3 Hacer los cambios al código ETL

Ver detalle completo en la [sección 9](#9-cambios-concretos-al-código).
En resumen:

- Añadir `get_pending_count()` en `src/etl/extract/boxscore.py`
- Refactorizar los `run_*.py` para aceptar `seasons` como parámetro opcional
- Escribir los ficheros DAG en `dags/`

### 3.4 Publicar los cambios

```bash
git add dags/ pyproject.toml src/
git commit -m "feat: add Airflow DAGs and pyproject.toml"
git push origin main
```

A partir de aquí, el flujo normal es: desarrollar en `[DEV]` → push → el portátil hace pull automático.

---

## 4. Setup inicial — Portátil Airflow

`[AIR]` Estos pasos se hacen **una sola vez** en el portátil secundario (WSL).

### 4.1 Clonar el repositorio

```bash
# En WSL del portátil
git clone <url-del-repo> ~/nba-analytics-pipeline
cd ~/nba-analytics-pipeline
```

### 4.2 Crear el entorno conda para Airflow

Airflow se instala con `pip` (no con `conda install airflow`), pero el
entorno lo gestiona conda. La razón: Airflow publica ficheros de
constraints diseñados para pip; el resolver de conda no los entiende y
genera conflictos.

```bash
# Crear entorno con Python 3.11, separado del entorno ETL del PC de desarrollo
mamba create -n airflow-env python=3.11
conda activate airflow-env

# Instalar Airflow con constraints oficiales para Python 3.11
AIRFLOW_VERSION=2.9.3
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.11.txt"

pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

# Instalar dependencias del proyecto ETL en el mismo entorno
cd ~/nba-analytics-pipeline
pip install -r requirements.txt

# Instalar el proyecto en modo editable (permite imports src.*)
pip install -e .
```

### 4.3 Configurar `AIRFLOW_HOME`

```bash
export AIRFLOW_HOME=~/airflow
echo 'export AIRFLOW_HOME=~/airflow' >> ~/.bashrc
source ~/.bashrc
```

### 4.4 Configurar `airflow.cfg`

**Opción A — SequentialExecutor + SQLite (recomendado para empezar):**

Las tareas se ejecutan de una en una. No requiere instalar nada extra.
Para este pipeline de pasos secuenciales es suficiente mientras se aprende.

```bash
# Inicializar la DB de Airflow (genera ~/airflow/airflow.cfg)
conda activate airflow-env
airflow db migrate
```

Editar `~/airflow/airflow.cfg`:

```ini
[core]
# Apunta a la carpeta dags/ dentro del repo clonado
dags_folder = /home/<tu-usuario-wsl>/nba-analytics-pipeline/dags
executor    = SequentialExecutor

[scheduler]
# Cada cuántos segundos el scheduler relee los ficheros DAG
min_file_process_interval = 30

[webserver]
web_server_port = 8080
```

**Opción B — LocalExecutor + PostgreSQL (para uso estable a largo plazo):**

Permite ejecutar tareas en paralelo. Requiere instalar PostgreSQL local.

```bash
# Instalar PostgreSQL en WSL
sudo apt update && sudo apt install postgresql postgresql-contrib

# Crear usuario y base de datos para Airflow
sudo service postgresql start
sudo -u postgres psql -c "CREATE USER airflow WITH PASSWORD 'airflow';"
sudo -u postgres psql -c "CREATE DATABASE airflow OWNER airflow;"
```

```ini
[core]
dags_folder      = /home/<tu-usuario-wsl>/nba-analytics-pipeline/dags
executor         = LocalExecutor
sql_alchemy_conn = postgresql+psycopg2://airflow:airflow@localhost/airflow

[scheduler]
min_file_process_interval = 30

[webserver]
web_server_port = 8080
```

### 4.5 Crear el fichero `.env` de Supabase

El `.env` no está versionado en Git (correcto). Hay que crearlo manualmente
en el portátil una vez:

```bash
cd ~/nba-analytics-pipeline

# Copiar desde .env.example si existe, o crearlo directamente
cat > .env << 'EOF'
SUPABASE_DB_HOST=<host>
SUPABASE_DB_PORT=5432
SUPABASE_DB_NAME=postgres
SUPABASE_DB_USER=postgres
SUPABASE_DB_PASSWORD=<password>
SUPABASE_DB_SSLMODE=require
EOF
```

Este fichero **nunca se sube a Git**. El PC de desarrollo ya tiene el suyo propio.

### 4.6 Crear el usuario administrador de Airflow

```bash
conda activate airflow-env
airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname Admin \
    --role Admin \
    --email admin@local.com
```

### 4.7 Arrancar los servicios

```bash
conda activate airflow-env

# En terminales separadas (o con & en background)
airflow scheduler &
airflow webserver --port 8080 &
```

UI accesible en `http://localhost:8080` desde el portátil.

### 4.8 Configurar las Variables de Airflow

Desde la UI: `Admin > Variables > +`

| Key | Value | Descripción |
|---|---|---|
| `nba_seasons` | `["2024-25", "2025-26"]` | Temporadas a procesar |
| `boxscore_max_loops` | `20` | Límite de auto-ejecuciones del boxscore |

Esto permite cambiar las temporadas desde la UI sin tocar código.

---

## 5. Estructura de DAGs

`[DEV]` Los DAGs se escriben y versionan en el PC de desarrollo.
`[AIR]` Airflow los ejecuta en el portátil tras cada `git pull`.

### 5.1 Dos DAGs, dos responsabilidades

| DAG | Nombre | Schedule | Responsabilidad |
|---|---|---|---|
| Principal | `nba_main_pipeline` | Diario 06:00 | teams → players → games → standings → shots |
| Boxscore | `nba_boxscore_pipeline` | Solo por trigger | Procesa bloques de 400 hasta vaciar la cola |

La razón de separarlos: el boxscore tiene una lógica de re-ejecución
propia que no encaja con el ritmo diario del pipeline principal. Separarlo
permite re-ejecutarlo manualmente, pausarlo, o monitorearlo de forma
independiente desde la UI.

### 5.2 DAG principal: `nba_main_pipeline`

Grafo de dependencias:

```
load_teams
    │
load_players
    │
load_games
    │
load_standings
    │
load_shots
    │
trigger_boxscore    ← dispara nba_boxscore_pipeline
```

```python
# dags/nba_main_pipeline.py
from __future__ import annotations

from datetime import datetime, timedelta
from functools import partial
import json

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable

from src.scripts.run_teams_load import run as run_teams
from src.scripts.run_players_load import run as run_players
from src.scripts.run_games_load import run as run_games
from src.scripts.run_standings_load import run as run_standings
from src.scripts.run_shots_load import run as run_shots

SEASONS = json.loads(Variable.get("nba_seasons", default_var='["2024-25", "2025-26"]'))

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="nba_main_pipeline",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule="0 6 * * *",    # todos los días a las 06:00
    catchup=False,            # no ejecuta runs atrasados al activar el DAG
    tags=["nba", "etl"],
) as dag:

    t_teams     = PythonOperator(task_id="load_teams",     python_callable=run_teams)
    t_players   = PythonOperator(task_id="load_players",   python_callable=run_players)
    t_games     = PythonOperator(task_id="load_games",     python_callable=partial(run_games, seasons=SEASONS))
    t_standings = PythonOperator(task_id="load_standings", python_callable=partial(run_standings, seasons=SEASONS))
    t_shots     = PythonOperator(task_id="load_shots",     python_callable=partial(run_shots, seasons=SEASONS))

    t_trigger_boxscore = TriggerDagRunOperator(
        task_id="trigger_boxscore",
        trigger_dag_id="nba_boxscore_pipeline",
        wait_for_completion=False,    # el pipeline principal no espera al boxscore
        reset_dag_run=True,
    )

    t_teams >> t_players >> t_games >> t_standings >> t_shots >> t_trigger_boxscore
```

> **`catchup=False`:** Con `catchup=True`, si se activa el DAG con `start_date` en enero, Airflow ejecutaría todos los runs atrasados. Con `catchup=False` solo ejecuta el próximo run programado. Para este pipeline siempre se quiere `catchup=False`.

> **`TriggerDagRunOperator` con `wait_for_completion=False`:** El pipeline principal marca su task final como completado en cuanto dispara el boxscore, sin bloquear. El boxscore corre de forma independiente y aparece como un DAG separado en la UI.

### 5.3 Sobre el schedule

Los partidos NBA se juegan por la noche (hora EE.UU.) y los datos
aparecen en la API a la mañana siguiente. Un schedule `"0 6 * * *"`
(6am hora WSL del portátil) captura los datos del día anterior.

Si el portátil estaba apagado a las 6am, el scheduler simplemente no
ejecutó ese run. No hay problema: la próxima vez que arranque Airflow,
el pipeline correrá en el siguiente ciclo programado. Los datos
perdidos se pueden cargar manualmente desde la UI con "Trigger DAG".

---

## 6. Caso especial: Boxscore en bloques de 400

### 6.1 El problema

El boxscore puede tener miles de game_ids pendientes. Procesarlos todos
en una sola ejecución sería una tarea de horas. La solución actual
(bloques de 400) es correcta, pero requiere re-ejecución manual.
Airflow puede automatizar ese "re-ejecutar hasta vaciar".

### 6.2 Patrón: DAG que se auto-dispara

```
                ┌─────────────────────────────────┐
                │      nba_boxscore_pipeline       │
                │                                  │
                │  count_pending                   │
                │      │                           │
                │  [ShortCircuitOperator]           │
                │  ¿pending > 0?                   │
                │      │  NO → marca todo SKIPPED  │
                │      │  SÍ ↓                     │
                │  load_boxscore_block              │
                │  (procesa 400 game_ids)           │
                │      │                           │
                │  check_still_pending             │
                │      │                           │
                │  [BranchPythonOperator]           │
                │  ¿quedan más?                    │
                │      │  NO → end                 │
                │      │  SÍ → trigger_self ──────►│ (nueva ejecución)
                └─────────────────────────────────┘
```

```python
# dags/nba_boxscore_pipeline.py
from __future__ import annotations

from datetime import datetime
import json

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator, BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable

from src.etl.extract.boxscore import fetch_pending_game_ids_for_boxscore, fetch_gamebox
from src.etl.load.core_boxscore import upsert_fct_boxscore

SEASONS    = json.loads(Variable.get("nba_seasons", default_var='["2024-25", "2025-26"]'))
BLOCK_SIZE = 400
MAX_LOOPS  = int(Variable.get("boxscore_max_loops", default_var=20))


def _count_pending(**context):
    """Cuenta pendientes y los publica via XCom. Devuelve True si hay trabajo."""
    pending = fetch_pending_game_ids_for_boxscore(SEASONS)
    count = len(pending)
    context["ti"].xcom_push(key="pending_count", value=count)
    print(f"Pendientes: {count} game_ids")
    return count > 0    # ShortCircuitOperator: True=continúa, False=corta


def _load_block():
    """Procesa el próximo bloque de 400 game_ids."""
    pending = fetch_pending_game_ids_for_boxscore(SEASONS)
    block   = pending[:BLOCK_SIZE]
    print(f"Procesando bloque de {len(block)} game_ids (total pendiente: {len(pending)})")
    df = fetch_gamebox(block)
    if not df.empty:
        upsert_fct_boxscore(df, page_size=1000)
        print(f"Cargados {len(df)} registros de boxscore.")
    else:
        print("El bloque no devolvió datos.")


def _decide_next(**context):
    """BranchPythonOperator: decide si re-disparar o terminar."""
    loop_count = int(context["dag_run"].conf.get("loop_count", 0))
    if loop_count >= MAX_LOOPS:
        print(f"ADVERTENCIA: límite de {MAX_LOOPS} loops alcanzado.")
        return "end"
    pending   = fetch_pending_game_ids_for_boxscore(SEASONS)
    remaining = len(pending)
    print(f"Tras el bloque, quedan {remaining} pendientes.")
    return "trigger_self" if remaining > 0 else "end"


with DAG(
    dag_id="nba_boxscore_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule=None,          # solo se ejecuta por trigger, nunca por schedule
    catchup=False,
    max_active_runs=1,      # CRÍTICO: evita dos ejecuciones paralelas del mismo DAG
    tags=["nba", "boxscore"],
) as dag:

    t_count = ShortCircuitOperator(
        task_id="count_pending",
        python_callable=_count_pending,
        provide_context=True,
    )

    t_load = PythonOperator(
        task_id="load_boxscore_block",
        python_callable=_load_block,
    )

    t_decide = BranchPythonOperator(
        task_id="check_still_pending",
        python_callable=_decide_next,
        provide_context=True,
    )

    t_trigger_self = TriggerDagRunOperator(
        task_id="trigger_self",
        trigger_dag_id="nba_boxscore_pipeline",
        conf={"loop_count": "{{ dag_run.conf.get('loop_count', 0) | int + 1 }}"},
        wait_for_completion=False,
    )

    t_end = EmptyOperator(task_id="end")

    t_count >> t_load >> t_decide >> [t_trigger_self, t_end]
```

> **`max_active_runs=1`:** Aunque `trigger_self` dispara una nueva ejecución del DAG, Airflow la encola y espera a que la actual termine antes de iniciarla. Sin esto, dos ejecuciones paralelas extraerían los mismos game_ids.

> **`ShortCircuitOperator`:** Si no hay pendientes, Airflow marca todas las tareas siguientes como `SKIPPED` (no `FAILED`). En la UI se ve claramente que no había trabajo, sin falsas alarmas.

> **`BranchPythonOperator`:** Bifurca el grafo devolviendo el `task_id` del camino a seguir. El camino no elegido queda en estado `SKIPPED`.

> **Protección contra bucles infinitos:** `boxscore_max_loops` (configurable desde la UI) limita las re-ejecuciones encadenadas. Por defecto: 20 (= 8.000 game_ids máximo por sesión).

---

## 7. Logging y alertas

`[AIR]` Toda la observabilidad se configura y consulta en el portátil.

### 7.1 Logging nativo

Airflow captura todo lo que va a `stdout` dentro de las tasks (los
`print()` del código actual) y lo guarda en logs por task/fecha/intento.
Se consultan en la UI en `DAG > Task > Log`.

Para clasificación por nivel de severidad, es mejor sustituir los
`print()` críticos por el logger estándar de Python:

```python
# Cambio mínimo en src/etl/extract/boxscore.py
import logging
log = logging.getLogger(__name__)

log.info(f"[{i}/{total}] GAME_ID={gid} ...")
log.warning(f"V3 falló para {gid}, intentando V2...")
log.error(f"FALLÓ {gid}: {msg}")
```

Airflow muestra `INFO`, `WARNING`, `ERROR` con colores distintos en la UI.

### 7.2 Callback de fallo

Se ejecuta automáticamente cuando una task falla:

```python
def on_task_failure(context):
    task_id   = context["task_instance"].task_id
    dag_id    = context["task_instance"].dag_id
    exec_date = context["execution_date"]
    exception = context.get("exception")

    with open("/home/<usuario>/airflow/logs/failures.log", "a") as f:
        f.write(f"{datetime.now()} | {dag_id}.{task_id} | {exec_date} | {exception}\n")

default_args = {
    "on_failure_callback": on_task_failure,
    ...
}
```

### 7.3 Alertas por email (activar cuando el pipeline sea estable)

En `~/airflow/airflow.cfg` en el portátil:

```ini
[smtp]
smtp_host      = smtp.gmail.com
smtp_starttls  = True
smtp_ssl       = False
smtp_user      = tu-email@gmail.com
smtp_password  = tu-app-password
smtp_port      = 587
smtp_mail_from = tu-email@gmail.com
```

En `default_args` del DAG:

```python
default_args = {
    "email": ["tu-email@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
}
```

### 7.4 SLA (alerta por tarea lenta)

```python
t_load = PythonOperator(
    task_id="load_boxscore_block",
    python_callable=_load_block,
    sla=timedelta(minutes=10),    # alerta si un bloque de 400 tarda más de 10 min
)
```

### 7.5 Vistas útiles en la UI

| Vista | Utilidad |
|---|---|
| **Grid view** | Historial de ejecuciones con colores (verde=OK, rojo=fallo, amarillo=running) |
| **Graph view** | Grafo del DAG con estado de cada task en tiempo real |
| **Gantt** | Duración de cada task — detecta cuellos de botella |
| **Task logs** | stdout + stderr completo de cada ejecución individual |

---

## 8. Flujo de trabajo Git entre equipos

### 8.1 Estructura de ramas

El código ETL y los DAGs viven en el mismo repositorio. No se necesitan
ramas especiales para Airflow: el portátil siempre trabaja sobre `main`.

```
Repositorio GitHub
├── main          ← rama estable; el portátil Airflow la sigue
└── <feature>     ← ramas de desarrollo en el PC (nunca en el portátil)
```

### 8.2 Flujo normal de cambios

```
[DEV]  Editar src/etl/... o dags/
[DEV]  git push origin main
                │
                ▼ GitHub
[AIR]  git pull origin main       ← manual o automatizado (ver 8.3)
                │
                ▼
        Scheduler detecta cambios en dags/ y recarga los DAGs (cada 30s)
        Cambios en src/etl/ surten efecto en la próxima ejecución de task
```

Los cambios en `src/etl/` no requieren reiniciar Airflow: el Worker
importa los módulos en tiempo de ejecución de cada task, no al arrancar.

### 8.3 Automatizar el git pull en el portátil

**Opción A — DAG de Airflow** (recomendada, visible en la UI):

```python
# dags/git_sync.py
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="git_sync",
    schedule="*/15 * * * *",     # cada 15 minutos
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["infra"],
) as dag:
    BashOperator(
        task_id="pull",
        bash_command=(
            "cd /home/<usuario>/nba-analytics-pipeline "
            "&& git pull origin main"
        ),
    )
```

**Opción B — cron del sistema operativo** (más simple):

```bash
# [AIR] crontab -e
*/15 * * * * cd /home/<usuario>/nba-analytics-pipeline && git pull origin main >> /tmp/git-pull.log 2>&1
```

### 8.4 Gestión del `.env`

El `.env` con las credenciales de Supabase no está en Git. Cada equipo
tiene el suyo propio y no necesita sincronizarse.

| Equipo | Necesita `.env` | Para qué |
|---|---|---|
| PC Desarrollo | Sí | Ejecutar scripts ETL manualmente durante el desarrollo |
| Portátil Airflow | Sí | Airflow ejecuta el ETL y necesita conectarse a Supabase |

El `.env` del portátil se crea una sola vez (ver [sección 4.5](#45-crear-el-fichero-env-de-supabase)).

---

## 9. Cambios concretos al código

`[DEV]` Todos estos cambios se hacen en el PC de desarrollo y se publican via Git.

### 9.1 Nuevos ficheros

| Fichero | Descripción |
|---|---|
| `dags/nba_main_pipeline.py` | DAG principal (ver sección 5.2) |
| `dags/nba_boxscore_pipeline.py` | DAG de boxscore con auto-trigger (ver sección 6.2) |
| `dags/git_sync.py` | DAG de sincronización Git (ver sección 8.3) |
| `pyproject.toml` | Permite `pip install -e .` en el portátil (ver sección 3.2) |

### 9.2 Añadir `get_pending_count()` en `boxscore.py`

Consulta el conteo de pendientes directamente en la DB, sin traer
la lista completa al cliente. Más eficiente que `len(fetch_pending...)`.

```python
# Añadir en src/etl/extract/boxscore.py

def get_pending_count(seasons: list[str] | None = None) -> int:
    """Devuelve el número de game_ids pendientes de boxscore."""
    conn = get_conn()
    cur  = conn.cursor()
    if seasons:
        sql = """
        SELECT COUNT(DISTINCT g.game_id)
        FROM core.fct_games g
        LEFT JOIN core.fct_boxscore b ON b.game_id = g.game_id
        WHERE b.game_id IS NULL AND g.season_num = ANY(%s)
        """
        cur.execute(sql, (seasons,))
    else:
        sql = """
        SELECT COUNT(DISTINCT g.game_id)
        FROM core.fct_games g
        LEFT JOIN core.fct_boxscore b ON b.game_id = g.game_id
        WHERE b.game_id IS NULL
        """
        cur.execute(sql)
    count = cur.fetchone()[0]
    cur.close(); conn.close()
    return count
```

### 9.3 Refactorizar scripts para aceptar `seasons` como parámetro

Cambio mínimo: los scripts siguen funcionando igual al ejecutarse
directamente; los DAGs pueden pasar temporadas via Variables de Airflow.

```python
# Patrón a aplicar en run_games_load.py, run_shots_load.py,
# run_standings_load.py y run_boxscore_load.py

DEFAULT_SEASONS = ['2024-25', '2025-26']

def run(seasons: list[str] | None = None):
    df = fetch_games(seasons or DEFAULT_SEASONS)
    upsert_fct_games(df)

if __name__ == "__main__":
    run()
```

### 9.4 Resumen de cambios por fichero

| Fichero | Tipo | Descripción |
|---|---|---|
| `dags/nba_main_pipeline.py` | **Nuevo** | DAG principal con 6 tasks |
| `dags/nba_boxscore_pipeline.py` | **Nuevo** | DAG con ShortCircuit + Branch + auto-trigger |
| `dags/git_sync.py` | **Nuevo** | Pull automático desde GitHub |
| `pyproject.toml` | **Nuevo** | Instalación editable en el portátil |
| `src/etl/extract/boxscore.py` | **Añadir función** | `get_pending_count()` |
| `src/scripts/run_*.py` | **Cambio menor** | Aceptar `seasons` como parámetro opcional |
| `src/db/connection.py` | **Sin cambios** | Funciona igual con `.env` |
| Todo `src/etl/` | **Sin cambios** | Los DAGs usan el código tal como está |

---

## 10. Orden de implementación paso a paso

### Fase 1 — PC de desarrollo: preparar el código `[DEV]`

1. Crear `dags/` y `pyproject.toml` en el repositorio
2. Añadir `get_pending_count()` en `boxscore.py`
3. Refactorizar los `run_*.py` para aceptar `seasons`
4. Escribir `dags/nba_main_pipeline.py` (solo con `load_teams` primero para verificar)
5. Push a `main`

### Fase 2 — Portátil Airflow: setup del entorno `[AIR]`

6. Clonar el repositorio en WSL
7. Crear el entorno conda `airflow-env` e instalar Airflow + deps del proyecto
8. Configurar `AIRFLOW_HOME` y `airflow.cfg` (empezar con SequentialExecutor)
9. Crear el `.env` con las credenciales de Supabase
10. `airflow db migrate` y crear usuario admin
11. Arrancar scheduler y webserver; abrir `http://localhost:8080`

### Fase 3 — Portátil Airflow: verificar el primer DAG `[AIR]`

12. Configurar las Variables de Airflow en la UI (`nba_seasons`, `boxscore_max_loops`)
13. Activar `nba_main_pipeline` en la UI (toggle ON)
14. Disparar manualmente con "Trigger DAG" y verificar que `load_teams` funciona
15. Añadir los demás tasks al DAG en `[DEV]`, push, y ver cómo el grafo crece en la UI tras el pull

### Fase 4 — Portátil Airflow: activar boxscore y git sync `[AIR]`

16. En `[DEV]`: escribir `dags/nba_boxscore_pipeline.py` y `dags/git_sync.py`, push
17. En `[AIR]`: `git pull`, activar ambos DAGs en la UI
18. Probar `nba_boxscore_pipeline` disparándolo manualmente con pocos game_ids pendientes
19. Verificar el auto-trigger en la UI (Grid view del DAG de boxscore)

### Fase 5 — Funcionamiento autónomo `[AIR]`

20. Verificar que `nba_main_pipeline` corre automáticamente a las 06:00
21. Activar alertas email cuando el pipeline lleve unos días estable
22. Migrar a LocalExecutor + PostgreSQL si se necesita paralelismo

---

## Diagrama final de la arquitectura

```
[DEV] Escribe código → git push
                            │
                         GitHub
                            │
[AIR] git pull (cada 15min via git_sync DAG)
                            │
                    Airflow Scheduler
                            │
                    06:00 diario
                            │
                            ▼
        ┌──────────────────────────────────────────────┐
        │              nba_main_pipeline                │
        │                                              │
        │  load_teams → load_players → load_games      │
        │                                  │           │
        │                          load_standings      │
        │                                  │           │
        │                          load_shots          │
        │                                  │           │
        │                       trigger_boxscore ──────┼──►
        └──────────────────────────────────────────────┘  │
                                                          │ dispara
                                                          ▼
                                     ┌────────────────────────────────┐
                                     │     nba_boxscore_pipeline       │
                                     │     (max_active_runs=1)         │
                                     │                                 │
                                     │  count_pending                  │
                                     │  [ShortCircuit: ¿hay > 0?]     │
                                     │       │ SÍ                      │
                                     │  load_boxscore_block            │
                                     │  (400 game_ids)                 │
                                     │       │                         │
                                     │  check_still_pending            │
                                     │  [Branch: ¿quedan más?]         │
                                     │     /           \               │
                                     │  trigger_self    end            │
                                     │     │                           │
                                     └─────┼───────────────────────────┘
                                           │
                                           └──► (nueva ejecución del mismo DAG)
```
