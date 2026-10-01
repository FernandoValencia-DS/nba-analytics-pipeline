# 🏀 NBA Analytics Pipeline

**Cada mañana, mientras duermes, este proyecto sale a buscar los datos de todos los partidos de la NBA del día anterior, los limpia, los organiza y los deja listos para analizar.**

Sin clics, sin descargas manuales, sin hojas de cálculo desordenadas. Solo datos frescos, confiables y listos para responder preguntas como: *¿Desde qué zonas de la cancha anota mejor un equipo? ¿Cómo evoluciona el rendimiento de un jugador a lo largo de la temporada? ¿Qué tan preciso es un lanzamiento según la distancia y la posición en la cancha?*

## ¿Por qué existe este proyecto?

La NBA genera una cantidad enorme de datos: cada partido, cada jugador, cada lanzamiento queda registrado con una precisión asombrosa (incluida la coordenada exacta desde donde se lanzó cada balón). El problema es que esos datos están dispersos, crudos y son difíciles de aprovechar.

Este proyecto resuelve eso: construye, de forma **totalmente automática**, una base de datos analítica de la NBA, actualizada todos los días a las 5:00 a.m. (hora Colombia), lista para alimentar dashboards, modelos predictivos o cualquier análisis que se te ocurra.

## ¿Qué hace, en la práctica?

1. **Recolecta** información oficial de la NBA: equipos, jugadores, partidos, marcadores, estadísticas de caja (boxscore) y, lo más interesante, **la ubicación exacta de cada lanzamiento** realizado en cada partido.
2. **Transforma** esos datos crudos en información útil: calcula desde qué zona de la cancha se lanzó cada balón (hay 15 zonas distintas, desde el garrafón hasta el triple de esquina), arma llaves consistentes entre tablas y limpia inconsistencias.
3. **Carga** todo en una base de datos en la nube (PostgreSQL vía Supabase), lista para consultarse en segundos.
4. **Se repite solo**, todos los días, gracias a una automatización que corre en la nube (GitHub Actions + Prefect) sin que nadie tenga que tocar nada.

## De los datos a las decisiones

Toda esta información alimenta un **dashboard interactivo en Power BI** (incluido en el repositorio), donde se puede explorar visualmente el rendimiento de equipos y jugadores, los mapas de calor de lanzamientos por zona, y las tendencias de la temporada — convirtiendo miles de filas de datos crudos en historias que cualquiera puede entender de un vistazo.

## ¿Qué lo hace interesante?

- 🎯 **Mapas de tiro con precisión quirúrgica**: no solo sabemos si un jugador anotó, sino desde exactamente qué punto de la cancha lo hizo.
- 🤖 **Cero intervención manual**: el pipeline corre solo, todos los días, y se recupera automáticamente si algo falla.
- 📊 **De dato crudo a insight visual**: desde la API oficial de la NBA hasta un dashboard interactivo, sin pasos intermedios manuales.
- 🧱 **Pensado para crecer**: la arquitectura modular permite sumar nuevas fuentes de datos, métricas o modelos predictivos sin reescribir nada desde cero.

## Estructura del proyecto

```text
src/
├── etl/            # Extracción, transformación y carga de datos
├── flows/          # Orquestación diaria del pipeline (Prefect)
├── scripts/        # Puntos de entrada ejecutables
├── db/             # Conexión a la base de datos
├── data/zones/     # Definición geográfica de las zonas de lanzamiento
└── visualization/  # Dashboard de Power BI
notebooks/          # Exploración y construcción de las zonas de cancha
docs/                # Documentación técnica
```

## Sobre el autor

Proyecto desarrollado y mantenido por **Fernando Valencia**, combinando ingeniería de datos, automatización en la nube y pasión por el baloncesto.

---

*¿Curioso por los detalles técnicos (esquema de base de datos, llaves compuestas, sistema de zonas de tiro)? Revisa la documentación técnica en `docs/`.*
