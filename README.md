# Análisis de Patrones de Movilidad Urbana

Pipeline ETL completo con **PySpark + Airflow + MongoDB + Streamlit** sobre el
dataset *NYC TLC Yellow Taxi* para responder:

- ¿En qué horarios hay más demanda?
- ¿Qué zonas generan más viajes de origen / destino?
- ¿Cuál es la duración promedio por franja horaria?
- ¿Cómo varía el ingreso por zona y día de la semana?

---

## Stack

| Componente | Tecnología | Puerto |
|---|---|---|
| Procesamiento | PySpark 3.5.1 (1 master + 2 workers) | 8080 |
| Orquestación | Apache Airflow 2.9.1 | 8090 |
| Almacenamiento analítico | MongoDB 7 | 27017 |
| Visualización | Streamlit | 8501 |
| Exploración interactiva | Jupyter Lab (PySpark 3.5.1) | 8888 |
| Metadata Airflow | PostgreSQL 15 | — |

---

## Estructura

```
.
├── docker-compose.yml
├── .env.example
├── reset.py                       # limpia bronze/silver/gold + MongoDB + DAG history
│
├── airflow/
│   ├── Dockerfile                 # Airflow + Java JRE + providers (constraints)
│   └── dags/mobility_pipeline.py
│
├── data/
│   ├── zones.csv                  # 12 zonas NYC con bbox + priority
│   ├── taxi_zone_centroids.csv    # 263 centroides reales del shapefile TLC
│   └── download_tlc_data.py       # descarga y samplea el parquet TLC
│
├── jobs/                          # scripts PySpark
│   ├── ingest.py                  # adapta schema TLC -> schema proyecto
│   ├── clean.py                   # nulls, casts, derivadas, outliers
│   ├── enrich.py                  # join por bbox -> pickup/dropoff_zone
│   ├── analysis.py                # 8 agregaciones gold
│   └── export_to_mongo.py         # parquet -> MongoDB
│
├── notebooks/
│   ├── Dockerfile                 # PySpark 3.5.1 + pymongo + plotly
│   ├── 01_exploration.ipynb       # EDA con PySpark local
│   └── 02_analysis_results.ipynb  # resultados finales
│
└── streamlit/
    ├── Dockerfile
    ├── requirements.txt
    └── app.py                     # 7 pestañas, lee de MongoDB
```

---

## Schema del pipeline

**Bronze** (ingest) → schema oficial del proyecto:

```
trip_id, pickup_datetime, dropoff_datetime,
pickup_longitude, pickup_latitude, dropoff_longitude, dropoff_latitude,
passenger_count, trip_distance_km, fare_amount, payment_type, driver_id
```

**Silver** (clean) añade derivadas:

```
+ trip_duration_min, hour_of_day, day_of_week  (0=Lunes ... 6=Domingo)
```

Outliers descartados:
`trip_distance_km<=0`, `fare_amount<=0`, `trip_duration_min<=0 o >180`, `passenger_count<=0`.

**Gold** (enrich) añade zonas:

```
+ pickup_zone, pickup_zone_id, dropoff_zone, dropoff_zone_id
```

## 8 agregaciones (colecciones MongoDB)

1. `demand_by_hour`
2. `demand_by_day`
3. `top_zones_origin`
4. `top_zones_destination`
5. `revenue_by_zone`
6. `avg_duration_by_hour`
7. `avg_duration_by_zone`
8. `revenue_by_payment_type`

---

## Pre-requisitos

- **Docker** + **Docker Compose** v2
- **Python 3.10+** en el host (solo para `download_tlc_data.py` y `reset.py`)
- Opcional pero recomendado: `pip install --user pyarrow` para que
  el sampleo del dataset funcione en el host

---

## Arranque (primera vez)

### 1. Configuración inicial

```bash
cp .env.example .env

# Genera una Fernet key para Airflow:
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
# Pega el resultado como FERNET_KEY en .env
```

> Si tu UID del host no es 1000, edita `.env` y agrega `HOST_UID=$(id -u)`.

### 2. Descargar dataset

```bash
python3 data/download_tlc_data.py --fraction 0.05
```

Baja `yellow_tripdata_2015-06.parquet` (~170 MB) y deja una muestra del 5%
(~13 MB, ~600k filas) en `data/raw/`. La URL se puede cambiar via
`TLC_PARQUET_URL` en `.env`.

> ⚠️ Si ves `pyarrow not installed; skipping sampling`, el archivo queda
> completo (170 MB) en vez de samplado. Para arreglarlo: `pip install --user pyarrow`
> y re-corre el script (no re-descarga, solo samplea in-place).

### 3. Levantar stack

```bash
docker compose up -d --build
```

Primera vez tarda 5–10 min porque construye las imágenes custom de Airflow,
Jupyter y Streamlit. Subsecuentes arranques son instantáneos.

Verifica estado:
```bash
docker compose ps
```

Todos los containers deben aparecer `Up` (algunos como `healthy`). El init
de Airflow corre una sola vez y termina con `Exited (0)` — es normal.

### 4. Ejecutar el pipeline

Abre **Airflow** en http://localhost:8090 (`admin` / `admin`).

1. En la lista de DAGs, click en `mobility_pipeline`
2. Click en el toggle ☑️ para activarlo (unpause)
3. Click en el botón ▶️ **Trigger DAG** arriba a la derecha
4. (Opcional) Vista **Graph** para ver la cadena en tiempo real:
   `ingest → clean → enrich → analysis → export_to_mongo`

> La conexión `spark_default` (Spark master) se crea automáticamente al
> primer arranque por el init container. No necesitas configurarla a mano.

Tiempo total del run: ~3–8 minutos con el sample al 5%.

### 5. Ver resultados

| Dónde | URL | Qué muestra |
|---|---|---|
| **Streamlit** | http://localhost:8501 | Dashboard con 7 pestañas (gráficas + mapa) |
| **Jupyter** | http://localhost:8888 | Notebooks de exploración. Token: `docker compose logs mobility-jupyter \| grep token` |
| **MongoDB shell** | — | `docker exec -it mobility-mongodb mongosh -u admin -p admin` |
| **Spark Master UI** | http://localhost:8080 | Estado del cluster y jobs |

Consulta MongoDB directo:
```javascript
use mobility
show collections
db.demand_by_hour.find().sort({hour_of_day: 1}).limit(5)
```

---

## Resetear el proyecto

Para volver a un estado limpio:

```bash
python3 reset.py --yes
```

Borra `data/bronze/silver/gold/`, todas las colecciones de MongoDB, el
historial de runs del DAG y los `__pycache__/`. **No** borra el parquet
descargado en `data/raw/`. Para borrar también el parquet:

```bash
python3 reset.py --raw --yes
```

---

## Ejecutar un job suelto (sin Airflow)

Útil para debugging:

```bash
docker exec mobility-spark-master spark-submit \
  --master spark://mobility-spark-master:7077 \
  /opt/bitnami/spark/jobs/analysis.py
```

Cada job acepta `--input` y `--output` como argumentos. Ver el docstring
de cada `jobs/*.py` para los defaults.

---

## Convenciones del código

- `SparkSession.builder.getOrCreate()` — nunca crear una sesión nueva.
- Rutas dentro de contenedores Spark: `/opt/bitnami/spark/data/`.
- Logging con `logging.getLogger(__name__)`, no `print()`.
- MongoDB se escribe vía `pandas` + `pymongo` tras agregar con PySpark.
- Credenciales sólo vía `.env` / variables de entorno, nunca hardcoded.

---

## Apagar

```bash
docker compose down            # conserva volúmenes (Mongo, Postgres)
docker compose down -v         # también borra los volúmenes
```

---

## Troubleshooting

| Síntoma | Solución |
|---|---|
| Puertos 8080/8090/8501/8888 ya en uso | Detén otros stacks Docker o cambia los puertos en `docker-compose.yml` |
| Init de Airflow en loop de restart | Verifica logs: `docker compose logs mobility-airflow-init`. Probable falta de Fernet key. |
| DAG falla con `Permission denied` en `data/` | Confirma que tu UID es 1000 o pasa `HOST_UID=$(id -u)` en `.env` |
| Streamlit muestra "Sin datos" en todas las pestañas | El DAG no ha terminado o falló `export_to_mongo`. Revisa logs en Airflow. |
| Jupyter notebook 01 falla con `FileNotFoundException` | Ejecuta primero al menos la tarea `ingest` del DAG |
| Spark UI vacío | `docker compose restart mobility-spark-master mobility-spark-worker-1 mobility-spark-worker-2` |
