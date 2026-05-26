# Análisis de Patrones de Movilidad Urbana

Proyecto final de Big Data: pipeline PySpark + Airflow + MongoDB + Streamlit
sobre el dataset **NYC TLC Yellow Taxi** para responder:

- ¿En qué horarios hay más demanda?
- ¿Qué zonas generan más viajes de origen / destino?
- ¿Cuál es la duración promedio por franja horaria?
- ¿Cómo varía el ingreso por zona y día de la semana?

## Stack

| Componente | Tecnología | Puerto |
|---|---|---|
| Procesamiento | PySpark 3.5 (1 master + 2 workers) | 8080 |
| Orquestación | Apache Airflow 2.9.1 | 8090 |
| Almacenamiento | MongoDB 7 | 27017 |
| Visualización | Streamlit | 8501 |
| Exploración | Jupyter (PySpark) | 8888 |
| Metadata Airflow | PostgreSQL 15 | — |

## Estructura

```
.
├── docker-compose.yml
├── .env.example
├── data/
│   ├── zones.csv               # 12 zonas NYC con bounding boxes
│   └── download_tlc_data.py    # baja yellow_tripdata_2016-06.parquet
├── jobs/                       # scripts PySpark
│   ├── ingest.py               # adapta schema TLC -> schema proyecto
│   ├── clean.py                # nulls, casts, derivadas, outliers
│   ├── enrich.py               # join por bbox -> pickup/dropoff_zone
│   ├── analysis.py             # 8 agregaciones gold
│   └── export_to_mongo.py      # parquet -> MongoDB
├── airflow/dags/
│   └── mobility_pipeline.py    # DAG en cadena
├── streamlit/
│   ├── app.py                  # 7 pestañas
│   ├── Dockerfile
│   └── requirements.txt
└── notebooks/
    ├── 01_exploration.ipynb
    └── 02_analysis_results.ipynb
```

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

## Arranque

### 1. Configuración inicial

```bash
cp .env.example .env
# Genera una Fernet key:
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
# Pega el resultado como FERNET_KEY en .env
```

### 2. Levantar stack

```bash
docker compose up -d
docker compose ps
```

UIs disponibles:
- Spark Master: http://localhost:8080
- Airflow:      http://localhost:8090   (admin / admin)
- Streamlit:    http://localhost:8501
- Jupyter:      http://localhost:8888   (token en `docker compose logs mobility-jupyter`)
- MongoDB:      `mongodb://admin:admin@localhost:27017/`

### 3. Descargar dataset (una sola vez)

```bash
python data/download_tlc_data.py --fraction 0.05
```

Esto baja `yellow_tripdata_2016-06.parquet` (último mes con lat/lon en TLC)
y deja una muestra del 5% en `data/raw/`.

### 4. Conexión Spark en Airflow

En la UI de Airflow → **Admin → Connections** crea / edita `spark_default`:

```
Conn Id   : spark_default
Conn Type : Spark
Host      : spark://mobility-spark-master
Port      : 7077
```

### 5. Ejecutar pipeline

Airflow UI → DAGs → `mobility_pipeline` → ☑️ unpause → ▶️ trigger.

Etapas: `ingest → clean → enrich → analysis → export_to_mongo`.

### 6. Ver resultados

- Streamlit: http://localhost:8501
- Notebooks: http://localhost:8888 → `02_analysis_results.ipynb`
- MongoDB shell:

  ```bash
  docker exec -it mobility-mongodb mongosh -u admin -p admin
  use mobility
  show collections
  db.demand_by_hour.find().sort({hour_of_day: 1}).limit(5)
  ```

## Ejecutar un job suelto sin Airflow

```bash
docker exec -it mobility-spark-master spark-submit \
  --master spark://mobility-spark-master:7077 \
  /opt/bitnami/spark/jobs/analysis.py
```

## Convenciones

- `SparkSession.builder.getOrCreate()` — nunca crear una sesión nueva.
- Rutas dentro de contenedores Spark: `/opt/bitnami/spark/data/`.
- Logging con `logging.getLogger(__name__)`, no `print()`.
- MongoDB se escribe vía `pandas` + `pymongo` tras agregar con PySpark.
- Credenciales sólo vía `.env` / variables de entorno, nunca hardcoded.

## Apagar

```bash
docker compose down            # conserva volúmenes
docker compose down -v         # borra Mongo + Postgres + datos generados
```
