FROM quay.io/jupyter/pyspark-notebook:spark-3.5.1

# Spark 3.5.1 matches the cluster (bitnamilegacy/spark:3.5.1) so the driver
# launched from a notebook can attach to the standalone master without
# serialVersionUID mismatches.

USER ${NB_UID}

RUN pip install --no-cache-dir \
        pymongo==4.7.2 \
        plotly==5.22.0
