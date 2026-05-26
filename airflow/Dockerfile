FROM apache/airflow:2.9.1-python3.11

# SparkSubmitOperator launches `spark-submit` from inside this container
# (even when --master points to a remote cluster), so we need a JRE and
# procps (`ps`) here. The actual job still runs on the Spark workers.
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        default-jre-headless \
        procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

# Pin providers via the official Airflow constraints file so transitive
# dependencies (protobuf, pandas, sqlalchemy) stay compatible with the
# Airflow runtime. Installing without constraints breaks the `airflow` CLI.
ARG AIRFLOW_VERSION=2.9.1
ARG PYTHON_VERSION=3.11
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

RUN pip install --no-cache-dir \
        "apache-airflow-providers-apache-spark" \
        "pymongo" \
        --constraint "${CONSTRAINT_URL}"
