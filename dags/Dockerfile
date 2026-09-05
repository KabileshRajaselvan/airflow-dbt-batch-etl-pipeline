ARG AIRFLOW_VERSION=2.8.4
ARG PYTHON_VERSION=3.11
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

USER root
# openjdk-17: PySpark's extraction/combine job runs local[*] inside this
# same container rather than a separate Spark cluster (see README Design
# Decisions) - no bitnami/spark image needed, just a JDK on this image.
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

# Installing extras against Airflow's own published constraints file avoids
# pip silently upgrading/downgrading a package Airflow itself depends on.
# Packages Airflow's own dependency graph already touches (requests,
# psycopg2-binary, pandas, sqlalchemy, boto3, pyarrow) are listed WITHOUT a
# version pin so pip takes whatever the constraints file demands instead of
# conflicting with it. pyspark has no such conflict, so it's installed here
# too. dbt is NOT installed into this environment - see below.
ARG AIRFLOW_VERSION=2.8.4
ARG PYTHON_VERSION=3.11
RUN pip install --no-cache-dir \
    pyspark==3.5.1 \
    requests \
    psycopg2-binary \
    boto3 \
    pandas \
    sqlalchemy \
    pyarrow \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# dbt-core requires sqlparse>=0.5, but Airflow 2.8.4's constraints file pins
# sqlparse==0.4.4 for its own DB layer - genuinely incompatible in the same
# environment, not just an overly strict pin. The standard fix (used by
# Astronomer's own guidance and the dbt/Airflow community generally) is to
# give dbt its own isolated venv and invoke it by full path from
# BashOperator, rather than installing it alongside Airflow itself.
USER root
RUN python3.11 -m venv /opt/dbt-venv && chown -R airflow:root /opt/dbt-venv
USER airflow
# Airflow's base image sets PIP_USER=true globally (it installs its own
# packages to --user site-packages); that env var leaks into this venv's pip
# and pip refuses "--user" installs inside a virtualenv, so it's unset here.
RUN PIP_USER=0 /opt/dbt-venv/bin/pip install --no-cache-dir dbt-core==1.8.2 dbt-postgres==1.8.2

COPY --chown=airflow:root extraction/src /opt/airflow/extraction_src
COPY --chown=airflow:root quality/src /opt/airflow/quality_src
COPY --chown=airflow:root dbt /opt/airflow/dbt
COPY --chown=airflow:root dags /opt/airflow/dags

ENV ETL_STAGING_DIR=/opt/airflow/staging \
    DBT_PROJECT_DIR=/opt/airflow/dbt \
    DBT_BIN=/opt/dbt-venv/bin/dbt \
    PYTHONPATH=/opt/airflow/extraction_src:/opt/airflow/quality_src:/opt/airflow/dags
