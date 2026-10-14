# Imagen de Airflow del proyecto: Airflow 2.9 + Java 17 (para el spark-submit) + dlt.
FROM apache/airflow:2.9.0

USER root

# OpenJDK 17: el driver de Spark (modo client) corre dentro de este contenedor
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk-headless procps wget && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64/

USER airflow

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# spark-submit viene con el paquete pyspark
ENV SPARK_HOME=/home/airflow/.local/lib/python3.12/site-packages/pyspark
ENV PATH="${PATH}:/home/airflow/.local/bin"
