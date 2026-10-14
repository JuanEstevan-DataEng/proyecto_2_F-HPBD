# DAG del Proyecto 2: pipeline Medallón de StackOverflow (Posts + Votes).
#
#   bronze_ingest     dlt:     votes 2021 (HTTP) -> s3://bronze/votes/votes_2021/  (Parquet, override)
#   silver_transform  PySpark: bronze votes 2020+2021 -> nessie.silver.votes_hist   (Iceberg, MERGE)
#   gold_agg          PySpark: silver post_hist ⋈ votes_hist -> nessie.gold.vote_stats_per_post
#                                                                                  (Iceberg, MERGE)
#
# Se ejecuta completo con un solo "Play" (Trigger DAG). Las dos tareas de Spark se envían al
# clúster (spark://spark-master:7077) con SparkSubmitOperator y aparecen en la UI de Spark (9090).
# Requisito previo (manual): Bronze de posts_2020, posts_2021, votes_2020 y silver.post_hist.

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

JOBS = "/opt/airflow/jobs"

# JARs que necesita el driver (corre dentro del contenedor de Airflow, modo client). Son los
# mismos de la imagen de Spark, compartidos por el volumen spark-jars.
JARS = ",".join(f"/opt/spark/jars/{jar}" for jar in [
    "iceberg-spark-runtime-3.5_2.12-1.5.0.jar",
    "nessie-spark-extensions-3.5_2.12-0.77.1.jar",
    "hadoop-aws-3.3.4.jar",
    "aws-java-sdk-bundle-1.12.262.jar",
    "bundle-2.24.8.jar",
    "url-connection-client-2.24.8.jar",
])

# recursos de cada job en el clúster (el worker ofrece 2 cores y 3 GB)
RECURSOS_SPARK = dict(
    conn_id="spark_default",
    jars=JARS,
    py_files=f"{JOBS}/comun.py",
    total_executor_cores=2,
    executor_cores=2,
    executor_memory="2g",
    driver_memory="1g",
    conf={"spark.sql.shuffle.partitions": "16"},
    verbose=False,
)

default_args = {
    "owner": "grupo_fhbd",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="medallon_stackoverflow",
    description="Bronze (dlt) -> Silver (Spark + Iceberg MERGE) -> Gold (Spark + Iceberg MERGE)",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,          # se ejecuta a mano con el botón Trigger (un solo "Play")
    catchup=False,
    max_active_runs=1,
    tags=["proyecto2", "medallon", "dlt", "spark", "iceberg", "nessie"],
) as dag:

    # 1. Ingesta Bronze con dlt (no usa Spark: corre como proceso Python en Airflow)
    bronze_ingest = BashOperator(
        task_id="bronze_ingest",
        bash_command=f"python {JOBS}/bronze_ingest.py",
    )

    # 2. Transformación Silver en el clúster de Spark
    silver_transform = SparkSubmitOperator(
        task_id="silver_transform",
        application=f"{JOBS}/silver_transform.py",
        name="silver_transform",
        **RECURSOS_SPARK,
    )

    # 3. Agregación Gold en el clúster de Spark
    gold_agg = SparkSubmitOperator(
        task_id="gold_agg",
        application=f"{JOBS}/gold_agg.py",
        name="gold_agg",
        **RECURSOS_SPARK,
    )

    bronze_ingest >> silver_transform >> gold_agg
