# Script MANUAL de Silver (fuera del DAG, como permite el enunciado):
# lee posts_2020 y posts_2021 de Bronze y los consolida en la tabla histórica
# nessie.silver.post_hist (Iceberg) con MERGE INTO.
#
# Ejecutar una vez, después de scripts/bronze_manual.py y antes del DAG:
#   docker compose exec spark-master /opt/spark/bin/spark-submit \
#       --master spark://spark-master:7077 /opt/spark/jobs/silver_post_hist_manual.py
# (o desde Jupyter con la celda correspondiente de silver_transform.ipynb)

import os
import sys
from functools import reduce

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import comun  # noqa: E402

COLUMNAS_COMPARAR = ["post_type_id", "post_type", "creation_date", "score", "view_count", "owner_user_id",
                     "title", "tags", "answer_count", "comment_count", "accepted_answer_id",
                     "parent_id", "closed_date"]


def leer_bronze_posts(spark):
    anios = []
    for anio in comun.ANIOS:
        ruta = comun.ruta_bronze("posts", anio)
        df = spark.read.parquet(ruta)
        print(f"Bronze {ruta}: {df.count():,} filas")
        anios.append(df)
    return reduce(lambda a, b: a.unionByName(b), anios)


def transformar_posts(df):
    """Curado: snake_case, tipos, 0/'' de la fuente -> NULL, tags 'a|b|c' sin pipes en los
    extremos, filtro del rango de meses, sin duplicados y con fecha_cargue."""
    from pyspark.sql import functions as F

    def id_o_nulo(col):
        return F.when(F.col(col).cast("bigint") > 0, F.col(col).cast("bigint"))

    def texto_o_nulo(col):
        return F.when(F.trim(F.col(col)) != "", F.col(col))

    def fecha_o_nula(col):
        # la fuente usa 1970-01-01 como "sin fecha"
        return F.when(F.year(F.col(col)) > 1970, F.col(col).cast("timestamp"))

    return (
        df.select(
            F.col("Id").cast("bigint").alias("id"),
            F.col("PostTypeId").cast("int").alias("post_type_id"),
            F.expr(comun.expr_mapa("PostTypeId", comun.TIPOS_POST)).alias("post_type"),
            F.col("CreationDate").cast("timestamp").alias("creation_date"),
            F.col("Score").cast("int").alias("score"),
            F.col("ViewCount").cast("int").alias("view_count"),
            id_o_nulo("OwnerUserId").alias("owner_user_id"),
            texto_o_nulo("Title").alias("title"),
            F.regexp_replace(texto_o_nulo("Tags"), r"^\||\|$", "").alias("tags"),
            F.col("AnswerCount").cast("int").alias("answer_count"),
            F.col("CommentCount").cast("int").alias("comment_count"),
            id_o_nulo("AcceptedAnswerId").alias("accepted_answer_id"),
            id_o_nulo("ParentId").alias("parent_id"),
            fecha_o_nula("ClosedDate").alias("closed_date"),
        )
        .withColumn("anio", F.year("creation_date"))
        .withColumn("mes", F.month("creation_date"))
        .filter(F.col("anio").isin(list(comun.ANIOS)))
        .filter(F.col("mes").between(comun.MES_INICIO, comun.MES_FIN))
        .filter(F.col("id").isNotNull())
        .dropDuplicates(["id"])
        .withColumn("fecha_cargue", F.current_timestamp())
    )


def crear_tabla_post_hist(spark):
    comun.crear_namespaces(spark)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {comun.TABLA_POST_HIST} (
            id                  BIGINT,
            post_type_id        INT,
            post_type           STRING,
            creation_date       TIMESTAMP,
            score               INT,
            view_count          INT,
            owner_user_id       BIGINT,
            title               STRING,
            tags                STRING,
            answer_count        INT,
            comment_count       INT,
            accepted_answer_id  BIGINT,
            parent_id           BIGINT,
            closed_date         TIMESTAMP,
            anio                INT,
            mes                 INT,
            fecha_cargue        TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (anio)
        LOCATION '{comun.UBICACION[comun.TABLA_POST_HIST]}'
        TBLPROPERTIES (
            'format-version' = '2',
            'write.parquet.compression-codec' = 'snappy',
            -- el lector vectorizado (Arrow) de Iceberg 1.5 tumbaba los executors con
            -- SIGSEGV / "free(): invalid pointer" en Java 11: se lee fila a fila
            'read.parquet.vectorization.enabled' = 'false'
        )
    """)


def merge_post_hist(spark, df):
    df.createOrReplaceTempView("posts_actualizacion")
    cambio = " OR ".join(f"NOT (t.{c} <=> s.{c})" for c in COLUMNAS_COMPARAR)
    spark.sql(f"""
        MERGE INTO {comun.TABLA_POST_HIST} t
        USING posts_actualizacion s
        ON t.id = s.id
        WHEN MATCHED AND ({cambio}) THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


def main(spark=None):
    propia = spark is None
    if propia:
        spark = comun.crear_spark("silver_post_hist_manual")

    df = transformar_posts(leer_bronze_posts(spark))
    crear_tabla_post_hist(spark)
    merge_post_hist(spark, df)
    total = comun.resumen_tabla(spark, comun.TABLA_POST_HIST)
    spark.sql(f"SELECT anio, post_type, COUNT(*) AS posts FROM {comun.TABLA_POST_HIST} "
              f"GROUP BY anio, post_type ORDER BY anio, post_type").show()

    if propia:
        spark.stop()
    return total


if __name__ == "__main__":
    main()
