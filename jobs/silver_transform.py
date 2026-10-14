# Tarea 2 del pipeline · TRANSFORMACIÓN SILVER (PySpark + Iceberg)
#
# Lee los dos años de votes en Bronze (votes_2020 subido a mano + votes_2021 traído por dlt),
# los cura y normaliza, y los consolida en la tabla histórica nessie.silver.votes_hist
# con MERGE INTO (no overwrite): los votos nuevos se insertan, los que cambiaron se actualizan
# y los repetidos no se duplican.
#
# Lo ejecuta Airflow (SparkSubmitOperator) y también el notebook silver_transform.ipynb.

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import comun  # noqa: E402

COLUMNAS_COMPARAR = ["post_id", "vote_type_id", "creation_date", "user_id", "bounty_amount"]


def leer_bronze_votes(spark):
    """Une los años de votes que están en Bronze (columnas originales de la fuente)."""
    from functools import reduce

    anios = []
    for anio in comun.ANIOS:
        ruta = comun.ruta_bronze("votes", anio)
        df = spark.read.parquet(ruta).select(
            "Id", "PostId", "VoteTypeId", "CreationDate", "UserId", "BountyAmount")
        print(f"Bronze {ruta}: {df.count():,} filas")
        anios.append(df)
    return reduce(lambda a, b: a.unionByName(b), anios)


def transformar_votes(df):
    """Curado y normalización: nombres snake_case, tipos, catálogo de tipos de voto,
    filtro del rango de meses, sin duplicados y con la columna de control fecha_cargue."""
    from pyspark.sql import functions as F

    curado = (
        df.select(
            F.col("Id").cast("bigint").alias("id"),
            F.col("PostId").cast("bigint").alias("post_id"),
            F.col("VoteTypeId").cast("int").alias("vote_type_id"),
            F.expr(comun.expr_mapa("VoteTypeId", comun.TIPOS_VOTO)).alias("vote_type"),
            F.col("CreationDate").cast("timestamp").alias("creation_date"),
            # UserId = 0 significa voto anónimo: se normaliza a NULL
            F.when(F.col("UserId") > 0, F.col("UserId").cast("bigint")).alias("user_id"),
            F.col("BountyAmount").cast("int").alias("bounty_amount"),
        )
        .withColumn("anio", F.year("creation_date"))
        .withColumn("mes", F.month("creation_date"))
        .filter(F.col("anio").isin(list(comun.ANIOS)))
        .filter(F.col("mes").between(comun.MES_INICIO, comun.MES_FIN))
        .filter(F.col("id").isNotNull() & F.col("post_id").isNotNull())
        # MERGE exige una sola fila de origen por llave
        .dropDuplicates(["id"])
        .withColumn("fecha_cargue", F.current_timestamp())
    )
    return curado


def crear_tabla_votes_hist(spark):
    comun.crear_namespaces(spark)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {comun.TABLA_VOTES_HIST} (
            id             BIGINT,
            post_id        BIGINT,
            vote_type_id   INT,
            vote_type      STRING,
            creation_date  TIMESTAMP,
            user_id        BIGINT,
            bounty_amount  INT,
            anio           INT,
            mes            INT,
            fecha_cargue   TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (anio)
        LOCATION '{comun.UBICACION[comun.TABLA_VOTES_HIST]}'
        TBLPROPERTIES (
            'format-version' = '2',
            'write.parquet.compression-codec' = 'snappy',
            -- el lector vectorizado (Arrow) de Iceberg 1.5 tumbaba los executors con
            -- SIGSEGV / "free(): invalid pointer" en Java 11: se lee fila a fila
            'read.parquet.vectorization.enabled' = 'false'
        )
    """)


def merge_votes_hist(spark, df):
    """MERGE INTO: inserta votos nuevos y actualiza solo los que cambiaron."""
    df.createOrReplaceTempView("votes_actualizacion")
    cambio = " OR ".join(f"NOT (t.{c} <=> s.{c})" for c in COLUMNAS_COMPARAR)
    spark.sql(f"""
        MERGE INTO {comun.TABLA_VOTES_HIST} t
        USING votes_actualizacion s
        ON t.id = s.id
        WHEN MATCHED AND ({cambio}) THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


def main(spark=None):
    propia = spark is None
    if propia:
        spark = comun.crear_spark("silver_transform")

    df = transformar_votes(leer_bronze_votes(spark))
    crear_tabla_votes_hist(spark)
    merge_votes_hist(spark, df)
    total = comun.resumen_tabla(spark, comun.TABLA_VOTES_HIST)
    spark.sql(f"SELECT anio, COUNT(*) AS votos FROM {comun.TABLA_VOTES_HIST} "
              f"GROUP BY anio ORDER BY anio").show()

    if propia:
        spark.stop()
    return total


if __name__ == "__main__":
    main()
