# Tarea 3 del pipeline · AGREGACIÓN GOLD (PySpark + Iceberg)
#
# Cruza las dos tablas históricas de Silver (post_hist ⋈ votes_hist) y genera la tabla de
# métricas nessie.gold.vote_stats_per_post: votos positivos/negativos/favoritos por post,
# balance y fechas del primer y último voto. Se escribe con MERGE INTO (no overwrite).
#
# La misma agregación, escrita en SQL de Trino sobre Silver, está en
# consultas/trino_replica_gold.sql (validación: deben dar las mismas filas).
#
# Lo ejecuta Airflow (SparkSubmitOperator) y también el notebook gold_agg.ipynb.

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import comun  # noqa: E402

# Regla de negocio: solo se agregan votos sobre posts que existen en post_hist (INNER JOIN),
# agrupados por post. Columnas solo enteras/texto/fechas para que la réplica en Trino sea exacta.
SQL_GOLD = f"""
SELECT
    p.id                                                    AS post_id,
    p.post_type                                             AS post_type,
    p.anio                                                  AS anio_post,
    p.title                                                 AS titulo,
    p.tags                                                  AS tags,
    p.owner_user_id                                         AS owner_user_id,
    p.score                                                 AS score_post,
    COUNT(v.id)                                             AS total_votos,
    SUM(CASE WHEN v.vote_type_id = 2 THEN 1 ELSE 0 END)     AS votos_positivos,
    SUM(CASE WHEN v.vote_type_id = 3 THEN 1 ELSE 0 END)     AS votos_negativos,
    SUM(CASE WHEN v.vote_type_id = 5 THEN 1 ELSE 0 END)     AS votos_favorito,
    SUM(CASE WHEN v.vote_type_id = 2 THEN 1 ELSE 0 END)
      - SUM(CASE WHEN v.vote_type_id = 3 THEN 1 ELSE 0 END) AS balance_votos,
    MIN(v.creation_date)                                    AS primer_voto,
    MAX(v.creation_date)                                    AS ultimo_voto
FROM {comun.TABLA_POST_HIST} p
JOIN {comun.TABLA_VOTES_HIST} v
  ON v.post_id = p.id
GROUP BY p.id, p.post_type, p.anio, p.title, p.tags, p.owner_user_id, p.score
"""

COLUMNAS_COMPARAR = ["total_votos", "votos_positivos", "votos_negativos", "votos_favorito",
                     "balance_votos", "primer_voto", "ultimo_voto", "score_post",
                     "titulo", "tags", "post_type", "anio_post", "owner_user_id"]


def crear_tabla_gold(spark):
    comun.crear_namespaces(spark)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {comun.TABLA_GOLD} (
            post_id          BIGINT,
            post_type        STRING,
            anio_post        INT,
            titulo           STRING,
            tags             STRING,
            owner_user_id    BIGINT,
            score_post       INT,
            total_votos      BIGINT,
            votos_positivos  BIGINT,
            votos_negativos  BIGINT,
            votos_favorito   BIGINT,
            balance_votos    BIGINT,
            primer_voto      TIMESTAMP,
            ultimo_voto      TIMESTAMP,
            fecha_cargue     TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (anio_post)
        LOCATION '{comun.UBICACION[comun.TABLA_GOLD]}'
        TBLPROPERTIES (
            'format-version' = '2',
            'write.parquet.compression-codec' = 'snappy',
            -- el lector vectorizado (Arrow) de Iceberg 1.5 tumbaba los executors con
            -- SIGSEGV / "free(): invalid pointer" en Java 11: se lee fila a fila
            'read.parquet.vectorization.enabled' = 'false'
        )
    """)


def calcular_gold(spark):
    from pyspark.sql import functions as F
    return spark.sql(SQL_GOLD).withColumn("fecha_cargue", F.current_timestamp())


def merge_gold(spark, df):
    """MERGE INTO: inserta posts nuevos, actualiza métricas que cambiaron y borra posts que
    ya no tengan votos en Silver, para que Gold sea siempre igual a la agregación de Silver."""
    df.createOrReplaceTempView("gold_actualizacion")
    cambio = " OR ".join(f"NOT (t.{c} <=> s.{c})" for c in COLUMNAS_COMPARAR)
    spark.sql(f"""
        MERGE INTO {comun.TABLA_GOLD} t
        USING gold_actualizacion s
        ON t.post_id = s.post_id
        WHEN MATCHED AND ({cambio}) THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        WHEN NOT MATCHED BY SOURCE THEN DELETE
    """)


def main(spark=None):
    propia = spark is None
    if propia:
        spark = comun.crear_spark("gold_agg")

    crear_tabla_gold(spark)
    merge_gold(spark, calcular_gold(spark))
    total = comun.resumen_tabla(spark, comun.TABLA_GOLD)
    spark.sql(f"""
        SELECT anio_post, post_type, COUNT(*) AS posts, SUM(total_votos) AS votos,
               SUM(votos_positivos) AS positivos, SUM(votos_negativos) AS negativos
        FROM {comun.TABLA_GOLD} GROUP BY anio_post, post_type ORDER BY anio_post, post_type
    """).show()
    spark.sql(f"SELECT post_id, titulo, total_votos, balance_votos FROM {comun.TABLA_GOLD} "
              f"ORDER BY total_votos DESC, post_id LIMIT 5").show(truncate=60)

    if propia:
        spark.stop()
    return total


if __name__ == "__main__":
    main()
