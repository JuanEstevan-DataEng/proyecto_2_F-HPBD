# Configuración compartida por los jobs del pipeline (Airflow) y los notebooks (Jupyter).
# Aquí están las rutas de Bronze, los nombres de las tablas Iceberg, el rango de datos y la
# creación de la SparkSession con el catálogo Nessie. Así Airflow y Jupyter ejecutan
# exactamente la misma lógica.

import os

# ================== ALCANCE DE LOS DATOS ==================
# Se cargan dos años y, de cada año, el mismo rango de meses (recomendación del profe para
# que el join posts ⋈ votes tenga coincidencias sin reventar la RAM del equipo).
ANIOS = (2020, 2021)
MES_INICIO = 1   # enero
MES_FIN = 3      # marzo (inclusive)

# Año que trae el DAG con dlt; los demás archivos de Bronze se suben a mano
ANIO_PIPELINE = 2021
URL_FUENTE = "https://datasets-documentation.s3.eu-west-3.amazonaws.com/stackoverflow/parquet"

# ================== MINIO / S3 ==================
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")


def credenciales_minio():
    return os.environ["AWS_ACCESS_KEY_ID"], os.environ["AWS_SECRET_ACCESS_KEY"]


def ruta_bronze(tabla, anio, esquema="s3a"):
    """Carpeta de un año en Bronze, p. ej. s3a://bronze/votes/votes_2021/"""
    return f"{esquema}://bronze/{tabla}/{tabla}_{anio}/"


# ================== TABLAS ICEBERG (catálogo Nessie) ==================
TABLA_POST_HIST = "nessie.silver.post_hist"
TABLA_VOTES_HIST = "nessie.silver.votes_hist"
TABLA_GOLD = "nessie.gold.vote_stats_per_post"

# Cada capa guarda sus archivos en su propio bucket
UBICACION = {
    TABLA_POST_HIST: "s3://silver/post_hist",
    TABLA_VOTES_HIST: "s3://silver/votes_hist",
    TABLA_GOLD: "s3://gold/vote_stats_per_post",
}

# Tipos de voto de StackOverflow (VoteTypeId)
TIPOS_VOTO = {
    1: "respuesta_aceptada",
    2: "voto_positivo",
    3: "voto_negativo",
    4: "ofensivo",
    5: "favorito",
    6: "cierre",
    7: "reapertura",
    8: "inicio_recompensa",
    9: "cierre_recompensa",
    10: "borrado",
    11: "restauracion",
    12: "spam",
    15: "revision_moderador",
    16: "edicion_aprobada",
}

# Tipos de post (PostTypeId)
TIPOS_POST = {
    1: "pregunta",
    2: "respuesta",
    3: "wiki_tag_huerfana",
    4: "extracto_wiki_tag",
    5: "wiki_tag",
    6: "nominacion_moderador",
    7: "wiki_placeholder",
    8: "wiki_privilegio",
}


def crear_spark(nombre_app, master=None):
    """SparkSession con Iceberg + Nessie y acceso S3A/S3FileIO a MinIO.

    - Desde Airflow (SparkSubmitOperator) el master lo fija spark-submit: master=None.
    - Desde Jupyter se pasa master=os.environ["SPARK_MASTER"] (spark://spark-master:7077).
    """
    from pyspark.sql import SparkSession

    access_key, secret_key = credenciales_minio()
    builder = SparkSession.builder.appName(nombre_app)
    if master:
        builder = (
            builder.master(master)
            # recursos cuando el notebook lanza la sesión (en Airflow los pone el operador)
            .config("spark.cores.max", "2")
            .config("spark.executor.cores", "2")
            .config("spark.executor.memory", "2g")
            .config("spark.driver.memory", "1g")
        )

    return (
        builder
        # extensiones de Iceberg (MERGE INTO) y Nessie
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
                "org.projectnessie.spark.extensions.NessieSparkSessionExtensions")
        # catálogo nessie (rama main)
        .config("spark.sql.defaultCatalog", "nessie")
        .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        .config("spark.sql.catalog.nessie.uri", "http://nessie:19120/api/v1")
        .config("spark.sql.catalog.nessie.ref", "main")
        .config("spark.sql.catalog.nessie.authentication.type", "NONE")
        .config("spark.sql.catalog.nessie.warehouse", "s3://warehouse/")
        # IO de Iceberg hacia MinIO
        .config("spark.sql.catalog.nessie.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.nessie.s3.endpoint", MINIO_ENDPOINT)
        .config("spark.sql.catalog.nessie.s3.path-style-access", "true")
        .config("spark.sql.catalog.nessie.s3.access-key-id", access_key)
        .config("spark.sql.catalog.nessie.s3.secret-access-key", secret_key)
        .config("spark.sql.catalog.nessie.client.region", "us-east-1")
        # S3A para leer los Parquet de Bronze
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.sql.session.timeZone", "UTC")
        # el lector vectorizado de Iceberg (Arrow + netty) corrompía memoria en los executors
        # (SIGSEGV en Java 11): se desactiva para todas las lecturas de tablas Iceberg
        .config("spark.sql.iceberg.vectorization.enabled", "false")
        .config("spark.sql.shuffle.partitions", "16")
        .getOrCreate()
    )


def expr_mapa(columna, mapa, otro="otro"):
    """CASE WHEN de SQL a partir de un diccionario (sin UDFs de Python)."""
    casos = " ".join(f"WHEN {columna} = {k} THEN '{v}'" for k, v in mapa.items())
    return f"CASE {casos} ELSE '{otro}' END"


def crear_namespaces(spark):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.gold")


def resumen_tabla(spark, tabla):
    """Conteo de filas y últimos snapshots (operación MERGE = 'overwrite' en Iceberg)."""
    total = spark.table(tabla).count()
    print(f"{tabla}: {total:,} filas")
    spark.sql(
        f"SELECT committed_at, snapshot_id, operation, "
        f"summary['added-records'] AS agregadas, summary['deleted-records'] AS borradas "
        f"FROM {tabla}.snapshots ORDER BY committed_at DESC LIMIT 3"
    ).show(truncate=False)
    return total
