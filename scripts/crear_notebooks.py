# Genera los 3 notebooks de respaldo (notebooks/*.ipynb). Solo hace falta si se quieren
# regenerar desde cero:  docker compose exec jupyter python /workspace/scripts/crear_notebooks.py

import nbformat as nbf

md, code = nbf.v4.new_markdown_cell, nbf.v4.new_code_cell

SETUP_JOBS = """import os, sys
sys.path.insert(0, os.environ.get("JOBS_DIR", "/workspace/jobs"))   # carpeta jobs/ del proyecto
import comun"""

SETUP_SPARK = SETUP_JOBS + """

# Sesión de Spark contra el clúster (spark://spark-master:7077): aparece en la UI de Spark (9090)
spark = comun.crear_spark(NOMBRE_APP, master=os.environ["SPARK_MASTER"])
spark"""


def nb(celdas):
    n = nbf.v4.new_notebook()
    n.cells = celdas
    n.metadata["kernelspec"] = {"name": "python3", "display_name": "Python 3", "language": "python"}
    return n


bronze = nb([
    md("""# 1 · Bronze — ingesta con dlt (`bronze_ingest.ipynb`)

Respaldo manual de la tarea **`bronze_ingest`** del DAG `medallon_stackoverflow`. Ejecuta **la misma función** que Airflow (`jobs/bronze_ingest.py`).

- **Fuente:** `votes/2021.parquet` del dataset StackOverflow de ClickHouse (HTTP).
- **Destino:** `s3://bronze/votes/votes_2021/` en MinIO, en **Parquet**.
- **Modo:** `write_disposition="replace"` → **override**: cada ejecución reemplaza el año en Bronze.
- Se conserva el mismo rango de meses que los archivos subidos a mano (`MES_INICIO`–`MES_FIN` en `jobs/comun.py`).

Ejecutar las celdas en orden (*Run All*)."""),
    code(SETUP_JOBS + """
import bronze_ingest
print("Año del pipeline:", comun.ANIO_PIPELINE, "| meses:", comun.MES_INICIO, "a", comun.MES_FIN)"""),
    md("## Ingesta (dlt → MinIO)"),
    code("bronze_ingest.main()"),
    md("## Verificación: contenido de Bronze"),
    code("""import pyarrow.dataset as ds
from pyarrow import fs

access_key, secret_key = comun.credenciales_minio()
s3 = fs.S3FileSystem(access_key=access_key, secret_key=secret_key,
                     endpoint_override="minio:9000", scheme="http", region="us-east-1")

for tabla, anio in [("posts", 2020), ("posts", 2021), ("votes", 2020), ("votes", 2021)]:
    carpeta = comun.ruta_bronze(tabla, anio, esquema="s3").replace("s3://", "")
    archivos = [f.path for f in s3.get_file_info(fs.FileSelector(carpeta)) if f.path.endswith(".parquet")]
    filas = ds.dataset(archivos, filesystem=s3, format="parquet").count_rows()
    print(f"{tabla}_{anio:<6} {len(archivos)} archivo(s)  {filas:>12,} filas   s3://{carpeta}")"""),
])

silver = nb([
    md("""# 2 · Silver — transformación con PySpark + Iceberg (`silver_transform.ipynb`)

Respaldo manual de la tarea **`silver_transform`** del DAG. Ejecuta **la misma función** que Airflow (`jobs/silver_transform.py`).

- Lee `votes_2020` (subido a mano) y `votes_2021` (traído por dlt) de Bronze.
- Cura y normaliza (snake_case, tipos, catálogo de tipos de voto, `UserId = 0` → `NULL`, sin duplicados) y agrega **`fecha_cargue`**.
- Consolida en la tabla histórica **`nessie.silver.votes_hist`** (Iceberg) con **`MERGE INTO`** (no overwrite).

La tabla histórica de posts (`nessie.silver.post_hist`) se construye con un **script manual** (`jobs/silver_post_hist_manual.py`), como permite el enunciado. Si todavía no existe, se puede crear con la celda opcional del final."""),
    code('NOMBRE_APP = "silver_transform_notebook"\n' + SETUP_SPARK),
    md("## Transformación y MERGE en `nessie.silver.votes_hist`"),
    code("""import silver_transform
silver_transform.main(spark)"""),
    md("## Verificación"),
    code("""spark.sql(f\"\"\"
    SELECT vote_type, COUNT(*) AS votos
    FROM {comun.TABLA_VOTES_HIST}
    GROUP BY vote_type ORDER BY votos DESC
\"\"\").show()
spark.table(comun.TABLA_VOTES_HIST).orderBy("id").show(5, truncate=False)"""),
    code("""# Historial de la tabla: cada MERGE deja un snapshot nuevo en Iceberg
spark.sql(f"SELECT committed_at, snapshot_id, operation FROM {comun.TABLA_VOTES_HIST}.snapshots ORDER BY committed_at").show(truncate=False)"""),
    md("""## (Opcional) Tabla histórica de posts — script manual

Solo hace falta la primera vez, después de subir `posts_2020` y `posts_2021` a Bronze (`scripts/bronze_manual.py`). Es idempotente: re-ejecutarla no duplica filas."""),
    code("""CREAR_POST_HIST = False   # poner True para (re)construir nessie.silver.post_hist

if CREAR_POST_HIST:
    import silver_post_hist_manual
    silver_post_hist_manual.main(spark)
else:
    print(f"{comun.TABLA_POST_HIST}: {spark.table(comun.TABLA_POST_HIST).count():,} filas")"""),
    code("spark.stop()"),
])

gold = nb([
    md("""# 3 · Gold — agregación con PySpark + Iceberg (`gold_agg.ipynb`)

Respaldo manual de la tarea **`gold_agg`** del DAG. Ejecuta **la misma función** que Airflow (`jobs/gold_agg.py`).

- Cruza las dos tablas históricas de Silver: **`post_hist ⋈ votes_hist`** (por `post_id`).
- Regla de negocio: métricas de votos por post (total, positivos, negativos, favoritos, balance, primer y último voto).
- Escribe **`nessie.gold.vote_stats_per_post`** (Iceberg) con **`MERGE INTO`** y la columna **`fecha_cargue`**.

La misma agregación, escrita en SQL de Trino sobre Silver, está en `consultas/trino_replica_gold.sql`."""),
    code('NOMBRE_APP = "gold_agg_notebook"\n' + SETUP_SPARK),
    md("## Agregación y MERGE en `nessie.gold.vote_stats_per_post`"),
    code("""import gold_agg
gold_agg.main(spark)"""),
    md("## Verificación"),
    code("""# Totales (deben coincidir con la consulta 2 de Trino y de Dremio)
spark.sql(f\"\"\"
    SELECT COUNT(*) AS filas, SUM(total_votos) AS votos,
           SUM(votos_positivos) AS positivos, SUM(votos_negativos) AS negativos
    FROM {comun.TABLA_GOLD}
\"\"\").show()

# Registro puntual (consulta 4 de Trino y de Dremio)
spark.sql(f"SELECT * FROM {comun.TABLA_GOLD} WHERE post_id = 59837318").show(vertical=True, truncate=False)"""),
    code("""# Gold recalculado desde Silver vs Gold escrito: 0 y 0 = idénticos
from pyspark.sql import functions as F
recalculado = spark.sql(gold_agg.SQL_GOLD)
escrito = spark.table(comun.TABLA_GOLD).drop("fecha_cargue")
print("solo en recalculado:", recalculado.exceptAll(escrito.select(*recalculado.columns)).count())
print("solo en gold:       ", escrito.select(*recalculado.columns).exceptAll(recalculado).count())"""),
    code("spark.stop()"),
])

for nombre, cuaderno in [("bronze_ingest", bronze), ("silver_transform", silver), ("gold_agg", gold)]:
    nbf.write(cuaderno, f"/workspace/notebooks/{nombre}.ipynb")
    print("creado", nombre)
