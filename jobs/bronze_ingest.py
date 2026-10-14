# Tarea 1 del pipeline · INGESTA BRONZE con dlt
#
# Fuente:  votes/2021.parquet del dataset StackOverflow de ClickHouse (HTTP)
# Destino: s3://bronze/votes/votes_2021/  (MinIO, formato Parquet)
# Modo:    write_disposition="replace" -> cada ejecución SOBRESCRIBE (override) el año en Bronze
#
# Se queda con el rango de meses definido en comun.py (el mismo que tienen los archivos que
# se suben a mano), para que los años sean comparables.
#
# Lo ejecuta Airflow (tarea bronze_ingest) y también el notebook bronze_ingest.ipynb.

import os
import sys
import tempfile

import dlt
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import comun  # noqa: E402

TABLA = "votes"
NOMBRE_TABLA_BRONZE = f"{TABLA}_{comun.ANIO_PIPELINE}"      # votes_2021


def descargar_fuente(destino):
    """Descarga el Parquet anual de la fuente por HTTP (en bloques, sin cargarlo en RAM)."""
    url = f"{comun.URL_FUENTE}/{TABLA}/{comun.ANIO_PIPELINE}.parquet"
    print(f"Descargando {url}")
    with requests.get(url, stream=True, timeout=60) as resp:
        resp.raise_for_status()
        with open(destino, "wb") as f:
            for bloque in resp.iter_content(chunk_size=8 * 1024 * 1024):
                f.write(bloque)
    print(f"Descargado: {os.path.getsize(destino) / 1e6:,.0f} MB")


def leer_por_lotes(archivo):
    """Lee el Parquet por row groups y entrega solo las filas del rango de meses."""
    pf = pq.ParquetFile(archivo)
    total = 0
    for i in range(pf.num_row_groups):
        lote = pf.read_row_group(i)
        mes = pc.month(lote["CreationDate"])
        lote = lote.filter(pc.and_(pc.greater_equal(mes, comun.MES_INICIO),
                                   pc.less_equal(mes, comun.MES_FIN)))
        if lote.num_rows == 0:
            continue
        # BountyAmount viene como uint64, que Spark no lee bien: se pasa a int64
        lote = lote.set_column(lote.schema.get_field_index("BountyAmount"), "BountyAmount",
                               lote["BountyAmount"].cast(pa.int64()))
        total += lote.num_rows
        yield lote
    print(f"Filas enviadas a Bronze: {total:,}")


def destino_minio():
    access_key, secret_key = comun.credenciales_minio()
    return dlt.destinations.filesystem(
        bucket_url="s3://bronze",
        credentials={
            "aws_access_key_id": access_key,
            "aws_secret_access_key": secret_key,
            "endpoint_url": comun.MINIO_ENDPOINT,
            "region_name": "us-east-1",
        },
        # s3://bronze/votes/votes_2021/<load_id>.<file_id>.parquet
        layout="{table_name}/{load_id}.{file_id}.{ext}",
        # el nombre de la tabla (votes_2021) no lleva "/", así que el layout es seguro
        warn_unsafe_layout_separators=False,
    )


def main():
    # conserva los nombres originales de las columnas (Id, PostId, ...), igual que los
    # archivos de Bronze que se suben a mano (dlt por defecto los pasa a snake_case)
    os.environ["SCHEMA__NAMING"] = "direct"

    with tempfile.TemporaryDirectory() as tmp:
        archivo = os.path.join(tmp, f"{NOMBRE_TABLA_BRONZE}.parquet")
        descargar_fuente(archivo)

        recurso = dlt.resource(
            leer_por_lotes(archivo),
            name=NOMBRE_TABLA_BRONZE,
            write_disposition="replace",   # override de Bronze
        )
        pipeline = dlt.pipeline(
            pipeline_name="bronze_votes",
            destination=destino_minio(),
            dataset_name=TABLA,             # carpeta s3://bronze/votes/
            pipelines_dir=os.path.join(tmp, "dlt_pipelines"),
        )
        info = pipeline.run(recurso, loader_file_format="parquet")
        print(info)

    print(f"Bronze listo: {comun.ruta_bronze(TABLA, comun.ANIO_PIPELINE, 's3')}")


if __name__ == "__main__":
    main()
