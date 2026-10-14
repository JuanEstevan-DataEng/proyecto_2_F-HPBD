# Carga MANUAL de Bronze (fuera del DAG, como permite el enunciado):
#   posts_2020, posts_2021 y votes_2020  ->  s3://bronze/<tabla>/<tabla>_<año>/<tabla>_<año>.parquet
# votes_2021 NO se sube aquí: lo trae el DAG con dlt (jobs/bronze_ingest.py).
#
# Para no reventar la RAM se deja el mismo rango de meses de los dos años (jobs/comun.py) y
# solo las columnas que usa el proyecto (se descarta Body, el HTML de cada post).
# Los textos de la fuente vienen como binary: se pasan a string para que Spark los lea como texto.
#
# Ejecutar dentro del contenedor de Jupyter (tiene pyarrow y ve /datos y MinIO):
#   docker compose exec jupyter python /workspace/scripts/bronze_manual.py
# Requisito: haber descargado los Parquet con scripts/descargar_dataset.sh

import os
import sys

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from pyarrow import fs

sys.path.insert(0, os.environ.get("JOBS_DIR", "/workspace/jobs"))
import comun  # noqa: E402

CARPETA_DATOS = os.environ.get("CARPETA_DATOS", "/datos/raw")

COLUMNAS = {
    "posts": ["Id", "PostTypeId", "AcceptedAnswerId", "CreationDate", "Score", "ViewCount",
              "OwnerUserId", "Title", "Tags", "AnswerCount", "CommentCount", "FavoriteCount",
              "ParentId", "ClosedDate"],
    "votes": ["Id", "PostId", "VoteTypeId", "CreationDate", "UserId", "BountyAmount"],
}

# (tabla, año) que se suben a mano
CARGAS_MANUALES = [("posts", 2020), ("posts", 2021), ("votes", 2020)]


def s3():
    access_key, secret_key = comun.credenciales_minio()
    return fs.S3FileSystem(access_key=access_key, secret_key=secret_key,
                           endpoint_override=comun.MINIO_ENDPOINT.replace("http://", ""),
                           scheme="http", region="us-east-1")


def ajustar_tipos(lote):
    """binary -> string y uint64 -> int64 (tipos que Spark/Trino leen sin problema)."""
    for i, campo in enumerate(lote.schema):
        if pa.types.is_binary(campo.type) or pa.types.is_large_binary(campo.type):
            lote = lote.set_column(i, campo.name, lote.column(i).cast(pa.string()))
        elif pa.types.is_unsigned_integer(campo.type):
            lote = lote.set_column(i, campo.name, lote.column(i).cast(pa.int64()))
    return lote


def cargar(s3fs, tabla, anio):
    origen = os.path.join(CARPETA_DATOS, f"{tabla}_{anio}.parquet")
    carpeta = comun.ruta_bronze(tabla, anio, esquema="s3").replace("s3://", "")
    destino = f"{carpeta}{tabla}_{anio}.parquet"
    print(f">> {origen} -> s3://{destino}")

    # override: se vacía la carpeta del año antes de escribir
    try:
        s3fs.delete_dir_contents(carpeta, missing_dir_ok=True)
    except FileNotFoundError:
        pass

    pf = pq.ParquetFile(origen)
    escritor, total = None, 0
    for i in range(pf.num_row_groups):
        lote = pf.read_row_group(i, columns=COLUMNAS[tabla])
        mes = pc.month(lote["CreationDate"])
        lote = lote.filter(pc.and_(pc.greater_equal(mes, comun.MES_INICIO),
                                   pc.less_equal(mes, comun.MES_FIN)))
        if lote.num_rows == 0:
            continue
        lote = ajustar_tipos(lote)
        if escritor is None:
            escritor = pq.ParquetWriter(destino, lote.schema, filesystem=s3fs,
                                        compression="snappy")
        escritor.write_table(lote)
        total += lote.num_rows
        print(f"   row group {i + 1}/{pf.num_row_groups}: {total:,} filas acumuladas")
    if escritor:
        escritor.close()
    print(f"   listo: {total:,} filas (meses {comun.MES_INICIO}-{comun.MES_FIN} de {anio})")
    return total


def main():
    s3fs = s3()
    resumen = {f"{t}_{a}": cargar(s3fs, t, a) for t, a in CARGAS_MANUALES}
    print("\nResumen Bronze manual:")
    for nombre, filas in resumen.items():
        print(f"  {nombre:<12} {filas:>12,} filas")


if __name__ == "__main__":
    main()
