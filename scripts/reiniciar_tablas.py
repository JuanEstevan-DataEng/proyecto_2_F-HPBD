# Borra las tablas Iceberg del proyecto (catálogo Nessie + archivos en MinIO) para volver a
# ejecutar el pipeline desde cero. NO toca Bronze.
#
#   docker compose exec jupyter python /workspace/scripts/reiniciar_tablas.py            # las 3
#   docker compose exec jupyter python /workspace/scripts/reiniciar_tablas.py gold       # solo gold
#
# Nessie no permite DROP TABLE ... PURGE (tiene el GC desactivado), por eso los archivos se
# borran aparte en MinIO.

import os
import sys

from pyarrow import fs

sys.path.insert(0, os.environ.get("JOBS_DIR", "/workspace/jobs"))
import comun  # noqa: E402


def main(filtro=None):
    tablas = [t for t in comun.UBICACION if not filtro or filtro in t]
    spark = comun.crear_spark("reiniciar_tablas", master="local[1]")
    access_key, secret_key = comun.credenciales_minio()
    s3 = fs.S3FileSystem(access_key=access_key, secret_key=secret_key,
                         endpoint_override=comun.MINIO_ENDPOINT.replace("http://", ""),
                         scheme="http", region="us-east-1")
    for tabla in tablas:
        spark.sql(f"DROP TABLE IF EXISTS {tabla}")
        carpeta = comun.UBICACION[tabla].replace("s3://", "")
        try:
            s3.delete_dir(carpeta)
        except (FileNotFoundError, OSError):
            pass
        print(f"borrada: {tabla} ({comun.UBICACION[tabla]})")
    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else None)
