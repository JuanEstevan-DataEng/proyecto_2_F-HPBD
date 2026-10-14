#!/usr/bin/env bash
# Descarga los Parquet de StackOverflow (dataset de ejemplo de ClickHouse) que se suben
# A MANO a Bronze: posts 2020, posts 2021 y votes 2020.
# votes 2021 NO se descarga aquí: lo trae el DAG de Airflow con dlt (tarea bronze_ingest).
#
# Uso (desde la raíz del proyecto):
#   bash scripts/descargar_dataset.sh [carpeta_destino]
# La carpeta por defecto es la variable DATOS_LOCALES del .env (o ./datos).
# Pesos aproximados: posts_2020 3,1 GB · posts_2021 2,6 GB · votes_2020 184 MB.
set -euo pipefail

BASE_URL="https://datasets-documentation.s3.eu-west-3.amazonaws.com/stackoverflow/parquet"

if [[ -f .env ]]; then
  DATOS_LOCALES=$(grep -E '^DATOS_LOCALES=' .env | cut -d= -f2- || true)
fi
DESTINO="${1:-${DATOS_LOCALES:-./datos}}/raw"
mkdir -p "$DESTINO"

for archivo in posts/2020 posts/2021 votes/2020; do
  salida="$DESTINO/$(echo "$archivo" | tr / _).parquet"   # posts/2020 -> posts_2020.parquet
  echo ">> $BASE_URL/$archivo.parquet -> $salida"
  # -C - reanuda la descarga si se cortó
  curl -fL -C - -o "$salida" "$BASE_URL/$archivo.parquet"
done

ls -lh "$DESTINO"
