#!/usr/bin/env bash
# Configura Dremio por su API REST (equivale a hacerlo a mano en la UI http://localhost:9047):
#   1. crea el usuario administrador (solo la primera vez; DREMIO_USER / DREMIO_PASSWORD del .env)
#   2. inicia sesión
#   3. crea la fuente "nessie" (catálogo Nessie + almacenamiento S3 en MinIO)
#
# Uso (desde la raíz del proyecto, con el stack arriba):
#   bash scripts/configurar_dremio.sh
# Después, en Dremio: nessie > gold > vote_stats_per_post (y nessie > silver > ...).
set -euo pipefail

set -a; source .env; set +a
DREMIO="${DREMIO_URL:-http://localhost:9047}"

echo ">> Esperando a Dremio en $DREMIO ..."
until curl -sf "$DREMIO" >/dev/null; do sleep 5; done

echo ">> Creando usuario administrador '$DREMIO_USER' (si ya existe, se ignora el error)"
curl -s -X PUT "$DREMIO/apiv2/bootstrap/firstuser" \
  -H 'Content-Type: application/json' \
  -d "{\"userName\":\"$DREMIO_USER\",\"firstName\":\"Admin\",\"lastName\":\"FHBD\",
       \"email\":\"admin@example.com\",\"createdAt\":$(date +%s)000,
       \"password\":\"$DREMIO_PASSWORD\"}" >/dev/null || true

echo ">> Iniciando sesión"
TOKEN=$(curl -sf -X POST "$DREMIO/apiv2/login" -H 'Content-Type: application/json' \
  -d "{\"userName\":\"$DREMIO_USER\",\"password\":\"$DREMIO_PASSWORD\"}" \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["token"])')
AUTH="Authorization: _dremio$TOKEN"

if curl -sf -H "$AUTH" "$DREMIO/api/v3/catalog/by-path/nessie" >/dev/null; then
  echo ">> La fuente 'nessie' ya existe: nada que hacer"
  exit 0
fi

echo ">> Creando la fuente 'nessie' (Nessie + MinIO)"
curl -sf -X POST "$DREMIO/api/v3/catalog" -H "$AUTH" -H 'Content-Type: application/json' -d @- <<EOF >/dev/null
{
  "entityType": "source",
  "name": "nessie",
  "type": "NESSIE",
  "config": {
    "nessieEndpoint": "http://nessie:19120/api/v2",
    "nessieAuthType": "NONE",
    "credentialType": "ACCESS_KEY",
    "awsAccessKey": "$MINIO_ROOT_USER",
    "awsAccessSecret": "$MINIO_ROOT_PASSWORD",
    "awsRootPath": "/warehouse",
    "secure": false,
    "propertyList": [
      {"name": "fs.s3a.path.style.access", "value": "true"},
      {"name": "fs.s3a.endpoint", "value": "minio:9000"},
      {"name": "dremio.s3.compat", "value": "true"}
    ]
  }
}
EOF
echo ">> Listo. Fuente creada:"
curl -sf -H "$AUTH" "$DREMIO/api/v3/catalog/by-path/nessie" \
  | python3 -c 'import json,sys; d=json.load(sys.stdin); print(" ", d["name"], "->", [c["path"][-1] for c in d.get("children", [])])'
