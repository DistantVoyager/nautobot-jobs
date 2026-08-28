#!/usr/bin/env bash
#
# Load an NDG demo dataset into a local Docker-based Nautobot dev environment.
#
# Uses the Postgres SQL artifact (-P-), which is the only NDG JSON/SQL format that
# actually restores cleanly. See docs/local-dev-data.md for why the -J- (dumpdata
# JSON) artifacts do not work.
#
# Usage:
#   ./load-ndg-dataset.sh <nautobot-container> <db-container> [version] [size]
#
# Example:
#   ./load-ndg-dataset.sh ai-cortex-nautobot-1 ai-cortex-db-1 3.1.4 S
#
# WARNING: this DROPS the public schema in the target database. Dev environments only.

set -euo pipefail

NAUTOBOT_CONTAINER="${1:?usage: $0 <nautobot-container> <db-container> [version] [size]}"
DB_CONTAINER="${2:?usage: $0 <nautobot-container> <db-container> [version] [size]}"
VERSION="${3:-3.1.4}"
SIZE="${4:-S}"

REPO="networktocode-llc/nautobot-data-generation"
TAG="branch_feature/no-jira/add-setup-job-persona_gizmo_nautobot3"
ASSET="nautobot_${VERSION}-P-${SIZE}-latest.tar.gz"
WORKDIR="${NDG_WORKDIR:-$HOME/ndg-data}"
DB_USER="${DB_USER:-nautobot}"
DB_NAME="${DB_NAME:-nautobot}"

echo ">>> target: $NAUTOBOT_CONTAINER (app) / $DB_CONTAINER (db)"
echo ">>> dataset: $ASSET"

mkdir -p "$WORKDIR"
cd "$WORKDIR"

if [[ ! -f nautobot.sql ]] || [[ "${FORCE_DOWNLOAD:-0}" == "1" ]]; then
    echo ">>> downloading $ASSET"
    gh release download "$TAG" --repo "$REPO" --pattern "$ASSET" --clobber
    tar -xzf "$ASSET"
fi
ls -la nautobot.sql

read -r -p ">>> This will DESTROY all data in $DB_CONTAINER/$DB_NAME. Continue? [y/N] " reply
[[ "$reply" == "y" || "$reply" == "Y" ]] || { echo "aborted"; exit 1; }

echo ">>> copying dump into $DB_CONTAINER"
docker cp nautobot.sql "$DB_CONTAINER:/tmp/nautobot.sql"

echo ">>> dropping and recreating public schema"
docker exec "$DB_CONTAINER" psql -q -U "$DB_USER" -d "$DB_NAME" \
    -c "DROP SCHEMA public CASCADE; CREATE SCHEMA public;"

echo ">>> restoring (this takes a minute)"
docker exec "$DB_CONTAINER" sh -c "psql -q -U $DB_USER -d $DB_NAME < /tmp/nautobot.sql" >/dev/null

echo ">>> applying migrations (bridges the dataset's Nautobot version to yours)"
docker exec "$NAUTOBOT_CONTAINER" nautobot-server migrate 2>&1 | tail -3

echo ">>> creating a superuser (the dump's accounts are not yours)"
echo ">>>   docker exec -it $NAUTOBOT_CONTAINER nautobot-server createsuperuser"

echo ">>> object counts:"
docker exec -i "$NAUTOBOT_CONTAINER" nautobot-server shell <<'PYEOF' 2>/dev/null | grep COUNTS
from nautobot.dcim.models import Cable, Device, Interface, Location, Rack
from nautobot.ipam.models import IPAddress, Prefix
print(
    "COUNTS devices=", Device.objects.count(),
    "locations=", Location.objects.count(),
    "interfaces=", Interface.objects.count(),
    "racks=", Rack.objects.count(),
    "cables=", Cable.objects.count(),
    "prefixes=", Prefix.objects.count(),
    "ips=", IPAddress.objects.count(),
)
PYEOF

echo ">>> done"
