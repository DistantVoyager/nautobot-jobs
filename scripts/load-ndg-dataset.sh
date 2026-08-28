#!/usr/bin/env bash
#
# Load an NDG demo dataset into a local Docker-based Nautobot dev environment.
#
# Downloads the dataset, restores it into the database, and runs migrations. Uses the
# Postgres SQL artifact (-P-); the -J- (dumpdata JSON) artifacts are not valid fixtures
# and cannot be loaded. See docs/local-dev-data.md.
#
# WARNING: this DROPS the public schema in the target database. Dev environments only.

set -euo pipefail

REPO="networktocode-llc/nautobot-data-generation"
# NDG publishes one release per build branch, and version/size combos are spread across
# them (e.g. 3.1.4-P-L exists only on the main tag). Search both, newest first.
TAGS=(
    "branch_feature/no-jira/add-setup-job-persona_gizmo_nautobot3"
    "branch_main-persona_gizmo_nautobot3"
)
VALID_SIZES="S L"
RAW_URL="https://raw.githubusercontent.com/DistantVoyager/nautobot-jobs/main/scripts/load-ndg-dataset.sh"

usage() {
    cat <<EOF
Usage: $(basename "$0") [options]

Options:
  -n, --nautobot NAME   Nautobot container (default: autodetect)
  -d, --db NAME         Database container (default: autodetect)
  -v, --version VER     Dataset Nautobot version, e.g. 3.0.0 3.0.6 3.1.4 develop next
                        (default: 3.1.4). Use --list to see what exists.
  -s, --size SIZE       Dataset size: S (~330 devices) or L (80 branches) (default: S)
  -u, --db-user USER    Database user (default: nautobot)
  -b, --db-name NAME    Database name (default: nautobot)
  -y, --yes             Skip the destructive-action confirmation
  -l, --list            List available datasets and exit
  -h, --help            Show this help

Pick a dataset version at or below your instance's Nautobot version -- migrations only
run forward.

Examples:
  $(basename "$0")                                  # autodetect containers, 3.1.4 small
  $(basename "$0") -v 3.0.6 -s L                    # larger 80-branch dataset
  $(basename "$0") -n my-nautobot-1 -d my-db-1 -y   # explicit containers, no prompt

Without cloning the repo:
  curl -fsSL $RAW_URL -o load-ndg-dataset.sh
  bash load-ndg-dataset.sh
EOF
}

NAUTOBOT_CONTAINER=""
DB_CONTAINER=""
VERSION="3.1.4"
SIZE="S"
DB_USER="nautobot"
DB_NAME="nautobot"
ASSUME_YES=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        -n|--nautobot) NAUTOBOT_CONTAINER="${2:?missing value for $1}"; shift 2 ;;
        -d|--db)       DB_CONTAINER="${2:?missing value for $1}"; shift 2 ;;
        -v|--version)  VERSION="${2:?missing value for $1}"; shift 2 ;;
        -s|--size)     SIZE="${2:?missing value for $1}"; shift 2 ;;
        -u|--db-user)  DB_USER="${2:?missing value for $1}"; shift 2 ;;
        -b|--db-name)  DB_NAME="${2:?missing value for $1}"; shift 2 ;;
        -y|--yes)      ASSUME_YES=1; shift ;;
        -l|--list)
            for tag in "${TAGS[@]}"; do
                echo "$tag:"
                gh release view "$tag" --repo "$REPO" --json assets \
                    --jq '.assets[] | select(.name | test("-P-.*latest")) | "  " + .name' \
                    2>/dev/null | sort || echo "  (unavailable)"
            done
            echo
            echo "Filenames are nautobot_{VERSION}-P-{SIZE}-latest.tar.gz"
            exit 0
            ;;
        -h|--help)     usage; exit 0 ;;
        *) echo "error: unknown argument '$1'" >&2; usage >&2; exit 2 ;;
    esac
done

SIZE="$(printf '%s' "$SIZE" | tr '[:lower:]' '[:upper:]')"

grep -qw "$SIZE" <<<"$VALID_SIZES" \
    || { echo "error: size '$SIZE' is not one of: $VALID_SIZES" >&2; exit 2; }

command -v gh >/dev/null || { echo "error: gh CLI not found" >&2; exit 1; }
command -v docker >/dev/null || { echo "error: docker not found" >&2; exit 1; }

# Autodetect containers when not given. Ambiguity is an error rather than a guess --
# picking the wrong container here destroys the wrong database.
autodetect() {
    local pattern="$1" label="$2" matches count
    matches="$(docker ps --format '{{.Names}}' | grep -E "$pattern" || true)"
    count="$(grep -c . <<<"${matches:-}" || true)"
    if [[ -z "$matches" ]]; then
        echo "error: could not autodetect the $label container; pass it explicitly" >&2
        docker ps --format '  {{.Names}}' >&2
        exit 1
    fi
    if [[ "$count" -gt 1 ]]; then
        echo "error: multiple $label containers found; pass one explicitly:" >&2
        sed 's/^/  /' <<<"$matches" >&2
        exit 1
    fi
    printf '%s' "$matches"
}

[[ -n "$NAUTOBOT_CONTAINER" ]] || NAUTOBOT_CONTAINER="$(autodetect 'nautobot-1$' 'Nautobot')"
[[ -n "$DB_CONTAINER" ]] || DB_CONTAINER="$(autodetect 'db-1$|postgres-1$' 'database')"

for container in "$NAUTOBOT_CONTAINER" "$DB_CONTAINER"; do
    docker inspect "$container" >/dev/null 2>&1 \
        || { echo "error: container '$container' not found" >&2; exit 1; }
done

ASSET="nautobot_${VERSION}-P-${SIZE}-latest.tar.gz"
# Cache per version+size so switching either does not silently reuse an old dump.
WORKDIR="${NDG_WORKDIR:-$HOME/.cache/ndg-data}/${VERSION}-${SIZE}"

echo ">>> app container: $NAUTOBOT_CONTAINER"
echo ">>> db  container: $DB_CONTAINER ($DB_USER@$DB_NAME)"
echo ">>> dataset:       $ASSET"

mkdir -p "$WORKDIR"
cd "$WORKDIR"

if [[ ! -f nautobot.sql ]]; then
    downloaded=0
    for tag in "${TAGS[@]}"; do
        echo ">>> trying $tag"
        if gh release download "$tag" --repo "$REPO" --pattern "$ASSET" --clobber 2>/dev/null; then
            downloaded=1
            break
        fi
    done
    if [[ "$downloaded" -ne 1 ]]; then
        echo "error: $ASSET not found in any known release." >&2
        echo "       Run with --list to see what exists." >&2
        exit 1
    fi
    tar -xzf "$ASSET"
    [[ -f nautobot.sql ]] || { echo "error: nautobot.sql not found in $ASSET" >&2; exit 1; }
    rm -f "$ASSET"
else
    echo ">>> using cached $WORKDIR/nautobot.sql (delete it to re-download)"
fi
du -h nautobot.sql

if [[ "$ASSUME_YES" -ne 1 ]]; then
    echo
    echo "!!! This DESTROYS all data in $DB_CONTAINER database '$DB_NAME'."
    # Read from the terminal, not stdin: when the script is piped (curl ... | bash),
    # stdin is the script itself and a plain `read` would consume it and see EOF.
    if [[ -r /dev/tty ]]; then
        read -r -p "!!! Continue? [y/N] " reply < /dev/tty
    else
        echo "error: no terminal available to confirm." >&2
        echo "       Re-run with --yes, or download the script and run it directly." >&2
        exit 1
    fi
    [[ "$reply" == "y" || "$reply" == "Y" ]] || { echo "aborted"; exit 1; }
fi

echo ">>> copying dump into $DB_CONTAINER"
docker cp nautobot.sql "$DB_CONTAINER:/tmp/nautobot.sql"

echo ">>> dropping and recreating public schema"
docker exec "$DB_CONTAINER" psql -q -U "$DB_USER" -d "$DB_NAME" \
    -c "DROP SCHEMA public CASCADE; CREATE SCHEMA public;"

echo ">>> restoring (takes a minute)"
docker exec "$DB_CONTAINER" sh -c "psql -q -U $DB_USER -d $DB_NAME < /tmp/nautobot.sql" >/dev/null

echo ">>> applying migrations (bridges the dataset's version to yours)"
docker exec "$NAUTOBOT_CONTAINER" nautobot-server migrate 2>&1 | tail -3

echo ">>> removing the dump from the container"
docker exec "$DB_CONTAINER" rm -f /tmp/nautobot.sql

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

echo
echo ">>> done. Create a login (the dump's accounts are not yours):"
echo "      docker exec -it $NAUTOBOT_CONTAINER nautobot-server createsuperuser"
