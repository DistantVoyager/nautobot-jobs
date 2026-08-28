# Loading demo data into a local dev Nautobot

For a local dev instance **without design-builder**, use the NDG **Postgres SQL**
artifact (`-P-`) and then run `migrate`. Tested working; see below.

The script downloads the dataset, restores it, and migrates. It autodetects the
container names, and prompts before destroying anything:

```bash
./scripts/load-ndg-dataset.sh
```

```bash
./scripts/load-ndg-dataset.sh --version 3.1.4 --size S
```

See what datasets exist:

```bash
./scripts/load-ndg-dataset.sh --list
```

Autodetection errors out rather than guessing when several Nautobot or database
containers are up — pass `-n`/`-d` explicitly then, since picking the wrong container
destroys the wrong database:

```bash
./scripts/load-ndg-dataset.sh -n my-nautobot-1 -d my-db-1
```

`--help` lists the rest (`--db-user`, `--db-name`, `--yes` to skip the prompt).
Downloads are cached per version+size under `~/.cache/ndg-data/`; note the `L` datasets
are ~290 MB uncompressed.

Result on a Nautobot 3.2.0 dev stack (PostgreSQL 17 restoring a PG 15 dump):

| | |
|---|---|
| devices | 328 |
| interfaces | 3,062 |
| locations | 38 |
| racks | 43 |
| cables | 380 |
| prefixes | 192 |
| IP addresses | 861 |
| VLANs | 38 |
| circuits | 7 |
| virtual machines | 495 |
| tenants | 10 |

After restore, `nautobot-server migrate` applied the 22 pending migrations that bridge
the dataset's 3.1.4 schema to the 3.2.0 codebase, leaving 0 unapplied and all data
intact. The app served HTTP 200 afterwards.

## Steps, if you'd rather do it by hand

```bash
gh release download branch_feature/no-jira/add-setup-job-persona_gizmo_nautobot3 \
  --repo networktocode-llc/nautobot-data-generation \
  --pattern "nautobot_3.1.4-P-S-latest.tar.gz"
```

```bash
tar -xzf nautobot_3.1.4-P-S-latest.tar.gz
```

```bash
docker cp nautobot.sql <db-container>:/tmp/nautobot.sql
```

```bash
docker exec <db-container> psql -q -U nautobot -d nautobot -c "DROP SCHEMA public CASCADE; CREATE SCHEMA public;"
```

```bash
docker exec <db-container> sh -c "psql -q -U nautobot -d nautobot < /tmp/nautobot.sql"
```

```bash
docker exec <nautobot-container> nautobot-server migrate
```

```bash
docker exec -it <nautobot-container> nautobot-server createsuperuser
```

The dump carries its own accounts, so create your own superuser to log in.

### Artifact naming

`nautobot_{VERSION}-{FORMAT}-{SIZE}-latest.tar.gz`

- **FORMAT**: `P` = Postgres SQL (use this), `T` = Postgres tar for `pg_restore`,
  `M` = MySQL, `J`/`I` = dumpdata JSON (**broken**, see below)
- **SIZE**: `S` = small (the ~330-device set above), `L` = large (80 branches, ~290 MB)
- **VERSION**: `3.0.0`, `3.0.6`, `3.1.4`, `develop`, `next`. Pick the closest at or
  below your instance's version — migrations only run forward.

Not every version+size combination is built, and they are spread across NDG's
per-branch release tags (`3.1.4-P-L` exists only on the main tag; `3.0.6` has no `S`
build at all). The script searches both known tags, so `--list` is the authority on
what you can actually get.

## Why not the JSON (`-J-`) artifacts

NDG's docs describe loading them with `nautobot-server loaddata`, but the artifacts are
not valid fixtures. Only **1 of 181 models** is serialized with an explicit `pk`, while
dozens of fields hold bare UUID references — `dcim.cable.termination_a_id`,
`extras.taggeditem.object_id`, `ipam.vlanlocationassignment.vlan`,
`vpn.vpntunnel.endpoint_a`, `extras.relationshipassociation.source_id` and more. Those
UUIDs have nothing to resolve against, so `loaddata` fails.

Verified by loading an **unfiltered** artifact into a clean 3.2.0 instance exactly as
documented; it fails immediately:

```
django.db.utils.IntegrityError: Problem installing fixture '/tmp/orig.json':
Could not load extras.Relationship(...): null value in column "source_type_id"
of relation "extras_relationship" violates not-null constraint
```

Working around it means dropping app objects, content-type references to uninstalled
apps, job links, transient health-check rows, identity records, and topologically
sorting self-referencing models — and even then you hit the unresolvable UUIDs, losing
cables, tags and relationship associations. Not worth it when `-P-` restores perfectly.

## Alternative: no dataset at all

Nautobot ships a generator, and `factory-boy`/`faker` **are** present in a dev install
(they're absent only from production images, which is what rules this out for Nautobot
Cloud):

```bash
docker exec <nautobot-container> nautobot-server generate_test_data --flush 100
```

Less realistic than NDG's data (no coherent site topology or cabling) but instant and
version-proof. Good enough when you just need populated tables.

## Caveats

- The restore **destroys** the target database. Dev only.
- Restoring a dump built on a *newer* Nautobot than yours will not work — migrations
  only run forward. Match or go below your version.
- The dump includes NDG's job records, which get deregistered on first `migrate` since
  those job files don't exist locally. Harmless.
