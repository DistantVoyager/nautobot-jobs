"""Nautobot Job: Location Performance Diagnostic.

Run this from the Nautobot UI to diagnose 502 errors and slow page loads
related to locations. No direct database access required.

Installation:
    1. Place this file in your JOBS_ROOT directory, or in a Git repository
       that Nautobot is configured to pull Jobs from.
    2. Run `nautobot-server post_upgrade` or restart workers to pick up the Job.
    3. Navigate to Jobs > Location Performance Diagnostic and click Run.
"""

import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from django.db import connection

from django.http import QueryDict
from django.test.utils import CaptureQueriesContext

from nautobot.apps.jobs import IntegerVar, Job, TextVar, register_jobs
from nautobot.dcim.filters import DeviceFilterSet, LocationFilterSet
from nautobot.dcim.models import Device, Location, Rack


class LocationPerformanceDiagnostic(Job):
    """Diagnose location-related performance issues.

    Checks tree structure, query timing, missing indexes, and PostgreSQL
    settings that commonly cause 502 errors at scale.
    """

    name = "Location Performance Diagnostic"
    description = (
        "Analyzes the location tree for performance bottlenecks. "
        "Safe to run in production — all queries are read-only."
    )
    custom_filter_params = TextVar(
        description=(
            "Filter query strings to benchmark through the actual FilterSet code path, "
            "one per line. Prefix with 'devices:' for DeviceFilterSet, otherwise "
            "LocationFilterSet is used.\n"
            "Examples:\n"
            "  q=campus\n"
            "  parent=Building-A&status=active\n"
            "  devices:location=Building-A"
        ),
        required=False,
        default="",
        label="Custom Filter Parameters",
    )
    concurrent_queries = IntegerVar(
        description="Number of parallel tree CTE queries for concurrent load simulation (1-20).",
        required=False,
        default=5,
        label="Concurrent Query Count",
    )

    class Meta:
        has_sensitive_variables = False

    def run(self, custom_filter_params="", concurrent_queries=5):
        self._custom_filter_params = custom_filter_params
        self._concurrent_queries = concurrent_queries
        self.section_table_sizes()
        self.section_tree_structure()
        self.section_query_benchmarks()
        self.section_filterset_benchmarks()
        self.section_concurrent_load()
        self.section_index_check()
        self.section_connection_diagnostics()
        self.section_pg_settings()
        self.section_recommendations()

    # ------------------------------------------------------------------
    # 1. Table sizes
    # ------------------------------------------------------------------
    def section_table_sizes(self):
        self.logger.info("=" * 60)
        self.logger.info("1. TABLE SIZES")
        self.logger.info("=" * 60)

        location_count = Location.objects.count()
        device_count = Device.objects.count()
        rack_count = Rack.objects.count()

        self.logger.info("Locations:  %s", f"{location_count:,}")
        self.logger.info("Devices:    %s", f"{device_count:,}")
        self.logger.info("Racks:      %s", f"{rack_count:,}")

        if location_count > 100_000:
            self.logger.warning(
                "Location count (%s) is very high. "
                "Tree queries (recursive CTEs) will be expensive.",
                f"{location_count:,}",
            )

        # Get actual table size from pg catalog
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    pg_size_pretty(pg_total_relation_size('dcim_location')) AS total,
                    pg_size_pretty(pg_relation_size('dcim_location')) AS data,
                    pg_size_pretty(pg_indexes_size('dcim_location'::regclass)) AS indexes
                """
            )
            row = cursor.fetchone()
            self.logger.info(
                "dcim_location disk usage — total: %s, data: %s, indexes: %s",
                row[0], row[1], row[2],
            )

    # ------------------------------------------------------------------
    # 2. Tree structure
    # ------------------------------------------------------------------
    def section_tree_structure(self):
        self.logger.info("")
        self.logger.info("=" * 60)
        self.logger.info("2. LOCATION TREE STRUCTURE")
        self.logger.info("=" * 60)

        root_count = Location.objects.filter(parent__isnull=True).count()
        self.logger.info("Root locations (no parent): %s", f"{root_count:,}")

        # Depth distribution via raw SQL (ORM can't do this efficiently)
        with connection.cursor() as cursor:
            cursor.execute(
                """
                WITH RECURSIVE tree AS (
                    SELECT id, parent_id, 0 AS depth
                    FROM dcim_location WHERE parent_id IS NULL
                    UNION ALL
                    SELECT loc.id, loc.parent_id, tree.depth + 1
                    FROM dcim_location loc
                    JOIN tree ON loc.parent_id = tree.id
                )
                SELECT depth, count(*) AS cnt
                FROM tree
                GROUP BY depth
                ORDER BY depth
                """
            )
            rows = cursor.fetchall()

        self.logger.info("Tree depth distribution:")
        for depth, cnt in rows:
            bar = "#" * min(cnt // max(1, sum(r[1] for r in rows) // 40), 40)
            self.logger.info("  Depth %d: %s locations %s", depth, f"{cnt:>12,}", bar)

        max_depth = rows[-1][0] if rows else 0
        if max_depth > 10:
            self.logger.warning(
                "Tree depth of %d is deep. Each level adds cost to recursive CTEs.", max_depth
            )

        # Largest subtrees
        self.logger.info("")
        self.logger.info("Top 10 largest root subtrees:")
        with connection.cursor() as cursor:
            cursor.execute(
                """
                WITH RECURSIVE tree AS (
                    SELECT id, id AS root_id
                    FROM dcim_location WHERE parent_id IS NULL
                    UNION ALL
                    SELECT loc.id, tree.root_id
                    FROM dcim_location loc
                    JOIN tree ON loc.parent_id = tree.id
                )
                SELECT root.name, count(*) AS descendants, lt.name AS location_type
                FROM tree
                JOIN dcim_location root ON tree.root_id = root.id
                JOIN dcim_locationtype lt ON root.location_type_id = lt.id
                GROUP BY root.name, lt.name
                ORDER BY count(*) DESC
                LIMIT 10
                """
            )
            for name, desc_count, loc_type in cursor.fetchall():
                self.logger.info(
                    "  %-40s  type=%-15s  descendants=%s",
                    name, loc_type, f"{desc_count:,}",
                )

    # ------------------------------------------------------------------
    # 3. Query benchmarks
    # ------------------------------------------------------------------
    def section_query_benchmarks(self):
        self.logger.info("")
        self.logger.info("=" * 60)
        self.logger.info("3. QUERY BENCHMARKS")
        self.logger.info("=" * 60)

        # --- Benchmark A: Full tree CTE ---
        self.logger.info("")
        self.logger.info("A) Full tree recursive CTE (what TreeNodeMultipleChoiceFilter triggers):")

        with connection.cursor() as cursor:
            start = time.perf_counter()
            cursor.execute(
                """
                WITH RECURSIVE __tree AS (
                    SELECT id, parent_id, 0 AS tree_depth
                    FROM dcim_location WHERE parent_id IS NULL
                    UNION ALL
                    SELECT t.id, t.parent_id, __tree.tree_depth + 1
                    FROM dcim_location t
                    INNER JOIN __tree ON t.parent_id = __tree.id
                )
                SELECT count(*) FROM __tree
                """
            )
            elapsed = time.perf_counter() - start
            count = cursor.fetchone()[0]

        self.logger.info("  Traversed %s nodes in %.2f seconds", f"{count:,}", elapsed)
        if elapsed > 5:
            self.logger.warning(
                "  ** SLOW ** — This CTE runs on EVERY location-filtered page load!"
            )
        elif elapsed > 1:
            self.logger.warning("  Borderline slow — may cause intermittent timeouts under load.")
        else:
            self.logger.info("  Acceptable performance.")

        # --- Benchmark B: Subtree of the largest root ---
        self.logger.info("")
        self.logger.info("B) Subtree lookup for the largest root location:")

        # Find root with the most total descendants (not just direct children)
        with connection.cursor() as cursor:
            cursor.execute(
                """
                WITH RECURSIVE tree AS (
                    SELECT id, id AS root_id
                    FROM dcim_location WHERE parent_id IS NULL
                    UNION ALL
                    SELECT loc.id, tree.root_id
                    FROM dcim_location loc
                    JOIN tree ON loc.parent_id = tree.id
                )
                SELECT root_id, count(*) AS cnt
                FROM tree
                GROUP BY root_id
                ORDER BY cnt DESC
                LIMIT 1
                """
            )
            row = cursor.fetchone()
        largest_root = Location.objects.get(pk=row[0]) if row else None

        if largest_root:
            with connection.cursor() as cursor:
                start = time.perf_counter()
                cursor.execute(
                    """
                    WITH RECURSIVE __tree AS (
                        SELECT id FROM dcim_location WHERE id = %s
                        UNION ALL
                        SELECT t.id
                        FROM dcim_location t
                        INNER JOIN __tree ON t.parent_id = __tree.id
                    )
                    SELECT count(*) FROM __tree
                    """,
                    [str(largest_root.pk)],
                )
                elapsed = time.perf_counter() - start
                count = cursor.fetchone()[0]

            self.logger.info(
                '  Root "%s" — %s descendants in %.2f seconds',
                largest_root.name, f"{count:,}", elapsed,
            )
        else:
            self.logger.info("  No root locations found.")

        # --- Benchmark C: ILIKE search (what ?q= does) ---
        self.logger.info("")
        self.logger.info("C) ILIKE search simulation (?q= filter):")

        # Pick a realistic search term from existing data
        sample_loc = Location.objects.first()
        if sample_loc and sample_loc.name:
            search_term = sample_loc.name[:4]  # first 4 chars
        else:
            search_term = "DC"

        with connection.cursor() as cursor:
            start = time.perf_counter()
            cursor.execute(
                "SELECT count(*) FROM dcim_location WHERE name ILIKE %s",
                [f"%{search_term}%"],
            )
            elapsed = time.perf_counter() - start
            count = cursor.fetchone()[0]

        self.logger.info(
            '  Search for "%%%s%%" — %s matches in %.2f seconds',
            search_term, f"{count:,}", elapsed,
        )

        # Check if it used a seq scan
        with connection.cursor() as cursor:
            cursor.execute(
                "EXPLAIN (FORMAT TEXT) SELECT count(*) FROM dcim_location WHERE name ILIKE %s",
                [f"%{search_term}%"],
            )
            plan = "\n".join(row[0] for row in cursor.fetchall())

        if "Seq Scan" in plan:
            self.logger.warning(
                "  ** SEQUENTIAL SCAN ** — No trigram index! "
                "This scans every row on every search."
            )
        elif "Bitmap Index Scan" in plan or "Index Scan" in plan:
            self.logger.info("  Using an index — good.")
        self.logger.info("  Query plan: %s", plan.split("\n")[0].strip())

        # --- Benchmark D: Device count by location (StatsPanel) ---
        self.logger.info("")
        self.logger.info("D) Device count for a location (what StatsPanel computes):")

        if largest_root:
            start = time.perf_counter()
            device_count = Device.objects.filter(location=largest_root).count()
            elapsed = time.perf_counter() - start
            self.logger.info(
                '  Devices at "%s": %s in %.2f seconds (direct, no descendants)',
                largest_root.name, f"{device_count:,}", elapsed,
            )

            # Now with descendants (the expensive version)
            with connection.cursor() as cursor:
                start = time.perf_counter()
                cursor.execute(
                    """
                    WITH RECURSIVE __tree AS (
                        SELECT id FROM dcim_location WHERE id = %s
                        UNION ALL
                        SELECT t.id FROM dcim_location t
                        INNER JOIN __tree ON t.parent_id = __tree.id
                    )
                    SELECT count(*) FROM dcim_device
                    WHERE location_id IN (SELECT id FROM __tree)
                    """,
                    [str(largest_root.pk)],
                )
                elapsed = time.perf_counter() - start
                count = cursor.fetchone()[0]

            self.logger.info(
                '  Devices at "%s" + all descendants: %s in %.2f seconds',
                largest_root.name, f"{count:,}", elapsed,
            )
            if elapsed > 3:
                self.logger.warning(
                    "  ** SLOW ** — This runs every time someone views this location's detail page!"
                )

    # ------------------------------------------------------------------
    # 4. FilterSet benchmarks
    # ------------------------------------------------------------------
    def section_filterset_benchmarks(self):
        """Benchmark actual FilterSet queries with pagination simulation and EXPLAIN."""
        self.logger.info("")
        self.logger.info("=" * 60)
        self.logger.info("4. FILTERSET BENCHMARKS (PAGINATION SIMULATION)")
        self.logger.info("=" * 60)
        self.logger.info("")
        self.logger.info(
            "Every Nautobot list page runs TWO queries per page load:"
        )
        self.logger.info(
            "  1) COUNT(*) over the full filtered queryset (for the paginator)"
        )
        self.logger.info(
            "  2) SELECT with LIMIT/OFFSET (for the current page of results)"
        )
        self.logger.info(
            "Both go through the FilterSet, so tree filters (parent=, location=)"
        )
        self.logger.info(
            "trigger recursive CTEs on EVERY page load, EVERY page navigation."
        )

        # Build default test cases: (label, filterset_class, query_string)
        test_cases = []

        # Baseline: location list page with no filters
        test_cases.append(("Location list (no filters)", LocationFilterSet, ""))

        # Search filter (?q=)
        sample_loc = Location.objects.first()
        if sample_loc and sample_loc.name:
            search_term = sample_loc.name[:4]
            test_cases.append(
                (f"Location search: q={search_term}", LocationFilterSet, f"q={search_term}")
            )

        # Parent filter (TreeNodeMultipleChoiceFilter) with the largest root
        with connection.cursor() as cursor:
            cursor.execute(
                """
                WITH RECURSIVE tree AS (
                    SELECT id, id AS root_id
                    FROM dcim_location WHERE parent_id IS NULL
                    UNION ALL
                    SELECT loc.id, tree.root_id
                    FROM dcim_location loc
                    JOIN tree ON loc.parent_id = tree.id
                )
                SELECT root_id, count(*) AS cnt
                FROM tree
                GROUP BY root_id
                ORDER BY cnt DESC
                LIMIT 1
                """
            )
            row = cursor.fetchone()
        largest_root = Location.objects.get(pk=row[0]) if row else None
        if largest_root:
            test_cases.append((
                f"Location parent={largest_root.name}",
                LocationFilterSet,
                f"parent={largest_root.pk}",
            ))
            test_cases.append((
                f"Devices at location={largest_root.name}",
                DeviceFilterSet,
                f"location={largest_root.pk}",
            ))

        # Custom filter params from user input
        custom_params = self._custom_filter_params
        if custom_params:
            for line in custom_params.strip().splitlines():
                line = line.strip()
                if not line:
                    continue
                if line.lower().startswith("devices:"):
                    fs_class = DeviceFilterSet
                    query_str = line.split(":", 1)[1]
                    label = f"Custom (Device): {query_str}"
                elif line.lower().startswith("locations:"):
                    fs_class = LocationFilterSet
                    query_str = line.split(":", 1)[1]
                    label = f"Custom (Location): {query_str}"
                else:
                    fs_class = LocationFilterSet
                    query_str = line
                    label = f"Custom (Location): {line}"
                test_cases.append((label, fs_class, query_str))

        # Run benchmarks
        self.logger.info("")
        for label, fs_class, query_str in test_cases:
            base_qs = Location.objects.all() if fs_class is LocationFilterSet else Device.objects.all()
            qd = QueryDict(query_str)
            try:
                filterset = fs_class(data=qd, queryset=base_qs)
                qs = filterset.qs

                # Phase 1: COUNT(*) — the paginator query
                with CaptureQueriesContext(connection) as count_ctx:
                    start = time.perf_counter()
                    total = qs.count()
                    count_elapsed = time.perf_counter() - start

                # Phase 2: Page slice — first page of results (LIMIT 50)
                with CaptureQueriesContext(connection) as page_ctx:
                    start = time.perf_counter()
                    list(qs[:50])
                    page_elapsed = time.perf_counter() - start

                combined = count_elapsed + page_elapsed

                self.logger.info("  --- %s ---", label)
                self.logger.info(
                    "    Results: %-8s  Count: %.3fs (%d queries)  Page: %.3fs (%d queries)  Total: %.3fs",
                    f"{total:,}", count_elapsed, len(count_ctx),
                    page_elapsed, len(page_ctx), combined,
                )

                if combined > 5:
                    self.logger.warning("    ** SLOW ** — likely causes 502 under load!")
                elif combined > 2:
                    self.logger.warning("    Borderline — may timeout with concurrent users.")

                # Phase 3: EXPLAIN ANALYZE on the count query (usually the bottleneck)
                if count_ctx.captured_queries:
                    count_sql = count_ctx.captured_queries[-1]["sql"]
                    self.logger.info("    Count SQL: %s", count_sql[:200])
                    with connection.cursor() as cursor:
                        cursor.execute(
                            "EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) %s" % count_sql  # noqa: S608
                        )
                        plan_lines = [row[0] for row in cursor.fetchall()]

                    self.logger.info("    EXPLAIN ANALYZE (count query):")
                    for plan_line in plan_lines:
                        self.logger.info("      %s", plan_line)

                    # Flag specific bottleneck patterns
                    plan_text = "\n".join(plan_lines)
                    if "Seq Scan" in plan_text:
                        self.logger.warning(
                            "    ** Sequential scan detected — consider adding an index"
                        )
                    if "Sort Method: external" in plan_text:
                        self.logger.warning(
                            "    ** Sort spilled to disk — work_mem is too low for this query"
                        )
                    if "Rows Removed by Filter" in plan_text:
                        for plan_line in plan_lines:
                            if "Rows Removed by Filter" in plan_line:
                                self.logger.info("    ^ %s", plan_line.strip())

                self.logger.info("")

            except Exception as e:
                self.logger.error("  %s  ERROR: %s", label, str(e))

    # ------------------------------------------------------------------
    # 5. Concurrent load simulation
    # ------------------------------------------------------------------
    def section_concurrent_load(self):
        """Simulate concurrent tree CTE queries to test degradation under load."""
        self.logger.info("")
        self.logger.info("=" * 60)
        self.logger.info("5. CONCURRENT LOAD SIMULATION")
        self.logger.info("=" * 60)

        num_queries = min(max(int(self._concurrent_queries), 1), 20)

        self.logger.info("")
        self.logger.info(
            "Running %d parallel tree CTE queries (each opens its own DB connection)...",
            num_queries,
        )

        cte_sql = """
            WITH RECURSIVE __tree AS (
                SELECT id, parent_id, 0 AS tree_depth
                FROM dcim_location WHERE parent_id IS NULL
                UNION ALL
                SELECT t.id, t.parent_id, __tree.tree_depth + 1
                FROM dcim_location t
                INNER JOIN __tree ON t.parent_id = __tree.id
            )
            SELECT count(*) FROM __tree
        """

        def _run_cte():
            from django.db import connection as thread_conn

            try:
                with thread_conn.cursor() as cursor:
                    start = time.perf_counter()
                    cursor.execute(cte_sql)
                    cursor.fetchone()
                    return time.perf_counter() - start
            finally:
                thread_conn.close()

        wall_start = time.perf_counter()
        with ThreadPoolExecutor(max_workers=num_queries) as pool:
            futures = [pool.submit(_run_cte) for _ in range(num_queries)]
            times = []
            for future in as_completed(futures):
                try:
                    times.append(future.result())
                except Exception as e:
                    self.logger.error("  Query failed: %s", str(e))
        wall_elapsed = time.perf_counter() - wall_start

        if times:
            avg_time = sum(times) / len(times)
            max_time = max(times)
            min_time = min(times)

            self.logger.info("  Completed:    %d / %d queries", len(times), num_queries)
            self.logger.info("  Wall clock:   %.2f seconds", wall_elapsed)
            self.logger.info(
                "  Per-query:    min=%.2fs  avg=%.2fs  max=%.2fs",
                min_time, avg_time, max_time,
            )
            self.logger.info(
                "  Throughput:   %.1f queries/sec",
                len(times) / wall_elapsed if wall_elapsed > 0 else 0,
            )

            if max_time > 10:
                self.logger.warning(
                    "  ** CRITICAL ** — Slowest query took %.1fs. "
                    "Under real traffic with %d+ concurrent users, 502s are expected.",
                    max_time, num_queries,
                )
            elif max_time > 5:
                self.logger.warning(
                    "  ** SLOW ** — Queries degrade under concurrency. "
                    "This explains intermittent 502s during peak traffic.",
                )
            elif avg_time > 2:
                self.logger.warning(
                    "  Elevated average (%.1fs) — may cause issues at higher concurrency.",
                    avg_time,
                )
            else:
                self.logger.info("  Performance holds under simulated concurrent load.")

    # ------------------------------------------------------------------
    # 6. Index check
    # ------------------------------------------------------------------
    def section_index_check(self):
        self.logger.info("")
        self.logger.info("=" * 60)
        self.logger.info("6. INDEX CHECK")
        self.logger.info("=" * 60)

        with connection.cursor() as cursor:
            # Trigram extension
            cursor.execute("SELECT 1 FROM pg_extension WHERE extname = 'pg_trgm'")
            has_trgm = cursor.fetchone() is not None

            # Trigram indexes
            cursor.execute(
                "SELECT indexname FROM pg_indexes "
                "WHERE tablename = 'dcim_location' AND indexdef LIKE '%%trgm%%'"
            )
            location_trgm = cursor.fetchone()

            cursor.execute(
                "SELECT indexname FROM pg_indexes "
                "WHERE tablename = 'dcim_device' AND indexdef LIKE '%%trgm%%'"
            )
            device_trgm = cursor.fetchone()

            # Parent index
            cursor.execute(
                "SELECT indexname, indexdef FROM pg_indexes "
                "WHERE tablename = 'dcim_location' AND indexdef LIKE '%%parent_id%%'"
            )
            parent_indexes = cursor.fetchall()

        # Report
        if has_trgm:
            self.logger.info("pg_trgm extension: INSTALLED")
        else:
            self.logger.warning("pg_trgm extension: ** MISSING **")

        if location_trgm:
            self.logger.info("Trigram index on dcim_location.name: EXISTS (%s)", location_trgm[0])
        else:
            self.logger.warning(
                "Trigram index on dcim_location.name: ** MISSING ** — "
                "all ?q= searches do full table scans!"
            )

        if device_trgm:
            self.logger.info("Trigram index on dcim_device.name: EXISTS (%s)", device_trgm[0])
        else:
            self.logger.warning(
                "Trigram index on dcim_device.name: ** MISSING ** — "
                "device searches do full table scans"
            )

        self.logger.info("")
        self.logger.info("Parent ID indexes on dcim_location:")
        if parent_indexes:
            for name, defn in parent_indexes:
                self.logger.info("  %s", name)
        else:
            self.logger.warning("  ** NONE ** — tree CTE joins on parent_id with no index!")

    # ------------------------------------------------------------------
    # 7. Connection & query diagnostics
    # ------------------------------------------------------------------
    def section_connection_diagnostics(self):
        """Check pg_stat_activity for connection and query health."""
        self.logger.info("")
        self.logger.info("=" * 60)
        self.logger.info("7. CONNECTION & QUERY DIAGNOSTICS")
        self.logger.info("=" * 60)

        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT state, count(*)
                FROM pg_stat_activity
                WHERE datname = current_database()
                GROUP BY state
                ORDER BY count(*) DESC
                """
            )
            rows = cursor.fetchall()

        self.logger.info("")
        self.logger.info("Connection states (current database):")
        total_conns = 0
        for state, count in rows:
            state_label = state or "NULL (background worker)"
            self.logger.info("  %-25s %d", state_label, count)
            total_conns += count
        self.logger.info("  %-25s %d", "TOTAL", total_conns)

        with connection.cursor() as cursor:
            cursor.execute("SELECT setting FROM pg_settings WHERE name = 'max_connections'")
            max_conns = int(cursor.fetchone()[0])

        usage_pct = (total_conns / max_conns * 100) if max_conns > 0 else 0
        self.logger.info(
            "  Usage: %d / %d (%.0f%%)", total_conns, max_conns, usage_pct
        )
        if usage_pct > 80:
            self.logger.warning(
                "  ** HIGH ** — %.0f%% of connections in use. "
                "New requests may queue or be rejected.",
                usage_pct,
            )

        # Long-running queries
        self.logger.info("")
        self.logger.info("Active queries running > 5 seconds:")
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    pid,
                    now() - query_start AS duration,
                    state,
                    left(query, 120) AS query_preview
                FROM pg_stat_activity
                WHERE datname = current_database()
                  AND state = 'active'
                  AND now() - query_start > interval '5 seconds'
                  AND pid != pg_backend_pid()
                ORDER BY query_start
                """
            )
            long_queries = cursor.fetchall()

        if long_queries:
            for pid, duration, state, query_preview in long_queries:
                self.logger.warning(
                    "  PID %s  running %s: %s...", pid, duration, query_preview
                )
            self.logger.warning(
                "  Found %d long-running queries — these hold connections and "
                "can cause 502s by exhausting the worker/connection pool.",
                len(long_queries),
            )
        else:
            self.logger.info("  None found (good).")

        # Blocked/waiting queries
        self.logger.info("")
        self.logger.info("Queries waiting on locks:")
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT count(*)
                FROM pg_stat_activity
                WHERE datname = current_database()
                  AND wait_event_type = 'Lock'
                """
            )
            blocked_count = cursor.fetchone()[0]

        if blocked_count > 0:
            self.logger.warning("  %d queries currently waiting on locks.", blocked_count)
        else:
            self.logger.info("  None found (good).")

    # ------------------------------------------------------------------
    # 8. PostgreSQL settings
    # ------------------------------------------------------------------
    def section_pg_settings(self):
        self.logger.info("")
        self.logger.info("=" * 60)
        self.logger.info("8. POSTGRESQL SETTINGS")
        self.logger.info("=" * 60)

        checks = {
            "work_mem": {
                "warn_below_kb": 65536,  # 64MB
                "message": "Low work_mem forces CTE intermediate results to disk. Recommend 256MB.",
            },
            "statement_timeout": {
                "warn_value": "0",
                "message": "No statement timeout — queries run until the proxy 502s. Set to 15s.",
            },
            "shared_buffers": {
                "warn_below_kb": 524288,  # 512MB
                "message": "Low shared_buffers. Recommend 25%% of system RAM.",
            },
        }

        with connection.cursor() as cursor:
            for setting_name, check in checks.items():
                cursor.execute(
                    "SELECT setting, unit FROM pg_settings WHERE name = %s",
                    [setting_name],
                )
                row = cursor.fetchone()
                if row:
                    value, unit = row
                    display = f"{value} {unit}" if unit else value

                    is_bad = False
                    if "warn_below_kb" in check:
                        try:
                            is_bad = int(value) < check["warn_below_kb"]
                        except ValueError:
                            pass
                    if "warn_value" in check:
                        is_bad = value == check["warn_value"]

                    if is_bad:
                        self.logger.warning("  %s = %s — %s", setting_name, display, check["message"])
                    else:
                        self.logger.info("  %s = %s", setting_name, display)

    # ------------------------------------------------------------------
    # 9. Recommendations
    # ------------------------------------------------------------------
    def section_recommendations(self):
        self.logger.info("")
        self.logger.info("=" * 60)
        self.logger.info("9. RECOMMENDATIONS")
        self.logger.info("=" * 60)
        self.logger.info("")
        self.logger.info("--- Missing indexes ---")
        self.logger.info("If trigram indexes are missing, a database administrator should run:")
        self.logger.info("  CREATE EXTENSION IF NOT EXISTS pg_trgm;")
        self.logger.info(
            "  CREATE INDEX CONCURRENTLY idx_dcim_location_name_trgm"
        )
        self.logger.info("      ON dcim_location USING gin (name gin_trgm_ops);")
        self.logger.info(
            "  CREATE INDEX CONCURRENTLY idx_dcim_device_name_trgm"
        )
        self.logger.info("      ON dcim_device USING gin (name gin_trgm_ops);")
        self.logger.info("")
        self.logger.info("--- PostgreSQL tuning ---")
        self.logger.info("If work_mem is low:")
        self.logger.info("  ALTER DATABASE nautobot SET work_mem = '256MB';")
        self.logger.info("")
        self.logger.info("If statement_timeout is unlimited:")
        self.logger.info("  ALTER ROLE nautobot SET statement_timeout = '15s';")
        self.logger.info("")
        self.logger.info("--- Concurrent load / connection issues ---")
        self.logger.info("If queries degrade under concurrent load:")
        self.logger.info("  - Ensure connection pooling (PgBouncer) is configured")
        self.logger.info("  - Reduce Nautobot worker count if DB CPU is saturated")
        self.logger.info("  - Consider increasing shared_buffers (25%% of system RAM)")
        self.logger.info("")
        self.logger.info("If connection usage is high:")
        self.logger.info("  - Check for connection leaks (idle connections that never close)")
        self.logger.info("  - Lower CONN_MAX_AGE in Django settings or configure PgBouncer")
        self.logger.info("")
        self.logger.info("--- Nautobot Cloud ---")
        self.logger.info("For Nautobot Cloud instances, share this job's full output with")
        self.logger.info("the Cloud support team — they manage PostgreSQL settings and can")
        self.logger.info("apply index and tuning changes on your behalf.")


register_jobs(LocationPerformanceDiagnostic)
