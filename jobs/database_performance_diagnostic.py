"""Nautobot Job: Database Performance Diagnostic.

Run this from the Nautobot UI to gather facts about database performance,
query patterns, indexes, and PostgreSQL configuration. Includes location-tree-
specific diagnostics because tree queries are a known hot path. No direct
database access required.

Installation:
    1. Place this file in your JOBS_ROOT directory, or in a Git repository
       that Nautobot is configured to pull Jobs from.
    2. Run `nautobot-server post_upgrade` or restart workers to pick up the Job.
    3. Navigate to Jobs > Database Performance Diagnostic and click Run.
"""

import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from django.db import connection

from django.http import QueryDict
from django.test.utils import CaptureQueriesContext

from nautobot.apps.jobs import IntegerVar, Job, TextVar, register_jobs
from nautobot.dcim.filters import DeviceFilterSet, LocationFilterSet
from nautobot.dcim.models import Device, Location


class DatabasePerformanceDiagnostic(Job):
    """Diagnose database performance issues in a Nautobot instance.

    Gathers table sizes, index coverage, query plans, connection state,
    cumulative PostgreSQL statistics, and configuration values. Includes
    location-tree-specific checks (sections 2, 3, 5) because tree queries
    are a known hot path for 502 errors at scale.
    """

    name = "Database Performance Diagnostic"
    description = (
        "Collects diagnostic facts about Nautobot's database: table sizes, indexes, "
        "query patterns (including location tree queries), PostgreSQL configuration, "
        "and cumulative query statistics. Produces a downloadable report intended "
        "for review. Safe to run in production — all queries are read-only."
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
    explain_query = TextVar(
        description=(
            "Optional SQL to EXPLAIN (ANALYZE, BUFFERS). Must start with SELECT or WITH "
            "and contain no DML. Replace pg_stat_statements placeholders ($1, $2, ...) "
            "with realistic values before pasting."
        ),
        required=False,
        default="",
        label="Ad-hoc EXPLAIN Query",
    )

    class Meta:
        has_sensitive_variables = False

    def run(self, custom_filter_params="", concurrent_queries=5, explain_query=""):
        self._custom_filter_params = custom_filter_params
        self._concurrent_queries = concurrent_queries
        self._explain_query = explain_query
        self._lines = []
        self._real_logger = self.logger

        sections = [
            ("Table sizes", self.section_table_sizes),
            ("Tree structure", self.section_tree_structure),
            ("Raw query benchmarks", self.section_query_benchmarks),
            ("FilterSet benchmarks", self.section_filterset_benchmarks),
            ("Concurrent load simulation", self.section_concurrent_load),
            ("Index check", self.section_index_check),
            ("Connection diagnostics", self.section_connection_diagnostics),
            ("Database statistics", self.section_database_stats),
            ("Ad-hoc EXPLAIN", self.section_ad_hoc_explain),
            ("PostgreSQL settings", self.section_pg_settings),
        ]
        for label, section in sections:
            self._real_logger.info("Running: %s", label)
            section()

        # Write full report to downloadable file
        content = "\n".join(self._lines)
        filename = "database_performance_report.txt"
        try:
            self.create_file(filename, content)
            self._real_logger.info(
                "Full report written to downloadable file: %s (%d lines)",
                filename, len(self._lines),
            )
        except Exception as exc:
            self._real_logger.warning(
                "Could not create file (%s) — dumping report to log instead.", exc
            )
            for line in self._lines:
                self._real_logger.info(line)

    # ------------------------------------------------------------------
    # Output helpers — write to buffer; only warnings/errors also go to log
    # ------------------------------------------------------------------
    def _info(self, message, *args):
        self._lines.append(message % args if args else message)

    def _warning(self, message, *args):
        formatted = message % args if args else message
        self._lines.append("[WARNING] " + formatted)
        self._real_logger.warning(message, *args)

    def _error(self, message, *args):
        formatted = message % args if args else message
        self._lines.append("[ERROR] " + formatted)
        self._real_logger.error(message, *args)

    # ------------------------------------------------------------------
    # 1. Table sizes
    # ------------------------------------------------------------------
    def section_table_sizes(self):
        self._info("=" * 60)
        self._info("1. TABLE SIZES")
        self._info("=" * 60)

        # Top 20 tables by total disk size (database-wide)
        self._info("Top 20 tables by total disk size:")
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    c.relname AS table_name,
                    pg_size_pretty(pg_total_relation_size(c.oid)) AS total,
                    pg_size_pretty(pg_relation_size(c.oid)) AS data,
                    pg_size_pretty(pg_indexes_size(c.oid)) AS indexes,
                    c.reltuples::bigint AS approx_rows
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE c.relkind = 'r'
                  AND n.nspname = 'public'
                ORDER BY pg_total_relation_size(c.oid) DESC
                LIMIT 20
                """
            )
            rows = cursor.fetchall()

        self._info(
            "  %-45s %12s %12s %12s %14s",
            "Table", "Total", "Data", "Indexes", "Approx rows",
        )
        for table_name, total, data, indexes, approx_rows in rows:
            self._info(
                "  %-45s %12s %12s %12s %14s",
                table_name, total, data, indexes, f"{approx_rows:,}",
            )

    # ------------------------------------------------------------------
    # 2. Tree structure
    # ------------------------------------------------------------------
    def section_tree_structure(self):
        self._info("")
        self._info("=" * 60)
        self._info("2. LOCATION TREE STRUCTURE")
        self._info("=" * 60)

        root_count = Location.objects.filter(parent__isnull=True).count()
        self._info("Root locations (no parent): %s", f"{root_count:,}")

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

        self._info("Tree depth distribution:")
        for depth, cnt in rows:
            bar = "#" * min(cnt // max(1, sum(r[1] for r in rows) // 40), 40)
            self._info("  Depth %d: %s locations %s", depth, f"{cnt:>12,}", bar)

        max_depth = rows[-1][0] if rows else 0
        if max_depth > 10:
            self._warning(
                "Tree depth of %d is deep. Each level adds cost to recursive CTEs.", max_depth
            )

        # Largest subtrees
        self._info("")
        self._info("Top 10 largest root subtrees:")
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
                SELECT root.id, root.name, count(*) AS descendants, lt.name AS location_type
                FROM tree
                JOIN dcim_location root ON tree.root_id = root.id
                JOIN dcim_locationtype lt ON root.location_type_id = lt.id
                GROUP BY root.id, root.name, lt.name
                ORDER BY count(*) DESC
                LIMIT 10
                """
            )
            largest_root_rows = cursor.fetchall()
        for _root_id, name, desc_count, loc_type in largest_root_rows:
            self._info(
                "  %-40s  type=%-15s  descendants=%s",
                name, loc_type, f"{desc_count:,}",
            )
        # Stash the top one so sections 3 and 4 can reuse it
        self._largest_root = (
            Location.objects.get(pk=largest_root_rows[0][0])
            if largest_root_rows else None
        )

        # Top 5 parents by direct-child count — impacts dropdown/list rendering
        self._info("")
        self._info("Top 5 parents by direct-child count:")
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT parent.name, parent_lt.name AS parent_type, child_count
                FROM (
                    SELECT parent_id, count(*) AS child_count
                    FROM dcim_location
                    WHERE parent_id IS NOT NULL
                    GROUP BY parent_id
                ) counts
                JOIN dcim_location parent ON counts.parent_id = parent.id
                JOIN dcim_locationtype parent_lt ON parent.location_type_id = parent_lt.id
                ORDER BY child_count DESC
                LIMIT 5
                """
            )
            widest_rows = cursor.fetchall()
        for p_name, p_type, c_count in widest_rows:
            self._info(
                "  %-40s  type=%-15s  direct_children=%s",
                p_name, p_type, f"{c_count:,}",
            )
        if widest_rows and widest_rows[0][2] > 500:
            self._warning(
                "Largest parent has %d direct children — parent-selection widgets "
                "may render slowly.", widest_rows[0][2],
            )

    # ------------------------------------------------------------------
    # 3. Location query benchmarks
    # ------------------------------------------------------------------
    def section_query_benchmarks(self):
        self._info("")
        self._info("=" * 60)
        self._info("3. LOCATION QUERY BENCHMARKS")
        self._info("=" * 60)

        # --- Benchmark A: Full tree CTE ---
        self._info("")
        self._info("A) Full tree recursive CTE (what TreeNodeMultipleChoiceFilter triggers):")

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

        self._info("  Traversed %s nodes in %.2f seconds", f"{count:,}", elapsed)
        if elapsed > 5:
            self._warning("  Execution time exceeds 5s threshold.")
        elif elapsed > 1:
            self._warning("  Execution time exceeds 1s threshold.")

        # --- Benchmark B: Subtree of the largest root (reused from section 2) ---
        self._info("")
        self._info("B) Subtree lookup for the largest root location:")

        largest_root = self._largest_root
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

            self._info(
                '  Root "%s" — %s descendants in %.2f seconds',
                largest_root.name, f"{count:,}", elapsed,
            )
        else:
            self._info("  No root locations found.")

        # --- Benchmark C: ILIKE search (what ?q= does) ---
        self._info("")
        self._info("C) ILIKE search simulation (?q= filter):")

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

        self._info(
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
            self._warning("  Plan uses Sequential Scan.")
        self._info("  Query plan: %s", plan.split("\n")[0].strip())

        # --- Benchmark D: Device count by location (StatsPanel) ---
        self._info("")
        self._info("D) Device count for a location (what StatsPanel computes):")

        if largest_root:
            start = time.perf_counter()
            device_count = Device.objects.filter(location=largest_root).count()
            elapsed = time.perf_counter() - start
            self._info(
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

            self._info(
                '  Devices at "%s" + all descendants: %s in %.2f seconds',
                largest_root.name, f"{count:,}", elapsed,
            )
            if elapsed > 3:
                self._warning("  Execution time exceeds 3s threshold.")

    # ------------------------------------------------------------------
    # 4. FilterSet benchmarks
    # ------------------------------------------------------------------
    def section_filterset_benchmarks(self):
        """Benchmark actual FilterSet queries with pagination simulation and EXPLAIN."""
        self._info("")
        self._info("=" * 60)
        self._info("4. FILTERSET BENCHMARKS (PAGINATION SIMULATION)")
        self._info("=" * 60)
        self._info("")
        self._info(
            "Every Nautobot list page runs TWO queries per page load:"
        )
        self._info(
            "  1) COUNT(*) over the full filtered queryset (for the paginator)"
        )
        self._info(
            "  2) SELECT with LIMIT/OFFSET (for the current page of results)"
        )
        self._info(
            "Both go through the FilterSet, so tree filters (parent=, location=)"
        )
        self._info(
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

        # Parent filter (TreeNodeMultipleChoiceFilter) — reuse largest root from section 2
        largest_root = self._largest_root
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
        self._info("")
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

                self._info("  --- %s ---", label)
                self._info(
                    "    Results: %-8s  Count: %.3fs (%d queries)  Page: %.3fs (%d queries)  Total: %.3fs",
                    f"{total:,}", count_elapsed, len(count_ctx),
                    page_elapsed, len(page_ctx), combined,
                )

                if combined > 5:
                    self._warning("    Combined time exceeds 5s threshold.")
                elif combined > 2:
                    self._warning("    Combined time exceeds 2s threshold.")

                # Phase 3: EXPLAIN ANALYZE on the count query (usually the bottleneck)
                if count_ctx.captured_queries:
                    count_sql = count_ctx.captured_queries[-1]["sql"]
                    self._info("    Count SQL: %s", count_sql[:200])
                    with connection.cursor() as cursor:
                        cursor.execute(
                            "EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) %s" % count_sql  # noqa: S608
                        )
                        plan_lines = [row[0] for row in cursor.fetchall()]

                    self._info("    EXPLAIN ANALYZE (count query):")
                    for plan_line in plan_lines:
                        self._info("      %s", plan_line)

                    # Flag specific bottleneck patterns
                    plan_text = "\n".join(plan_lines)
                    if "Seq Scan" in plan_text:
                        self._warning("    Plan contains Sequential Scan.")
                    if "Sort Method: external" in plan_text:
                        self._warning("    Plan contains external (on-disk) sort.")
                    if "Rows Removed by Filter" in plan_text:
                        for plan_line in plan_lines:
                            if "Rows Removed by Filter" in plan_line:
                                self._info("    ^ %s", plan_line.strip())

                self._info("")

            except Exception as e:
                self._error("  %s  ERROR: %s", label, str(e))

    # ------------------------------------------------------------------
    # 5. Concurrent load simulation
    # ------------------------------------------------------------------
    def section_concurrent_load(self):
        """Simulate concurrent tree CTE queries to test degradation under load."""
        self._info("")
        self._info("=" * 60)
        self._info("5. CONCURRENT LOAD SIMULATION")
        self._info("=" * 60)

        num_queries = min(max(int(self._concurrent_queries), 1), 20)

        self._info("")
        self._info(
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
                    self._error("  Query failed: %s", str(e))
        wall_elapsed = time.perf_counter() - wall_start

        if times:
            avg_time = sum(times) / len(times)
            max_time = max(times)
            min_time = min(times)

            self._info("  Completed:    %d / %d queries", len(times), num_queries)
            self._info("  Wall clock:   %.2f seconds", wall_elapsed)
            self._info(
                "  Per-query:    min=%.2fs  avg=%.2fs  max=%.2fs",
                min_time, avg_time, max_time,
            )
            self._info(
                "  Throughput:   %.1f queries/sec",
                len(times) / wall_elapsed if wall_elapsed > 0 else 0,
            )

            if max_time > 10:
                self._warning("  Max query time %.1fs exceeds 10s threshold.", max_time)
            elif max_time > 5:
                self._warning("  Max query time %.1fs exceeds 5s threshold.", max_time)
            elif avg_time > 2:
                self._warning("  Average query time %.1fs exceeds 2s threshold.", avg_time)

    # ------------------------------------------------------------------
    # 6. Index check
    # ------------------------------------------------------------------
    def section_index_check(self):
        self._info("")
        self._info("=" * 60)
        self._info("6. INDEX CHECK")
        self._info("=" * 60)

        # --- pg_trgm extension ---
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1 FROM pg_extension WHERE extname = 'pg_trgm'")
            has_trgm = cursor.fetchone() is not None
        self._info("")
        if has_trgm:
            self._info("pg_trgm extension: INSTALLED")
        else:
            self._warning("pg_trgm extension: MISSING (all trigram checks below will show MISSING)")

        # --- Trigram indexes on 'name' columns (>1000 rows only, sorted desc) ---
        self._info("")
        self._info("--- Trigram 'name' index coverage (tables >1,000 rows) ---")
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT c.table_name,
                       pc.reltuples::bigint AS approx_rows
                FROM information_schema.columns c
                JOIN pg_class pc ON pc.relname = c.table_name AND pc.relkind = 'r'
                JOIN pg_namespace pn ON pn.oid = pc.relnamespace AND pn.nspname = 'public'
                WHERE c.table_schema = 'public'
                  AND c.column_name = 'name'
                  AND c.data_type IN ('character varying', 'text', 'character')
                  AND pc.reltuples > 1000
                ORDER BY pc.reltuples DESC
                """
            )
            name_tables = cursor.fetchall()

        if not name_tables:
            self._info("  (No tables with >1,000 rows and a 'name' column)")
        else:
            self._info("  %-50s %12s  %s", "Table", "Rows", "Trigram index")
            with connection.cursor() as cursor:
                for table_name, approx_rows in name_tables:
                    cursor.execute(
                        "SELECT indexname FROM pg_indexes "
                        "WHERE tablename = %s AND indexdef LIKE %s",
                        [table_name, "%trgm%"],
                    )
                    trgm_idx = cursor.fetchone()
                    status = trgm_idx[0] if trgm_idx else "MISSING"
                    self._info(
                        "  %-50s %12s  %s", table_name, f"{approx_rows:,}", status,
                    )
                    if not trgm_idx:
                        self._warning(
                            "Table '%s' (~%s rows) has no trigram index on 'name'.",
                            table_name, f"{approx_rows:,}",
                        )

        # --- JSONB GIN indexes on _custom_field_data columns (>1000 rows) ---
        self._info("")
        self._info("--- JSONB GIN index coverage on '_custom_field_data' (tables >1,000 rows) ---")
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT c.table_name, pc.reltuples::bigint AS approx_rows
                FROM information_schema.columns c
                JOIN pg_class pc ON pc.relname = c.table_name AND pc.relkind = 'r'
                JOIN pg_namespace pn ON pn.oid = pc.relnamespace AND pn.nspname = 'public'
                WHERE c.table_schema = 'public'
                  AND c.column_name = '_custom_field_data'
                  AND pc.reltuples > 1000
                ORDER BY pc.reltuples DESC
                """
            )
            cfd_tables = cursor.fetchall()

        if not cfd_tables:
            self._info("  (No tables with >1,000 rows and a '_custom_field_data' column)")
        else:
            self._info("  %-50s %12s  %s", "Table", "Rows", "GIN index")
            with connection.cursor() as cursor:
                for table_name, approx_rows in cfd_tables:
                    cursor.execute(
                        "SELECT indexname FROM pg_indexes "
                        "WHERE tablename = %s "
                        "AND indexdef ILIKE %s "
                        "AND indexdef ILIKE %s",
                        [table_name, "%_custom_field_data%", "%gin%"],
                    )
                    gin_idx = cursor.fetchone()
                    status = gin_idx[0] if gin_idx else "MISSING"
                    self._info(
                        "  %-50s %12s  %s", table_name, f"{approx_rows:,}", status,
                    )
                    if not gin_idx:
                        self._warning(
                            "Table '%s' (~%s rows) has no GIN index on '_custom_field_data'.",
                            table_name, f"{approx_rows:,}",
                        )

        # --- GiST/GIN indexes on inet/cidr columns (network containment queries) ---
        self._info("")
        self._info("--- Network-type (inet/cidr) column index coverage ---")
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT c.table_name, c.column_name, c.data_type,
                       COALESCE(pc.reltuples::bigint, 0) AS approx_rows
                FROM information_schema.columns c
                LEFT JOIN pg_class pc ON pc.relname = c.table_name AND pc.relkind = 'r'
                LEFT JOIN pg_namespace pn ON pn.oid = pc.relnamespace
                WHERE c.table_schema = 'public'
                  AND c.data_type IN ('inet', 'cidr')
                  AND (pn.nspname = 'public' OR pn.nspname IS NULL)
                ORDER BY approx_rows DESC, c.table_name, c.column_name
                """
            )
            network_cols = cursor.fetchall()

        if not network_cols:
            self._info("  (No inet/cidr columns found)")
        else:
            self._info(
                "  %-40s %-25s %-8s %10s  %s",
                "Table", "Column", "Type", "Rows", "Index (type)",
            )
            with connection.cursor() as cursor:
                for table_name, column_name, data_type, approx_rows in network_cols:
                    cursor.execute(
                        """
                        SELECT i.relname, am.amname
                        FROM pg_index idx
                        JOIN pg_class c ON c.oid = idx.indrelid
                        JOIN pg_class i ON i.oid = idx.indexrelid
                        JOIN pg_am am ON am.oid = i.relam
                        JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(idx.indkey)
                        WHERE c.relname = %s AND a.attname = %s
                        """,
                        [table_name, column_name],
                    )
                    idx_rows = cursor.fetchall()
                    if idx_rows:
                        descriptions = ", ".join(f"{name} ({amname})" for name, amname in idx_rows)
                    else:
                        descriptions = "MISSING"
                    self._info(
                        "  %-40s %-25s %-8s %10s  %s",
                        table_name, column_name, data_type,
                        f"{approx_rows:,}", descriptions,
                    )
                    if not idx_rows and approx_rows > 1000:
                        self._warning(
                            "%s.%s (%s, ~%s rows) has no index — "
                            "containment queries will seq-scan.",
                            table_name, column_name, data_type, f"{approx_rows:,}",
                        )

        # --- Unused indexes (idx_scan = 0, non-unique, non-primary) ---
        self._info("")
        self._info("--- Unused indexes (idx_scan=0, excluding unique/primary) — top 20 by size ---")
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    s.relname AS table_name,
                    s.indexrelname AS index_name,
                    pg_size_pretty(pg_relation_size(s.indexrelid)) AS size,
                    pg_relation_size(s.indexrelid) AS size_bytes
                FROM pg_stat_user_indexes s
                JOIN pg_index i ON i.indexrelid = s.indexrelid
                WHERE s.schemaname = 'public'
                  AND s.idx_scan = 0
                  AND NOT i.indisunique
                  AND NOT i.indisprimary
                ORDER BY pg_relation_size(s.indexrelid) DESC
                LIMIT 20
                """
            )
            unused_rows = cursor.fetchall()

        if not unused_rows:
            self._info("  (None found)")
        else:
            self._info("  %-42s %-50s %10s", "Table", "Index", "Size")
            for table_name, index_name, size, _size_bytes in unused_rows:
                self._info("  %-42s %-50s %10s", table_name, index_name, size)

        # --- Per-table index inventory (tables >1,000 rows) ---
        self._info("")
        self._info("--- Per-table index inventory (tables >1,000 rows) ---")
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT t.relname, t.reltuples::bigint, s.seq_scan, s.idx_scan
                FROM pg_class t
                JOIN pg_namespace n ON n.oid = t.relnamespace
                LEFT JOIN pg_stat_user_tables s ON s.relid = t.oid
                WHERE n.nspname = 'public'
                  AND t.relkind = 'r'
                  AND t.reltuples > 1000
                ORDER BY t.reltuples DESC
                """
            )
            large_tables = cursor.fetchall()

        for table_name, rows, seq_scan, idx_scan in large_tables:
            self._info("")
            self._info(
                "  %s  (rows=%s, seq_scan=%s, idx_scan=%s)",
                table_name, f"{rows:,}",
                f"{seq_scan or 0:,}", f"{idx_scan or 0:,}",
            )
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        i.relname AS index_name,
                        am.amname AS index_type,
                        idx.indisunique,
                        idx.indisprimary,
                        array_to_string(
                            array_agg(a.attname ORDER BY
                                array_position(idx.indkey::int[], a.attnum::int)),
                            ', '
                        ) AS columns,
                        COALESCE(s.idx_scan, 0) AS scans,
                        pg_size_pretty(pg_relation_size(i.oid)) AS size
                    FROM pg_class t
                    JOIN pg_index idx ON idx.indrelid = t.oid
                    JOIN pg_class i ON i.oid = idx.indexrelid
                    JOIN pg_am am ON am.oid = i.relam
                    JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(idx.indkey)
                    LEFT JOIN pg_stat_user_indexes s ON s.indexrelid = i.oid
                    WHERE t.relname = %s
                    GROUP BY i.relname, am.amname, idx.indisunique,
                             idx.indisprimary, s.idx_scan, i.oid
                    ORDER BY idx.indisprimary DESC, idx.indisunique DESC, i.relname
                    """,
                    [table_name],
                )
                idx_rows = cursor.fetchall()

            if not idx_rows:
                self._info("    (no indexes found)")
                continue
            self._info(
                "    %-55s %-8s %-7s %-40s %10s %10s",
                "Index", "Type", "Unique", "Columns", "Scans", "Size",
            )
            for idx_name, idx_type, is_unique, is_primary, columns, scans, size in idx_rows:
                flags = "PK" if is_primary else ("UNIQ" if is_unique else "")
                self._info(
                    "    %-55s %-8s %-7s %-40s %10s %10s",
                    idx_name, idx_type, flags, columns, f"{scans:,}", size,
                )

        # --- Parent_id indexes on dcim_location (location-specific) ---
        self._info("")
        self._info("--- Parent ID indexes on dcim_location ---")
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT indexname, indexdef FROM pg_indexes "
                "WHERE tablename = 'dcim_location' AND indexdef LIKE '%%parent_id%%'"
            )
            parent_indexes = cursor.fetchall()
        if parent_indexes:
            for name, _defn in parent_indexes:
                self._info("  %s", name)
        else:
            self._warning("  No parent_id indexes found.")

    # ------------------------------------------------------------------
    # 7. Connection & query diagnostics
    # ------------------------------------------------------------------
    def section_connection_diagnostics(self):
        """Check pg_stat_activity for connection and query health."""
        self._info("")
        self._info("=" * 60)
        self._info("7. CONNECTION & QUERY DIAGNOSTICS")
        self._info("=" * 60)

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

        self._info("")
        self._info("Connection states (current database):")
        total_conns = 0
        for state, count in rows:
            state_label = state or "NULL (background worker)"
            self._info("  %-25s %d", state_label, count)
            total_conns += count
        self._info("  %-25s %d", "TOTAL", total_conns)

        with connection.cursor() as cursor:
            cursor.execute("SELECT setting FROM pg_settings WHERE name = 'max_connections'")
            max_conns = int(cursor.fetchone()[0])

        usage_pct = (total_conns / max_conns * 100) if max_conns > 0 else 0
        self._info(
            "  Usage: %d / %d (%.0f%%)", total_conns, max_conns, usage_pct
        )
        if usage_pct > 80:
            self._warning("  Connection pool utilization %.0f%% exceeds 80%% threshold.", usage_pct)

        # Long-running queries
        self._info("")
        self._info("Active queries running > 5 seconds:")
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
                self._warning(
                    "  PID %s  running %s: %s...", pid, duration, query_preview
                )
            self._warning(
                "  Found %d queries running longer than 5 seconds.",
                len(long_queries),
            )
        else:
            self._info("  None found (good).")

        # Blocked/waiting queries
        self._info("")
        self._info("Queries waiting on locks:")
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
            self._warning("  %d queries currently waiting on locks.", blocked_count)
        else:
            self._info("  None found (good).")

    # ------------------------------------------------------------------
    # 8. Database statistics (cumulative)
    # ------------------------------------------------------------------
    def section_database_stats(self):
        """Cumulative stats — shows what has actually been slow over time."""
        self._info("")
        self._info("=" * 60)
        self._info("8. DATABASE STATISTICS (CUMULATIVE)")
        self._info("=" * 60)

        # --- pg_stat_statements: top queries by total time ---
        self._info("")
        self._info("--- Top queries by cumulative execution time ---")
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements'")
            has_pgss = cursor.fetchone() is not None

        if not has_pgss:
            self._info("  pg_stat_statements extension is NOT installed.")
        else:
            try:
                with connection.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT
                            round(total_exec_time::numeric, 1) AS total_ms,
                            calls,
                            round(mean_exec_time::numeric, 2) AS mean_ms,
                            round((100.0 * total_exec_time /
                                NULLIF(sum(total_exec_time) OVER (), 0))::numeric, 1) AS pct,
                            left(regexp_replace(query, '\\s+', ' ', 'g'), 200) AS query_preview
                        FROM pg_stat_statements
                        WHERE query NOT LIKE '%%pg_stat_statements%%'
                          AND query NOT LIKE '%%EXPLAIN%%'
                        ORDER BY total_exec_time DESC
                        LIMIT 10
                        """
                    )
                    rows = cursor.fetchall()
                self._info("")
                for idx, (total_ms, calls, mean_ms, pct, preview) in enumerate(rows, 1):
                    self._info(
                        "  #%d  total=%sms  calls=%s  avg=%sms  %s%% of DB time",
                        idx, f"{total_ms:,}", f"{calls:,}", mean_ms, pct,
                    )
                    self._info("      %s", preview)
            except Exception as e:
                self._warning("  Could not query pg_stat_statements: %s", str(e))

        # --- Sequential scans vs index scans per table ---
        self._info("")
        self._info("--- Sequential scans vs index scans (tables with >1K rows) ---")
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    relname,
                    seq_scan,
                    COALESCE(idx_scan, 0) AS idx_scan,
                    n_live_tup,
                    CASE WHEN seq_scan + COALESCE(idx_scan, 0) = 0 THEN 0
                         ELSE round(100.0 * seq_scan / (seq_scan + COALESCE(idx_scan, 0)), 1)
                    END AS seq_pct
                FROM pg_stat_user_tables
                WHERE schemaname = 'public'
                  AND n_live_tup > 1000
                  AND (seq_scan + COALESCE(idx_scan, 0)) > 0
                ORDER BY (seq_scan::bigint * n_live_tup) DESC
                LIMIT 15
                """
            )
            scan_rows = cursor.fetchall()

        self._info(
            "  %-42s %12s %12s %12s %7s",
            "Table", "Seq scans", "Idx scans", "Rows", "Seq %",
        )
        for relname, seq, idx, n_rows, seq_pct in scan_rows:
            self._info(
                "  %-42s %12s %12s %12s %6s%%",
                relname, f"{seq:,}", f"{idx:,}", f"{n_rows:,}", seq_pct,
            )
        # Flag tables doing heavy sequential scanning
        for relname, seq, idx, n_rows, seq_pct in scan_rows:
            if seq_pct > 50 and n_rows > 10000 and seq > 100:
                self._warning(
                    "%s: %s%% sequential scans on %s rows (%s scans total).",
                    relname, seq_pct, f"{n_rows:,}", f"{seq:,}",
                )

        # --- Buffer cache hit ratio ---
        self._info("")
        self._info("--- Buffer cache hit ratio ---")
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    COALESCE(sum(heap_blks_read), 0) AS disk_reads,
                    COALESCE(sum(heap_blks_hit), 0) AS cache_hits,
                    CASE WHEN COALESCE(sum(heap_blks_hit), 0) + COALESCE(sum(heap_blks_read), 0) = 0
                         THEN 0
                         ELSE round(100.0 * sum(heap_blks_hit) /
                              (sum(heap_blks_hit) + sum(heap_blks_read)), 2)
                    END AS hit_ratio
                FROM pg_statio_user_tables
                """
            )
            row = cursor.fetchone()

        if row:
            disk, hits, ratio = row
            self._info(
                "  Disk reads: %s  |  Cache hits: %s  |  Hit ratio: %s%%",
                f"{disk:,}", f"{hits:,}", ratio,
            )
            if ratio and ratio < 99:
                self._warning("Cache hit ratio %s%% below 99%% threshold.", ratio)

    # ------------------------------------------------------------------
    # 9. Ad-hoc EXPLAIN ANALYZE
    # ------------------------------------------------------------------
    def section_ad_hoc_explain(self):
        """Run EXPLAIN (ANALYZE, BUFFERS) on a user-provided query."""
        query = (self._explain_query or "").strip().rstrip(";").strip()
        if not query:
            return  # Skip section entirely if no query provided

        self._info("")
        self._info("=" * 60)
        self._info("9. AD-HOC EXPLAIN ANALYZE")
        self._info("=" * 60)
        self._info("")

        # Safety: only allow read-only queries
        first_token = query.split(None, 1)[0].upper() if query else ""
        if first_token not in ("SELECT", "WITH"):
            self._warning(
                "Query rejected: must start with SELECT or WITH. First token: %s",
                first_token,
            )
            return

        # Reject queries containing DML / DDL keywords
        forbidden = (
            "INSERT", "UPDATE", "DELETE", "TRUNCATE", "DROP", "ALTER",
            "CREATE", "GRANT", "REVOKE", "COPY", "VACUUM", "REINDEX",
        )
        for kw in forbidden:
            if re.search(r"\b" + kw + r"\b", query, flags=re.IGNORECASE):
                self._warning(
                    "Query rejected: contains forbidden keyword '%s'.", kw,
                )
                return

        self._info("Query:")
        for line in query.splitlines():
            self._info("  %s", line)
        self._info("")

        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) {query}"  # noqa: S608
                )
                plan_lines = [row[0] for row in cursor.fetchall()]

            self._info("EXPLAIN (ANALYZE, BUFFERS) plan:")
            for plan_line in plan_lines:
                self._info("  %s", plan_line)
        except Exception as e:
            self._error("EXPLAIN failed: %s", str(e))

    # ------------------------------------------------------------------
    # 10. PostgreSQL settings
    # ------------------------------------------------------------------
    def section_pg_settings(self):
        self._info("")
        self._info("=" * 60)
        self._info("10. POSTGRESQL SETTINGS")
        self._info("=" * 60)

        checks = {
            "work_mem": {
                "warn_below_kb": 65536,  # 64MB
                "message": "Low work_mem forces sort/hash/CTE intermediate results to spill to disk.",
            },
            "statement_timeout": {
                "warn_value": "0",
                "message": "No statement timeout — long-running queries will run until the reverse proxy times out and returns a 502.",
            },
            "shared_buffers": {
                "warn_below_kb": 524288,  # 512MB
                "message": "Low shared_buffers — less data cached in PostgreSQL memory, causing more disk reads.",
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
                        self._warning("  %s = %s — %s", setting_name, display, check["message"])
                    else:
                        self._info("  %s = %s", setting_name, display)

register_jobs(DatabasePerformanceDiagnostic)
