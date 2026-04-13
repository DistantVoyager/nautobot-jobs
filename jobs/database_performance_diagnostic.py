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

        self._lines.append("# Database Performance Diagnostic Report")
        self._lines.append("")
        self._lines.append("All queries are read-only.")

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
        filename = "database_performance_report.md"
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
    # Output helpers — write to buffer; warnings/errors also go to log
    # ------------------------------------------------------------------
    def _info(self, message, *args):
        self._lines.append(message % args if args else message)

    def _warning(self, message, *args):
        formatted = message % args if args else message
        self._lines.append(f"> ⚠️ **WARNING:** {formatted}")
        self._lines.append("")
        self._real_logger.warning(message, *args)

    def _error(self, message, *args):
        formatted = message % args if args else message
        self._lines.append(f"> 🛑 **ERROR:** {formatted}")
        self._lines.append("")
        self._real_logger.error(message, *args)

    def _h1(self, text):
        self._lines.append("")
        self._lines.append(f"# {text}")
        self._lines.append("")

    def _h2(self, text):
        self._lines.append("")
        self._lines.append(f"## {text}")
        self._lines.append("")

    def _h3(self, text):
        self._lines.append("")
        self._lines.append(f"### {text}")
        self._lines.append("")

    def _h4(self, text):
        self._lines.append("")
        self._lines.append(f"#### {text}")
        self._lines.append("")

    def _code_block(self, content, language=""):
        self._lines.append(f"```{language}")
        if isinstance(content, str):
            for line in content.splitlines():
                self._lines.append(line)
        else:
            for line in content:
                self._lines.append(line)
        self._lines.append("```")
        self._lines.append("")

    def _table(self, headers, rows):
        """Emit a GFM markdown table. All cells coerced to str."""
        def _escape(cell):
            s = "" if cell is None else str(cell)
            return s.replace("|", "\\|").replace("\n", " ")
        self._lines.append("| " + " | ".join(_escape(h) for h in headers) + " |")
        self._lines.append("| " + " | ".join(["---"] * len(headers)) + " |")
        for row in rows:
            self._lines.append("| " + " | ".join(_escape(c) for c in row) + " |")
        self._lines.append("")

    def _details(self, summary, content_lines, language=""):
        """Collapsible section (renders in GitHub/VS Code markdown viewers)."""
        self._lines.append(f"<details><summary>{summary}</summary>")
        self._lines.append("")
        self._lines.append(f"```{language}")
        if isinstance(content_lines, str):
            for line in content_lines.splitlines():
                self._lines.append(line)
        else:
            for line in content_lines:
                self._lines.append(line)
        self._lines.append("```")
        self._lines.append("")
        self._lines.append("</details>")
        self._lines.append("")

    # ------------------------------------------------------------------
    # 1. Table sizes
    # ------------------------------------------------------------------
    def section_table_sizes(self):
        self._h2("1. Table Sizes")

        self._h3("Top 20 tables by total disk size")
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

        self._table(
            ["Table", "Total", "Data", "Indexes", "Approx rows"],
            [
                (table_name, total, data, indexes, f"{approx_rows:,}")
                for table_name, total, data, indexes, approx_rows in rows
            ],
        )

    # ------------------------------------------------------------------
    # 2. Tree structure
    # ------------------------------------------------------------------
    def section_tree_structure(self):
        self._h2("2. Location Tree Structure")
        self._info("")

        root_count = Location.objects.filter(parent__isnull=True).count()
        self._info(f"**Root locations (no parent):** {root_count:,}")

        self._h3("Tree depth distribution")
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

        self._table(
            ["Depth", "Location count"],
            [(depth, f"{cnt:,}") for depth, cnt in rows],
        )

        max_depth = rows[-1][0] if rows else 0
        if max_depth > 10:
            self._warning(
                "Tree depth of %d is deep. Each level adds cost to recursive CTEs.", max_depth
            )

        self._h3("Top 10 largest root subtrees (by total descendants)")
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
        self._table(
            ["Root", "Location type", "Descendants"],
            [
                (name, loc_type, f"{desc_count:,}")
                for _root_id, name, desc_count, loc_type in largest_root_rows
            ],
        )
        # Stash the top one so sections 3 and 4 can reuse it
        self._largest_root = (
            Location.objects.get(pk=largest_root_rows[0][0])
            if largest_root_rows else None
        )

        self._h3("Top 5 parents by direct-child count")
        self._info("_Large direct-child counts can slow parent-selection dropdown/list rendering._")
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
        self._table(
            ["Parent", "Location type", "Direct children"],
            [(p_name, p_type, f"{c_count:,}") for p_name, p_type, c_count in widest_rows],
        )
        if widest_rows and widest_rows[0][2] > 500:
            self._warning(
                "Largest parent has %d direct children.", widest_rows[0][2],
            )

    # ------------------------------------------------------------------
    # 3. Location query benchmarks
    # ------------------------------------------------------------------
    def section_query_benchmarks(self):
        self._h2("3. Location Query Benchmarks")

        # --- Benchmark A: Full tree CTE ---
        self._h3("A) Full tree recursive CTE")
        self._info("_This is what `TreeNodeMultipleChoiceFilter` triggers on any location-filtered page._")

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

        self._info(f"Traversed **{count:,}** nodes in **{elapsed:.2f}s**.")
        if elapsed > 5:
            self._warning("Execution time exceeds 5s threshold.")
        elif elapsed > 1:
            self._warning("Execution time exceeds 1s threshold.")

        # --- Benchmark B: Subtree of the largest root (reused from section 2) ---
        self._h3("B) Subtree lookup for the largest root")

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
                f'Root `{largest_root.name}` — **{count:,}** descendants in **{elapsed:.2f}s**.'
            )
        else:
            self._info("_No root locations found._")

        # --- Benchmark C: ILIKE search (what ?q= does) ---
        self._h3("C) ILIKE search simulation (`?q=` filter)")

        sample_loc = Location.objects.first()
        if sample_loc and sample_loc.name:
            search_term = sample_loc.name[:4]
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
            f'Search for `%{search_term}%` — **{count:,}** matches in **{elapsed:.2f}s**.'
        )

        with connection.cursor() as cursor:
            cursor.execute(
                "EXPLAIN (FORMAT TEXT) SELECT count(*) FROM dcim_location WHERE name ILIKE %s",
                [f"%{search_term}%"],
            )
            plan_lines = [row[0] for row in cursor.fetchall()]
            plan = "\n".join(plan_lines)

        self._info("**Query plan:**")
        self._code_block(plan_lines)
        if "Seq Scan" in plan:
            self._warning("Plan uses Sequential Scan.")

        # --- Benchmark D: Device count by location (StatsPanel) ---
        self._h3("D) Device count for a location (StatsPanel)")

        if largest_root:
            start = time.perf_counter()
            device_count = Device.objects.filter(location=largest_root).count()
            elapsed_direct = time.perf_counter() - start

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
                elapsed_with_desc = time.perf_counter() - start
                count = cursor.fetchone()[0]

            self._table(
                ["Query", "Result", "Time"],
                [
                    (f"Direct at `{largest_root.name}`", f"{device_count:,} devices", f"{elapsed_direct:.2f}s"),
                    (f"`{largest_root.name}` + descendants", f"{count:,} devices", f"{elapsed_with_desc:.2f}s"),
                ],
            )
            if elapsed_with_desc > 3:
                self._warning("Descendant query exceeds 3s threshold.")

    # ------------------------------------------------------------------
    # 4. FilterSet benchmarks
    # ------------------------------------------------------------------
    def section_filterset_benchmarks(self):
        """Benchmark actual FilterSet list-view queries with EXPLAIN ANALYZE on both count and page slice."""
        self._h2("4. FilterSet Benchmarks (Pagination Simulation)")
        self._info(
            "Each scenario below simulates a single page load of a filtered Nautobot "
            "list view. Every list page issues **two** queries:"
        )
        self._info("")
        self._info("1. **Count query** — `SELECT COUNT(*)` over the full filtered queryset (drives the paginator)")
        self._info("2. **Page query** — `SELECT ... LIMIT 50` for the current page's rows")
        self._info("")
        self._info(
            "Both queries go through the same FilterSet. Tree filters like `parent=` "
            "or `location=` trigger recursive CTEs on **every page load, every page "
            "navigation**. Each scenario captures timing, generated SQL, and an "
            "`EXPLAIN ANALYZE` plan for **both** queries."
        )

        # Build default test cases: (label, url_path, filterset_class, query_string)
        test_cases = []

        # Baseline: location list page with no filters
        test_cases.append((
            "Location list page (no filters)",
            "/dcim/locations/",
            LocationFilterSet, "",
        ))

        # Search filter (?q=)
        sample_loc = Location.objects.first()
        if sample_loc and sample_loc.name:
            search_term = sample_loc.name[:4]
            test_cases.append((
                f"Location list with ?q= search (term={search_term!r})",
                f"/dcim/locations/?q={search_term}",
                LocationFilterSet, f"q={search_term}",
            ))

        # Parent filter (TreeNodeMultipleChoiceFilter) — reuse largest root from section 2
        largest_root = self._largest_root
        if largest_root:
            test_cases.append((
                f"Location list filtered by parent={largest_root.name}",
                f"/dcim/locations/?parent={largest_root.pk}",
                LocationFilterSet, f"parent={largest_root.pk}",
            ))
            test_cases.append((
                f"Device list filtered by location={largest_root.name}",
                f"/dcim/devices/?location={largest_root.pk}",
                DeviceFilterSet, f"location={largest_root.pk}",
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
                    url_path = f"/dcim/devices/?{query_str}"
                    scenario = f"Custom Device filter: {query_str}"
                elif line.lower().startswith("locations:"):
                    fs_class = LocationFilterSet
                    query_str = line.split(":", 1)[1]
                    url_path = f"/dcim/locations/?{query_str}"
                    scenario = f"Custom Location filter: {query_str}"
                else:
                    fs_class = LocationFilterSet
                    query_str = line
                    url_path = f"/dcim/locations/?{line}"
                    scenario = f"Custom Location filter: {line}"
                test_cases.append((scenario, url_path, fs_class, query_str))

        # Run benchmarks
        for idx, (scenario, url_path, fs_class, query_str) in enumerate(test_cases, 1):
            base_qs = Location.objects.all() if fs_class is LocationFilterSet else Device.objects.all()
            qd = QueryDict(query_str)
            self._h3(f"Scenario {idx}: {scenario}")
            self._info(f"**URL equivalent:** `{url_path}`")
            self._info("")
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

                # Summary table
                self._table(
                    ["Metric", "Count query", "Page query", "Combined"],
                    [
                        ["Time", f"{count_elapsed:.3f}s", f"{page_elapsed:.3f}s", f"{combined:.3f}s"],
                        ["Queries issued", len(count_ctx), len(page_ctx), len(count_ctx) + len(page_ctx)],
                        ["Rows matched (count)", f"{total:,}", "—", "—"],
                    ],
                )

                if combined > 5:
                    self._warning(f"Combined time {combined:.2f}s exceeds 5s threshold.")
                elif combined > 2:
                    self._warning(f"Combined time {combined:.2f}s exceeds 2s threshold.")

                # Per-query detail: SQL + EXPLAIN
                for phase_label, ctx in (
                    ("Count query", count_ctx),
                    ("Page query", page_ctx),
                ):
                    if not ctx.captured_queries:
                        continue
                    sql = ctx.captured_queries[-1]["sql"]
                    self._h4(phase_label)
                    self._code_block(sql, language="sql")

                    try:
                        with connection.cursor() as cursor:
                            cursor.execute(
                                "EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) %s" % sql  # noqa: S608
                            )
                            plan_lines = [row[0] for row in cursor.fetchall()]
                    except Exception as plan_err:
                        self._warning("EXPLAIN failed for %s: %s", phase_label, str(plan_err))
                        continue

                    plan_text = "\n".join(plan_lines)
                    # Use collapsible block for very long plans (common for tree filters)
                    if len(plan_text) > 2000:
                        self._details(f"EXPLAIN ANALYZE plan ({phase_label}) — click to expand", plan_lines)
                    else:
                        self._info(f"**EXPLAIN ANALYZE ({phase_label}):**")
                        self._code_block(plan_lines)

                    if "Seq Scan" in plan_text:
                        self._warning(f"{phase_label} plan contains Sequential Scan.")
                    if "Sort Method: external" in plan_text:
                        self._warning(f"{phase_label} plan contains external (on-disk) sort.")
                    for plan_line in plan_lines:
                        if "Rows Removed by Filter" in plan_line:
                            self._info(f"- _{plan_line.strip()}_")

            except Exception as e:
                self._error("Scenario %d (%s) failed: %s", idx, scenario, str(e))

    # ------------------------------------------------------------------
    # 5. Concurrent load simulation
    # ------------------------------------------------------------------
    def section_concurrent_load(self):
        """Simulate concurrent tree CTE queries to test degradation under load."""
        self._h2("5. Concurrent Load Simulation")

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
            throughput = len(times) / wall_elapsed if wall_elapsed > 0 else 0

            self._table(
                ["Metric", "Value"],
                [
                    ("Completed", f"{len(times)} / {num_queries}"),
                    ("Wall clock", f"{wall_elapsed:.2f}s"),
                    ("Per-query min", f"{min_time:.2f}s"),
                    ("Per-query avg", f"{avg_time:.2f}s"),
                    ("Per-query max", f"{max_time:.2f}s"),
                    ("Throughput", f"{throughput:.1f} queries/sec"),
                ],
            )

            if max_time > 10:
                self._warning("Max query time %.1fs exceeds 10s threshold.", max_time)
            elif max_time > 5:
                self._warning("Max query time %.1fs exceeds 5s threshold.", max_time)
            elif avg_time > 2:
                self._warning("Average query time %.1fs exceeds 2s threshold.", avg_time)

    # ------------------------------------------------------------------
    # 6. Index check
    # ------------------------------------------------------------------
    def section_index_check(self):
        self._h2("6. Index Check")

        self._info(
            "This section audits index coverage on large tables (>1,000 rows). "
            "It has two parts:"
        )
        self._info("")
        self._info(
            "- **Part 1 — Coverage matrix:** flags *missing* specialized indexes "
            "that Nautobot's query patterns need (trigram for `?q=` search, GIN "
            "for custom-field filtering, GiST/btree for IP/prefix containment)."
        )
        self._info(
            "- **Part 2 — Full index inventory:** lists *every* index that "
            "actually exists on each large table, so a reviewer can verify "
            "whether a specific column is already covered by some index."
        )

        # --- pg_trgm extension ---
        self._h3("pg_trgm extension")
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1 FROM pg_extension WHERE extname = 'pg_trgm'")
            has_trgm = cursor.fetchone() is not None
        if has_trgm:
            self._info("**Status:** INSTALLED")
        else:
            self._info("**Status:** MISSING")
            self._warning(
                "pg_trgm extension is not installed — trigram indexes cannot exist, "
                "all `?q=` ILIKE searches will sequential-scan."
            )

        # --- Part 1: Coverage matrix ---
        self._h3("Part 1 — Specialized index coverage matrix (tables >1,000 rows)")
        self._info(
            "Columns with `—` mean the column doesn't exist on this table (N/A). "
            "`MISSING` means the column exists but has no appropriate specialized index."
        )

        # Gather the list of large tables and which special columns they have
        with connection.cursor() as cursor:
            cursor.execute(
                """
                WITH large_tables AS (
                    SELECT t.relname AS table_name, t.reltuples::bigint AS rows
                    FROM pg_class t
                    JOIN pg_namespace n ON n.oid = t.relnamespace
                    WHERE n.nspname = 'public'
                      AND t.relkind = 'r'
                      AND t.reltuples > 1000
                )
                SELECT
                    lt.table_name,
                    lt.rows,
                    bool_or(
                        c.column_name = 'name'
                        AND c.data_type IN ('character varying', 'text', 'character')
                    ) AS has_name,
                    bool_or(c.column_name = '_custom_field_data') AS has_cfd,
                    bool_or(c.data_type IN ('inet', 'cidr')) AS has_network
                FROM large_tables lt
                LEFT JOIN information_schema.columns c
                    ON c.table_schema = 'public' AND c.table_name = lt.table_name
                GROUP BY lt.table_name, lt.rows
                ORDER BY lt.rows DESC
                """
            )
            coverage_tables = cursor.fetchall()

        matrix_rows = []
        critical_missing = []
        with connection.cursor() as cursor:
            for table_name, rows, has_name, has_cfd, has_network in coverage_tables:
                # Trigram on name
                if has_name:
                    cursor.execute(
                        "SELECT indexname FROM pg_indexes "
                        "WHERE tablename = %s AND indexdef LIKE %s",
                        [table_name, "%trgm%"],
                    )
                    trgm = cursor.fetchone()
                    trgm_cell = trgm[0] if trgm else "**MISSING**"
                    if not trgm:
                        critical_missing.append((table_name, rows, "trigram on `name`"))
                else:
                    trgm_cell = "—"

                # GIN on _custom_field_data
                if has_cfd:
                    cursor.execute(
                        "SELECT indexname FROM pg_indexes "
                        "WHERE tablename = %s "
                        "AND indexdef ILIKE %s AND indexdef ILIKE %s",
                        [table_name, "%_custom_field_data%", "%gin%"],
                    )
                    gin = cursor.fetchone()
                    gin_cell = gin[0] if gin else "**MISSING**"
                    if not gin:
                        critical_missing.append((table_name, rows, "GIN on `_custom_field_data`"))
                else:
                    gin_cell = "—"

                # Index on inet/cidr column (any type)
                if has_network:
                    cursor.execute(
                        """
                        SELECT i.relname, am.amname, a.attname, col.data_type
                        FROM pg_index idx
                        JOIN pg_class c ON c.oid = idx.indrelid
                        JOIN pg_class i ON i.oid = idx.indexrelid
                        JOIN pg_am am ON am.oid = i.relam
                        JOIN pg_attribute a ON a.attrelid = c.oid
                            AND a.attnum = ANY(idx.indkey)
                        JOIN information_schema.columns col
                            ON col.table_name = c.relname AND col.column_name = a.attname
                        WHERE c.relname = %s
                          AND col.data_type IN ('inet', 'cidr')
                        """,
                        [table_name],
                    )
                    net_idx = cursor.fetchall()
                    if net_idx:
                        net_cell = ", ".join(
                            f"{name} ({amname}) on `{attname}`"
                            for name, amname, attname, _dt in net_idx
                        )
                    else:
                        net_cell = "**MISSING**"
                        critical_missing.append((table_name, rows, "network-column index"))
                else:
                    net_cell = "—"

                matrix_rows.append((
                    table_name, f"{rows:,}", trgm_cell, gin_cell, net_cell,
                ))

        if matrix_rows:
            self._table(
                [
                    "Table", "Rows",
                    "Trigram (`name`)",
                    "GIN (`_custom_field_data`)",
                    "Network (`inet`/`cidr`)",
                ],
                matrix_rows,
            )
        else:
            self._info("_(No tables with >1,000 rows found)_")

        # Emit a concise warning summary for the gaps surfaced above
        for table_name, rows, gap_kind in critical_missing:
            self._warning(
                "%s (~%s rows) is missing %s index.",
                table_name, f"{rows:,}", gap_kind,
            )

        # --- Unused indexes ---
        self._h3("Unused indexes (top 20 by size)")
        self._info(
            "_Non-unique, non-primary indexes with `idx_scan = 0` since stats "
            "were last reset. These incur write overhead without helping reads._"
        )
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    s.relname AS table_name,
                    s.indexrelname AS index_name,
                    pg_size_pretty(pg_relation_size(s.indexrelid)) AS size
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
            self._info("_(None found)_")
        else:
            self._table(["Table", "Index", "Size"], unused_rows)

        # --- Part 2: Full index inventory (collapsible per table) ---
        self._h3("Part 2 — Full index inventory (tables >1,000 rows)")
        self._info(
            "_Each table below is collapsible. Expand to see every index that "
            "currently exists on that table (PK, uniques, and regular indexes)._"
        )
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

            summary = (
                f"<code>{table_name}</code> — {rows:,} rows, "
                f"{seq_scan or 0:,} seq scans, {idx_scan or 0:,} idx scans "
                f"({len(idx_rows)} indexes)"
            )
            self._lines.append(f"<details><summary>{summary}</summary>")
            self._lines.append("")
            if not idx_rows:
                self._lines.append("_(no indexes found)_")
            else:
                headers = ["Index", "Type", "Flags", "Columns", "Scans", "Size"]
                self._lines.append("| " + " | ".join(headers) + " |")
                self._lines.append("| " + " | ".join(["---"] * len(headers)) + " |")
                for idx_name, idx_type, is_unique, is_primary, columns, scans, size in idx_rows:
                    flags = "PK" if is_primary else ("UNIQ" if is_unique else "")
                    cells = [idx_name, idx_type, flags, columns, f"{scans:,}", size]
                    self._lines.append(
                        "| " + " | ".join(str(c).replace("|", "\\|") for c in cells) + " |"
                    )
            self._lines.append("")
            self._lines.append("</details>")
            self._lines.append("")

    # ------------------------------------------------------------------
    # 7. Connection & query diagnostics
    # ------------------------------------------------------------------
    def section_connection_diagnostics(self):
        """Check pg_stat_activity for connection and query health."""
        self._h2("7. Connection & Query Diagnostics")

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

        self._h3("Connection states (current database)")
        total_conns = 0
        conn_rows = []
        for state, count in rows:
            state_label = state or "NULL (background worker)"
            conn_rows.append((state_label, count))
            total_conns += count
        conn_rows.append(("**TOTAL**", total_conns))
        self._table(["State", "Count"], conn_rows)

        with connection.cursor() as cursor:
            cursor.execute("SELECT setting FROM pg_settings WHERE name = 'max_connections'")
            max_conns = int(cursor.fetchone()[0])

        usage_pct = (total_conns / max_conns * 100) if max_conns > 0 else 0
        self._info(f"**Pool usage:** {total_conns} / {max_conns} ({usage_pct:.0f}%)")
        if usage_pct > 80:
            self._warning("Connection pool utilization %.0f%% exceeds 80%% threshold.", usage_pct)

        # Long-running queries
        self._h3("Active queries running > 5 seconds")
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    pid,
                    now() - query_start AS duration,
                    state,
                    left(query, 200) AS query_preview
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
            self._table(
                ["PID", "Duration", "State", "Query preview"],
                [(pid, str(duration), state, preview) for pid, duration, state, preview in long_queries],
            )
            self._warning(
                "Found %d queries running longer than 5 seconds.",
                len(long_queries),
            )
        else:
            self._info("_None found._")

        # Blocked/waiting queries
        self._h3("Queries waiting on locks")
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
            self._warning("%d queries currently waiting on locks.", blocked_count)
        else:
            self._info("_None found._")

    # ------------------------------------------------------------------
    # 8. Database statistics (cumulative)
    # ------------------------------------------------------------------
    def section_database_stats(self):
        """Cumulative stats — shows what has actually been slow over time."""
        self._h2("8. Database Statistics (Cumulative)")

        # --- pg_stat_statements: top queries by total time ---
        self._h3("Top 10 queries by cumulative execution time")
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements'")
            has_pgss = cursor.fetchone() is not None

        if not has_pgss:
            self._info("_pg_stat_statements extension is **NOT** installed._")
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
                            regexp_replace(query, '\\s+', ' ', 'g') AS query_text
                        FROM pg_stat_statements
                        WHERE query NOT LIKE '%%pg_stat_statements%%'
                          AND query NOT LIKE '%%EXPLAIN%%'
                        ORDER BY total_exec_time DESC
                        LIMIT 10
                        """
                    )
                    rows = cursor.fetchall()
                for idx, (total_ms, calls, mean_ms, pct, query_text) in enumerate(rows, 1):
                    self._h4(f"#{idx} — {pct}% of total DB time")
                    self._table(
                        ["Metric", "Value"],
                        [
                            ("Total time (ms)", f"{total_ms:,}"),
                            ("Calls", f"{calls:,}"),
                            ("Avg time (ms)", mean_ms),
                            ("% of all DB time", f"{pct}%"),
                        ],
                    )
                    self._code_block(query_text, language="sql")
            except Exception as e:
                self._warning("Could not query pg_stat_statements: %s", str(e))

        # --- Sequential scans vs index scans per table ---
        self._h3("Sequential vs index scans (tables with >1K rows, top 15)")
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

        self._table(
            ["Table", "Seq scans", "Idx scans", "Rows", "Seq %"],
            [
                (relname, f"{seq:,}", f"{idx:,}", f"{n_rows:,}", f"{seq_pct}%")
                for relname, seq, idx, n_rows, seq_pct in scan_rows
            ],
        )
        # Flag tables doing heavy sequential scanning
        for relname, seq, idx, n_rows, seq_pct in scan_rows:
            if seq_pct > 50 and n_rows > 10000 and seq > 100:
                self._warning(
                    "%s: %s%% sequential scans on %s rows (%s scans total).",
                    relname, seq_pct, f"{n_rows:,}", f"{seq:,}",
                )

        # --- Buffer cache hit ratio ---
        self._h3("Buffer cache hit ratio")
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
            self._table(
                ["Metric", "Value"],
                [
                    ("Disk reads", f"{disk:,}"),
                    ("Cache hits", f"{hits:,}"),
                    ("Hit ratio", f"{ratio}%"),
                ],
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

        self._h2("9. Ad-hoc EXPLAIN ANALYZE")
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

        self._h3("Query")
        self._code_block(query, language="sql")

        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) {query}"  # noqa: S608
                )
                plan_lines = [row[0] for row in cursor.fetchall()]

            self._h3("EXPLAIN (ANALYZE, BUFFERS) plan")
            self._code_block(plan_lines)
        except Exception as e:
            self._error("EXPLAIN failed: %s", str(e))

    # ------------------------------------------------------------------
    # 10. PostgreSQL settings
    # ------------------------------------------------------------------
    def section_pg_settings(self):
        self._h2("10. PostgreSQL Settings")

        checks = {
            "work_mem": "Memory used per sort/hash/CTE operation before spilling to disk.",
            "statement_timeout": "Maximum time a query can run before being cancelled (0 = unlimited).",
            "shared_buffers": "Amount of memory PostgreSQL uses for caching table and index data.",
        }

        settings_rows = []
        with connection.cursor() as cursor:
            for setting_name, description in checks.items():
                cursor.execute(
                    "SELECT setting, unit FROM pg_settings WHERE name = %s",
                    [setting_name],
                )
                row = cursor.fetchone()
                if row:
                    value, unit = row
                    display = f"{value} {unit}" if unit else value
                    settings_rows.append((setting_name, display, description))

        self._table(
            ["Setting", "Current value", "What it controls"],
            settings_rows,
        )

register_jobs(DatabasePerformanceDiagnostic)
