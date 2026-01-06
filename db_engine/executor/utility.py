"""
Utility command handlers.

This module provides:
- UtilityMixin: Handlers for EXPLAIN, ANALYZE, VACUUM
"""

from ..catalog import TableStatistics
from ..parser import ExplainCommand, AnalyzeCommand, VacuumCommand, SelectCommand


class UtilityMixin:
    """Mixin class for utility command execution"""

    def execute_explain(self, cmd: ExplainCommand) -> str:
        """Execute EXPLAIN [VERBOSE] command"""
        inner_cmd = cmd.command

        # VERBOSE mode: Execute with full instrumentation
        if cmd.verbose:
            return self._execute_explain_verbose(inner_cmd)

        # Basic EXPLAIN mode
        if isinstance(inner_cmd, SelectCommand):
            plan = []
            plan.append(f"Query Plan for: SELECT from {inner_cmd.table_name}")
            plan.append("")

            # Analyze WHERE clause
            if inner_cmd.where:
                scan_method = self._choose_scan_method(inner_cmd.table_name, inner_cmd.where)
                plan.append(f"Scan Method: {scan_method.upper()}")

                if scan_method == 'index':
                    plan.append("  -> Index Scan")
                    plan.append("     Reason: WHERE clause can use index")
                else:
                    plan.append("  -> Sequential Scan")
                    plan.append("     Reason: No suitable index or full table scan needed")
            else:
                plan.append("Scan Method: SEQUENTIAL")
                plan.append("  -> Full table scan (no WHERE clause)")

            # Statistics
            stats = self.catalog.get_statistics(inner_cmd.table_name)
            plan.append("")
            plan.append(f"Estimated rows: {stats.row_count}")
            plan.append(f"Table pages: {stats.page_count}")

            # ORDER BY cost
            if inner_cmd.order_by:
                plan.append("")
                plan.append(f"Sort: ORDER BY {', '.join(c for c, _ in inner_cmd.order_by)}")
                plan.append("  Cost: O(n log n)")

            return "\n".join(plan)
        else:
            return f"EXPLAIN not supported for {type(inner_cmd).__name__}"

    def _execute_explain_verbose(self, cmd: SelectCommand) -> str:
        """Execute query with full instrumentation and return detailed metrics"""
        from ..instrumentation import ExecutionMetrics, Timer
        from ..explain_formatter import ExplainFormatter

        if not isinstance(cmd, SelectCommand):
            return f"EXPLAIN VERBOSE not yet supported for {type(cmd).__name__}"

        metrics = ExecutionMetrics()

        # Parse time (already completed, set to 0 for now)
        metrics.parse_time = 0.1  # Placeholder

        # Planning phase
        with Timer() as t:
            scan_method, index_cost, seq_cost = self._plan_query_with_costs(cmd.table_name, cmd.where)
        metrics.plan_time = t.elapsed
        metrics.scan_method = scan_method.upper()
        metrics.index_scan_cost = index_cost
        metrics.sequential_scan_cost = seq_cost

        # Get statistics
        stats = self.catalog.get_statistics(cmd.table_name)
        metrics.estimated_rows = stats.row_count

        # Execute query with instrumentation
        # For now, just execute the regular SELECT - we'll add instrumentation in next steps
        result = self.execute_select(cmd)
        metrics.rows_returned = len(result)

        # Format output
        formatter = ExplainFormatter()
        return formatter.format_verbose(cmd, metrics)

    def _plan_query_with_costs(self, table_name: str, where_expr) -> tuple:
        """Plan query and return (scan_method, index_cost, seq_cost)"""
        scan_method = self._choose_scan_method(table_name, where_expr)

        # Calculate costs
        stats = self.catalog.get_statistics(table_name)
        seq_cost = stats.row_count * 1.0 + stats.page_count * 10.0

        index_cost = 0.0
        if scan_method == 'index':
            # Index cost: log(N) for tree traversal + heap fetch
            import math
            if stats.row_count > 0:
                tree_height = math.ceil(math.log2(stats.row_count + 1))
                index_cost = tree_height * 1.0 + 1.5  # tree traversal + heap fetch
            else:
                index_cost = 1.0

        return (scan_method, index_cost, seq_cost)

    def execute_analyze(self, cmd: AnalyzeCommand) -> str:
        """Execute ANALYZE command - update statistics"""
        if cmd.table_name:
            tables = [cmd.table_name]
        else:
            tables = self.catalog.list_tables()

        for table_name in tables:
            schema = self.catalog.get_table(table_name)
            heap = self._get_heap_file(table_name)

            # Count rows and pages
            row_count = 0
            distinct_values = {}

            for tuple_obj, ctid in heap.scan_all():
                row_count += 1

                # Track distinct values for each column
                for i, col in enumerate(schema.columns):
                    if col.name not in distinct_values:
                        distinct_values[col.name] = set()
                    value = tuple_obj.values[i]
                    if value is not None:
                        distinct_values[col.name].add(value)

            # Convert sets to counts
            distinct_counts = {col: len(vals) for col, vals in distinct_values.items()}

            # Update statistics
            stats = TableStatistics(
                table_name=table_name,
                row_count=row_count,
                page_count=heap.page_count,
                dead_tuple_count=0,  # Reset after analyze
                distinct_values=distinct_counts,
                modification_count=0  # Reset
            )
            self.catalog.update_statistics(table_name, stats)

        if cmd.table_name:
            return f"Analyzed table '{cmd.table_name}'"
        else:
            return f"Analyzed {len(tables)} tables"

    def execute_vacuum(self, cmd: VacuumCommand) -> str:
        """Execute VACUUM command - reclaim space"""
        if cmd.table_name:
            tables = [cmd.table_name]
        else:
            tables = self.catalog.list_tables()

        total_reclaimed = 0
        for table_name in tables:
            schema = self.catalog.get_table(table_name)
            heap = self._get_heap_file(table_name)
            old_fsm = dict(heap.free_space_map)

            heap.vacuum()

            # IMPORTANT: Rebuild all indexes since tuples moved to new ctids
            for index_meta in self.catalog.get_indexes_for_table(table_name):
                index = self._get_index(index_meta)

                # Re-create the index file (clear it)
                index.create()

                # Re-populate from heap with new ctids
                for tuple_obj, ctid in heap.scan_all():
                    key = self._extract_key_from_tuple(tuple_obj, schema, index_meta.columns)
                    index.insert(key, ctid)

            # Calculate reclaimed space
            new_fsm = heap.free_space_map
            for page_num in old_fsm:
                if page_num in new_fsm:
                    total_reclaimed += new_fsm[page_num] - old_fsm[page_num]

        if cmd.table_name:
            return f"Vacuumed table '{cmd.table_name}'"
        else:
            return f"Vacuumed {len(tables)} tables"
