"""
Formatter for EXPLAIN VERBOSE output.

Formats execution metrics into human-readable detailed reports.
"""

from typing import Any
from .instrumentation import ExecutionMetrics
from .parser.ast import SelectCommand, Expression


class ExplainFormatter:
    """Format EXPLAIN VERBOSE output with detailed execution metrics"""

    def format_verbose(self, command: Any, metrics: ExecutionMetrics) -> str:
        """Format complete verbose explain output"""
        sections = []

        # Header
        sections.append(self._format_header(command))
        sections.append("")

        # 1. Parsing phase
        sections.append(self._format_parsing_section(command, metrics))
        sections.append("")

        # 2. Query planning
        sections.append(self._format_planning_section(metrics))
        sections.append("")

        # 3. Execution sections (varies by scan method)
        if metrics.scan_method == "INDEX":
            sections.append(self._format_index_lookup_section(metrics))
            sections.append("")

        sections.append(self._format_heap_access_section(metrics))
        sections.append("")

        sections.append(self._format_filtering_section(command, metrics))
        sections.append("")

        if hasattr(command, 'order_by') and command.order_by:
            sections.append(self._format_sort_section(command, metrics))
            sections.append("")

        sections.append(self._format_projection_section(command, metrics))
        sections.append("")

        # 4. Summary
        sections.append(self._format_summary(metrics))

        return "\n".join(sections)

    def _format_header(self, command: Any) -> str:
        """Format the header section"""
        cmd_str = self._command_to_sql(command)
        separator = "=" * (len(cmd_str) + 20)
        return f"{separator}\nEXPLAIN VERBOSE: {cmd_str}\n{separator}"

    def _command_to_sql(self, command: Any) -> str:
        """Convert command object to SQL string"""
        if isinstance(command, SelectCommand):
            cols = ", ".join(command.columns)
            sql = f"SELECT {cols} FROM {command.table_name}"
            if command.where:
                sql += f" WHERE {self._expr_to_string(command.where)}"
            if command.order_by:
                order_parts = [f"{col} {direction}" for col, direction in command.order_by]
                sql += f" ORDER BY {', '.join(order_parts)}"
            if command.limit:
                sql += f" LIMIT {command.limit}"
            if command.offset:
                sql += f" OFFSET {command.offset}"
            return sql
        return str(command)

    def _expr_to_string(self, expr: Expression) -> str:
        """Convert expression to string"""
        from .parser.ast import BinaryOp, UnaryOp, Literal, ColumnRef

        if isinstance(expr, BinaryOp):
            left = self._expr_to_string(expr.left)
            right = self._expr_to_string(expr.right)
            return f"{left} {expr.op} {right}"
        elif isinstance(expr, UnaryOp):
            operand = self._expr_to_string(expr.operand)
            return f"{expr.op} {operand}"
        elif isinstance(expr, Literal):
            if expr.datatype == 'TEXT':
                return f"'{expr.value}'"
            return str(expr.value)
        elif isinstance(expr, ColumnRef):
            return expr.column_name
        return str(expr)

    def _format_parsing_section(self, command: Any, metrics: ExecutionMetrics) -> str:
        """Format parsing phase details"""
        lines = []
        lines.append("[1. PARSING PHASE]")
        lines.append(f"Duration: {metrics.parse_time:.2f}ms")
        lines.append("")
        lines.append("AST Structure:")

        if isinstance(command, SelectCommand):
            lines.append(f"  SelectCommand(")
            lines.append(f"    table: '{command.table_name}'")
            lines.append(f"    columns: {command.columns}")
            if command.where:
                lines.append(f"    where: {self._format_expression_ast(command.where)}")
            if command.order_by:
                lines.append(f"    order_by: {command.order_by}")
            if command.limit:
                lines.append(f"    limit: {command.limit}")
            if command.offset:
                lines.append(f"    offset: {command.offset}")
            lines.append("  )")

        return "\n".join(lines)

    def _format_expression_ast(self, expr: Expression, indent: int = 0) -> str:
        """Format expression AST with proper indentation"""
        from .parser.ast import BinaryOp, UnaryOp, Literal, ColumnRef

        prefix = "  " * indent
        if isinstance(expr, BinaryOp):
            left_str = self._format_expression_ast(expr.left, indent + 1)
            right_str = self._format_expression_ast(expr.right, indent + 1)
            return f"BinaryOp(\n{prefix}  op: '{expr.op}'\n{prefix}  left: {left_str}\n{prefix}  right: {right_str}\n{prefix})"
        elif isinstance(expr, Literal):
            return f"Literal(value={repr(expr.value)}, type={expr.datatype})"
        elif isinstance(expr, ColumnRef):
            return f"ColumnRef('{expr.column_name}')"
        return str(expr)

    def _format_planning_section(self, metrics: ExecutionMetrics) -> str:
        """Format query planning details"""
        lines = []
        lines.append("[2. QUERY PLANNING]")
        lines.append(f"Duration: {metrics.plan_time:.2f}ms")
        lines.append("")

        if metrics.scan_method:
            lines.append(f"Scan Method: {metrics.scan_method}")
            lines.append("")
            lines.append("Cost Analysis:")
            if metrics.index_scan_cost > 0:
                lines.append(f"  Index Scan Cost: {metrics.index_scan_cost:.2f}")
            if metrics.sequential_scan_cost > 0:
                lines.append(f"  Sequential Scan Cost: {metrics.sequential_scan_cost:.2f}")

            if metrics.index_scan_cost > 0 and metrics.sequential_scan_cost > 0:
                if metrics.scan_method == "INDEX":
                    ratio = metrics.sequential_scan_cost / metrics.index_scan_cost
                    lines.append(f"  ✓ DECISION: Index Scan ({ratio:.1f}x cheaper)")
                else:
                    lines.append(f"  ✓ DECISION: Sequential Scan")

        return "\n".join(lines)

    def _format_index_lookup_section(self, metrics: ExecutionMetrics) -> str:
        """Format index lookup execution details"""
        lines = []
        lines.append("[3. EXECUTION - INDEX LOOKUP]")
        lines.append(f"Duration: {metrics.index_lookup_time:.2f}ms")
        if metrics.index_used:
            lines.append(f"Index: {metrics.index_used}")
        lines.append("")

        if metrics.btree_nodes_visited:
            lines.append("B-tree Traversal:")
            for i, node_visit in enumerate(metrics.btree_nodes_visited):
                node_type = "Leaf" if node_visit.is_leaf else "Internal"
                lines.append(f"  Node {i+1} (offset={node_visit.offset}):")
                lines.append(f"    Type: {node_type}")
                lines.append(f"    Keys: {node_visit.keys}")
                if node_visit.comparison_result:
                    lines.append(f"    {node_visit.comparison_result}")
                if node_visit.found:
                    lines.append(f"    ✓ Found!")
            lines.append("")
            lines.append("Statistics:")
            lines.append(f"  Tree depth: {metrics.btree_depth}")
            lines.append(f"  Nodes visited: {len(metrics.btree_nodes_visited)}")
            lines.append(f"  Comparisons: {metrics.btree_comparisons}")
            buffer_status = "all nodes in buffer" if metrics.buffer_pool_misses == 0 else f"{metrics.buffer_pool_hits} hits, {metrics.buffer_pool_misses} misses"
            lines.append(f"  Disk reads: {metrics.buffer_pool_misses} ({buffer_status})")

        return "\n".join(lines)

    def _format_heap_access_section(self, metrics: ExecutionMetrics) -> str:
        """Format heap file access details"""
        section_num = 4 if metrics.scan_method == "INDEX" else 3
        lines = []
        lines.append(f"[{section_num}. EXECUTION - HEAP ACCESS]")
        lines.append(f"Duration: {metrics.heap_access_time:.2f}ms")
        lines.append("")

        if metrics.pages_accessed:
            lines.append("Page Access:")
            for page_access in metrics.pages_accessed:
                cache_status = "cache hit" if page_access.cache_hit else "cache miss"
                lines.append(f"  Page {page_access.page_num} ({cache_status}):")
                lines.append(f"    Free space: {page_access.free_space} bytes")
                lines.append(f"    Live tuples: {page_access.live_tuples}")
                lines.append(f"    Dead tuples: {page_access.dead_tuples}")

        if metrics.tuples_fetched:
            lines.append("")
            for tuple_det in metrics.tuples_fetched:
                lines.append(f"Tuple (ctid={tuple_det.ctid}):")
                lines.append(f"  Offset in page: {tuple_det.offset_in_page} bytes")
                lines.append(f"  Header size: {tuple_det.header_size} bytes")
                lines.append(f"  Null bitmap: {tuple_det.null_bitmap_size} bytes")
                lines.append(f"  Data size: {tuple_det.data_size} bytes")
                lines.append(f"  Total: {tuple_det.total_size} bytes")
                if tuple_det.column_sizes:
                    col_layout = [f"{col}:{size}" for col, size in tuple_det.column_sizes.items()]
                    lines.append(f"  Layout: [{']['.join(col_layout)}]")

        return "\n".join(lines)

    def _format_filtering_section(self, command: Any, metrics: ExecutionMetrics) -> str:
        """Format WHERE clause filtering details"""
        section_num = 5 if metrics.scan_method == "INDEX" else 4
        lines = []
        lines.append(f"[{section_num}. EXECUTION - FILTERING]")
        lines.append(f"Duration: {metrics.filter_time:.2f}ms")
        lines.append("")

        if isinstance(command, SelectCommand) and command.where:
            lines.append("WHERE Clause:")
            lines.append(f"  {self._expr_to_string(command.where)}")

        if metrics.rows_scanned > 0:
            pct = (metrics.rows_filtered / metrics.rows_scanned * 100) if metrics.rows_scanned > 0 else 0
            lines.append(f"  Rows scanned: {metrics.rows_scanned}")
            lines.append(f"  Rows matched: {metrics.rows_filtered} ({pct:.1f}%)")

        return "\n".join(lines)

    def _format_sort_section(self, command: Any, metrics: ExecutionMetrics) -> str:
        """Format sorting details"""
        section_num = 6 if metrics.scan_method == "INDEX" else 5
        lines = []
        lines.append(f"[{section_num}. EXECUTION - SORTING]")
        lines.append(f"Duration: {metrics.sort_time:.2f}ms")
        lines.append("")

        if isinstance(command, SelectCommand) and command.order_by:
            order_parts = [f"{col} {direction}" for col, direction in command.order_by]
            lines.append(f"ORDER BY: {', '.join(order_parts)}")
            lines.append(f"Rows sorted: {metrics.rows_filtered}")

        return "\n".join(lines)

    def _format_projection_section(self, command: Any, metrics: ExecutionMetrics) -> str:
        """Format result projection details"""
        section_num = 7 if metrics.scan_method == "INDEX" else 6
        if isinstance(command, SelectCommand) and command.order_by:
            section_num += 1

        lines = []
        lines.append(f"[{section_num}. RESULT PROJECTION]")
        lines.append(f"Duration: {metrics.projection_time:.2f}ms")
        lines.append("")

        if isinstance(command, SelectCommand):
            if command.columns == ['*']:
                lines.append("Columns: * (all columns)")
            else:
                lines.append(f"Columns: {', '.join(command.columns)}")
            lines.append(f"Rows returned: {metrics.rows_returned}")

        return "\n".join(lines)

    def _format_summary(self, metrics: ExecutionMetrics) -> str:
        """Format execution summary"""
        section_num = 8 if metrics.scan_method == "INDEX" else 7
        lines = []
        lines.append(f"[{section_num}. SUMMARY]")
        lines.append(f"Total Execution Time: {metrics.total_time():.2f}ms")
        lines.append("")
        lines.append("Timing Breakdown:")

        total = metrics.total_time()
        if total > 0:
            lines.append(f"  Parsing: {metrics.parse_time:.2f}ms ({metrics.parse_time/total*100:.0f}%)")
            lines.append(f"  Planning: {metrics.plan_time:.2f}ms ({metrics.plan_time/total*100:.0f}%)")
            if metrics.index_lookup_time > 0:
                lines.append(f"  Index Lookup: {metrics.index_lookup_time:.2f}ms ({metrics.index_lookup_time/total*100:.0f}%)")
            lines.append(f"  Heap Access: {metrics.heap_access_time:.2f}ms ({metrics.heap_access_time/total*100:.0f}%)")
            lines.append(f"  Filtering: {metrics.filter_time:.2f}ms ({metrics.filter_time/total*100:.0f}%)")
            if metrics.sort_time > 0:
                lines.append(f"  Sorting: {metrics.sort_time:.2f}ms ({metrics.sort_time/total*100:.0f}%)")
            lines.append(f"  Projection: {metrics.projection_time:.2f}ms ({metrics.projection_time/total*100:.0f}%)")

        lines.append("")
        lines.append("Memory Usage:")
        lines.append(f"  Total: {metrics.memory_used} bytes")

        lines.append("")
        lines.append("Buffer Pool Stats:")
        total_accesses = metrics.buffer_pool_hits + metrics.buffer_pool_misses
        if total_accesses > 0:
            hit_rate = metrics.buffer_pool_hits / total_accesses * 100
            lines.append(f"  Cache hits: {metrics.buffer_pool_hits} ({hit_rate:.1f}%)")
            lines.append(f"  Cache misses: {metrics.buffer_pool_misses}")
        else:
            lines.append(f"  No buffer pool accesses")

        lines.append("")
        lines.append("Row Statistics:")
        if metrics.estimated_rows > 0:
            lines.append(f"  Estimated rows: {metrics.estimated_rows}")
        lines.append(f"  Actual rows scanned: {metrics.rows_scanned}")
        lines.append(f"  Rows filtered: {metrics.rows_filtered}")
        lines.append(f"  Rows returned: {metrics.rows_returned}")

        return "\n".join(lines)
