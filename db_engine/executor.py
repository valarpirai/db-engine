"""
Query Executor - Executes parsed SQL commands

This module provides:
- QueryExecutor: Orchestrates catalog, storage, and indexes
- Command execution for all SQL operations
- Cost-based query planning (index scan vs sequential scan)
- Expression evaluation for WHERE clauses
"""

from typing import List, Dict, Tuple as TupleType, Optional, Any
import os
import re

from .catalog import Catalog, TableSchema, ColumnDef, IndexMetadata, TableStatistics
from .storage import BufferPool, Tuple, Page, HeapFile
from .btree import BTreeIndex
from .parser import (
    SelectCommand, InsertCommand, UpdateCommand, DeleteCommand,
    CreateTableCommand, CreateIndexCommand, DropTableCommand,
    ExplainCommand, AnalyzeCommand, VacuumCommand,
    Expression, BinaryOp, UnaryOp, Literal, ColumnRef
)


class QueryExecutor:
    """Executes SQL commands - orchestrates all database components"""

    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        self.catalog = Catalog(data_dir)
        self.catalog.load()
        self.buffer_pool = BufferPool()
        self.heap_files: Dict[str, HeapFile] = {}
        self.indexes: Dict[str, BTreeIndex] = {}

    def execute(self, command):
        """Main entry point - dispatch to specific executors"""
        if isinstance(command, CreateTableCommand):
            return self.execute_create_table(command)
        elif isinstance(command, CreateIndexCommand):
            return self.execute_create_index(command)
        elif isinstance(command, DropTableCommand):
            return self.execute_drop_table(command)
        elif isinstance(command, InsertCommand):
            return self.execute_insert(command)
        elif isinstance(command, SelectCommand):
            return self.execute_select(command)
        elif isinstance(command, UpdateCommand):
            return self.execute_update(command)
        elif isinstance(command, DeleteCommand):
            return self.execute_delete(command)
        elif isinstance(command, ExplainCommand):
            return self.execute_explain(command)
        elif isinstance(command, AnalyzeCommand):
            return self.execute_analyze(command)
        elif isinstance(command, VacuumCommand):
            return self.execute_vacuum(command)
        else:
            raise ValueError(f"Unknown command type: {type(command)}")

    # ========================================================================
    # CREATE TABLE
    # ========================================================================

    def execute_create_table(self, cmd: CreateTableCommand) -> str:
        """Execute CREATE TABLE command"""
        # Build column definitions
        columns = []
        for col_name, datatype, nullable, unique in cmd.columns:
            columns.append(ColumnDef(col_name, datatype, nullable, unique))

        # Create schema
        schema = TableSchema(cmd.table_name, columns, cmd.primary_key)

        # Register in catalog (also creates primary key index metadata)
        self.catalog.create_table(schema)

        # Create heap file
        heap_path = os.path.join(self.data_dir, schema.heap_file)
        heap = HeapFile(heap_path, schema, self.buffer_pool)
        heap.create()

        # Create primary key index file
        pk_index_name = f"{cmd.table_name}_pkey"
        pk_index_file = os.path.join(self.data_dir, f"{cmd.table_name}_pkey.idx")
        pk_index = BTreeIndex(pk_index_file, cmd.primary_key, unique=True)
        pk_index.create()

        return f"Table '{cmd.table_name}' created with primary key {cmd.primary_key}"

    # ========================================================================
    # CREATE INDEX
    # ========================================================================

    def execute_create_index(self, cmd: CreateIndexCommand) -> str:
        """Execute CREATE [UNIQUE] INDEX command"""
        schema = self.catalog.get_table(cmd.table_name)

        # Validate columns exist
        for col in cmd.columns:
            if schema.get_column(col) is None:
                raise ValueError(f"Column '{col}' does not exist in table '{cmd.table_name}'")

        # Create index metadata
        index_meta = IndexMetadata(cmd.index_name, cmd.table_name, cmd.columns, cmd.unique)
        self.catalog.create_index(index_meta)

        # Create index file
        index_file = os.path.join(self.data_dir, index_meta.index_file)
        index = BTreeIndex(index_file, cmd.columns, cmd.unique)
        index.create()

        # Populate index from existing data
        heap = self._get_heap_file(cmd.table_name)
        for tuple_obj, ctid in heap.scan_all():
            key = self._extract_key_from_tuple(tuple_obj, schema, cmd.columns)
            index.insert(key, ctid)

        return f"Index '{cmd.index_name}' created on {cmd.table_name}({', '.join(cmd.columns)})"

    # ========================================================================
    # DROP TABLE
    # ========================================================================

    def execute_drop_table(self, cmd: DropTableCommand) -> str:
        """Execute DROP TABLE command"""
        schema = self.catalog.get_table(cmd.table_name)

        # Remove heap file
        heap_path = os.path.join(self.data_dir, schema.heap_file)
        if os.path.exists(heap_path):
            os.remove(heap_path)

        # Remove all index files
        for index_meta in self.catalog.get_indexes_for_table(cmd.table_name):
            index_path = os.path.join(self.data_dir, index_meta.index_file)
            if os.path.exists(index_path):
                os.remove(index_path)

        # Remove from catalog
        self.catalog.drop_table(cmd.table_name)

        # Clear from cache
        if cmd.table_name in self.heap_files:
            del self.heap_files[cmd.table_name]

        return f"Table '{cmd.table_name}' dropped"

    # ========================================================================
    # INSERT
    # ========================================================================

    def execute_insert(self, cmd: InsertCommand) -> str:
        """Execute INSERT command"""
        schema = self.catalog.get_table(cmd.table_name)

        # Map values to columns
        if cmd.columns is None:
            # INSERT INTO table VALUES (...) - use all columns
            if len(cmd.values) != len(schema.columns):
                raise ValueError(
                    f"Value count ({len(cmd.values)}) does not match column count ({len(schema.columns)})"
                )
            values = cmd.values
        else:
            # INSERT INTO table (col1, col2) VALUES (...) - map to specified columns
            if len(cmd.values) != len(cmd.columns):
                raise ValueError(
                    f"Value count ({len(cmd.values)}) does not match specified column count ({len(cmd.columns)})"
                )

            # Build full value list with NULLs for unspecified columns
            values = []
            for col in schema.columns:
                if col.name in cmd.columns:
                    idx = cmd.columns.index(col.name)
                    values.append(cmd.values[idx])
                else:
                    # Column not specified - use NULL if nullable
                    if not col.nullable:
                        raise ValueError(f"Column '{col.name}' cannot be NULL")
                    values.append(None)

        # Validate NOT NULL constraints
        for i, col in enumerate(schema.columns):
            if not col.nullable and values[i] is None:
                raise ValueError(f"Column '{col.name}' cannot be NULL")

        # Check PRIMARY KEY uniqueness
        pk_index = self._get_primary_key_index(cmd.table_name)
        pk_value = self._extract_key(values, schema, schema.primary_key)

        if pk_index.search(pk_value) is not None:
            raise ValueError(f"Duplicate primary key: {pk_value}")

        # Check UNIQUE constraints
        for col in schema.columns:
            if col.unique and not col.name in schema.primary_key:
                # Check if this column has a unique index
                col_index = schema.get_column_index(col.name)
                col_value = values[col_index]
                if col_value is not None:  # NULL is allowed in UNIQUE columns
                    # For now, we'll skip UNIQUE enforcement on columns without indexes
                    # In a full implementation, we'd scan the table or require indexes
                    pass

        # Create tuple
        tuple_obj = Tuple(values, schema)

        # Insert into heap
        heap = self._get_heap_file(cmd.table_name)
        ctid = heap.insert_tuple(tuple_obj)

        # Update all indexes
        for index_meta in self.catalog.get_indexes_for_table(cmd.table_name):
            index = self._get_index(index_meta)
            key = self._extract_key(values, schema, index_meta.columns)
            try:
                index.insert(key, ctid)
            except ValueError as e:
                # Rollback: delete from heap and indexes
                heap.delete_tuple(ctid)
                raise e

        # Update statistics
        stats = self.catalog.get_statistics(cmd.table_name)
        stats.row_count += 1
        stats.modification_count += 1
        self.catalog.update_statistics(cmd.table_name, stats)

        return f"Inserted 1 row"

    # ========================================================================
    # SELECT
    # ========================================================================

    def execute_select(self, cmd: SelectCommand) -> List[tuple]:
        """Execute SELECT command"""
        schema = self.catalog.get_table(cmd.table_name)

        # Decide scan method: index scan or sequential scan
        scan_method = self._choose_scan_method(cmd.table_name, cmd.where)

        # Get tuples
        if scan_method == 'index':
            tuples_with_ctids = list(self._index_scan(cmd.table_name, cmd.where))
        else:
            heap = self._get_heap_file(cmd.table_name)
            tuples_with_ctids = list(heap.scan_all())

        # Filter with WHERE clause
        filtered = []
        for tuple_obj, ctid in tuples_with_ctids:
            if cmd.where is None or self._evaluate_expression(cmd.where, tuple_obj, schema):
                filtered.append(tuple_obj)

        # Apply ORDER BY
        if cmd.order_by:
            filtered = self._apply_order_by(filtered, schema, cmd.order_by)

        # Apply LIMIT and OFFSET
        if cmd.offset:
            filtered = filtered[cmd.offset:]
        if cmd.limit:
            filtered = filtered[:cmd.limit]

        # Project columns
        results = []
        if cmd.columns == ['*']:
            # Return all columns
            for tuple_obj in filtered:
                results.append(tuple(tuple_obj.values))
        else:
            # Return specific columns
            col_indexes = [schema.get_column_index(col) for col in cmd.columns]
            for tuple_obj in filtered:
                row = tuple(tuple_obj.values[i] for i in col_indexes)
                results.append(row)

        return results

    def _choose_scan_method(self, table_name: str, where_expr: Optional[Expression]) -> str:
        """Cost-based decision: index scan vs sequential scan"""
        if where_expr is None:
            return 'sequential'

        # Check if WHERE can use an index
        indexed_columns = set()
        for index_meta in self.catalog.get_indexes_for_table(table_name):
            indexed_columns.update(index_meta.columns)

        # Simple heuristic: if WHERE has equality/range on indexed column, use index
        if self._can_use_index(where_expr, indexed_columns):
            return 'index'

        return 'sequential'

    def _can_use_index(self, expr: Expression, indexed_columns: set) -> bool:
        """Check if expression can benefit from index"""
        if isinstance(expr, BinaryOp):
            if expr.op in ['=', '<', '>', '<=', '>=']:
                if isinstance(expr.left, ColumnRef) and expr.left.column_name in indexed_columns:
                    return True
        return False

    def _index_scan(self, table_name: str, where_expr: Expression):
        """Use index to find matching tuples"""
        # Extract index-able condition from WHERE
        # This is simplified - full implementation would handle complex expressions
        if isinstance(where_expr, BinaryOp):
            if where_expr.op == '=' and isinstance(where_expr.left, ColumnRef):
                col_name = where_expr.left.column_name
                value = self._literal_value(where_expr.right)

                # Find index on this column
                for index_meta in self.catalog.get_indexes_for_table(table_name):
                    if col_name in index_meta.columns:
                        index = self._get_index(index_meta)
                        ctid = index.search(value)
                        if ctid:
                            heap = self._get_heap_file(table_name)
                            tuple_obj = heap.read_tuple(ctid)
                            if tuple_obj:
                                yield (tuple_obj, ctid)
                        return

        # Fallback to sequential scan
        heap = self._get_heap_file(table_name)
        for tuple_obj, ctid in heap.scan_all():
            yield (tuple_obj, ctid)

    def _apply_order_by(self, tuples: List[Tuple], schema: TableSchema, order_by: List[tuple]) -> List[Tuple]:
        """Sort tuples by ORDER BY clause"""
        def sort_key(tuple_obj):
            key = []
            for col_name, direction in order_by:
                col_idx = schema.get_column_index(col_name)
                value = tuple_obj.values[col_idx]
                # Handle NULL values (put them last)
                if value is None:
                    value = float('inf') if direction == 'ASC' else float('-inf')
                key.append(value if direction == 'ASC' else -value if isinstance(value, (int, float)) else value)
            return tuple(key)

        return sorted(tuples, key=sort_key)

    # ========================================================================
    # UPDATE
    # ========================================================================

    def execute_update(self, cmd: UpdateCommand) -> str:
        """Execute UPDATE command"""
        schema = self.catalog.get_table(cmd.table_name)
        heap = self._get_heap_file(cmd.table_name)

        # Find matching tuples
        tuples_to_update = []
        for tuple_obj, ctid in heap.scan_all():
            if cmd.where is None or self._evaluate_expression(cmd.where, tuple_obj, schema):
                tuples_to_update.append((tuple_obj, ctid))

        # Update each tuple
        for old_tuple, old_ctid in tuples_to_update:
            # Build new values
            new_values = list(old_tuple.values)
            for col_name, value_expr in cmd.assignments:
                col_idx = schema.get_column_index(col_name)
                new_value = self._evaluate_expression(value_expr, old_tuple, schema)
                new_values[col_idx] = new_value

            # Validate constraints
            for i, col in enumerate(schema.columns):
                if not col.nullable and new_values[i] is None:
                    raise ValueError(f"Column '{col.name}' cannot be NULL")

            # Check if primary key changed
            old_pk = self._extract_key(old_tuple.values, schema, schema.primary_key)
            new_pk = self._extract_key(new_values, schema, schema.primary_key)

            if old_pk != new_pk:
                # Primary key changed - check uniqueness
                pk_index = self._get_primary_key_index(cmd.table_name)
                if pk_index.search(new_pk) is not None:
                    raise ValueError(f"Duplicate primary key: {new_pk}")

            # Delete old tuple and indexes
            heap.delete_tuple(old_ctid)
            for index_meta in self.catalog.get_indexes_for_table(cmd.table_name):
                index = self._get_index(index_meta)
                old_key = self._extract_key(old_tuple.values, schema, index_meta.columns)
                index.delete(old_key)

            # Insert new tuple
            new_tuple = Tuple(new_values, schema)
            new_ctid = heap.insert_tuple(new_tuple)

            # Update indexes
            for index_meta in self.catalog.get_indexes_for_table(cmd.table_name):
                index = self._get_index(index_meta)
                new_key = self._extract_key(new_values, schema, index_meta.columns)
                index.insert(new_key, new_ctid)

        # Update statistics
        stats = self.catalog.get_statistics(cmd.table_name)
        stats.modification_count += len(tuples_to_update)
        self.catalog.update_statistics(cmd.table_name, stats)

        return f"Updated {len(tuples_to_update)} rows"

    # ========================================================================
    # DELETE
    # ========================================================================

    def execute_delete(self, cmd: DeleteCommand) -> str:
        """Execute DELETE command"""
        schema = self.catalog.get_table(cmd.table_name)
        heap = self._get_heap_file(cmd.table_name)

        # Find matching tuples
        tuples_to_delete = []
        for tuple_obj, ctid in heap.scan_all():
            if cmd.where is None or self._evaluate_expression(cmd.where, tuple_obj, schema):
                tuples_to_delete.append((tuple_obj, ctid))

        # Delete each tuple
        for tuple_obj, ctid in tuples_to_delete:
            # Delete from heap
            heap.delete_tuple(ctid)

            # Delete from all indexes
            for index_meta in self.catalog.get_indexes_for_table(cmd.table_name):
                index = self._get_index(index_meta)
                key = self._extract_key_from_tuple(tuple_obj, schema, index_meta.columns)
                index.delete(key)

        # Update statistics
        stats = self.catalog.get_statistics(cmd.table_name)
        stats.row_count -= len(tuples_to_delete)
        stats.dead_tuple_count += len(tuples_to_delete)
        stats.modification_count += len(tuples_to_delete)
        self.catalog.update_statistics(cmd.table_name, stats)

        return f"Deleted {len(tuples_to_delete)} rows"

    # ========================================================================
    # EXPLAIN
    # ========================================================================

    def execute_explain(self, cmd: ExplainCommand) -> str:
        """Execute EXPLAIN command"""
        inner_cmd = cmd.command

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
                    plan.append(f"     Reason: WHERE clause can use index")
                else:
                    plan.append("  -> Sequential Scan")
                    plan.append(f"     Reason: No suitable index or full table scan needed")
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
                plan.append(f"  Cost: O(n log n)")

            return "\n".join(plan)
        else:
            return f"EXPLAIN not supported for {type(inner_cmd).__name__}"

    # ========================================================================
    # ANALYZE
    # ========================================================================

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
            dead_tuple_count = 0
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

    # ========================================================================
    # VACUUM
    # ========================================================================

    def execute_vacuum(self, cmd: VacuumCommand) -> str:
        """Execute VACUUM command - reclaim space"""
        if cmd.table_name:
            tables = [cmd.table_name]
        else:
            tables = self.catalog.list_tables()

        total_reclaimed = 0
        for table_name in tables:
            heap = self._get_heap_file(table_name)
            old_fsm = dict(heap.free_space_map)

            heap.vacuum()

            # Calculate reclaimed space
            new_fsm = heap.free_space_map
            for page_num in old_fsm:
                if page_num in new_fsm:
                    total_reclaimed += new_fsm[page_num] - old_fsm[page_num]

        if cmd.table_name:
            return f"Vacuumed table '{cmd.table_name}'"
        else:
            return f"Vacuumed {len(tables)} tables"

    # ========================================================================
    # Helper methods - Expression evaluation
    # ========================================================================

    def _evaluate_expression(self, expr: Expression, tuple_obj: Tuple, schema: TableSchema) -> Any:
        """Evaluate WHERE expression against tuple"""
        if isinstance(expr, BinaryOp):
            left_val = self._eval_operand(expr.left, tuple_obj, schema)
            right_val = self._eval_operand(expr.right, tuple_obj, schema)

            if expr.op == '=':
                return left_val == right_val
            elif expr.op == '!=':
                return left_val != right_val
            elif expr.op == '<':
                return left_val < right_val if (left_val is not None and right_val is not None) else False
            elif expr.op == '>':
                return left_val > right_val if (left_val is not None and right_val is not None) else False
            elif expr.op == '<=':
                return left_val <= right_val if (left_val is not None and right_val is not None) else False
            elif expr.op == '>=':
                return left_val >= right_val if (left_val is not None and right_val is not None) else False
            elif expr.op == 'LIKE':
                return self._like_match(str(left_val) if left_val is not None else '', str(right_val))
            elif expr.op == 'AND':
                return self._evaluate_expression(expr.left, tuple_obj, schema) and \
                       self._evaluate_expression(expr.right, tuple_obj, schema)
            elif expr.op == 'OR':
                return self._evaluate_expression(expr.left, tuple_obj, schema) or \
                       self._evaluate_expression(expr.right, tuple_obj, schema)

        elif isinstance(expr, UnaryOp):
            if expr.op == 'NOT':
                return not self._evaluate_expression(expr.operand, tuple_obj, schema)

        elif isinstance(expr, Literal):
            return expr.value

        elif isinstance(expr, ColumnRef):
            col_idx = schema.get_column_index(expr.column_name)
            return tuple_obj.values[col_idx]

        return True

    def _eval_operand(self, operand: Expression, tuple_obj: Tuple, schema: TableSchema) -> Any:
        """Evaluate single operand"""
        if isinstance(operand, ColumnRef):
            col_idx = schema.get_column_index(operand.column_name)
            return tuple_obj.values[col_idx]
        elif isinstance(operand, Literal):
            return operand.value
        elif isinstance(operand, (BinaryOp, UnaryOp)):
            return self._evaluate_expression(operand, tuple_obj, schema)
        else:
            return operand

    def _literal_value(self, expr: Expression) -> Any:
        """Extract literal value from expression"""
        if isinstance(expr, Literal):
            return expr.value
        else:
            raise ValueError(f"Expected literal, got {type(expr)}")

    def _like_match(self, text: str, pattern: str) -> bool:
        """SQL LIKE pattern matching: % = wildcard, _ = single char"""
        # Convert SQL LIKE pattern to regex
        regex_pattern = pattern.replace('%', '.*').replace('_', '.')
        regex_pattern = re.escape(pattern).replace('\\%', '.*').replace('\\_', '.')
        return re.match(f'^{regex_pattern}$', text) is not None

    # ========================================================================
    # Helper methods - Resource management
    # ========================================================================

    def _get_heap_file(self, table_name: str) -> HeapFile:
        """Get or load HeapFile for table"""
        if table_name not in self.heap_files:
            schema = self.catalog.get_table(table_name)
            file_path = os.path.join(self.data_dir, schema.heap_file)
            heap_file = HeapFile(file_path, schema, self.buffer_pool)

            if os.path.exists(file_path):
                heap_file.open()
            else:
                raise ValueError(f"Heap file for table '{table_name}' does not exist")

            self.heap_files[table_name] = heap_file

        return self.heap_files[table_name]

    def _get_index(self, index_meta: IndexMetadata) -> BTreeIndex:
        """Get or load BTreeIndex"""
        index_key = f"{index_meta.table_name}_{index_meta.index_name}"

        if index_key not in self.indexes:
            file_path = os.path.join(self.data_dir, index_meta.index_file)
            index = BTreeIndex(file_path, index_meta.columns, index_meta.unique)

            if os.path.exists(file_path):
                index.open()
            else:
                raise ValueError(f"Index file '{index_meta.index_file}' does not exist")

            self.indexes[index_key] = index

        return self.indexes[index_key]

    def _get_primary_key_index(self, table_name: str) -> BTreeIndex:
        """Get primary key index for table"""
        pkey_index_name = f"{table_name}_pkey"
        index_key = f"{table_name}_pkey"

        if index_key not in self.indexes:
            schema = self.catalog.get_table(table_name)
            file_path = os.path.join(self.data_dir, f"{table_name}_pkey.idx")
            index = BTreeIndex(file_path, schema.primary_key, unique=True)

            if os.path.exists(file_path):
                index.open()
            else:
                raise ValueError(f"Primary key index for '{table_name}' does not exist")

            self.indexes[index_key] = index

        return self.indexes[index_key]

    def _extract_key(self, values: List[Any], schema: TableSchema, key_columns: List[str]) -> Any:
        """Extract key value(s) from values list"""
        if len(key_columns) == 1:
            col_idx = schema.get_column_index(key_columns[0])
            return values[col_idx]
        else:
            # Composite key
            return tuple(values[schema.get_column_index(col)] for col in key_columns)

    def _extract_key_from_tuple(self, tuple_obj: Tuple, schema: TableSchema, key_columns: List[str]) -> Any:
        """Extract key value(s) from Tuple object"""
        return self._extract_key(tuple_obj.values, schema, key_columns)

    # ========================================================================
    # Shutdown
    # ========================================================================

    def shutdown(self):
        """Flush all buffers and close files"""
        self.buffer_pool.flush_all()
        self.catalog.save()
