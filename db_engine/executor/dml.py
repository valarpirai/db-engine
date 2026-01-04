"""
DML (Data Manipulation Language) command handlers.

This module provides:
- DMLMixin: Handlers for INSERT, SELECT, UPDATE, DELETE
"""

from typing import List, Optional

from ..storage import Tuple
from ..parser import (
    InsertCommand, SelectCommand, UpdateCommand, DeleteCommand,
    Expression, BinaryOp, ColumnRef
)


class DMLMixin:
    """Mixin class for DML command execution"""

    def execute_insert(self, cmd: InsertCommand) -> str:
        """Execute INSERT command"""
        schema = self.catalog.get_table(cmd.table_name)
        stats = self.catalog.get_statistics(cmd.table_name)

        # Map values to columns
        if cmd.columns is None:
            # INSERT INTO table VALUES (...) - use all columns
            if len(cmd.values) != len(schema.columns):
                raise ValueError(
                    f"Value count ({len(cmd.values)}) does not match column count ({len(schema.columns)})"
                )
            values = list(cmd.values)
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
                    # Column not specified - use NULL (will be auto-filled for autoincrement)
                    values.append(None)

        # Handle AUTOINCREMENT columns
        autoincrement_used = False
        for i, col in enumerate(schema.columns):
            if col.autoincrement and values[i] is None:
                # Auto-generate next value
                next_val = stats.autoincrement_counters.get(col.name, 1)
                values[i] = next_val
                stats.autoincrement_counters[col.name] = next_val + 1
                autoincrement_used = True

        # Validate NOT NULL constraints (after autoincrement fills in values)
        for i, col in enumerate(schema.columns):
            if not col.nullable and not col.autoincrement and values[i] is None:
                raise ValueError(f"Column '{col.name}' cannot be NULL")

        # Check PRIMARY KEY uniqueness
        pk_index = self._get_primary_key_index(cmd.table_name)
        pk_value = self._extract_key(values, schema, schema.primary_key)

        if pk_index.search(pk_value) is not None:
            raise ValueError(f"Duplicate primary key: {pk_value}")

        # Check UNIQUE constraints
        for col in schema.columns:
            if col.unique and col.name not in schema.primary_key:
                col_index = schema.get_column_index(col.name)
                col_value = values[col_index]
                if col_value is not None:
                    # For now, skip UNIQUE enforcement on columns without indexes
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

        return "Inserted 1 row"

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
            for tuple_obj in filtered:
                results.append(tuple(tuple_obj.values))
        else:
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

    def _apply_order_by(self, tuples: List, schema, order_by: List[tuple]) -> List:
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
