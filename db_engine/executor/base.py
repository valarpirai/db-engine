"""
Base executor functionality - helpers and expression evaluation.

This module provides:
- ExecutorBase: Base class with resource management and expression evaluation
"""

from typing import List, Dict, Optional, Any
import os
import re

from ..catalog import Catalog, TableSchema, ColumnDef, IndexMetadata, TableStatistics
from ..storage import BufferPool, Tuple, Page, HeapFile
from ..btree import BTreeIndex
from ..parser import Expression, BinaryOp, UnaryOp, Literal, ColumnRef


class ExecutorBase:
    """Base class with helper methods for query execution"""

    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        self.catalog = Catalog(data_dir)
        self.catalog.load()
        self.buffer_pool = BufferPool()
        self.heap_files: Dict[str, HeapFile] = {}
        self.indexes: Dict[str, BTreeIndex] = {}

        # Transaction state
        self.in_transaction = False
        self.transaction_operations = []
        self.transaction_index_backups = {}

    # ========================================================================
    # Expression evaluation
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
            elif expr.op == 'IS':
                # IS NULL check: left IS NULL (right is always Literal(None))
                return left_val is None

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
        regex_pattern = re.escape(pattern).replace('\\%', '.*').replace('\\_', '.')
        return re.match(f'^{regex_pattern}$', text) is not None

    # ========================================================================
    # Resource management
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

    def _rebuild_primary_key_index(self, table_name: str, schema: TableSchema):
        """Rebuild primary key index for table after schema change"""
        pkey_index_name = "pkey"
        index_metadata = IndexMetadata(
            index_name=pkey_index_name,
            table_name=table_name,
            columns=schema.primary_key,
            unique=True
        )
        self.catalog.create_index(index_metadata)

        # Create the index file
        idx_file = os.path.join(self.data_dir, index_metadata.index_file)
        index = BTreeIndex(idx_file, schema.primary_key, unique=True)
        index.create()

        # Populate index from heap file
        heap = self._get_heap_file(table_name)
        for tuple_obj, ctid in heap.scan_all():
            key = self._extract_key_from_tuple(tuple_obj, schema, schema.primary_key)
            index.insert(key, ctid)

        # Cache the index
        index_key = f"{table_name}_{pkey_index_name}"
        self.indexes[index_key] = index

        self.catalog.save()

    # ========================================================================
    # Shutdown
    # ========================================================================

    def shutdown(self):
        """Flush all buffers and close files"""
        self.buffer_pool.flush_all()
        self.catalog.save()
