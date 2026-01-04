"""
DDL (Data Definition Language) command handlers.

This module provides:
- DDLMixin: Handlers for CREATE TABLE, CREATE INDEX, DROP TABLE, DROP INDEX, TRUNCATE TABLE
"""

import os

from ..catalog import TableSchema, ColumnDef, IndexMetadata
from ..storage import HeapFile
from ..btree import BTreeIndex
from ..parser import CreateTableCommand, CreateIndexCommand, DropTableCommand, DropIndexCommand, TruncateTableCommand


class DDLMixin:
    """Mixin class for DDL command execution"""

    def execute_create_table(self, cmd: CreateTableCommand) -> str:
        """Execute CREATE TABLE command"""
        # Build column definitions
        columns = []
        autoincrement_cols = []
        for col_tuple in cmd.columns:
            # Handle both 4-tuple (legacy) and 5-tuple (with autoincrement)
            if len(col_tuple) == 5:
                col_name, datatype, nullable, unique, autoincrement = col_tuple
            else:
                col_name, datatype, nullable, unique = col_tuple
                autoincrement = False

            # Validate autoincrement constraints
            if autoincrement:
                if datatype not in ('INT', 'BIGINT'):
                    raise ValueError(f"AUTOINCREMENT can only be used with INT or BIGINT columns")
                autoincrement_cols.append(col_name)

            columns.append(ColumnDef(col_name, datatype, nullable, unique, autoincrement))

        # Create schema
        schema = TableSchema(cmd.table_name, columns, cmd.primary_key)

        # Register in catalog (also creates primary key index metadata)
        self.catalog.create_table(schema)

        # Create heap file
        heap_path = os.path.join(self.data_dir, schema.heap_file)
        heap = HeapFile(heap_path, schema, self.buffer_pool)
        heap.create()

        # Create primary key index file
        pk_index_file = os.path.join(self.data_dir, f"{cmd.table_name}_pkey.idx")
        pk_index = BTreeIndex(pk_index_file, cmd.primary_key, unique=True)
        pk_index.create()

        # Initialize autoincrement counters
        if autoincrement_cols:
            stats = self.catalog.get_statistics(cmd.table_name)
            for col in autoincrement_cols:
                stats.autoincrement_counters[col] = 1  # Start at 1
            self.catalog.update_statistics(cmd.table_name, stats)

        return f"Table '{cmd.table_name}' created with primary key {cmd.primary_key}"

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

    def execute_drop_index(self, cmd: DropIndexCommand) -> str:
        """Execute DROP INDEX command"""
        # Validate table exists
        schema = self.catalog.get_table(cmd.table_name)

        # Get index metadata to find file path
        idx_key = f"{cmd.table_name}_{cmd.index_name}"
        if idx_key not in self.catalog.indexes:
            raise ValueError(f"Index '{cmd.index_name}' does not exist on table '{cmd.table_name}'")

        index_meta = self.catalog.indexes[idx_key]

        # Remove index file
        index_path = os.path.join(self.data_dir, index_meta.index_file)
        if os.path.exists(index_path):
            os.remove(index_path)

        # Remove from catalog
        self.catalog.drop_index(cmd.index_name, cmd.table_name)

        # Clear from cache
        if idx_key in self.indexes:
            del self.indexes[idx_key]

        return f"Index '{cmd.index_name}' dropped from table '{cmd.table_name}'"

    def execute_truncate_table(self, cmd: TruncateTableCommand) -> str:
        """Execute TRUNCATE TABLE command - fast table clear"""
        schema = self.catalog.get_table(cmd.table_name)

        # Clear caches FIRST (before removing files)
        if cmd.table_name in self.heap_files:
            del self.heap_files[cmd.table_name]
        # Clear any cached indexes for this table
        indexes_to_clear = [k for k in self.indexes if k.startswith(f"{cmd.table_name}_")]
        for idx_key in indexes_to_clear:
            del self.indexes[idx_key]

        # Clear buffer pool pages for this table's heap file
        heap_path = os.path.join(self.data_dir, schema.heap_file)
        # Remove pages from buffer pool that belong to this file
        pages_to_remove = [key for key in self.buffer_pool.cache.keys() if key[0] == heap_path]
        for key in pages_to_remove:
            del self.buffer_pool.cache[key]
            self.buffer_pool.dirty_pages.discard(key)

        # Clear heap file (recreate it)
        if os.path.exists(heap_path):
            os.remove(heap_path)

        # Recreate empty heap file
        heap = HeapFile(heap_path, schema, self.buffer_pool)
        heap.create()

        # Clear all indexes and recreate them
        for index_meta in self.catalog.get_indexes_for_table(cmd.table_name):
            index_path = os.path.join(self.data_dir, index_meta.index_file)
            if os.path.exists(index_path):
                os.remove(index_path)

            # Recreate empty index
            index = BTreeIndex(index_path, index_meta.columns, index_meta.unique)
            index.create()

        # Reset statistics (but keep autoincrement counters)
        stats = self.catalog.get_statistics(cmd.table_name)
        old_counters = stats.autoincrement_counters.copy() if stats.autoincrement_counters else {}
        stats.row_count = 0
        stats.page_count = 0
        stats.dead_tuple_count = 0
        stats.modification_count = 0
        stats.distinct_values = {}
        stats.autoincrement_counters = old_counters  # Preserve autoincrement
        self.catalog.update_statistics(cmd.table_name, stats)

        return f"Table '{cmd.table_name}' truncated"
