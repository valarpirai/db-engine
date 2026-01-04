"""
DDL (Data Definition Language) command handlers.

This module provides:
- DDLMixin: Handlers for CREATE TABLE, CREATE INDEX, DROP TABLE
"""

import os

from ..catalog import TableSchema, ColumnDef, IndexMetadata
from ..storage import HeapFile
from ..btree import BTreeIndex
from ..parser import CreateTableCommand, CreateIndexCommand, DropTableCommand


class DDLMixin:
    """Mixin class for DDL command execution"""

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
        pk_index_file = os.path.join(self.data_dir, f"{cmd.table_name}_pkey.idx")
        pk_index = BTreeIndex(pk_index_file, cmd.primary_key, unique=True)
        pk_index.create()

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
