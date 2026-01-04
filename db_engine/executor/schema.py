"""
Schema modification command handlers.

This module provides:
- SchemaMixin: Handlers for ALTER TABLE (ADD/DROP/RENAME COLUMN)
"""

import os
import shutil

from ..catalog import TableSchema, ColumnDef, IndexMetadata
from ..storage import Tuple, HeapFile
from ..btree import BTreeIndex
from ..parser import (
    AlterTableAddColumnCommand,
    AlterTableDropColumnCommand,
    AlterTableRenameColumnCommand
)


class SchemaMixin:
    """Mixin class for schema modification commands"""

    def execute_alter_table_add_column(self, cmd: AlterTableAddColumnCommand) -> str:
        """Execute ALTER TABLE ... ADD COLUMN"""
        schema = self.catalog.get_table(cmd.table_name)

        # Check if column already exists
        try:
            schema.get_column(cmd.column_name)
            raise ValueError(f"Column '{cmd.column_name}' already exists in table '{cmd.table_name}'")
        except ValueError:
            pass  # Column doesn't exist, which is what we want

        # Save old schema for tuple migration
        old_schema = TableSchema(
            table_name=schema.table_name,
            columns=list(schema.columns),
            primary_key=list(schema.primary_key)
        )

        # Check if table has existing data
        heap = self._get_heap_file(cmd.table_name)
        has_existing_data = any(True for _ in heap.scan_all())

        # NOT NULL columns require either no existing data or a DEFAULT value
        if not cmd.nullable and has_existing_data:
            raise ValueError(
                f"Cannot add NOT NULL column '{cmd.column_name}' to table '{cmd.table_name}' "
                f"with existing data. Use a nullable column or add the column to an empty table."
            )

        # Add column definition to schema
        new_column = ColumnDef(
            name=cmd.column_name,
            datatype=cmd.datatype,
            nullable=cmd.nullable,
            unique=cmd.unique
        )
        schema.columns.append(new_column)

        # Update all existing tuples (add NULL for new column)
        tuples_to_update = []

        # Manually scan pages to get raw tuple data
        for page_num in range(heap.page_count):
            page = heap._read_page(page_num)
            for offset, tuple_data in page.tuples:
                # Skip deleted tuples
                if len(tuple_data) > 0 and tuple_data[0] == 0xFF:
                    continue

                # Deserialize with OLD schema
                old_tuple = Tuple.deserialize(tuple_data, old_schema)

                # Create new values list with added column
                new_values = old_tuple.values + [None]

                ctid = (page_num, offset)
                tuples_to_update.append((ctid, new_values))

        # Update catalog first
        self.catalog.save()

        # Rebuild the heap file with new schema
        heap_path = os.path.join(self.data_dir, schema.heap_file)
        temp_heap_path = heap_path + ".tmp"

        # Create new heap file with updated schema
        new_heap = HeapFile(temp_heap_path, schema, self.buffer_pool)
        new_heap.create()

        # Insert all updated tuples into new heap
        for ctid, new_values in tuples_to_update:
            new_tuple = Tuple(new_values, schema)
            new_heap.insert_tuple(new_tuple)

        # Flush buffer pool to write new heap's pages to disk
        self.buffer_pool.flush_all()

        # Replace old heap file with new one
        if cmd.table_name in self.heap_files:
            del self.heap_files[cmd.table_name]

        # Clear buffer pool cache for all related files
        keys_to_remove = [k for k in self.buffer_pool.cache.keys()
                         if k[0] == heap_path or k[0] == temp_heap_path]
        for k in keys_to_remove:
            del self.buffer_pool.cache[k]

        shutil.move(temp_heap_path, heap_path)

        # Remove all old indexes for this table since ctids changed
        indexes = self.catalog.get_indexes_for_table(cmd.table_name)
        for idx_metadata in indexes:
            idx_file = os.path.join(self.data_dir, idx_metadata.index_file)
            if os.path.exists(idx_file):
                os.remove(idx_file)
            idx_key = f"{cmd.table_name}_{idx_metadata.index_name}"
            if idx_key in self.indexes:
                del self.indexes[idx_key]
            catalog_key = f"{cmd.table_name}_{idx_metadata.index_name}"
            if catalog_key in self.catalog.indexes:
                del self.catalog.indexes[catalog_key]

        # Save updated catalog without the old indexes
        self.catalog.save()

        # Rebuild primary key index (required for table operations)
        self._rebuild_primary_key_index(cmd.table_name, schema)

        # If column is unique, create an index
        if cmd.unique:
            index_name = f"{cmd.table_name}_{cmd.column_name}_idx"
            index_metadata = IndexMetadata(
                index_name=index_name,
                table_name=cmd.table_name,
                columns=[cmd.column_name],
                unique=True
            )
            self.catalog.create_index(index_metadata)

            idx_file = os.path.join(self.data_dir, index_metadata.index_file)
            index = BTreeIndex(idx_file, index_metadata.columns, unique=True)
            index.create()

        return f"Added column '{cmd.column_name}' to table '{cmd.table_name}'"

    def execute_alter_table_drop_column(self, cmd: AlterTableDropColumnCommand) -> str:
        """Execute ALTER TABLE ... DROP COLUMN"""
        schema = self.catalog.get_table(cmd.table_name)

        # Check if column exists
        try:
            col_idx = schema.get_column_index(cmd.column_name)
        except ValueError:
            raise ValueError(f"Column '{cmd.column_name}' does not exist in table '{cmd.table_name}'")

        # Cannot drop primary key column
        if cmd.column_name in schema.primary_key:
            raise ValueError(f"Cannot drop primary key column '{cmd.column_name}'")

        # Save old schema for tuple migration
        old_schema = TableSchema(
            table_name=schema.table_name,
            columns=list(schema.columns),
            primary_key=list(schema.primary_key)
        )

        # Remove column from schema
        del schema.columns[col_idx]

        # Remove any indexes on this column
        indexes_to_remove = []
        for idx_name, idx_metadata in self.catalog.indexes.items():
            if idx_metadata.table_name == cmd.table_name and cmd.column_name in idx_metadata.columns:
                indexes_to_remove.append(idx_name)

        for idx_name in indexes_to_remove:
            idx_metadata = self.catalog.indexes[idx_name]
            idx_file = os.path.join(self.data_dir, idx_metadata.index_file)
            if os.path.exists(idx_file):
                os.remove(idx_file)
            del self.catalog.indexes[idx_name]

        # Update all existing tuples (remove column value)
        heap = self._get_heap_file(cmd.table_name)
        tuples_to_update = []

        for page_num in range(heap.page_count):
            page = heap._read_page(page_num)
            for offset, tuple_data in page.tuples:
                if len(tuple_data) > 0 and tuple_data[0] == 0xFF:
                    continue

                old_tuple = Tuple.deserialize(tuple_data, old_schema)
                new_values = old_tuple.values[:col_idx] + old_tuple.values[col_idx+1:]

                ctid = (page_num, offset)
                tuples_to_update.append((ctid, new_values))

        # Update catalog first
        self.catalog.save()

        # Rebuild the heap file with new schema
        heap_path = os.path.join(self.data_dir, schema.heap_file)
        temp_heap_path = heap_path + ".tmp"

        new_heap = HeapFile(temp_heap_path, schema, self.buffer_pool)
        new_heap.create()

        for ctid, new_values in tuples_to_update:
            new_tuple = Tuple(new_values, schema)
            new_heap.insert_tuple(new_tuple)

        self.buffer_pool.flush_all()

        if cmd.table_name in self.heap_files:
            del self.heap_files[cmd.table_name]

        keys_to_remove = [k for k in self.buffer_pool.cache.keys()
                         if k[0] == heap_path or k[0] == temp_heap_path]
        for k in keys_to_remove:
            del self.buffer_pool.cache[k]

        shutil.move(temp_heap_path, heap_path)

        # Remove all remaining indexes for this table since ctids changed
        indexes = self.catalog.get_indexes_for_table(cmd.table_name)
        for idx_metadata in indexes:
            idx_file = os.path.join(self.data_dir, idx_metadata.index_file)
            if os.path.exists(idx_file):
                os.remove(idx_file)
            idx_key = f"{cmd.table_name}_{idx_metadata.index_name}"
            if idx_key in self.indexes:
                del self.indexes[idx_key]
            catalog_key = f"{cmd.table_name}_{idx_metadata.index_name}"
            if catalog_key in self.catalog.indexes:
                del self.catalog.indexes[catalog_key]

        self.catalog.save()

        # Rebuild primary key index
        self._rebuild_primary_key_index(cmd.table_name, schema)

        return f"Dropped column '{cmd.column_name}' from table '{cmd.table_name}'"

    def execute_alter_table_rename_column(self, cmd: AlterTableRenameColumnCommand) -> str:
        """Execute ALTER TABLE ... RENAME COLUMN"""
        schema = self.catalog.get_table(cmd.table_name)

        # Check if old column exists
        try:
            col_idx = schema.get_column_index(cmd.old_column_name)
        except ValueError:
            raise ValueError(f"Column '{cmd.old_column_name}' does not exist in table '{cmd.table_name}'")

        # Check if new column name already exists
        existing_column_names = [col.name for col in schema.columns]
        if cmd.new_column_name in existing_column_names:
            raise ValueError(f"Column '{cmd.new_column_name}' already exists in table '{cmd.table_name}'")

        # Rename the column
        schema.columns[col_idx].name = cmd.new_column_name

        # Update primary key if this column is part of it
        if cmd.old_column_name in schema.primary_key:
            pk_idx = schema.primary_key.index(cmd.old_column_name)
            schema.primary_key[pk_idx] = cmd.new_column_name

        # Update indexes that reference this column
        for idx_metadata in self.catalog.indexes.values():
            if idx_metadata.table_name == cmd.table_name:
                for i, col in enumerate(idx_metadata.columns):
                    if col == cmd.old_column_name:
                        idx_metadata.columns[i] = cmd.new_column_name

        # Update catalog
        self.catalog.save()

        return f"Renamed column '{cmd.old_column_name}' to '{cmd.new_column_name}' in table '{cmd.table_name}'"
