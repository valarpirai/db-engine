"""
Catalog System - Manages database metadata (tables, columns, indexes, statistics)

This module provides the system catalog that stores:
- Table schemas (columns, types, constraints)
- Index metadata
- Table statistics (for query planning)
"""

from typing import List, Dict, Optional
from dataclasses import dataclass
import os
import struct
import pickle
from .config import CATALOG_MAGIC


@dataclass
class ColumnDef:
    """Column definition with type and constraints"""
    name: str
    datatype: str  # 'INT', 'BIGINT', 'FLOAT', 'TEXT', 'BOOLEAN', 'TIMESTAMP'
    nullable: bool = True
    unique: bool = False

    def __repr__(self):
        constraints = []
        if not self.nullable:
            constraints.append("NOT NULL")
        if self.unique:
            constraints.append("UNIQUE")
        constraint_str = " " + " ".join(constraints) if constraints else ""
        return f"{self.name} {self.datatype}{constraint_str}"


@dataclass
class TableSchema:
    """Table metadata - schema definition"""
    table_name: str
    columns: List[ColumnDef]
    primary_key: List[str]  # Column names in primary key

    def __post_init__(self):
        """Generate heap file name"""
        self.heap_file = f"{self.table_name}.dat"

    def has_nullable_columns(self) -> bool:
        """Check if any columns are nullable (for null bitmap optimization)"""
        return any(col.nullable for col in self.columns)

    def get_column(self, name: str) -> Optional[ColumnDef]:
        """Get column by name"""
        for col in self.columns:
            if col.name == name:
                return col
        return None

    def get_column_index(self, name: str) -> int:
        """Get column position by name"""
        for i, col in enumerate(self.columns):
            if col.name == name:
                return i
        raise ValueError(f"Column '{name}' not found in table '{self.table_name}'")


@dataclass
class IndexMetadata:
    """Index metadata"""
    index_name: str
    table_name: str
    columns: List[str]  # Column names in index (supports composite)
    unique: bool

    def __post_init__(self):
        """Generate index file name"""
        self.index_file = f"{self.table_name}_{self.index_name}.idx"


@dataclass
class TableStatistics:
    """Table statistics for query planning"""
    table_name: str
    row_count: int = 0
    page_count: int = 0
    dead_tuple_count: int = 0
    distinct_values: Dict[str, int] = None  # column_name -> distinct count
    modification_count: int = 0  # Incremented on INSERT/UPDATE/DELETE

    def __post_init__(self):
        if self.distinct_values is None:
            self.distinct_values = {}

    def needs_update(self, threshold: int = 1000) -> bool:
        """Check if statistics need auto-update"""
        return self.modification_count >= threshold

    def dead_tuple_percentage(self) -> float:
        """Percentage of dead tuples"""
        if self.row_count == 0:
            return 0.0
        return (self.dead_tuple_count / (self.row_count + self.dead_tuple_count)) * 100


class Catalog:
    """System catalog - manages all database metadata"""

    def __init__(self, data_dir: str, catalog_file: str = 'catalog.dat'):
        self.data_dir = data_dir
        self.catalog_file = os.path.join(data_dir, catalog_file)
        self.tables: Dict[str, TableSchema] = {}
        self.indexes: Dict[str, IndexMetadata] = {}
        self.statistics: Dict[str, TableStatistics] = {}

    def load(self):
        """Load catalog from disk if it exists"""
        if not os.path.exists(self.catalog_file):
            # New database - catalog doesn't exist yet
            return

        with open(self.catalog_file, 'rb') as f:
            # Read and verify magic number
            magic = f.read(len(CATALOG_MAGIC))
            if magic != CATALOG_MAGIC:
                raise ValueError(f"Invalid catalog file: bad magic number")

            # Read version
            version = struct.unpack('I', f.read(4))[0]
            if version != 1:
                raise ValueError(f"Unsupported catalog version: {version}")

            # Read pickled data length
            data_length = struct.unpack('I', f.read(4))[0]

            # Read and unpickle the catalog data
            pickled_data = f.read(data_length)
            catalog_data = pickle.loads(pickled_data)

            # Restore catalog state
            self.tables = catalog_data.get('tables', {})
            self.indexes = catalog_data.get('indexes', {})
            self.statistics = catalog_data.get('statistics', {})

    def save(self):
        """Persist catalog to disk"""
        # Ensure data directory exists
        os.makedirs(self.data_dir, exist_ok=True)

        with open(self.catalog_file, 'wb') as f:
            # Write header: magic + version
            f.write(CATALOG_MAGIC)
            f.write(struct.pack('I', 1))  # Version 1

            # Serialize the entire catalog using pickle for simplicity
            # In production, you'd use a proper binary format, but pickle is
            # fine for educational purposes and makes serialization trivial
            catalog_data = {
                'tables': self.tables,
                'indexes': self.indexes,
                'statistics': self.statistics
            }

            pickled_data = pickle.dumps(catalog_data)

            # Write length of pickled data, then the data itself
            f.write(struct.pack('I', len(pickled_data)))
            f.write(pickled_data)

    def create_table(self, schema: TableSchema):
        """Register a new table in the catalog"""
        # Validate table doesn't already exist
        if schema.table_name in self.tables:
            raise ValueError(f"Table '{schema.table_name}' already exists")

        # Validate primary key exists
        if not schema.primary_key:
            raise ValueError(f"Table '{schema.table_name}' must have a PRIMARY KEY")

        # Validate primary key columns exist
        for pk_col in schema.primary_key:
            if not schema.get_column(pk_col):
                raise ValueError(f"PRIMARY KEY column '{pk_col}' not found in table")

        # Add table to catalog
        self.tables[schema.table_name] = schema

        # Initialize statistics
        self.statistics[schema.table_name] = TableStatistics(schema.table_name)

        # Auto-create primary key index
        pk_index_name = "pkey"
        pk_index = IndexMetadata(
            index_name=pk_index_name,
            table_name=schema.table_name,
            columns=schema.primary_key,
            unique=True
        )
        self.indexes[f"{schema.table_name}_{pk_index_name}"] = pk_index

        # Persist changes
        self.save()

    def drop_table(self, table_name: str):
        """Remove table and all its indexes"""
        if table_name not in self.tables:
            raise ValueError(f"Table '{table_name}' does not exist")

        # Remove table
        del self.tables[table_name]

        # Remove statistics
        if table_name in self.statistics:
            del self.statistics[table_name]

        # Remove all indexes for this table
        indexes_to_remove = [
            idx_name for idx_name, idx in self.indexes.items()
            if idx.table_name == table_name
        ]
        for idx_name in indexes_to_remove:
            del self.indexes[idx_name]

        # Persist changes
        self.save()

    def get_table(self, table_name: str) -> TableSchema:
        """Retrieve table schema"""
        if table_name not in self.tables:
            raise ValueError(f"Table '{table_name}' does not exist")
        return self.tables[table_name]

    def create_index(self, index_metadata: IndexMetadata):
        """Register a new index in the catalog"""
        # Validate table exists
        if index_metadata.table_name not in self.tables:
            raise ValueError(f"Table '{index_metadata.table_name}' does not exist")

        schema = self.tables[index_metadata.table_name]

        # Validate columns exist
        for col_name in index_metadata.columns:
            if not schema.get_column(col_name):
                raise ValueError(f"Column '{col_name}' not found in table '{index_metadata.table_name}'")

        # Validate index doesn't already exist
        idx_key = f"{index_metadata.table_name}_{index_metadata.index_name}"
        if idx_key in self.indexes:
            raise ValueError(f"Index '{index_metadata.index_name}' already exists on table '{index_metadata.table_name}'")

        # Add index to catalog
        self.indexes[idx_key] = index_metadata

        # Persist changes
        self.save()

    def get_indexes_for_table(self, table_name: str) -> List[IndexMetadata]:
        """Get all indexes for a table"""
        return [
            idx for idx_name, idx in self.indexes.items()
            if idx.table_name == table_name
        ]

    def get_statistics(self, table_name: str) -> TableStatistics:
        """Get statistics for a table"""
        if table_name not in self.statistics:
            # Initialize if not exists
            self.statistics[table_name] = TableStatistics(table_name)
        return self.statistics[table_name]

    def update_statistics(self, table_name: str, stats: TableStatistics):
        """Update statistics for a table"""
        self.statistics[table_name] = stats
        # Persist changes
        self.save()

    def list_tables(self) -> List[str]:
        """List all table names"""
        return list(self.tables.keys())

    def list_indexes(self) -> List[str]:
        """List all index names"""
        return list(self.indexes.keys())
