"""
DB Engine - Educational Database Implementation

A PostgreSQL-inspired database engine built from scratch in Python.
"""

__version__ = '0.1.0'

from db_engine.config import *
from db_engine.catalog import Catalog, TableSchema, ColumnDef, IndexMetadata, TableStatistics
from db_engine.storage import BufferPool, Tuple, Page, HeapFile
from db_engine.btree import BTreeNode, BTreeIndex

__all__ = [
    'Catalog', 'TableSchema', 'ColumnDef', 'IndexMetadata', 'TableStatistics',
    'BufferPool', 'Tuple', 'Page', 'HeapFile',
    'BTreeNode', 'BTreeIndex'
]
