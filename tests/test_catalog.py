"""
Quick test script for catalog.py
Tests basic catalog functionality: create/load/save tables, indexes, statistics
"""

import os
import shutil
import sys

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db_engine.catalog import Catalog, TableSchema, ColumnDef, IndexMetadata, TableStatistics

def test_catalog():
    """Test catalog basic operations"""
    print("Testing catalog.py...")

    # Clean up any existing test data
    test_dir = './test_data'
    if os.path.exists(test_dir):
        shutil.rmtree(test_dir)
    os.makedirs(test_dir)

    # Test 1: Create catalog
    print("\n1. Creating catalog...")
    catalog = Catalog(test_dir)
    catalog.load()  # Should work on empty directory
    print("✓ Catalog created")

    # Test 2: Create table schema
    print("\n2. Creating table schema...")
    schema = TableSchema(
        table_name='users',
        columns=[
            ColumnDef('id', 'INT', nullable=False),
            ColumnDef('name', 'TEXT', nullable=False),
            ColumnDef('age', 'INT', nullable=True),
            ColumnDef('email', 'TEXT', nullable=True, unique=True)
        ],
        primary_key=['id']
    )
    print(f"   Table: {schema.table_name}")
    print(f"   Columns: {[str(col) for col in schema.columns]}")
    print(f"   Primary key: {schema.primary_key}")
    print(f"   Has nullable columns: {schema.has_nullable_columns()}")
    print("✓ Schema created")

    # Test 3: Add table to catalog
    print("\n3. Adding table to catalog...")
    catalog.create_table(schema)
    print(f"   Tables in catalog: {catalog.list_tables()}")
    print(f"   Indexes in catalog: {catalog.list_indexes()}")
    print("✓ Table added (with auto-created primary key index)")

    # Test 4: Create secondary index
    print("\n4. Creating secondary index...")
    idx = IndexMetadata(
        index_name='idx_age',
        table_name='users',
        columns=['age'],
        unique=False
    )
    catalog.create_index(idx)
    print(f"   Indexes: {catalog.list_indexes()}")
    print("✓ Index created")

    # Test 5: Update statistics
    print("\n5. Updating statistics...")
    stats = catalog.get_statistics('users')
    stats.row_count = 100
    stats.page_count = 2
    stats.modification_count = 50
    stats.distinct_values['age'] = 25
    catalog.update_statistics('users', stats)
    print(f"   Row count: {stats.row_count}")
    print(f"   Page count: {stats.page_count}")
    print(f"   Needs update: {stats.needs_update(threshold=1000)}")
    print("✓ Statistics updated")

    # Test 6: Save catalog
    print("\n6. Saving catalog to disk...")
    catalog.save()
    catalog_file = os.path.join(test_dir, 'catalog.dat')
    print(f"   Catalog file exists: {os.path.exists(catalog_file)}")
    print(f"   Catalog file size: {os.path.getsize(catalog_file)} bytes")
    print("✓ Catalog saved")

    # Test 7: Load catalog from disk
    print("\n7. Loading catalog from disk...")
    catalog2 = Catalog(test_dir)
    catalog2.load()
    print(f"   Tables loaded: {catalog2.list_tables()}")
    print(f"   Indexes loaded: {catalog2.list_indexes()}")

    # Verify data matches
    loaded_schema = catalog2.get_table('users')
    print(f"   Schema matches: {loaded_schema.table_name == schema.table_name}")
    print(f"   Columns match: {len(loaded_schema.columns) == len(schema.columns)}")

    loaded_stats = catalog2.get_statistics('users')
    print(f"   Stats match: row_count={loaded_stats.row_count}, page_count={loaded_stats.page_count}")
    print("✓ Catalog loaded successfully")

    # Test 8: Get indexes for table
    print("\n8. Getting indexes for table...")
    indexes = catalog2.get_indexes_for_table('users')
    print(f"   Indexes for 'users': {[idx.index_name for idx in indexes]}")
    print("✓ Index lookup works")

    # Test 9: Error handling
    print("\n9. Testing error handling...")
    try:
        catalog2.create_table(schema)  # Should fail - already exists
        print("✗ Should have raised error for duplicate table")
    except ValueError as e:
        print(f"   ✓ Correctly raised error: {e}")

    try:
        catalog2.get_table('nonexistent')  # Should fail
        print("✗ Should have raised error for nonexistent table")
    except ValueError as e:
        print(f"   ✓ Correctly raised error: {e}")

    # Test 10: Drop table
    print("\n10. Dropping table...")
    print(f"   Before: {catalog2.list_tables()}, {catalog2.list_indexes()}")
    catalog2.drop_table('users')
    print(f"   After: {catalog2.list_tables()}, {catalog2.list_indexes()}")
    print("✓ Table and indexes dropped")

    print("\n" + "="*50)
    print("✅ All catalog tests passed!")
    print("="*50)

    # Clean up
    shutil.rmtree(test_dir)

if __name__ == '__main__':
    test_catalog()
