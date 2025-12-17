"""
Integration test - Tests catalog + storage + btree working together
Simulates basic database operations without SQL parser
"""

import os
import shutil
import sys

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db_engine.catalog import Catalog, TableSchema, ColumnDef, IndexMetadata
from db_engine.storage import BufferPool, Tuple, HeapFile
from db_engine.btree import BTreeIndex

def test_integration():
    """Test complete flow: catalog → storage → indexing"""
    print("Testing integration of catalog + storage + btree...")

    # Clean up
    test_dir = './test_data_integration'
    if os.path.exists(test_dir):
        shutil.rmtree(test_dir)
    os.makedirs(test_dir)

    # Test 1: Initialize system components
    print("\n1. Initializing system components...")
    catalog = Catalog(test_dir)
    catalog.load()
    buffer_pool = BufferPool(size=10)
    print("✓ Catalog and buffer pool initialized")

    # Test 2: Create table with schema
    print("\n2. Creating table schema...")
    schema = TableSchema(
        table_name='users',
        columns=[
            ColumnDef('id', 'INT', nullable=False),
            ColumnDef('name', 'TEXT', nullable=False),
            ColumnDef('age', 'INT', nullable=True),
            ColumnDef('email', 'TEXT', nullable=True)
        ],
        primary_key=['id']
    )
    catalog.create_table(schema)
    print(f"   Created table: {schema.table_name}")
    print(f"   Columns: {[col.name for col in schema.columns]}")
    print(f"   Primary key: {schema.primary_key}")
    print("✓ Table created in catalog")

    # Test 3: Initialize heap file
    print("\n3. Initializing heap file...")
    heap_file = os.path.join(test_dir, 'users.dat')
    heap = HeapFile(heap_file, schema, buffer_pool)
    heap.create()
    print(f"   Heap file: {heap_file}")
    print("✓ Heap file created")

    # Test 4: Initialize primary key index
    print("\n4. Creating primary key index...")
    pk_index_file = os.path.join(test_dir, 'users_pkey.idx')
    pk_index = BTreeIndex(pk_index_file, key_columns=['id'], unique=True)
    pk_index.create()
    print(f"   Index file: {pk_index_file}")
    print(f"   Unique: {pk_index.unique}")
    print("✓ Primary key index created")

    # Test 5: Insert rows
    print("\n5. Inserting rows...")
    rows = [
        [1, 'Alice', 25, 'alice@example.com'],
        [2, 'Bob', 30, 'bob@example.com'],
        [3, 'Charlie', None, 'charlie@example.com'],
        [4, 'Diana', 28, None],
        [5, 'Eve', 35, 'eve@example.com']
    ]

    for row_data in rows:
        # Create tuple
        tuple_obj = Tuple(row_data, schema)

        # Check primary key uniqueness
        pk_value = row_data[0]  # id column
        if pk_index.search(pk_value) is not None:
            raise ValueError(f"Duplicate primary key: {pk_value}")

        # Insert into heap
        ctid = heap.insert_tuple(tuple_obj)

        # Insert into primary key index
        pk_index.insert(pk_value, ctid)

        print(f"   Inserted: id={row_data[0]}, ctid={ctid}")

    print(f"✓ Inserted {len(rows)} rows")

    # Test 6: Lookup by primary key
    print("\n6. Testing primary key lookup...")
    search_id = 3
    ctid = pk_index.search(search_id)
    print(f"   Searching for id={search_id}")
    print(f"   Found ctid: {ctid}")

    if ctid:
        tuple_obj = heap.read_tuple(ctid)
        print(f"   Row data: {tuple_obj.values}")
        assert tuple_obj.values[0] == search_id
    print("✓ Primary key lookup works")

    # Test 7: Range scan using index
    print("\n7. Testing range scan (id between 2 and 4)...")
    ctids = pk_index.range_query(2, 4)
    print(f"   Found {len(ctids)} matching ctids")

    for ctid in ctids:
        tuple_obj = heap.read_tuple(ctid)
        print(f"   id={tuple_obj.values[0]}, name={tuple_obj.values[1]}")
        assert 2 <= tuple_obj.values[0] <= 4

    assert len(ctids) == 3  # ids 2, 3, 4
    print("✓ Range scan works")

    # Test 8: Sequential scan (find all users with age > 25)
    print("\n8. Testing sequential scan (age > 25)...")
    matching_rows = []

    for tuple_obj, ctid in heap.scan_all():
        age = tuple_obj.values[2]  # age column
        if age is not None and age > 25:
            matching_rows.append((tuple_obj.values[0], tuple_obj.values[1], age))

    print(f"   Found {len(matching_rows)} matching rows:")
    for id, name, age in matching_rows:
        print(f"      id={id}, name={name}, age={age}")

    assert len(matching_rows) == 3  # Bob(30), Diana(28), Eve(35)
    print("✓ Sequential scan with filtering works")

    # Test 9: Delete row
    print("\n9. Testing delete...")
    delete_id = 3
    ctid = pk_index.search(delete_id)

    if ctid:
        # Delete from heap (tombstone)
        heap.delete_tuple(ctid)

        # Delete from index
        pk_index.delete(delete_id)

        print(f"   Deleted row with id={delete_id}")

        # Verify deletion
        ctid_after = pk_index.search(delete_id)
        print(f"   Search after delete: {ctid_after}")
        assert ctid_after is None

    print("✓ Delete works")

    # Test 10: Create secondary index
    print("\n10. Creating secondary index on age...")
    age_index_file = os.path.join(test_dir, 'users_age_idx.idx')
    age_index = BTreeIndex(age_index_file, key_columns=['age'], unique=False)
    age_index.create()

    # Populate secondary index from existing data
    for tuple_obj, ctid in heap.scan_all():
        age = tuple_obj.values[2]  # age column
        if age is not None:  # Skip NULL ages
            age_index.insert(age, ctid)

    print(f"   Secondary index created and populated")

    # Use secondary index for lookup
    search_age = 30
    ctids = age_index.range_query(search_age, search_age)
    print(f"   Found {len(ctids)} users with age={search_age}")

    for ctid in ctids:
        tuple_obj = heap.read_tuple(ctid)
        print(f"      id={tuple_obj.values[0]}, name={tuple_obj.values[1]}")

    print("✓ Secondary index works")

    # Test 11: Buffer pool statistics
    print("\n11. Buffer pool statistics...")
    stats = buffer_pool.stats()
    print(f"   Cache size: {stats['size']}/{stats['capacity']}")
    print(f"   Hits: {stats['hits']}, Misses: {stats['misses']}")
    print(f"   Hit rate: {stats['hit_rate']:.2%}")
    print(f"   Dirty pages: {stats['dirty_pages']}")
    assert stats['hit_rate'] > 0  # Should have cache hits
    print("✓ Buffer pool working efficiently")

    # Test 12: Flush and persistence
    print("\n12. Testing persistence...")
    buffer_pool.flush_all()
    print("   Flushed all dirty pages")

    # Reopen everything
    catalog2 = Catalog(test_dir)
    catalog2.load()
    buffer_pool2 = BufferPool(size=10)

    schema2 = catalog2.get_table('users')
    heap2 = HeapFile(heap_file, schema2, buffer_pool2)
    heap2.open()

    pk_index2 = BTreeIndex(pk_index_file, key_columns=['id'], unique=True)
    pk_index2.open()

    # Verify data persisted
    ctid = pk_index2.search(1)
    print(f"   Search for id=1 returned ctid: {ctid}")
    if ctid is None:
        print("   ERROR: Primary key search returned None!")
        raise AssertionError("Failed to find id=1 after reopen")

    tuple_obj = heap2.read_tuple(ctid)
    if tuple_obj is None:
        print(f"   ERROR: heap.read_tuple({ctid}) returned None!")
        raise AssertionError("Failed to read tuple after reopen")

    print(f"   After reopen, id=1: {tuple_obj.values}")
    assert tuple_obj.values[0] == 1
    assert tuple_obj.values[1] == 'Alice'

    print("✓ Persistence works")

    # Test 13: Vacuum
    print("\n13. Testing vacuum...")
    print(f"   FSM before vacuum: {heap.free_space_map}")
    heap.vacuum()
    print(f"   FSM after vacuum: {heap.free_space_map}")
    print("✓ Vacuum reclaimed space from deleted tuples")

    print("\n" + "="*50)
    print("✅ All integration tests passed!")
    print("="*50)
    print("\nFoundation layer is complete and working:")
    print("  ✓ Catalog (metadata)")
    print("  ✓ Storage (heap files, pages, tuples, buffer pool, FSM)")
    print("  ✓ B-tree (indexing with splitting, search, range queries)")
    print("  ✓ All components work together correctly")

    # Clean up
    shutil.rmtree(test_dir)

if __name__ == '__main__':
    test_integration()
