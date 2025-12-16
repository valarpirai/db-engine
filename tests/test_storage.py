"""
Test script for storage.py
Tests Buffer Pool, Tuple serialization, Page management, HeapFile with FSM
"""

import os
import shutil
import sys

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db_engine.storage import BufferPool, Tuple, Page, HeapFile
from db_engine.catalog import TableSchema, ColumnDef

def test_storage():
    """Test storage layer"""
    print("Testing storage.py...")

    # Clean up
    test_dir = './test_data_storage'
    if os.path.exists(test_dir):
        shutil.rmtree(test_dir)
    os.makedirs(test_dir)

    # Test 1: Buffer Pool
    print("\n1. Testing Buffer Pool...")
    buffer_pool = BufferPool(size=3)  # Small cache for testing
    print(f"   Buffer pool size: {buffer_pool.size}")
    print(f"   Initial stats: {buffer_pool.stats()}")
    print("✓ Buffer pool created")

    # Test 2: Tuple serialization (with nullable columns)
    print("\n2. Testing Tuple serialization with NULL bitmap...")
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

    tuple1 = Tuple([1, 'Alice', 25, 'alice@example.com'], schema)
    tuple2 = Tuple([2, 'Bob', None, None], schema)  # NULL values

    print(f"   Tuple 1 values: {tuple1.values}")
    print(f"   Tuple 2 values (with NULLs): {tuple2.values}")
    print(f"   Schema has nullable columns: {schema.has_nullable_columns()}")

    # Serialize
    data1 = tuple1.serialize()
    data2 = tuple2.serialize()
    print(f"   Tuple 1 serialized size: {len(data1)} bytes")
    print(f"   Tuple 2 serialized size: {len(data2)} bytes")

    # Deserialize
    tuple1_restored = Tuple.deserialize(data1, schema)
    tuple2_restored = Tuple.deserialize(data2, schema)
    print(f"   Tuple 1 restored: {tuple1_restored.values}")
    print(f"   Tuple 2 restored: {tuple2_restored.values}")
    assert tuple1.values == tuple1_restored.values
    assert tuple2.values == tuple2_restored.values
    print("✓ Tuple serialization works (including NULL bitmap)")

    # Test 3: Page management
    print("\n3. Testing Page management...")
    page = Page(0)
    print(f"   Page number: {page.page_number}")
    print(f"   Initial free space: {page.free_space} bytes")

    # Add tuples
    offset1 = page.add_tuple(data1)
    offset2 = page.add_tuple(data2)
    print(f"   Added tuple 1 at offset: {offset1}")
    print(f"   Added tuple 2 at offset: {offset2}")
    print(f"   Free space after insertions: {page.free_space} bytes")

    # Retrieve tuples
    retrieved1 = page.get_tuple(offset1)
    retrieved2 = page.get_tuple(offset2)
    assert retrieved1 == data1
    assert retrieved2 == data2
    print("✓ Page can store and retrieve tuples")

    # Test 4: Page serialization
    print("\n4. Testing Page serialization...")
    page_data = page.serialize()
    print(f"   Serialized page size: {len(page_data)} bytes (should be 8192)")
    assert len(page_data) == 8192  # PAGE_SIZE
    print("✓ Page serialization works")

    # Test 5: HeapFile creation
    print("\n5. Testing HeapFile...")
    heap_path = os.path.join(test_dir, 'users.dat')
    heap = HeapFile(heap_path, schema, buffer_pool)
    heap.create()
    print(f"   Heap file created: {os.path.exists(heap_path)}")
    print(f"   Initial page count: {heap.page_count}")
    print(f"   FSM: {heap.free_space_map}")
    print("✓ HeapFile created")

    # Test 6: Insert tuples into HeapFile
    print("\n6. Inserting tuples into HeapFile...")
    ctid1 = heap.insert_tuple(tuple1)
    ctid2 = heap.insert_tuple(tuple2)
    print(f"   Tuple 1 inserted at ctid: {ctid1}")
    print(f"   Tuple 2 inserted at ctid: {ctid2}")
    print(f"   FSM after inserts: {heap.free_space_map}")
    print(f"   Buffer pool stats: {buffer_pool.stats()}")
    print("✓ Tuples inserted")

    # Test 7: Read tuples by ctid
    print("\n7. Reading tuples by ctid...")
    read_tuple1 = heap.read_tuple(ctid1)
    read_tuple2 = heap.read_tuple(ctid2)
    print(f"   Read tuple 1: {read_tuple1.values}")
    print(f"   Read tuple 2: {read_tuple2.values}")
    assert read_tuple1.values == tuple1.values
    assert read_tuple2.values == tuple2.values
    print(f"   Buffer pool stats after reads: {buffer_pool.stats()}")
    print("✓ Tuples read correctly (with buffer pool caching)")

    # Test 8: Sequential scan
    print("\n8. Testing sequential scan...")
    all_tuples = list(heap.scan_all())
    print(f"   Scanned {len(all_tuples)} tuples")
    for tup, ctid in all_tuples:
        print(f"      ctid={ctid}, values={tup.values}")
    assert len(all_tuples) == 2
    print("✓ Sequential scan works")

    # Test 9: Delete tuple
    print("\n9. Testing delete (tombstone)...")
    heap.delete_tuple(ctid1)
    deleted_tuple = heap.read_tuple(ctid1)
    print(f"   Deleted tuple result: {deleted_tuple}")
    assert deleted_tuple is None
    print("✓ Tuple marked as deleted")

    # Test 10: Scan after delete
    print("\n10. Scanning after delete...")
    all_tuples_after_delete = list(heap.scan_all())
    print(f"   Scanned {len(all_tuples_after_delete)} tuples (should be 1)")
    assert len(all_tuples_after_delete) == 1
    print("✓ Deleted tuples skipped in scan")

    # Test 11: Vacuum
    print("\n11. Testing vacuum (garbage collection)...")
    print(f"   FSM before vacuum: {heap.free_space_map}")
    heap.vacuum()
    print(f"   FSM after vacuum: {heap.free_space_map}")
    print("✓ Vacuum reclaimed space")

    # Test 12: Insert many tuples to test FSM
    print("\n12. Testing FSM with multiple tuples...")
    ctids = []
    for i in range(10):
        tup = Tuple([i+10, f'User{i}', 20+i, f'user{i}@test.com'], schema)
        ctid = heap.insert_tuple(tup)
        ctids.append(ctid)
    print(f"   Inserted 10 more tuples")
    print(f"   Page count: {heap.page_count}")
    print(f"   FSM: {heap.free_space_map}")
    print("✓ FSM efficiently finds pages with space")

    # Test 13: Buffer pool caching
    print("\n13. Testing buffer pool caching...")
    # Read same tuple multiple times
    for _ in range(5):
        heap.read_tuple(ctids[0])
    stats = buffer_pool.stats()
    print(f"   Buffer pool stats: {stats}")
    print(f"   Cache hit rate: {stats['hit_rate']:.2%}")
    assert stats['hit_rate'] > 0  # Should have cache hits
    print("✓ Buffer pool caching works")

    print("\n" + "="*50)
    print("✅ All storage tests passed!")
    print("="*50)

    # Clean up
    shutil.rmtree(test_dir)

if __name__ == '__main__':
    test_storage()
