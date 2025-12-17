"""
Test script for btree.py
Tests B-tree node serialization, insert with splitting, search, range queries, delete
"""

import os
import shutil
import sys

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db_engine.btree import BTreeNode, BTreeIndex

def test_btree():
    """Test B-tree index"""
    print("Testing btree.py...")

    # Clean up
    test_dir = './test_data_btree'
    if os.path.exists(test_dir):
        shutil.rmtree(test_dir)
    os.makedirs(test_dir)

    # Test 1: BTreeNode creation and basic properties
    print("\n1. Testing BTreeNode creation...")
    node = BTreeNode(is_leaf=True)
    print(f"   Is leaf: {node.is_leaf}")
    print(f"   Is full: {node.is_full()}")
    print(f"   Keys: {node.keys}")
    print("✓ BTreeNode created")

    # Test 2: Key truncation
    print("\n2. Testing key truncation...")
    long_text = "This is a very long text that should be truncated"
    truncated = BTreeNode.truncate_key(long_text)
    print(f"   Original length: {len(long_text)}")
    print(f"   Truncated length: {len(truncated)}")
    print(f"   Truncated text: '{truncated}'")
    assert len(truncated) <= 10
    print("✓ Key truncation works")

    # Test 3: Composite key truncation
    print("\n3. Testing composite key truncation...")
    composite_key = (123, "LongTextValue", 456.789)
    truncated_composite = BTreeNode.truncate_key(composite_key)
    print(f"   Original: {composite_key}")
    print(f"   Truncated: {truncated_composite}")
    assert len(truncated_composite[1]) <= 10
    print("✓ Composite key truncation works")

    # Test 4: Key comparison
    print("\n4. Testing key comparison...")
    assert BTreeNode.compare_keys(10, 20) == -1
    assert BTreeNode.compare_keys(20, 20) == 0
    assert BTreeNode.compare_keys(30, 20) == 1
    assert BTreeNode.compare_keys((1, 'a'), (1, 'b')) == -1
    print("✓ Key comparison works")

    # Test 5: Node serialization/deserialization
    print("\n5. Testing node serialization...")
    node.keys = [10, 20, 30]
    node.values = [(0, 100), (0, 200), (0, 300)]
    serialized = node.serialize()
    print(f"   Serialized size: {len(serialized)} bytes (should be 4096)")
    assert len(serialized) == 4096

    deserialized = BTreeNode.deserialize(serialized, file_offset=0)
    print(f"   Deserialized keys: {deserialized.keys}")
    print(f"   Deserialized values: {deserialized.values}")
    assert deserialized.keys == node.keys
    assert deserialized.values == node.values
    print("✓ Node serialization works")

    # Test 6: BTreeIndex creation
    print("\n6. Testing BTreeIndex creation...")
    index_file = os.path.join(test_dir, 'test_index.idx')
    index = BTreeIndex(index_file, key_columns=['id'], unique=True)
    index.create()
    print(f"   Index file created: {os.path.exists(index_file)}")
    print(f"   Root is leaf: {index.root.is_leaf}")
    print(f"   Node count: {index.node_count}")
    print("✓ BTreeIndex created")

    # Test 7: Insert and search
    print("\n7. Testing insert and search...")
    index.insert(10, (0, 100))
    index.insert(20, (0, 200))
    index.insert(5, (0, 50))
    print(f"   Inserted 3 keys: 10, 20, 5")

    # Debug: check root
    print(f"   Root keys: {index.root.keys}")
    print(f"   Root values: {index.root.values}")
    print(f"   Root is leaf: {index.root.is_leaf}")

    index.insert(15, (0, 150))
    print(f"   Inserted key 15")

    # Debug: check root after potential split
    print(f"   Root keys after 4th insert: {index.root.keys}")
    print(f"   Root values: {index.root.values}")
    print(f"   Root is leaf: {index.root.is_leaf}")

    result = index.search(10)
    print(f"   Search for key 10: {result}")
    assert result == (0, 100)

    result = index.search(15)
    print(f"   Search for key 15: {result}")
    assert result == (0, 150)

    result = index.search(99)
    print(f"   Search for key 99 (not found): {result}")
    assert result is None
    print("✓ Insert and search work")

    # Test 8: Uniqueness constraint
    print("\n8. Testing uniqueness constraint...")
    try:
        index.insert(10, (1, 100))  # Duplicate key
        print("✗ Should have raised ValueError for duplicate key")
        assert False
    except ValueError as e:
        print(f"   ✓ Correctly raised error: {e}")

    # Test 9: Reopen index
    print("\n9. Testing index persistence...")
    index2 = BTreeIndex(index_file, key_columns=['id'], unique=True)
    index2.open()
    print(f"   Reopened index, node count: {index2.node_count}")
    result = index2.search(20)
    print(f"   Search after reopen: {result}")
    assert result == (0, 200)
    print("✓ Index persistence works")

    # Test 10: Insert many keys to trigger splits
    print("\n10. Testing insert with splitting...")
    index3_file = os.path.join(test_dir, 'test_index3.idx')
    index3 = BTreeIndex(index3_file, key_columns=['id'], unique=False)
    index3.create()

    # Insert enough keys to cause splits (ORDER=4, so 3 keys max per node)
    keys = [50, 10, 90, 30, 70, 20, 40, 60, 80, 100, 5, 15, 25, 35]
    for key in keys:
        index3.insert(key, (0, key * 10))
    print(f"   Inserted {len(keys)} keys")
    print(f"   Node count after splits: {index3.node_count}")

    # Verify all keys can be found
    for key in keys:
        result = index3.search(key)
        assert result == (0, key * 10), f"Key {key} not found correctly"
    print("✓ Insert with splitting works")

    # Test 11: Range query
    print("\n11. Testing range query...")
    results = index3.range_query(20, 50)
    result_keys = [r[1] // 10 for r in results]  # Extract keys from ctids
    print(f"   Range query [20, 50]: found {len(results)} keys")
    print(f"   Keys: {sorted(result_keys)}")
    assert 20 in result_keys
    assert 50 in result_keys
    assert 10 not in result_keys  # Outside range
    assert 90 not in result_keys  # Outside range
    print("✓ Range query works")

    # Test 12: Delete
    print("\n12. Testing delete...")
    index3.delete(30)
    result = index3.search(30)
    print(f"   Deleted key 30, search result: {result}")
    assert result is None

    # Verify other keys still exist
    result = index3.search(20)
    assert result == (0, 200)
    print("✓ Delete works")

    # Test 13: Composite keys
    print("\n13. Testing composite keys...")
    index4_file = os.path.join(test_dir, 'test_composite.idx')
    index4 = BTreeIndex(index4_file, key_columns=['category', 'id'], unique=False)
    index4.create()

    index4.insert(('electronics', 1), (0, 100))
    index4.insert(('electronics', 2), (0, 200))
    index4.insert(('books', 1), (0, 300))
    index4.insert(('electronics', 3), (0, 400))
    print("   Inserted composite keys")

    result = index4.search(('electronics', 2))
    print(f"   Search for ('electronics', 2): {result}")
    assert result == (0, 200)

    result = index4.search(('books', 1))
    print(f"   Search for ('books', 1): {result}")
    assert result == (0, 300)
    print("✓ Composite keys work")

    # Test 14: TEXT key truncation in index
    print("\n14. Testing TEXT key truncation in index...")
    index5_file = os.path.join(test_dir, 'test_text.idx')
    index5 = BTreeIndex(index5_file, key_columns=['name'], unique=False)
    index5.create()

    long_key = "VeryLongTextKeyThatExceedsMaxLength"
    index5.insert(long_key, (0, 1000))

    # Search with truncated key should work
    truncated_search = long_key[:10]
    result = index5.search(truncated_search)
    print(f"   Original key: '{long_key}'")
    print(f"   Truncated for search: '{truncated_search}'")
    print(f"   Search result: {result}")
    assert result == (0, 1000)
    print("✓ TEXT key truncation in index works")

    print("\n" + "="*50)
    print("✅ All B-tree tests passed!")
    print("="*50)

    # Clean up
    shutil.rmtree(test_dir)

if __name__ == '__main__':
    test_btree()
