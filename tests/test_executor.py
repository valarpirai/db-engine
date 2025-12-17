"""
Test script for executor.py
Tests complete end-to-end SQL execution
"""

import sys
import os
import shutil

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db_engine.executor import QueryExecutor
from db_engine.parser import parse_sql

def test_executor():
    """Test query executor end-to-end"""
    print("Testing executor.py...")

    # Clean up
    test_dir = './test_data_executor'
    if os.path.exists(test_dir):
        shutil.rmtree(test_dir)
    os.makedirs(test_dir)

    # Create executor
    executor = QueryExecutor(test_dir)

    # Test 1: CREATE TABLE
    print("\n1. Testing CREATE TABLE...")
    sql = """
        CREATE TABLE users (
            id INT PRIMARY KEY,
            name TEXT NOT NULL,
            age INT,
            email TEXT
        )
    """
    cmd = parse_sql(sql)
    result = executor.execute(cmd)
    print(f"   {result}")
    assert "created" in result.lower()
    print("✓ CREATE TABLE works")

    # Test 2: INSERT
    print("\n2. Testing INSERT...")
    sql = "INSERT INTO users VALUES (1, 'Alice', 25, 'alice@test.com')"
    cmd = parse_sql(sql)
    result = executor.execute(cmd)
    print(f"   {result}")
    assert "1 row" in result.lower()
    print("✓ INSERT works")

    # Test 3: INSERT multiple rows
    print("\n3. Inserting more rows...")
    sqls = [
        "INSERT INTO users VALUES (2, 'Bob', 30, 'bob@test.com')",
        "INSERT INTO users VALUES (3, 'Charlie', 22, NULL)",
        "INSERT INTO users VALUES (4, 'Diana', 28, 'diana@test.com')",
        "INSERT INTO users VALUES (5, 'Eve', 35, 'eve@test.com')",
    ]
    for sql in sqls:
        cmd = parse_sql(sql)
        executor.execute(cmd)
    print("   Inserted 4 more rows")
    print("✓ Multiple INSERTs work")

    # Test 4: SELECT *
    print("\n4. Testing SELECT *...")
    sql = "SELECT * FROM users"
    cmd = parse_sql(sql)
    results = executor.execute(cmd)
    print(f"   Found {len(results)} rows")
    assert len(results) == 5
    print("✓ SELECT * works")

    # Test 5: SELECT with columns
    print("\n5. Testing SELECT with columns...")
    sql = "SELECT name, age FROM users"
    cmd = parse_sql(sql)
    results = executor.execute(cmd)
    print(f"   First row: {results[0]}")
    assert len(results[0]) == 2
    print("✓ SELECT with columns works")

    # Test 6: SELECT with WHERE
    print("\n6. Testing SELECT with WHERE...")
    sql = "SELECT * FROM users WHERE age > 25"
    cmd = parse_sql(sql)
    results = executor.execute(cmd)
    print(f"   Found {len(results)} rows with age > 25")
    for row in results:
        print(f"      {row}")
    assert len(results) == 3  # Bob(30), Diana(28), Eve(35)
    print("✓ SELECT with WHERE works")

    # Test 7: SELECT with complex WHERE
    print("\n7. Testing SELECT with complex WHERE...")
    sql = "SELECT * FROM users WHERE (age > 20 AND age < 30) OR name = 'Eve'"
    cmd = parse_sql(sql)
    results = executor.execute(cmd)
    print(f"   Found {len(results)} rows")
    assert len(results) == 4  # Alice(25), Charlie(22), Diana(28), Eve(35)
    print("✓ Complex WHERE works")

    # Test 8: SELECT with ORDER BY
    print("\n8. Testing SELECT with ORDER BY...")
    sql = "SELECT name, age FROM users ORDER BY age DESC"
    cmd = parse_sql(sql)
    results = executor.execute(cmd)
    print(f"   Ordered by age DESC:")
    for row in results:
        print(f"      {row}")
    assert results[0][1] == 35  # Eve first (highest age)
    print("✓ ORDER BY works")

    # Test 9: SELECT with LIMIT
    print("\n9. Testing SELECT with LIMIT...")
    sql = "SELECT * FROM users LIMIT 2"
    cmd = parse_sql(sql)
    results = executor.execute(cmd)
    assert len(results) == 2
    print(f"   Limited to {len(results)} rows")
    print("✓ LIMIT works")

    # Test 10: SELECT with OFFSET
    print("\n10. Testing SELECT with OFFSET...")
    sql = "SELECT * FROM users LIMIT 2 OFFSET 1"
    cmd = parse_sql(sql)
    results = executor.execute(cmd)
    assert len(results) == 2
    print(f"   Got rows {results[0][0]} and {results[1][0]} (skipped first)")
    print("✓ OFFSET works")

    # Test 11: UPDATE
    print("\n11. Testing UPDATE...")
    sql = "UPDATE users SET age = 26 WHERE name = 'Alice'"
    cmd = parse_sql(sql)
    result = executor.execute(cmd)
    print(f"   {result}")

    # Verify update
    sql = "SELECT age FROM users WHERE name = 'Alice'"
    cmd = parse_sql(sql)
    results = executor.execute(cmd)
    assert results[0][0] == 26
    print("✓ UPDATE works")

    # Test 12: DELETE
    print("\n12. Testing DELETE...")
    sql = "DELETE FROM users WHERE age < 25"
    cmd = parse_sql(sql)
    result = executor.execute(cmd)
    print(f"   {result}")

    # Verify deletion
    sql = "SELECT * FROM users"
    cmd = parse_sql(sql)
    results = executor.execute(cmd)
    print(f"   Rows remaining: {len(results)}")
    assert len(results) == 4  # Deleted Charlie(22)
    print("✓ DELETE works")

    # Test 13: CREATE INDEX
    print("\n13. Testing CREATE INDEX...")
    sql = "CREATE INDEX idx_age ON users (age)"
    cmd = parse_sql(sql)
    result = executor.execute(cmd)
    print(f"   {result}")
    print("✓ CREATE INDEX works")

    # Test 14: Primary key constraint
    print("\n14. Testing primary key constraint...")
    try:
        sql = "INSERT INTO users VALUES (1, 'Duplicate', 99, 'dup@test.com')"
        cmd = parse_sql(sql)
        executor.execute(cmd)
        print("✗ Should have raised error for duplicate primary key")
        assert False
    except ValueError as e:
        print(f"   ✓ Correctly raised error: {str(e)[:60]}...")

    # Test 15: NOT NULL constraint
    print("\n15. Testing NOT NULL constraint...")
    try:
        sql = "INSERT INTO users VALUES (10, NULL, 25, 'test@test.com')"
        cmd = parse_sql(sql)
        executor.execute(cmd)
        print("✗ Should have raised error for NULL in NOT NULL column")
        assert False
    except ValueError as e:
        print(f"   ✓ Correctly raised error: {str(e)[:60]}...")

    # Test 16: EXPLAIN
    print("\n16. Testing EXPLAIN...")
    sql = "EXPLAIN SELECT * FROM users WHERE age > 25"
    cmd = parse_sql(sql)
    result = executor.execute(cmd)
    print("   Plan:")
    for line in result.split('\n')[:5]:
        print(f"      {line}")
    assert "Query Plan" in result
    print("✓ EXPLAIN works")

    # Test 17: ANALYZE
    print("\n17. Testing ANALYZE...")
    sql = "ANALYZE users"
    cmd = parse_sql(sql)
    result = executor.execute(cmd)
    print(f"   {result}")
    print("✓ ANALYZE works")

    # Test 18: VACUUM
    print("\n18. Testing VACUUM...")
    sql = "VACUUM users"
    cmd = parse_sql(sql)
    result = executor.execute(cmd)
    print(f"   {result}")
    print("✓ VACUUM works")

    # Test 19: DROP TABLE
    print("\n19. Testing DROP TABLE...")
    sql = "DROP TABLE users"
    cmd = parse_sql(sql)
    result = executor.execute(cmd)
    print(f"   {result}")

    # Verify table is gone
    try:
        sql = "SELECT * FROM users"
        cmd = parse_sql(sql)
        executor.execute(cmd)
        print("✗ Should have raised error for non-existent table")
        assert False
    except ValueError:
        print("   ✓ Table successfully dropped")

    print("\n" + "="*50)
    print("✅ All executor tests passed!")
    print("="*50)

    # Shutdown
    executor.shutdown()

    # Clean up
    shutil.rmtree(test_dir)

if __name__ == '__main__':
    test_executor()
