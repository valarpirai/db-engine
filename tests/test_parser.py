"""
Test script for parser.py
Tests tokenizer and parser with various SQL statements
"""

import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db_engine.parser import (
    Tokenizer, Parser, parse_sql, TokenType,
    SelectCommand, InsertCommand, CreateTableCommand, CreateIndexCommand,
    UpdateCommand, DeleteCommand, DropTableCommand,
    BinaryOp, UnaryOp, Literal, ColumnRef
)

def test_parser():
    """Test SQL parser"""
    print("Testing parser.py...")

    # Test 1: Tokenizer basic functionality
    print("\n1. Testing tokenizer...")
    tokenizer = Tokenizer("SELECT * FROM users WHERE age > 18;")
    tokens = tokenizer.tokenize()
    print(f"   Token count: {len(tokens)}")
    print(f"   First few tokens: {[t.type.name for t in tokens[:5]]}")
    assert tokens[0].type == TokenType.SELECT
    assert tokens[1].type == TokenType.STAR
    assert tokens[2].type == TokenType.FROM
    print("✓ Tokenizer works")

    # Test 2: SELECT with WHERE
    print("\n2. Testing SELECT with WHERE...")
    cmd = parse_sql("SELECT id, name FROM users WHERE age > 25")
    print(f"   Command type: {type(cmd).__name__}")
    print(f"   Table: {cmd.table_name}")
    print(f"   Columns: {cmd.columns}")
    assert isinstance(cmd, SelectCommand)
    assert cmd.table_name == "users"
    assert cmd.columns == ["id", "name"]
    assert isinstance(cmd.where, BinaryOp)
    assert cmd.where.op == '>'
    print("✓ SELECT with WHERE works")

    # Test 3: SELECT with complex WHERE
    print("\n3. Testing SELECT with complex WHERE...")
    cmd = parse_sql("SELECT * FROM products WHERE price < 100 AND in_stock = TRUE")
    print(f"   WHERE clause type: {type(cmd.where).__name__}")
    print(f"   WHERE operator: {cmd.where.op}")
    assert isinstance(cmd.where, BinaryOp)
    assert cmd.where.op == 'AND'
    print("✓ Complex WHERE with AND works")

    # Test 4: SELECT with OR and parentheses
    print("\n4. Testing SELECT with OR and parentheses...")
    cmd = parse_sql("SELECT * FROM users WHERE (age > 18 AND age < 65) OR status = 'active'")
    assert cmd.where.op == 'OR'
    print("✓ WHERE with OR and parentheses works")

    # Test 5: SELECT with LIKE
    print("\n5. Testing SELECT with LIKE...")
    cmd = parse_sql("SELECT * FROM users WHERE name LIKE 'John%'")
    assert cmd.where.op == 'LIKE'
    assert isinstance(cmd.where.right, Literal)
    print("✓ LIKE operator works")

    # Test 6: SELECT with ORDER BY
    print("\n6. Testing SELECT with ORDER BY...")
    cmd = parse_sql("SELECT * FROM users ORDER BY age DESC, name ASC")
    print(f"   ORDER BY: {cmd.order_by}")
    assert cmd.order_by == [('age', 'DESC'), ('name', 'ASC')]
    print("✓ ORDER BY works")

    # Test 7: SELECT with LIMIT and OFFSET
    print("\n7. Testing SELECT with LIMIT and OFFSET...")
    cmd = parse_sql("SELECT * FROM users LIMIT 10 OFFSET 20")
    assert cmd.limit == 10
    assert cmd.offset == 20
    print("✓ LIMIT and OFFSET work")

    # Test 8: INSERT with all columns
    print("\n8. Testing INSERT...")
    cmd = parse_sql("INSERT INTO users VALUES (1, 'Alice', 25, 'alice@test.com')")
    print(f"   Command type: {type(cmd).__name__}")
    print(f"   Table: {cmd.table_name}")
    print(f"   Values: {cmd.values}")
    assert isinstance(cmd, InsertCommand)
    assert cmd.table_name == "users"
    assert cmd.columns is None  # No column list
    assert len(cmd.values) == 4
    print("✓ INSERT works")

    # Test 9: INSERT with column list
    print("\n9. Testing INSERT with column list...")
    cmd = parse_sql("INSERT INTO users (id, name) VALUES (2, 'Bob')")
    assert cmd.columns == ['id', 'name']
    assert cmd.values == [2, 'Bob']
    print("✓ INSERT with column list works")

    # Test 10: CREATE TABLE
    print("\n10. Testing CREATE TABLE...")
    sql = """
        CREATE TABLE users (
            id INT PRIMARY KEY,
            name TEXT NOT NULL,
            age INT,
            email TEXT UNIQUE
        )
    """
    cmd = parse_sql(sql)
    print(f"   Command type: {type(cmd).__name__}")
    print(f"   Table: {cmd.table_name}")
    print(f"   Columns: {len(cmd.columns)}")
    print(f"   Primary key: {cmd.primary_key}")
    assert isinstance(cmd, CreateTableCommand)
    assert cmd.table_name == "users"
    assert len(cmd.columns) == 4
    assert cmd.primary_key == ['id']
    print("✓ CREATE TABLE works")

    # Test 11: CREATE TABLE with composite primary key
    print("\n11. Testing CREATE TABLE with composite primary key...")
    sql = """
        CREATE TABLE orders (
            user_id INT,
            order_id INT,
            total FLOAT,
            PRIMARY KEY (user_id, order_id)
        )
    """
    cmd = parse_sql(sql)
    assert cmd.primary_key == ['user_id', 'order_id']
    print("✓ Composite primary key works")

    # Test 12: CREATE INDEX
    print("\n12. Testing CREATE INDEX...")
    cmd = parse_sql("CREATE INDEX idx_age ON users (age)")
    print(f"   Command type: {type(cmd).__name__}")
    print(f"   Index name: {cmd.index_name}")
    print(f"   Table: {cmd.table_name}")
    print(f"   Columns: {cmd.columns}")
    print(f"   Unique: {cmd.unique}")
    assert isinstance(cmd, CreateIndexCommand)
    assert cmd.index_name == "idx_age"
    assert cmd.unique == False
    print("✓ CREATE INDEX works")

    # Test 13: CREATE UNIQUE INDEX
    print("\n13. Testing CREATE UNIQUE INDEX...")
    cmd = parse_sql("CREATE UNIQUE INDEX idx_email ON users (email)")
    assert cmd.unique == True
    print("✓ CREATE UNIQUE INDEX works")

    # Test 14: UPDATE
    print("\n14. Testing UPDATE...")
    cmd = parse_sql("UPDATE users SET age = 30, name = 'Alice Smith' WHERE id = 1")
    print(f"   Command type: {type(cmd).__name__}")
    print(f"   Table: {cmd.table_name}")
    print(f"   Assignments: {len(cmd.assignments)}")
    assert isinstance(cmd, UpdateCommand)
    assert len(cmd.assignments) == 2
    assert cmd.assignments[0][0] == 'age'
    print("✓ UPDATE works")

    # Test 15: DELETE
    print("\n15. Testing DELETE...")
    cmd = parse_sql("DELETE FROM users WHERE age < 18")
    print(f"   Command type: {type(cmd).__name__}")
    print(f"   Table: {cmd.table_name}")
    assert isinstance(cmd, DeleteCommand)
    assert cmd.table_name == "users"
    assert cmd.where is not None
    print("✓ DELETE works")

    # Test 16: DROP TABLE
    print("\n16. Testing DROP TABLE...")
    cmd = parse_sql("DROP TABLE users")
    assert isinstance(cmd, DropTableCommand)
    assert cmd.table_name == "users"
    print("✓ DROP TABLE works")

    # Test 17: Error handling - invalid syntax
    print("\n17. Testing error handling...")
    try:
        parse_sql("SELECT FROM users")  # Missing column list
        print("✗ Should have raised SyntaxError")
        assert False
    except SyntaxError as e:
        print(f"   ✓ Correctly raised error: {str(e)[:60]}...")

    # Test 18: Error handling - unterminated string
    print("\n18. Testing unterminated string error...")
    try:
        parse_sql("SELECT * FROM users WHERE name = 'Alice")
        print("✗ Should have raised SyntaxError")
        assert False
    except SyntaxError as e:
        print(f"   ✓ Correctly raised error: {str(e)[:60]}...")

    # Test 19: Comments
    print("\n19. Testing comments...")
    cmd = parse_sql("""
        -- This is a comment
        SELECT * FROM users -- Another comment
        WHERE age > 18
    """)
    assert isinstance(cmd, SelectCommand)
    print("✓ Comments are correctly ignored")

    # Test 20: NULL values
    print("\n20. Testing NULL values...")
    cmd = parse_sql("INSERT INTO users VALUES (1, 'Bob', NULL, NULL)")
    assert cmd.values[2] is None
    assert cmd.values[3] is None
    print("✓ NULL values work")

    print("\n" + "="*50)
    print("✅ All parser tests passed!")
    print("="*50)

if __name__ == '__main__':
    test_parser()
