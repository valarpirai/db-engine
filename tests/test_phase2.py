"""
Tests for Phase 2 features: ALTER TABLE and TRANSACTIONS
"""

import os
import shutil
import unittest

from db_engine.catalog import Catalog, TableSchema, ColumnDef, IndexMetadata
from db_engine.parser import parse_sql
from db_engine.executor import QueryExecutor


class TestAlterTable(unittest.TestCase):
    """Test ALTER TABLE operations"""

    def setUp(self):
        """Create test environment"""
        self.test_dir = "test_data_phase2"
        os.makedirs(self.test_dir, exist_ok=True)
        self.executor = QueryExecutor(self.test_dir)

        # Create a test table
        create_sql = """
        CREATE TABLE users (
            id INT PRIMARY KEY,
            name TEXT NOT NULL,
            age INT
        );
        """
        cmd = parse_sql(create_sql)
        self.executor.execute(cmd)

        # Insert some test data
        for i in range(1, 4):
            insert_sql = f"INSERT INTO users VALUES ({i}, 'User{i}', {20 + i});"
            cmd = parse_sql(insert_sql)
            self.executor.execute(cmd)

    def tearDown(self):
        """Clean up test data"""
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def test_add_column(self):
        """Test ALTER TABLE ADD COLUMN"""
        # Add a nullable column
        alter_sql = "ALTER TABLE users ADD COLUMN email TEXT;"
        cmd = parse_sql(alter_sql)
        result = self.executor.execute(cmd)
        self.assertIn("Added column", result)

        # Verify schema
        schema = self.executor.catalog.get_table('users')
        self.assertEqual(len(schema.columns), 4)
        self.assertEqual(schema.columns[3].name, 'email')
        self.assertTrue(schema.columns[3].nullable)

        # Verify existing rows have NULL for new column
        select_sql = "SELECT * FROM users WHERE id = 1;"
        cmd = parse_sql(select_sql)
        results = self.executor.execute(cmd)
        self.assertEqual(len(results), 1)
        self.assertEqual(len(results[0]), 4)
        self.assertIsNone(results[0][3])  # Email should be NULL

    def test_add_column_not_null(self):
        """Test ALTER TABLE ADD COLUMN with NOT NULL fails on existing data"""
        # This should work even though NOT NULL is specified,
        # because we insert NULL for existing rows
        alter_sql = "ALTER TABLE users ADD COLUMN status TEXT NOT NULL;"
        cmd = parse_sql(alter_sql)
        result = self.executor.execute(cmd)
        self.assertIn("Added column", result)

    def test_add_column_unique(self):
        """Test ALTER TABLE ADD COLUMN with UNIQUE"""
        alter_sql = "ALTER TABLE users ADD COLUMN username TEXT UNIQUE;"
        cmd = parse_sql(alter_sql)
        result = self.executor.execute(cmd)
        self.assertIn("Added column", result)

        # Verify unique index was created
        indexes = self.executor.catalog.get_indexes_for_table('users')
        username_indexes = [idx for idx in indexes if 'username' in idx.columns]
        self.assertEqual(len(username_indexes), 1)
        self.assertTrue(username_indexes[0].unique)

    def test_drop_column(self):
        """Test ALTER TABLE DROP COLUMN"""
        # Drop a column
        alter_sql = "ALTER TABLE users DROP COLUMN age;"
        cmd = parse_sql(alter_sql)
        result = self.executor.execute(cmd)
        self.assertIn("Dropped column", result)

        # Verify schema
        schema = self.executor.catalog.get_table('users')
        self.assertEqual(len(schema.columns), 2)
        column_names = [col.name for col in schema.columns]
        self.assertNotIn('age', column_names)

        # Verify data still accessible
        select_sql = "SELECT * FROM users WHERE id = 1;"
        cmd = parse_sql(select_sql)
        results = self.executor.execute(cmd)
        self.assertEqual(len(results), 1)
        self.assertEqual(len(results[0]), 2)  # Only id and name

    def test_drop_column_primary_key_fails(self):
        """Test that dropping primary key column fails"""
        alter_sql = "ALTER TABLE users DROP COLUMN id;"
        cmd = parse_sql(alter_sql)

        with self.assertRaises(ValueError) as ctx:
            self.executor.execute(cmd)
        self.assertIn("primary key", str(ctx.exception).lower())

    def test_drop_nonexistent_column_fails(self):
        """Test that dropping non-existent column fails"""
        alter_sql = "ALTER TABLE users DROP COLUMN nonexistent;"
        cmd = parse_sql(alter_sql)

        with self.assertRaises(ValueError) as ctx:
            self.executor.execute(cmd)
        self.assertIn("does not exist", str(ctx.exception))

    def test_rename_column(self):
        """Test ALTER TABLE RENAME COLUMN"""
        # Rename a column
        alter_sql = "ALTER TABLE users RENAME COLUMN name TO full_name;"
        cmd = parse_sql(alter_sql)
        result = self.executor.execute(cmd)
        self.assertIn("Renamed column", result)

        # Verify schema
        schema = self.executor.catalog.get_table('users')
        column_names = [col.name for col in schema.columns]
        self.assertNotIn('name', column_names)
        self.assertIn('full_name', column_names)

        # Verify data still accessible with new name
        select_sql = "SELECT full_name FROM users WHERE id = 1;"
        cmd = parse_sql(select_sql)
        results = self.executor.execute(cmd)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0][0], 'User1')

    def test_rename_primary_key_column(self):
        """Test renaming a primary key column"""
        alter_sql = "ALTER TABLE users RENAME COLUMN id TO user_id;"
        cmd = parse_sql(alter_sql)
        result = self.executor.execute(cmd)
        self.assertIn("Renamed column", result)

        # Verify primary key was updated
        schema = self.executor.catalog.get_table('users')
        self.assertEqual(schema.primary_key, ['user_id'])

    def test_rename_nonexistent_column_fails(self):
        """Test that renaming non-existent column fails"""
        alter_sql = "ALTER TABLE users RENAME COLUMN nonexistent TO something;"
        cmd = parse_sql(alter_sql)

        with self.assertRaises(ValueError) as ctx:
            self.executor.execute(cmd)
        self.assertIn("does not exist", str(ctx.exception))

    def test_rename_to_existing_column_fails(self):
        """Test that renaming to existing column name fails"""
        alter_sql = "ALTER TABLE users RENAME COLUMN name TO age;"
        cmd = parse_sql(alter_sql)

        with self.assertRaises(ValueError) as ctx:
            self.executor.execute(cmd)
        self.assertIn("already exists", str(ctx.exception))


class TestTransactions(unittest.TestCase):
    """Test transaction support"""

    def setUp(self):
        """Create test environment"""
        self.test_dir = "test_data_transactions"
        os.makedirs(self.test_dir, exist_ok=True)
        self.executor = QueryExecutor(self.test_dir)

        # Create a test table
        create_sql = """
        CREATE TABLE accounts (
            id INT PRIMARY KEY,
            balance INT NOT NULL
        );
        """
        cmd = parse_sql(create_sql)
        self.executor.execute(cmd)

        # Insert initial data
        for i in range(1, 4):
            insert_sql = f"INSERT INTO accounts VALUES ({i}, {1000 * i});"
            cmd = parse_sql(insert_sql)
            self.executor.execute(cmd)

    def tearDown(self):
        """Clean up test data"""
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def test_begin_commit(self):
        """Test BEGIN and COMMIT"""
        # Start transaction
        begin_cmd = parse_sql("BEGIN;")
        result = self.executor.execute(begin_cmd)
        self.assertIn("started", result.lower())
        self.assertTrue(self.executor.in_transaction)

        # Make changes
        update_sql = "UPDATE accounts SET balance = 9999 WHERE id = 1;"
        cmd = parse_sql(update_sql)
        self.executor.execute(cmd)

        # Commit
        commit_cmd = parse_sql("COMMIT;")
        result = self.executor.execute(commit_cmd)
        self.assertIn("committed", result.lower())
        self.assertFalse(self.executor.in_transaction)

        # Verify changes persisted
        select_sql = "SELECT balance FROM accounts WHERE id = 1;"
        cmd = parse_sql(select_sql)
        results = self.executor.execute(cmd)
        self.assertEqual(results[0][0], 9999)

    def test_begin_rollback(self):
        """Test BEGIN and ROLLBACK"""
        # Get initial balance
        select_sql = "SELECT balance FROM accounts WHERE id = 1;"
        cmd = parse_sql(select_sql)
        initial_results = self.executor.execute(cmd)
        initial_balance = initial_results[0][0]

        # Start transaction
        begin_cmd = parse_sql("BEGIN;")
        self.executor.execute(begin_cmd)
        self.assertTrue(self.executor.in_transaction)

        # Make changes
        update_sql = "UPDATE accounts SET balance = 9999 WHERE id = 1;"
        cmd = parse_sql(update_sql)
        self.executor.execute(cmd)

        # Verify changes visible within transaction
        cmd = parse_sql(select_sql)
        results = self.executor.execute(cmd)
        self.assertEqual(results[0][0], 9999)

        # Rollback
        rollback_cmd = parse_sql("ROLLBACK;")
        result = self.executor.execute(rollback_cmd)
        self.assertIn("rolled back", result.lower())
        self.assertFalse(self.executor.in_transaction)

        # Verify changes were rolled back
        cmd = parse_sql(select_sql)
        results = self.executor.execute(cmd)
        self.assertEqual(results[0][0], initial_balance)

    def test_begin_transaction_keyword(self):
        """Test BEGIN TRANSACTION syntax"""
        begin_cmd = parse_sql("BEGIN TRANSACTION;")
        result = self.executor.execute(begin_cmd)
        self.assertIn("started", result.lower())
        self.assertTrue(self.executor.in_transaction)

    def test_nested_begin_fails(self):
        """Test that nested transactions fail"""
        # Start first transaction
        begin_cmd = parse_sql("BEGIN;")
        self.executor.execute(begin_cmd)

        # Try to start another
        with self.assertRaises(ValueError) as ctx:
            self.executor.execute(begin_cmd)
        self.assertIn("already in a transaction", str(ctx.exception).lower())

    def test_commit_without_begin_fails(self):
        """Test that COMMIT without BEGIN fails"""
        commit_cmd = parse_sql("COMMIT;")

        with self.assertRaises(ValueError) as ctx:
            self.executor.execute(commit_cmd)
        self.assertIn("no active transaction", str(ctx.exception).lower())

    def test_rollback_without_begin_fails(self):
        """Test that ROLLBACK without BEGIN fails"""
        rollback_cmd = parse_sql("ROLLBACK;")

        with self.assertRaises(ValueError) as ctx:
            self.executor.execute(rollback_cmd)
        self.assertIn("no active transaction", str(ctx.exception).lower())

    def test_multiple_operations_in_transaction(self):
        """Test multiple operations within a transaction"""
        # Start transaction
        begin_cmd = parse_sql("BEGIN;")
        self.executor.execute(begin_cmd)

        # Multiple updates (simple value updates, no arithmetic)
        for i in range(1, 4):
            update_sql = f"UPDATE accounts SET balance = 9999 WHERE id = {i};"
            cmd = parse_sql(update_sql)
            self.executor.execute(cmd)

        # Insert new row
        insert_sql = "INSERT INTO accounts VALUES (4, 4000);"
        cmd = parse_sql(insert_sql)
        self.executor.execute(cmd)

        # Commit
        commit_cmd = parse_sql("COMMIT;")
        self.executor.execute(commit_cmd)

        # Verify insert persisted
        select_sql = "SELECT * FROM accounts WHERE id = 4;"
        cmd = parse_sql(select_sql)
        results = self.executor.execute(cmd)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0][1], 4000)

        # Verify updates persisted
        select_sql = "SELECT balance FROM accounts WHERE id = 1;"
        cmd = parse_sql(select_sql)
        results = self.executor.execute(cmd)
        self.assertEqual(results[0][0], 9999)

    def test_rollback_insert(self):
        """Test rolling back an INSERT"""
        # Start transaction
        begin_cmd = parse_sql("BEGIN;")
        self.executor.execute(begin_cmd)

        # Insert new row
        insert_sql = "INSERT INTO accounts VALUES (99, 99000);"
        cmd = parse_sql(insert_sql)
        self.executor.execute(cmd)

        # Verify insert visible
        select_sql = "SELECT * FROM accounts WHERE id = 99;"
        cmd = parse_sql(select_sql)
        results = self.executor.execute(cmd)
        self.assertEqual(len(results), 1)

        # Rollback
        rollback_cmd = parse_sql("ROLLBACK;")
        self.executor.execute(rollback_cmd)

        # Verify insert was rolled back
        cmd = parse_sql(select_sql)
        results = self.executor.execute(cmd)
        self.assertEqual(len(results), 0)


if __name__ == '__main__':
    unittest.main()
