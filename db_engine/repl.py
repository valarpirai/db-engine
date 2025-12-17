"""
REPL - Read-Eval-Print Loop interface for the database

Provides interactive command-line interface with:
- SQL command execution
- Meta-commands (\dt, \di, \d table, \q)
- Pretty table output
- Multi-line input support
- Error handling
"""

import sys
import os
from typing import List, Any

from .executor import QueryExecutor
from .parser import parse_sql


class REPL:
    """Interactive shell for database commands"""

    def __init__(self, executor: QueryExecutor):
        self.executor = executor
        self.running = False

    def start(self):
        """Main command loop"""
        self.running = True

        # Display welcome message
        print("=" * 60)
        print("SimpleDB - Educational Database Engine")
        print("=" * 60)
        print("Type SQL commands or use meta-commands:")
        print("  \\dt          - List all tables")
        print("  \\di          - List all indexes")
        print("  \\d <table>   - Describe table schema")
        print("  \\q           - Quit")
        print("=" * 60)
        print()

        # Command loop
        while self.running:
            try:
                # Read input (support multi-line)
                command = self._read_command()

                if not command.strip():
                    continue

                # Handle meta-commands
                if command.startswith('\\'):
                    self._handle_meta_command(command)
                else:
                    # Parse and execute SQL
                    self._execute_sql(command)

            except KeyboardInterrupt:
                print("\nUse \\q to quit")
            except EOFError:
                print("\nGoodbye!")
                break
            except Exception as e:
                print(f"Error: {e}")

        # Shutdown
        print("\nShutting down...")
        self.executor.shutdown()

    def _read_command(self) -> str:
        """Read command (possibly multi-line)"""
        lines = []
        prompt = "SimpleDB> "

        while True:
            try:
                if lines:
                    prompt = "       -> "  # Continuation prompt

                line = input(prompt)
                lines.append(line)

                # Check if command is complete (ends with semicolon or is meta-command)
                combined = ' '.join(lines).strip()
                if combined.startswith('\\') or combined.endswith(';'):
                    return combined

            except EOFError:
                raise

    def _execute_sql(self, sql: str):
        """Parse and execute SQL command"""
        try:
            # Parse SQL
            command = parse_sql(sql)

            # Execute
            result = self.executor.execute(command)

            # Display results
            self._display_result(command, result)

        except SyntaxError as e:
            print(f"Syntax Error: {e}")
        except ValueError as e:
            print(f"Error: {e}")
        except Exception as e:
            print(f"Unexpected error: {e}")

    def _display_result(self, command, result):
        """Display execution result"""
        # Import command types
        from .parser import SelectCommand

        if isinstance(command, SelectCommand):
            # SELECT returns list of tuples - display as table
            if not result:
                print("(0 rows)")
            else:
                self._display_table(result, command.columns, command.table_name)
        else:
            # Other commands return status message
            print(result)

    def _display_table(self, rows: List[tuple], columns: List[str], table_name: str):
        """Display results in formatted table"""
        if not rows:
            print("(0 rows)")
            return

        # Get column names
        if columns == ['*']:
            # Get all column names from schema
            try:
                schema = self.executor.catalog.get_table(table_name)
                col_names = [col.name for col in schema.columns]
            except:
                col_names = [f"col{i}" for i in range(len(rows[0]))]
        else:
            col_names = columns

        # Calculate column widths
        widths = [len(str(name)) for name in col_names]
        for row in rows:
            for i, value in enumerate(row):
                widths[i] = max(widths[i], len(str(value) if value is not None else 'NULL'))

        # Print header
        header = " | ".join(str(name).ljust(widths[i]) for i, name in enumerate(col_names))
        separator = "-+-".join("-" * w for w in widths)

        print(header)
        print(separator)

        # Print rows
        for row in rows:
            formatted_row = " | ".join(
                str(value if value is not None else 'NULL').ljust(widths[i])
                for i, value in enumerate(row)
            )
            print(formatted_row)

        # Print row count
        print(f"({len(rows)} row{'s' if len(rows) != 1 else ''})")

    def _handle_meta_command(self, command: str):
        """Process backslash commands"""
        parts = command.strip().split()
        cmd = parts[0].lower()

        if cmd == '\\q' or cmd == '\\quit':
            print("Goodbye!")
            self.running = False

        elif cmd == '\\dt':
            # List all tables
            self._list_tables()

        elif cmd == '\\di':
            # List all indexes
            self._list_indexes()

        elif cmd == '\\d':
            # Describe table
            if len(parts) < 2:
                print("Usage: \\d <table_name>")
            else:
                self._describe_table(parts[1])

        elif cmd == '\\?':
            # Help
            self._print_help()

        else:
            print(f"Unknown command: {cmd}")
            print("Type \\? for help")

    def _list_tables(self):
        """List all tables"""
        tables = self.executor.catalog.list_tables()

        if not tables:
            print("No tables found")
            return

        print("\nList of tables:")
        print("-" * 40)
        for table_name in sorted(tables):
            schema = self.executor.catalog.get_table(table_name)
            stats = self.executor.catalog.get_statistics(table_name)
            print(f"  {table_name:20} ({stats.row_count} rows)")
        print()

    def _list_indexes(self):
        """List all indexes"""
        indexes = self.executor.catalog.list_indexes()

        if not indexes:
            print("No indexes found")
            return

        print("\nList of indexes:")
        print("-" * 60)
        print(f"{'Index Name':30} {'Table':15} {'Columns':15}")
        print("-" * 60)

        for index_key in sorted(indexes):
            index_meta = self.executor.catalog.indexes[index_key]
            unique_flag = "UNIQUE" if index_meta.unique else ""
            cols = ", ".join(index_meta.columns)
            print(f"  {index_meta.index_name:28} {index_meta.table_name:15} {cols:15} {unique_flag}")
        print()

    def _describe_table(self, table_name: str):
        """Describe table schema"""
        try:
            schema = self.executor.catalog.get_table(table_name)
            stats = self.executor.catalog.get_statistics(table_name)

            print(f"\nTable: {table_name}")
            print("-" * 60)
            print(f"{'Column':20} {'Type':12} {'Nullable':10} {'Key':10}")
            print("-" * 60)

            for col in schema.columns:
                nullable = "YES" if col.nullable else "NO"
                key = "PRI" if col.name in schema.primary_key else ""
                if col.unique and not key:
                    key = "UNI"

                print(f"  {col.name:18} {col.datatype:12} {nullable:10} {key:10}")

            print("-" * 60)
            print(f"Primary Key: {', '.join(schema.primary_key)}")
            print(f"Rows: {stats.row_count}, Pages: {stats.page_count}")

            # List indexes on this table
            indexes = self.executor.catalog.get_indexes_for_table(table_name)
            if indexes:
                print(f"\nIndexes:")
                for idx in indexes:
                    unique_flag = "UNIQUE" if idx.unique else ""
                    print(f"  {idx.index_name} on ({', '.join(idx.columns)}) {unique_flag}")

            print()

        except ValueError as e:
            print(f"Error: {e}")

    def _print_help(self):
        """Print help message"""
        print("\nMeta-commands:")
        print("  \\dt              - List all tables")
        print("  \\di              - List all indexes")
        print("  \\d <table>       - Describe table schema")
        print("  \\?               - Show this help")
        print("  \\q               - Quit")
        print("\nSQL Commands:")
        print("  CREATE TABLE ... - Create a new table")
        print("  CREATE INDEX ... - Create an index")
        print("  DROP TABLE ...   - Drop a table")
        print("  INSERT INTO ...  - Insert rows")
        print("  SELECT ...       - Query data")
        print("  UPDATE ...       - Update rows")
        print("  DELETE FROM ...  - Delete rows")
        print("  EXPLAIN ...      - Show query plan")
        print("  ANALYZE ...      - Update statistics")
        print("  VACUUM ...       - Reclaim space")
        print()
