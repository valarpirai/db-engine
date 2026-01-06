"""
REPL - Read-Eval-Print Loop interface for the database

Provides interactive command-line interface with:
- SQL command execution
- Meta-commands (\dt, \di, \d table, \q)
- Pretty table output
- Multi-line input support with syntax highlighting
- Command history with arrow keys
- Tab completion
- Error handling
"""

import sys
import os
from typing import List, Any
from pathlib import Path

from prompt_toolkit import PromptSession
from prompt_toolkit.history import FileHistory
from prompt_toolkit.lexers import PygmentsLexer
from prompt_toolkit.completion import Completer, Completion
from prompt_toolkit.styles import Style
from pygments.lexer import RegexLexer, bygroups
from pygments.token import Keyword, Name, String, Number, Operator, Punctuation, Comment, Text, Whitespace

from .executor import QueryExecutor
from .parser import parse_sql


class SQLLexer(RegexLexer):
    """Custom SQL lexer for syntax highlighting"""

    name = 'SQL'
    aliases = ['sql']

    # SQL keywords
    keywords = [
        'SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'ALTER',
        'TABLE', 'INDEX', 'FROM', 'WHERE', 'ORDER', 'BY', 'GROUP', 'HAVING',
        'JOIN', 'LEFT', 'RIGHT', 'INNER', 'OUTER', 'ON', 'AS', 'AND', 'OR',
        'NOT', 'NULL', 'IS', 'IN', 'BETWEEN', 'LIKE', 'LIMIT', 'OFFSET',
        'ASC', 'DESC', 'PRIMARY', 'KEY', 'UNIQUE', 'FOREIGN', 'REFERENCES',
        'INTO', 'VALUES', 'SET', 'BEGIN', 'COMMIT', 'ROLLBACK', 'TRANSACTION',
        'TRUNCATE', 'EXPLAIN', 'ANALYZE', 'VACUUM', 'ADD', 'COLUMN', 'RENAME',
        'TO', 'INT', 'BIGINT', 'FLOAT', 'TEXT', 'BOOLEAN', 'TIMESTAMP',
        'AUTOINCREMENT'
    ]

    tokens = {
        'root': [
            (r'\s+', Whitespace),
            (r'--.*$', Comment.Single),
            (r'/\*', Comment.Multiline, 'multiline-comment'),
            (r'(' + '|'.join(keywords) + r')\b', Keyword, 'root'),
            (r'\\[a-z?]+', Name.Builtin),  # Meta-commands like \dt, \di
            (r"'[^']*'", String.Single),
            (r'"[^"]*"', String.Double),
            (r'\d+', Number.Integer),
            (r'[+\-*/<>=!]+', Operator),
            (r'[(),;]', Punctuation),
            (r'[a-zA-Z_][a-zA-Z0-9_]*', Name),
            (r'.', Text),
        ],
        'multiline-comment': [
            (r'[^*/]+', Comment.Multiline),
            (r'/\*', Comment.Multiline, 'multiline-comment'),
            (r'\*/', Comment.Multiline, '#pop'),
            (r'[*/]', Comment.Multiline),
        ],
    }


class SQLCompleter(Completer):
    """Auto-completion for SQL keywords, table names, column names, and meta-commands"""

    def __init__(self, executor: QueryExecutor):
        self.executor = executor

        # SQL keywords
        self.keywords = [
            'SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'ALTER',
            'TABLE', 'INDEX', 'FROM', 'WHERE', 'ORDER', 'BY', 'GROUP', 'HAVING',
            'AND', 'OR', 'NOT', 'NULL', 'IS', 'IN', 'BETWEEN', 'LIKE', 'LIMIT',
            'OFFSET', 'ASC', 'DESC', 'PRIMARY', 'KEY', 'UNIQUE', 'INTO', 'VALUES',
            'SET', 'BEGIN', 'COMMIT', 'ROLLBACK', 'TRANSACTION', 'TRUNCATE',
            'EXPLAIN', 'ANALYZE', 'VACUUM', 'ADD', 'COLUMN', 'RENAME', 'TO',
            'INT', 'BIGINT', 'FLOAT', 'TEXT', 'BOOLEAN', 'TIMESTAMP',
            'AUTOINCREMENT'
        ]

        # Meta-commands
        self.meta_commands = ['\\dt', '\\di', '\\d', '\\q', '\\?']

    def get_completions(self, document, complete_event):
        """Generate completions based on current input"""
        word = document.get_word_before_cursor()
        text = document.text_before_cursor.upper()

        # Meta-commands
        if word.startswith('\\'):
            for cmd in self.meta_commands:
                if cmd.startswith(word.lower()):
                    yield Completion(cmd, start_position=-len(word))
            return

        # SQL keywords
        for keyword in self.keywords:
            if keyword.startswith(word.upper()):
                yield Completion(keyword, start_position=-len(word))

        # Table names (if we have FROM, UPDATE, INSERT INTO, etc.)
        if any(kw in text for kw in ['FROM', 'UPDATE', 'INSERT INTO', 'TABLE', 'TRUNCATE', 'ALTER']):
            for table_name in self.executor.catalog.list_tables():
                if table_name.upper().startswith(word.upper()):
                    yield Completion(table_name, start_position=-len(word))

        # Column names (if we have SELECT, WHERE, ORDER BY, etc.)
        if any(kw in text for kw in ['SELECT', 'WHERE', 'ORDER BY', 'GROUP BY', 'SET']):
            # Try to find the table name in the query
            table_name = self._extract_table_name(text)
            if table_name:
                try:
                    schema = self.executor.catalog.get_table(table_name)
                    for col in schema.columns:
                        if col.name.upper().startswith(word.upper()):
                            yield Completion(col.name, start_position=-len(word))
                except ValueError:
                    pass

    def _extract_table_name(self, text: str) -> str:
        """Extract table name from partial SQL query"""
        # Simple heuristic: look for FROM <table> or UPDATE <table>
        words = text.split()
        for i, word in enumerate(words):
            if word in ['FROM', 'UPDATE', 'INTO', 'TABLE', 'TRUNCATE', 'ALTER'] and i + 1 < len(words):
                return words[i + 1].strip('(),;')
        return None


class LimitedFileHistory(FileHistory):
    """FileHistory with maximum entry limit"""

    def __init__(self, filename: str, max_entries: int = 100):
        super().__init__(filename)
        self.max_entries = max_entries

    def store_string(self, string: str) -> None:
        """Store string and trim history if needed"""
        super().store_string(string)
        self._trim_history()

    def _trim_history(self) -> None:
        """Keep only the last max_entries in the history file"""
        try:
            # Read all history entries
            if os.path.exists(self.filename):
                with open(self.filename, 'r', encoding='utf-8') as f:
                    lines = f.readlines()

                # Keep only the last max_entries
                if len(lines) > self.max_entries:
                    with open(self.filename, 'w', encoding='utf-8') as f:
                        f.writelines(lines[-self.max_entries:])
        except Exception:
            # Silently ignore errors in trimming
            pass


class REPL:
    """Interactive shell for database commands"""

    def __init__(self, executor: QueryExecutor):
        self.executor = executor
        self.running = False

        # Setup history file with 100 entry limit
        history_file = Path.home() / '.simpledb_history'
        self.history = LimitedFileHistory(str(history_file), max_entries=100)

        # Setup prompt session with syntax highlighting (no auto-completion)
        self.session = PromptSession(
            lexer=PygmentsLexer(SQLLexer),
            history=self.history,
            style=Style.from_dict({
                'pygments.keyword': '#569cd6 bold',       # Blue for keywords
                'pygments.name.builtin': '#c586c0',       # Purple for meta-commands
                'pygments.string': '#ce9178',             # Orange for strings
                'pygments.number': '#b5cea8',             # Light green for numbers
                'pygments.operator': '#d4d4d4',           # White for operators
                'pygments.comment': '#6a9955 italic',     # Green italic for comments
            })
        )

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
        print("Command history saved to: ~/.simpledb_history (max 100 entries)")
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

        while True:
            # Determine prompt based on whether we have partial input
            if not lines:
                prompt_text = 'SimpleDB> '
            else:
                prompt_text = '       -> '

            # Read a line
            try:
                line = self.session.prompt(prompt_text)

                # If we get an empty line at the start, ignore it and continue
                if not line.strip() and not lines:
                    continue

                lines.append(line)

                # Check if command is complete
                combined = '\n'.join(lines).strip()
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
