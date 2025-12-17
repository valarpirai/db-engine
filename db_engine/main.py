"""
Main entry point for SimpleDB

Usage:
    python -m db_engine.main [--data-dir <path>]

Example:
    python -m db_engine.main --data-dir ./mydb
"""

import argparse
import os
import sys

from .executor import QueryExecutor
from .repl import REPL


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='SimpleDB - Educational Database Engine',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Start with default data directory (./data)
  python -m db_engine.main

  # Start with custom data directory
  python -m db_engine.main --data-dir ./mydb

  # Execute SQL from command line
  python -m db_engine.main --execute "SELECT * FROM users"
        """
    )

    parser.add_argument(
        '--data-dir',
        default='./data',
        help='Data directory for database files (default: ./data)'
    )

    parser.add_argument(
        '--execute', '-e',
        metavar='SQL',
        help='Execute SQL command and exit'
    )

    parser.add_argument(
        '--file', '-f',
        metavar='FILE',
        help='Execute SQL commands from file'
    )

    args = parser.parse_args()

    # Create data directory if it doesn't exist
    os.makedirs(args.data_dir, exist_ok=True)

    # Initialize executor
    try:
        executor = QueryExecutor(args.data_dir)
    except Exception as e:
        print(f"Error initializing database: {e}", file=sys.stderr)
        return 1

    # Execute mode
    if args.execute:
        return execute_sql(executor, args.execute)

    # File mode
    if args.file:
        return execute_file(executor, args.file)

    # Interactive REPL mode
    repl = REPL(executor)
    repl.start()

    return 0


def execute_sql(executor: QueryExecutor, sql: str) -> int:
    """Execute single SQL command"""
    from .parser import parse_sql, SelectCommand

    try:
        command = parse_sql(sql)
        result = executor.execute(command)

        # Display result
        if isinstance(command, SelectCommand):
            if result:
                # Print rows
                for row in result:
                    print('\t'.join(str(v) if v is not None else 'NULL' for v in row))
            else:
                print("(0 rows)")
        else:
            print(result)

        executor.shutdown()
        return 0

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        executor.shutdown()
        return 1


def execute_file(executor: QueryExecutor, filename: str) -> int:
    """Execute SQL commands from file"""
    from .parser import parse_sql, SelectCommand

    try:
        with open(filename, 'r') as f:
            lines = f.readlines()

        # Remove comment lines and join
        sql_lines = []
        for line in lines:
            line = line.strip()
            if line and not line.startswith('--'):
                sql_lines.append(line)

        sql = ' '.join(sql_lines)

        # Split by semicolons
        commands = [cmd.strip() for cmd in sql.split(';') if cmd.strip()]

        for cmd_sql in commands:
            if not cmd_sql:
                continue

            try:
                command = parse_sql(cmd_sql + ';')
                result = executor.execute(command)

                # Display result
                if isinstance(command, SelectCommand):
                    if result:
                        for row in result:
                            print('\t'.join(str(v) if v is not None else 'NULL' for v in row))
                    print(f"({len(result)} rows)")
                else:
                    print(result)

            except Exception as e:
                print(f"Error executing: {cmd_sql[:50]}...")
                print(f"  {e}")
                executor.shutdown()
                return 1

        executor.shutdown()
        return 0

    except FileNotFoundError:
        print(f"Error: File not found: {filename}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        executor.shutdown()
        return 1


if __name__ == '__main__':
    sys.exit(main())
