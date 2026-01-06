"""
Main entry point for SimpleDB

Usage:
    python -m db_engine.main [--data-dir <path>]

Example:
    python -m db_engine.main --data-dir ./mydb
"""

import click
import os
import sys

from .executor import QueryExecutor
from .repl import REPL


@click.command()
@click.option(
    '--data-dir',
    default='./data',
    help='Data directory for database files',
    show_default=True
)
@click.option(
    '--execute', '-e',
    'sql_command',
    help='Execute SQL command and exit'
)
@click.option(
    '--file', '-f',
    'sql_file',
    type=click.Path(exists=True),
    help='Execute SQL commands from file'
)
def main(data_dir, sql_command, sql_file):
    """SimpleDB - Educational Database Engine

    Examples:

      # Start interactive REPL (default)
      python -m db_engine.main

      # Start with custom data directory
      python -m db_engine.main --data-dir ./mydb

      # Execute SQL from command line
      python -m db_engine.main --execute "SELECT * FROM users"

      # Execute SQL from file
      python -m db_engine.main --file demo.sql
    """
    # Create data directory if it doesn't exist
    os.makedirs(data_dir, exist_ok=True)

    # Initialize executor
    try:
        executor = QueryExecutor(data_dir)
    except Exception as e:
        click.echo(f"Error initializing database: {e}", err=True)
        sys.exit(1)

    # Execute mode
    if sql_command:
        exit_code = execute_sql(executor, sql_command)
        sys.exit(exit_code)

    # File mode
    if sql_file:
        exit_code = execute_file(executor, sql_file)
        sys.exit(exit_code)

    # Interactive REPL mode
    repl = REPL(executor)
    repl.start()


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
    main()
