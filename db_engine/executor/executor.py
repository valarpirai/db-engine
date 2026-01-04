"""
Query Executor - Main executor class combining all functionality.

This module provides:
- QueryExecutor: Orchestrates catalog, storage, and indexes for SQL execution
"""

from ..parser import (
    SelectCommand, InsertCommand, UpdateCommand, DeleteCommand,
    CreateTableCommand, CreateIndexCommand, DropTableCommand, DropIndexCommand,
    TruncateTableCommand, ExplainCommand, AnalyzeCommand, VacuumCommand,
    AlterTableAddColumnCommand, AlterTableDropColumnCommand, AlterTableRenameColumnCommand,
    BeginCommand, CommitCommand, RollbackCommand
)

from .base import ExecutorBase
from .ddl import DDLMixin
from .dml import DMLMixin
from .utility import UtilityMixin
from .schema import SchemaMixin
from .transaction import TransactionMixin


class QueryExecutor(
    ExecutorBase,
    DDLMixin,
    DMLMixin,
    UtilityMixin,
    SchemaMixin,
    TransactionMixin
):
    """
    Executes SQL commands - orchestrates all database components.

    Combines functionality from:
    - ExecutorBase: Helpers and expression evaluation
    - DDLMixin: CREATE TABLE, CREATE INDEX, DROP TABLE, DROP INDEX, TRUNCATE TABLE
    - DMLMixin: INSERT, SELECT, UPDATE, DELETE
    - UtilityMixin: EXPLAIN, ANALYZE, VACUUM
    - SchemaMixin: ALTER TABLE (ADD/DROP/RENAME COLUMN)
    - TransactionMixin: BEGIN, COMMIT, ROLLBACK
    """

    def __init__(self, data_dir: str):
        super().__init__(data_dir)

    def execute(self, command):
        """Main entry point - dispatch to specific executors"""
        if isinstance(command, CreateTableCommand):
            return self.execute_create_table(command)
        elif isinstance(command, CreateIndexCommand):
            return self.execute_create_index(command)
        elif isinstance(command, DropTableCommand):
            return self.execute_drop_table(command)
        elif isinstance(command, DropIndexCommand):
            return self.execute_drop_index(command)
        elif isinstance(command, TruncateTableCommand):
            return self.execute_truncate_table(command)
        elif isinstance(command, InsertCommand):
            return self.execute_insert(command)
        elif isinstance(command, SelectCommand):
            return self.execute_select(command)
        elif isinstance(command, UpdateCommand):
            return self.execute_update(command)
        elif isinstance(command, DeleteCommand):
            return self.execute_delete(command)
        elif isinstance(command, ExplainCommand):
            return self.execute_explain(command)
        elif isinstance(command, AnalyzeCommand):
            return self.execute_analyze(command)
        elif isinstance(command, VacuumCommand):
            return self.execute_vacuum(command)
        elif isinstance(command, AlterTableAddColumnCommand):
            return self.execute_alter_table_add_column(command)
        elif isinstance(command, AlterTableDropColumnCommand):
            return self.execute_alter_table_drop_column(command)
        elif isinstance(command, AlterTableRenameColumnCommand):
            return self.execute_alter_table_rename_column(command)
        elif isinstance(command, BeginCommand):
            return self.execute_begin(command)
        elif isinstance(command, CommitCommand):
            return self.execute_commit(command)
        elif isinstance(command, RollbackCommand):
            return self.execute_rollback(command)
        else:
            raise ValueError(f"Unknown command type: {type(command)}")
