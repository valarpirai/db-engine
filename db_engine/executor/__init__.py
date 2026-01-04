"""
Query Executor Package

This package provides SQL execution functionality:
- executor: Main QueryExecutor class
- base: Helper methods and expression evaluation
- ddl: DDL command handlers (CREATE, DROP)
- dml: DML command handlers (INSERT, SELECT, UPDATE, DELETE)
- utility: Utility command handlers (EXPLAIN, ANALYZE, VACUUM)
- schema: Schema modification handlers (ALTER TABLE)
- transaction: Transaction handlers (BEGIN, COMMIT, ROLLBACK)

For backward compatibility, QueryExecutor is re-exported here.
"""

from .executor import QueryExecutor

__all__ = ['QueryExecutor']
