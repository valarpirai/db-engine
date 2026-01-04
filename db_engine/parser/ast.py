"""
Abstract Syntax Tree (AST) nodes for parsed SQL.

This module provides:
- Expression classes: For WHERE clause expressions
- Command classes: For parsed SQL statements
"""

from typing import List, Optional, Any
from dataclasses import dataclass


# ============================================================================
# Expression Tree (for WHERE clauses)
# ============================================================================

class Expression:
    """Base class for expression nodes"""
    pass


@dataclass
class BinaryOp(Expression):
    """Binary operation: left op right"""
    op: str  # '=', '!=', '<', '>', '<=', '>=', 'AND', 'OR', 'LIKE', 'IS'
    left: Expression
    right: Expression


@dataclass
class UnaryOp(Expression):
    """Unary operation: op operand"""
    op: str  # 'NOT'
    operand: Expression


@dataclass
class Literal(Expression):
    """Literal value (number, string, boolean, NULL)"""
    value: Any
    datatype: str  # 'INT', 'FLOAT', 'STRING', 'BOOLEAN', 'NULL'


@dataclass
class ColumnRef(Expression):
    """Reference to a column"""
    column_name: str


# ============================================================================
# Command Objects (parsed SQL commands)
# ============================================================================

@dataclass
class CreateTableCommand:
    """CREATE TABLE table_name (columns...) PRIMARY KEY (...)"""
    table_name: str
    columns: List[tuple]  # [(name, datatype, nullable, unique, autoincrement), ...]
    primary_key: List[str]  # Column names


@dataclass
class CreateIndexCommand:
    """CREATE [UNIQUE] INDEX index_name ON table_name (columns)"""
    index_name: str
    table_name: str
    columns: List[str]
    unique: bool


@dataclass
class DropTableCommand:
    """DROP TABLE table_name"""
    table_name: str


@dataclass
class DropIndexCommand:
    """DROP INDEX index_name ON table_name"""
    index_name: str
    table_name: str


@dataclass
class TruncateTableCommand:
    """TRUNCATE TABLE table_name"""
    table_name: str


@dataclass
class InsertCommand:
    """INSERT INTO table_name [(columns)] VALUES (values)"""
    table_name: str
    columns: Optional[List[str]]  # None means all columns
    values: List[Any]


@dataclass
class SelectCommand:
    """SELECT columns FROM table_name [WHERE expr] [ORDER BY ...] [LIMIT n] [OFFSET n]"""
    table_name: str
    columns: List[str]  # ['*'] or specific columns
    where: Optional[Expression]
    order_by: Optional[List[tuple]]  # [(column, 'ASC'|'DESC'), ...]
    limit: Optional[int]
    offset: Optional[int]


@dataclass
class UpdateCommand:
    """UPDATE table_name SET col=val, ... [WHERE expr]"""
    table_name: str
    assignments: List[tuple]  # [(column, value_expr), ...]
    where: Optional[Expression]


@dataclass
class DeleteCommand:
    """DELETE FROM table_name [WHERE expr]"""
    table_name: str
    where: Optional[Expression]


@dataclass
class ExplainCommand:
    """EXPLAIN query"""
    command: Any  # The command to explain (SELECT, UPDATE, DELETE)


@dataclass
class AnalyzeCommand:
    """ANALYZE [table_name]"""
    table_name: Optional[str]  # None means all tables


@dataclass
class VacuumCommand:
    """VACUUM [table_name]"""
    table_name: Optional[str]  # None means all tables


@dataclass
class AlterTableAddColumnCommand:
    """ALTER TABLE table_name ADD COLUMN column_name datatype [constraints]"""
    table_name: str
    column_name: str
    datatype: str
    nullable: bool
    unique: bool


@dataclass
class AlterTableDropColumnCommand:
    """ALTER TABLE table_name DROP COLUMN column_name"""
    table_name: str
    column_name: str


@dataclass
class AlterTableRenameColumnCommand:
    """ALTER TABLE table_name RENAME COLUMN old_name TO new_name"""
    table_name: str
    old_column_name: str
    new_column_name: str


@dataclass
class BeginCommand:
    """BEGIN [TRANSACTION]"""
    pass


@dataclass
class CommitCommand:
    """COMMIT"""
    pass


@dataclass
class RollbackCommand:
    """ROLLBACK"""
    pass
