"""
SQL Parser Package

This package provides SQL parsing functionality:
- tokens: TokenType enum and Token dataclass
- ast: Expression and Command AST nodes
- parser: Tokenizer, Parser, and parse_sql function

For backward compatibility, all public symbols are re-exported here.
"""

# Token types and Token class
from .tokens import TokenType, Token

# AST nodes - Expressions
from .ast import (
    Expression,
    BinaryOp,
    UnaryOp,
    Literal,
    ColumnRef,
)

# AST nodes - Commands
from .ast import (
    CreateTableCommand,
    CreateIndexCommand,
    DropTableCommand,
    DropIndexCommand,
    TruncateTableCommand,
    InsertCommand,
    SelectCommand,
    UpdateCommand,
    DeleteCommand,
    ExplainCommand,
    AnalyzeCommand,
    VacuumCommand,
    AlterTableAddColumnCommand,
    AlterTableDropColumnCommand,
    AlterTableRenameColumnCommand,
    BeginCommand,
    CommitCommand,
    RollbackCommand,
)

# Parser classes and functions
from .parser import Tokenizer, Parser, parse_sql

__all__ = [
    # Tokens
    'TokenType',
    'Token',
    # Expressions
    'Expression',
    'BinaryOp',
    'UnaryOp',
    'Literal',
    'ColumnRef',
    # Commands
    'CreateTableCommand',
    'CreateIndexCommand',
    'DropTableCommand',
    'DropIndexCommand',
    'TruncateTableCommand',
    'InsertCommand',
    'SelectCommand',
    'UpdateCommand',
    'DeleteCommand',
    'ExplainCommand',
    'AnalyzeCommand',
    'VacuumCommand',
    'AlterTableAddColumnCommand',
    'AlterTableDropColumnCommand',
    'AlterTableRenameColumnCommand',
    'BeginCommand',
    'CommitCommand',
    'RollbackCommand',
    # Parser
    'Tokenizer',
    'Parser',
    'parse_sql',
]
