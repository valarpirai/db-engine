"""
Token definitions for SQL lexical analysis.

This module provides:
- TokenType: Enum of all SQL token types
- Token: Dataclass representing a single token
"""

from typing import Any
from dataclasses import dataclass
from enum import Enum, auto


class TokenType(Enum):
    """Token types for SQL lexical analysis"""
    # Keywords
    SELECT = auto()
    FROM = auto()
    WHERE = auto()
    INSERT = auto()
    INTO = auto()
    VALUES = auto()
    CREATE = auto()
    TABLE = auto()
    INDEX = auto()
    DROP = auto()
    DELETE = auto()
    UPDATE = auto()
    SET = auto()
    PRIMARY = auto()
    KEY = auto()
    UNIQUE = auto()
    NOT = auto()
    NULL = auto()
    AND = auto()
    OR = auto()
    LIKE = auto()
    EXPLAIN = auto()
    VERBOSE = auto()
    ANALYZE = auto()
    VACUUM = auto()
    LIMIT = auto()
    OFFSET = auto()
    ORDER = auto()
    BY = auto()
    ASC = auto()
    DESC = auto()
    ALTER = auto()
    ADD = auto()
    COLUMN = auto()
    RENAME = auto()
    TO = auto()
    BEGIN = auto()
    COMMIT = auto()
    ROLLBACK = auto()
    TRANSACTION = auto()
    BETWEEN = auto()
    IS = auto()
    TRUNCATE = auto()
    AUTOINCREMENT = auto()

    # Data types
    INT = auto()
    BIGINT = auto()
    FLOAT = auto()
    TEXT = auto()
    BOOLEAN = auto()
    TIMESTAMP = auto()

    # Literals
    NUMBER = auto()
    STRING = auto()
    TRUE = auto()
    FALSE = auto()

    # Identifiers
    IDENTIFIER = auto()

    # Operators
    EQ = auto()          # =
    NEQ = auto()         # !=
    LT = auto()          # <
    GT = auto()          # >
    LTE = auto()         # <=
    GTE = auto()         # >=

    # Punctuation
    LPAREN = auto()      # (
    RPAREN = auto()      # )
    COMMA = auto()       # ,
    SEMICOLON = auto()   # ;
    STAR = auto()        # *

    # Special
    EOF = auto()


@dataclass
class Token:
    """Represents a single token in SQL input"""
    type: TokenType
    value: Any
    position: int  # Character position in input
    line: int      # Line number (for error messages)
    column: int    # Column number (for error messages)

    def __repr__(self):
        return f"Token({self.type.name}, {self.value!r}, pos={self.position})"
