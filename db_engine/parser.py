"""
SQL Parser - Tokenizer and recursive descent parser with detailed error messages

This module provides:
- Tokenizer: Lexical analysis (SQL text → tokens)
- Parser: Syntax analysis (tokens → command objects)
- Command objects: Structured representation of SQL commands
"""

from typing import List, Optional, Any
from dataclasses import dataclass
from enum import Enum, auto
import re


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


class Tokenizer:
    """Lexical analyzer - converts SQL text to tokens"""

    # Keywords mapping (case-insensitive)
    KEYWORDS = {
        'SELECT': TokenType.SELECT,
        'FROM': TokenType.FROM,
        'WHERE': TokenType.WHERE,
        'INSERT': TokenType.INSERT,
        'INTO': TokenType.INTO,
        'VALUES': TokenType.VALUES,
        'CREATE': TokenType.CREATE,
        'TABLE': TokenType.TABLE,
        'INDEX': TokenType.INDEX,
        'DROP': TokenType.DROP,
        'DELETE': TokenType.DELETE,
        'UPDATE': TokenType.UPDATE,
        'SET': TokenType.SET,
        'PRIMARY': TokenType.PRIMARY,
        'KEY': TokenType.KEY,
        'UNIQUE': TokenType.UNIQUE,
        'NOT': TokenType.NOT,
        'NULL': TokenType.NULL,
        'AND': TokenType.AND,
        'OR': TokenType.OR,
        'LIKE': TokenType.LIKE,
        'EXPLAIN': TokenType.EXPLAIN,
        'ANALYZE': TokenType.ANALYZE,
        'VACUUM': TokenType.VACUUM,
        'LIMIT': TokenType.LIMIT,
        'OFFSET': TokenType.OFFSET,
        'ORDER': TokenType.ORDER,
        'BY': TokenType.BY,
        'ASC': TokenType.ASC,
        'DESC': TokenType.DESC,
        'ALTER': TokenType.ALTER,
        'ADD': TokenType.ADD,
        'COLUMN': TokenType.COLUMN,
        'RENAME': TokenType.RENAME,
        'TO': TokenType.TO,
        'BEGIN': TokenType.BEGIN,
        'COMMIT': TokenType.COMMIT,
        'ROLLBACK': TokenType.ROLLBACK,
        'TRANSACTION': TokenType.TRANSACTION,
        'INT': TokenType.INT,
        'BIGINT': TokenType.BIGINT,
        'FLOAT': TokenType.FLOAT,
        'TEXT': TokenType.TEXT,
        'BOOLEAN': TokenType.BOOLEAN,
        'TIMESTAMP': TokenType.TIMESTAMP,
        'TRUE': TokenType.TRUE,
        'FALSE': TokenType.FALSE,
    }

    def __init__(self, sql: str):
        self.sql = sql
        self.position = 0
        self.line = 1
        self.column = 1
        self.tokens: List[Token] = []

    def tokenize(self) -> List[Token]:
        """Convert SQL string to list of tokens"""
        while self.position < len(self.sql):
            # Skip whitespace
            if self._current_char().isspace():
                self._skip_whitespace()
                continue

            # Skip comments
            if self._current_char() == '-' and self._peek() == '-':
                self._skip_comment()
                continue

            # String literals
            if self._current_char() == "'":
                self._read_string()
                continue

            # Numbers
            if self._current_char().isdigit():
                self._read_number()
                continue

            # Identifiers and keywords
            if self._current_char().isalpha() or self._current_char() == '_':
                self._read_identifier_or_keyword()
                continue

            # Operators and punctuation
            if self._try_operator():
                continue

            # Unknown character
            raise SyntaxError(
                f"Unexpected character '{self._current_char()}' at line {self.line}, column {self.column}"
            )

        # Add EOF token
        self.tokens.append(Token(TokenType.EOF, None, self.position, self.line, self.column))
        return self.tokens

    def _current_char(self) -> str:
        """Get current character"""
        if self.position >= len(self.sql):
            return '\0'
        return self.sql[self.position]

    def _peek(self, offset: int = 1) -> str:
        """Look ahead at next character"""
        pos = self.position + offset
        if pos >= len(self.sql):
            return '\0'
        return self.sql[pos]

    def _advance(self) -> str:
        """Move to next character"""
        char = self._current_char()
        self.position += 1
        if char == '\n':
            self.line += 1
            self.column = 1
        else:
            self.column += 1
        return char

    def _skip_whitespace(self):
        """Skip whitespace characters"""
        while self._current_char().isspace():
            self._advance()

    def _skip_comment(self):
        """Skip single-line comment (-- ...)"""
        while self._current_char() != '\n' and self._current_char() != '\0':
            self._advance()

    def _read_string(self):
        """Read string literal enclosed in single quotes"""
        start_pos = self.position
        start_line = self.line
        start_col = self.column

        self._advance()  # Skip opening quote

        value = ''
        while self._current_char() != "'" and self._current_char() != '\0':
            if self._current_char() == '\\' and self._peek() == "'":
                # Escaped quote
                self._advance()
                value += self._advance()
            else:
                value += self._advance()

        if self._current_char() != "'":
            raise SyntaxError(
                f"Unterminated string literal at line {start_line}, column {start_col}"
            )

        self._advance()  # Skip closing quote

        self.tokens.append(Token(TokenType.STRING, value, start_pos, start_line, start_col))

    def _read_number(self):
        """Read integer or float literal"""
        start_pos = self.position
        start_line = self.line
        start_col = self.column

        value = ''
        has_dot = False

        while self._current_char().isdigit() or self._current_char() == '.':
            if self._current_char() == '.':
                if has_dot:
                    raise SyntaxError(
                        f"Invalid number format at line {start_line}, column {start_col}"
                    )
                has_dot = True
            value += self._advance()

        # Convert to appropriate type
        if has_dot:
            num_value = float(value)
        else:
            num_value = int(value)

        self.tokens.append(Token(TokenType.NUMBER, num_value, start_pos, start_line, start_col))

    def _read_identifier_or_keyword(self):
        """Read identifier or keyword"""
        start_pos = self.position
        start_line = self.line
        start_col = self.column

        value = ''
        while self._current_char().isalnum() or self._current_char() == '_':
            value += self._advance()

        # Check if it's a keyword
        upper_value = value.upper()
        if upper_value in self.KEYWORDS:
            token_type = self.KEYWORDS[upper_value]
        else:
            token_type = TokenType.IDENTIFIER

        self.tokens.append(Token(token_type, value, start_pos, start_line, start_col))

    def _try_operator(self) -> bool:
        """Try to read operator or punctuation"""
        start_pos = self.position
        start_line = self.line
        start_col = self.column

        char = self._current_char()
        next_char = self._peek()

        # Two-character operators
        if char == '!' and next_char == '=':
            self._advance()
            self._advance()
            self.tokens.append(Token(TokenType.NEQ, '!=', start_pos, start_line, start_col))
            return True
        elif char == '<' and next_char == '=':
            self._advance()
            self._advance()
            self.tokens.append(Token(TokenType.LTE, '<=', start_pos, start_line, start_col))
            return True
        elif char == '>' and next_char == '=':
            self._advance()
            self._advance()
            self.tokens.append(Token(TokenType.GTE, '>=', start_pos, start_line, start_col))
            return True

        # Single-character operators/punctuation
        single_chars = {
            '=': TokenType.EQ,
            '<': TokenType.LT,
            '>': TokenType.GT,
            '(': TokenType.LPAREN,
            ')': TokenType.RPAREN,
            ',': TokenType.COMMA,
            ';': TokenType.SEMICOLON,
            '*': TokenType.STAR,
        }

        if char in single_chars:
            self._advance()
            self.tokens.append(Token(single_chars[char], char, start_pos, start_line, start_col))
            return True

        return False


# ============================================================================
# Expression Tree (for WHERE clauses)
# ============================================================================

class Expression:
    """Base class for expression nodes"""
    pass


@dataclass
class BinaryOp(Expression):
    """Binary operation: left op right"""
    op: str  # '=', '!=', '<', '>', '<=', '>=', 'AND', 'OR', 'LIKE'
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
    columns: List[tuple]  # [(name, datatype, nullable, unique), ...]
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


# ============================================================================
# Parser (Recursive Descent)
# ============================================================================

class Parser:
    """Syntax analyzer - converts tokens to command objects"""

    def __init__(self, tokens: List[Token]):
        self.tokens = tokens
        self.position = 0

    def parse(self):
        """Main entry point - parse SQL command"""
        if self._at_end():
            raise SyntaxError("Empty SQL statement")

        token = self._current()

        # Dispatch based on first keyword
        if token.type == TokenType.SELECT:
            return self._parse_select()
        elif token.type == TokenType.INSERT:
            return self._parse_insert()
        elif token.type == TokenType.UPDATE:
            return self._parse_update()
        elif token.type == TokenType.DELETE:
            return self._parse_delete()
        elif token.type == TokenType.CREATE:
            return self._parse_create()
        elif token.type == TokenType.DROP:
            return self._parse_drop()
        elif token.type == TokenType.EXPLAIN:
            return self._parse_explain()
        elif token.type == TokenType.ANALYZE:
            return self._parse_analyze()
        elif token.type == TokenType.VACUUM:
            return self._parse_vacuum()
        elif token.type == TokenType.ALTER:
            return self._parse_alter()
        elif token.type == TokenType.BEGIN:
            return self._parse_begin()
        elif token.type == TokenType.COMMIT:
            return self._parse_commit()
        elif token.type == TokenType.ROLLBACK:
            return self._parse_rollback()
        else:
            raise SyntaxError(
                f"Unexpected token '{token.value}' at line {token.line}, column {token.column}. "
                f"Expected SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, EXPLAIN, ANALYZE, VACUUM, ALTER, BEGIN, COMMIT, or ROLLBACK."
            )

    # ========================================================================
    # Helper methods for token navigation
    # ========================================================================

    def _current(self) -> Token:
        """Get current token"""
        if self.position >= len(self.tokens):
            return self.tokens[-1]  # Return EOF
        return self.tokens[self.position]

    def _peek(self, offset: int = 1) -> Token:
        """Look ahead at token"""
        pos = self.position + offset
        if pos >= len(self.tokens):
            return self.tokens[-1]  # Return EOF
        return self.tokens[pos]

    def _advance(self) -> Token:
        """Move to next token and return current"""
        token = self._current()
        if not self._at_end():
            self.position += 1
        return token

    def _at_end(self) -> bool:
        """Check if at end of tokens"""
        return self._current().type == TokenType.EOF

    def _expect(self, token_type: TokenType, message: str = None) -> Token:
        """Consume token of expected type or raise error"""
        token = self._current()
        if token.type != token_type:
            if message:
                raise SyntaxError(
                    f"{message} at line {token.line}, column {token.column}. "
                    f"Got '{token.value}' ({token.type.name})"
                )
            else:
                raise SyntaxError(
                    f"Expected {token_type.name} at line {token.line}, column {token.column}. "
                    f"Got '{token.value}' ({token.type.name})"
                )
        return self._advance()

    def _match(self, *token_types: TokenType) -> bool:
        """Check if current token matches any of the given types"""
        return self._current().type in token_types

    def _consume_if(self, token_type: TokenType) -> bool:
        """Consume token if it matches, return True if consumed"""
        if self._match(token_type):
            self._advance()
            return True
        return False

    # ========================================================================
    # SELECT parsing
    # ========================================================================

    def _parse_select(self) -> SelectCommand:
        """Parse SELECT statement"""
        self._expect(TokenType.SELECT)

        # Parse columns
        columns = []
        if self._match(TokenType.STAR):
            columns.append('*')
            self._advance()
        else:
            # Column list
            columns.append(self._expect(TokenType.IDENTIFIER, "Expected column name").value)
            while self._consume_if(TokenType.COMMA):
                columns.append(self._expect(TokenType.IDENTIFIER, "Expected column name").value)

        # FROM clause
        self._expect(TokenType.FROM, "Expected FROM after column list")
        table_name = self._expect(TokenType.IDENTIFIER, "Expected table name").value

        # Optional WHERE clause
        where_expr = None
        if self._consume_if(TokenType.WHERE):
            where_expr = self._parse_expression()

        # Optional ORDER BY clause
        order_by = None
        if self._match(TokenType.ORDER):
            order_by = self._parse_order_by()

        # Optional LIMIT clause
        limit = None
        if self._consume_if(TokenType.LIMIT):
            limit_token = self._expect(TokenType.NUMBER, "Expected number after LIMIT")
            limit = int(limit_token.value)

        # Optional OFFSET clause
        offset = None
        if self._consume_if(TokenType.OFFSET):
            offset_token = self._expect(TokenType.NUMBER, "Expected number after OFFSET")
            offset = int(offset_token.value)

        # Optional semicolon
        self._consume_if(TokenType.SEMICOLON)

        return SelectCommand(table_name, columns, where_expr, order_by, limit, offset)

    def _parse_order_by(self) -> List[tuple]:
        """Parse ORDER BY clause"""
        self._expect(TokenType.ORDER)
        self._expect(TokenType.BY)

        order_by = []
        # First column
        col = self._expect(TokenType.IDENTIFIER, "Expected column name").value
        direction = 'ASC'
        if self._match(TokenType.ASC, TokenType.DESC):
            direction = self._advance().type.name
        order_by.append((col, direction))

        # Additional columns
        while self._consume_if(TokenType.COMMA):
            col = self._expect(TokenType.IDENTIFIER, "Expected column name").value
            direction = 'ASC'
            if self._match(TokenType.ASC, TokenType.DESC):
                direction = self._advance().type.name
            order_by.append((col, direction))

        return order_by

    # ========================================================================
    # WHERE clause expression parsing (with operator precedence)
    # ========================================================================

    def _parse_expression(self) -> Expression:
        """Parse expression (entry point for WHERE clause)"""
        return self._parse_or()

    def _parse_or(self) -> Expression:
        """Parse OR expression (lowest precedence)"""
        left = self._parse_and()

        while self._consume_if(TokenType.OR):
            right = self._parse_and()
            left = BinaryOp('OR', left, right)

        return left

    def _parse_and(self) -> Expression:
        """Parse AND expression"""
        left = self._parse_not()

        while self._consume_if(TokenType.AND):
            right = self._parse_not()
            left = BinaryOp('AND', left, right)

        return left

    def _parse_not(self) -> Expression:
        """Parse NOT expression"""
        if self._consume_if(TokenType.NOT):
            operand = self._parse_not()  # Right-associative
            return UnaryOp('NOT', operand)

        return self._parse_comparison()

    def _parse_comparison(self) -> Expression:
        """Parse comparison expression"""
        left = self._parse_primary()

        # Comparison operators
        if self._match(TokenType.EQ, TokenType.NEQ, TokenType.LT, TokenType.GT,
                       TokenType.LTE, TokenType.GTE, TokenType.LIKE):
            op_token = self._advance()

            # Map token type to operator string
            op_map = {
                TokenType.EQ: '=',
                TokenType.NEQ: '!=',
                TokenType.LT: '<',
                TokenType.GT: '>',
                TokenType.LTE: '<=',
                TokenType.GTE: '>=',
                TokenType.LIKE: 'LIKE',
            }
            op = op_map[op_token.type]

            right = self._parse_primary()
            return BinaryOp(op, left, right)

        return left

    def _parse_primary(self) -> Expression:
        """Parse primary expression (literals, column refs, parentheses)"""
        token = self._current()

        # Parenthesized expression
        if self._consume_if(TokenType.LPAREN):
            expr = self._parse_expression()
            self._expect(TokenType.RPAREN, "Expected ')' after expression")
            return expr

        # NULL literal
        if self._consume_if(TokenType.NULL):
            return Literal(None, 'NULL')

        # Boolean literals
        if self._consume_if(TokenType.TRUE):
            return Literal(True, 'BOOLEAN')

        if self._consume_if(TokenType.FALSE):
            return Literal(False, 'BOOLEAN')

        # Number literal
        if self._match(TokenType.NUMBER):
            value = self._advance().value
            datatype = 'FLOAT' if isinstance(value, float) else 'INT'
            return Literal(value, datatype)

        # String literal
        if self._match(TokenType.STRING):
            value = self._advance().value
            return Literal(value, 'STRING')

        # Column reference
        if self._match(TokenType.IDENTIFIER):
            col_name = self._advance().value
            return ColumnRef(col_name)

        raise SyntaxError(
            f"Unexpected token '{token.value}' at line {token.line}, column {token.column}. "
            f"Expected expression (literal, column name, or parenthesized expression)"
        )
    # ========================================================================
    # INSERT parsing
    # ========================================================================

    def _parse_insert(self) -> InsertCommand:
        """Parse INSERT statement"""
        self._expect(TokenType.INSERT)
        self._expect(TokenType.INTO)
        table_name = self._expect(TokenType.IDENTIFIER, "Expected table name").value

        # Optional column list
        columns = None
        if self._consume_if(TokenType.LPAREN):
            columns = []
            columns.append(self._expect(TokenType.IDENTIFIER, "Expected column name").value)
            while self._consume_if(TokenType.COMMA):
                columns.append(self._expect(TokenType.IDENTIFIER, "Expected column name").value)
            self._expect(TokenType.RPAREN, "Expected ')' after column list")

        # VALUES clause
        self._expect(TokenType.VALUES, "Expected VALUES keyword")
        self._expect(TokenType.LPAREN, "Expected '(' after VALUES")

        # Parse values
        values = []
        values.append(self._parse_value())
        while self._consume_if(TokenType.COMMA):
            values.append(self._parse_value())

        self._expect(TokenType.RPAREN, "Expected ')' after values")
        self._consume_if(TokenType.SEMICOLON)

        return InsertCommand(table_name, columns, values)

    def _parse_value(self) -> Any:
        """Parse a value in VALUES clause"""
        token = self._current()

        if self._consume_if(TokenType.NULL):
            return None
        elif self._consume_if(TokenType.TRUE):
            return True
        elif self._consume_if(TokenType.FALSE):
            return False
        elif self._match(TokenType.NUMBER):
            return self._advance().value
        elif self._match(TokenType.STRING):
            return self._advance().value
        else:
            raise SyntaxError(
                f"Expected value at line {token.line}, column {token.column}. "
                f"Got '{token.value}'"
            )

    # ========================================================================
    # CREATE parsing
    # ========================================================================

    def _parse_create(self):
        """Parse CREATE TABLE or CREATE INDEX"""
        self._expect(TokenType.CREATE)

        if self._match(TokenType.TABLE):
            return self._parse_create_table()
        elif self._match(TokenType.UNIQUE):
            return self._parse_create_index(unique=True)
        elif self._match(TokenType.INDEX):
            return self._parse_create_index(unique=False)
        else:
            token = self._current()
            raise SyntaxError(
                f"Expected TABLE or INDEX after CREATE at line {token.line}, column {token.column}"
            )

    def _parse_create_table(self) -> CreateTableCommand:
        """Parse CREATE TABLE statement"""
        self._expect(TokenType.TABLE)
        table_name = self._expect(TokenType.IDENTIFIER, "Expected table name").value

        self._expect(TokenType.LPAREN, "Expected '(' after table name")

        # Parse column definitions
        columns = []
        primary_key = []

        while not self._match(TokenType.RPAREN):
            # Check for PRIMARY KEY constraint
            if self._match(TokenType.PRIMARY):
                self._advance()
                self._expect(TokenType.KEY, "Expected KEY after PRIMARY")
                self._expect(TokenType.LPAREN, "Expected '(' after PRIMARY KEY")

                # Parse primary key columns
                primary_key.append(self._expect(TokenType.IDENTIFIER, "Expected column name").value)
                while self._consume_if(TokenType.COMMA):
                    primary_key.append(self._expect(TokenType.IDENTIFIER, "Expected column name").value)

                self._expect(TokenType.RPAREN, "Expected ')' after primary key columns")

                # Optional trailing comma
                self._consume_if(TokenType.COMMA)
                continue

            # Parse column definition
            col_name = self._expect(TokenType.IDENTIFIER, "Expected column name").value

            # Data type
            if not self._match(TokenType.INT, TokenType.BIGINT, TokenType.FLOAT,
                               TokenType.TEXT, TokenType.BOOLEAN, TokenType.TIMESTAMP):
                token = self._current()
                raise SyntaxError(
                    f"Expected data type at line {token.line}, column {token.column}. "
                    f"Got '{token.value}'"
                )

            datatype = self._advance().type.name

            # Column constraints
            nullable = True
            unique = False
            is_primary_key = False

            while True:
                if self._match(TokenType.PRIMARY):
                    self._advance()
                    self._expect(TokenType.KEY, "Expected KEY after PRIMARY")
                    is_primary_key = True
                    nullable = False
                    primary_key.append(col_name)
                elif self._match(TokenType.NOT):
                    self._advance()
                    self._expect(TokenType.NULL, "Expected NULL after NOT")
                    nullable = False
                elif self._match(TokenType.UNIQUE):
                    self._advance()
                    unique = True
                else:
                    break

            columns.append((col_name, datatype, nullable, unique))

            # Check for comma
            if not self._consume_if(TokenType.COMMA):
                break

        self._expect(TokenType.RPAREN, "Expected ')' after column definitions")
        self._consume_if(TokenType.SEMICOLON)

        if not primary_key:
            raise SyntaxError("Table must have a PRIMARY KEY")

        return CreateTableCommand(table_name, columns, primary_key)

    def _parse_create_index(self, unique: bool) -> CreateIndexCommand:
        """Parse CREATE [UNIQUE] INDEX statement"""
        if unique:
            self._expect(TokenType.UNIQUE)

        self._expect(TokenType.INDEX)
        index_name = self._expect(TokenType.IDENTIFIER, "Expected index name").value

        # Handle ON keyword (it's parsed as IDENTIFIER)
        if not self._match(TokenType.IDENTIFIER) or self._current().value.upper() != 'ON':
            token = self._current()
            raise SyntaxError(f"Expected ON keyword at line {token.line}, column {token.column}")
        self._advance()

        table_name = self._expect(TokenType.IDENTIFIER, "Expected table name").value

        self._expect(TokenType.LPAREN, "Expected '(' after table name")

        # Parse column list
        columns = []
        columns.append(self._expect(TokenType.IDENTIFIER, "Expected column name").value)
        while self._consume_if(TokenType.COMMA):
            columns.append(self._expect(TokenType.IDENTIFIER, "Expected column name").value)

        self._expect(TokenType.RPAREN, "Expected ')' after column list")
        self._consume_if(TokenType.SEMICOLON)

        return CreateIndexCommand(index_name, table_name, columns, unique)

    # ========================================================================
    # UPDATE parsing
    # ========================================================================

    def _parse_update(self) -> UpdateCommand:
        """Parse UPDATE statement"""
        self._expect(TokenType.UPDATE)
        table_name = self._expect(TokenType.IDENTIFIER, "Expected table name").value

        self._expect(TokenType.SET, "Expected SET keyword")

        # Parse assignments: col = value
        assignments = []
        col = self._expect(TokenType.IDENTIFIER, "Expected column name").value
        self._expect(TokenType.EQ, "Expected '=' after column name")
        value_expr = self._parse_primary()
        assignments.append((col, value_expr))

        while self._consume_if(TokenType.COMMA):
            col = self._expect(TokenType.IDENTIFIER, "Expected column name").value
            self._expect(TokenType.EQ, "Expected '=' after column name")
            value_expr = self._parse_primary()
            assignments.append((col, value_expr))

        # Optional WHERE clause
        where_expr = None
        if self._consume_if(TokenType.WHERE):
            where_expr = self._parse_expression()

        self._consume_if(TokenType.SEMICOLON)

        return UpdateCommand(table_name, assignments, where_expr)

    # ========================================================================
    # DELETE parsing
    # ========================================================================

    def _parse_delete(self) -> DeleteCommand:
        """Parse DELETE statement"""
        self._expect(TokenType.DELETE)
        self._expect(TokenType.FROM, "Expected FROM after DELETE")
        table_name = self._expect(TokenType.IDENTIFIER, "Expected table name").value

        # Optional WHERE clause
        where_expr = None
        if self._consume_if(TokenType.WHERE):
            where_expr = self._parse_expression()

        self._consume_if(TokenType.SEMICOLON)

        return DeleteCommand(table_name, where_expr)

    # ========================================================================
    # DROP parsing
    # ========================================================================

    def _parse_drop(self) -> DropTableCommand:
        """Parse DROP TABLE statement"""
        self._expect(TokenType.DROP)
        self._expect(TokenType.TABLE, "Expected TABLE after DROP")
        table_name = self._expect(TokenType.IDENTIFIER, "Expected table name").value
        self._consume_if(TokenType.SEMICOLON)

        return DropTableCommand(table_name)

    # ========================================================================
    # Utility commands
    # ========================================================================

    def _parse_explain(self) -> ExplainCommand:
        """Parse EXPLAIN statement"""
        self._expect(TokenType.EXPLAIN)

        # Parse the query to explain
        query = self.parse()

        return ExplainCommand(query)

    def _parse_analyze(self) -> AnalyzeCommand:
        """Parse ANALYZE statement"""
        self._expect(TokenType.ANALYZE)

        # Optional table name
        table_name = None
        if self._match(TokenType.IDENTIFIER):
            table_name = self._advance().value

        self._consume_if(TokenType.SEMICOLON)

        return AnalyzeCommand(table_name)

    def _parse_vacuum(self) -> VacuumCommand:
        """Parse VACUUM statement"""
        self._expect(TokenType.VACUUM)

        # Optional table name
        table_name = None
        if self._match(TokenType.IDENTIFIER):
            table_name = self._advance().value

        self._consume_if(TokenType.SEMICOLON)

        return VacuumCommand(table_name)

    def _parse_alter(self):
        """Parse ALTER TABLE statement"""
        self._expect(TokenType.ALTER)
        self._expect(TokenType.TABLE)

        # Table name
        table_name_token = self._expect(TokenType.IDENTIFIER)
        table_name = table_name_token.value

        # Determine ALTER operation
        if self._match(TokenType.ADD):
            return self._parse_alter_add_column(table_name)
        elif self._match(TokenType.DROP):
            return self._parse_alter_drop_column(table_name)
        elif self._match(TokenType.RENAME):
            return self._parse_alter_rename_column(table_name)
        else:
            token = self._current()
            raise SyntaxError(
                f"Unexpected token '{token.value}' at line {token.line}, column {token.column}. "
                f"Expected ADD, DROP, or RENAME after ALTER TABLE."
            )

    def _parse_alter_add_column(self, table_name: str) -> AlterTableAddColumnCommand:
        """Parse ALTER TABLE ... ADD COLUMN"""
        self._expect(TokenType.ADD)
        self._consume_if(TokenType.COLUMN)  # COLUMN is optional

        # Column name
        col_name_token = self._expect(TokenType.IDENTIFIER)
        col_name = col_name_token.value

        # Datatype
        datatype_token = self._advance()
        if datatype_token.type not in [TokenType.INT, TokenType.BIGINT, TokenType.FLOAT,
                                       TokenType.TEXT, TokenType.BOOLEAN, TokenType.TIMESTAMP]:
            raise SyntaxError(
                f"Invalid datatype '{datatype_token.value}' at line {datatype_token.line}, "
                f"column {datatype_token.column}."
            )
        datatype = datatype_token.value

        # Parse constraints (UNIQUE, NOT NULL)
        nullable = True
        unique = False

        while True:
            if self._match(TokenType.UNIQUE):
                self._advance()
                unique = True
            elif self._match(TokenType.NOT):
                self._advance()
                self._expect(TokenType.NULL)
                nullable = False
            else:
                break

        self._consume_if(TokenType.SEMICOLON)

        return AlterTableAddColumnCommand(table_name, col_name, datatype, nullable, unique)

    def _parse_alter_drop_column(self, table_name: str) -> AlterTableDropColumnCommand:
        """Parse ALTER TABLE ... DROP COLUMN"""
        self._expect(TokenType.DROP)
        self._consume_if(TokenType.COLUMN)  # COLUMN is optional

        # Column name
        col_name_token = self._expect(TokenType.IDENTIFIER)
        col_name = col_name_token.value

        self._consume_if(TokenType.SEMICOLON)

        return AlterTableDropColumnCommand(table_name, col_name)

    def _parse_alter_rename_column(self, table_name: str) -> AlterTableRenameColumnCommand:
        """Parse ALTER TABLE ... RENAME COLUMN old_name TO new_name"""
        self._expect(TokenType.RENAME)
        self._consume_if(TokenType.COLUMN)  # COLUMN is optional

        # Old column name
        old_name_token = self._expect(TokenType.IDENTIFIER)
        old_name = old_name_token.value

        # TO keyword
        self._expect(TokenType.TO)

        # New column name
        new_name_token = self._expect(TokenType.IDENTIFIER)
        new_name = new_name_token.value

        self._consume_if(TokenType.SEMICOLON)

        return AlterTableRenameColumnCommand(table_name, old_name, new_name)

    def _parse_begin(self) -> BeginCommand:
        """Parse BEGIN [TRANSACTION] statement"""
        self._expect(TokenType.BEGIN)
        self._consume_if(TokenType.TRANSACTION)  # TRANSACTION is optional
        self._consume_if(TokenType.SEMICOLON)
        return BeginCommand()

    def _parse_commit(self) -> CommitCommand:
        """Parse COMMIT statement"""
        self._expect(TokenType.COMMIT)
        self._consume_if(TokenType.SEMICOLON)
        return CommitCommand()

    def _parse_rollback(self) -> RollbackCommand:
        """Parse ROLLBACK statement"""
        self._expect(TokenType.ROLLBACK)
        self._consume_if(TokenType.SEMICOLON)
        return RollbackCommand()


# ============================================================================
# Convenience function
# ============================================================================

def parse_sql(sql: str):
    """Parse SQL string to command object"""
    tokenizer = Tokenizer(sql)
    tokens = tokenizer.tokenize()
    parser = Parser(tokens)
    return parser.parse()
