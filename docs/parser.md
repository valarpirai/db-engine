# SQL Parser Package Documentation

## Table of Contents
1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Package Structure](#package-structure)
4. [Token System (tokens.py)](#token-system)
5. [AST Nodes (ast.py)](#ast-nodes)
6. [Tokenizer (parser.py)](#tokenizer)
7. [Parser (parser.py)](#parser)
8. [Expression Parsing](#expression-parsing)
9. [Usage Examples](#usage-examples)

---

## Overview

The `db_engine/parser/` package implements a **hand-written SQL parser** that converts SQL text into structured command objects (Abstract Syntax Trees). It's designed as an educational tool to understand parsing fundamentals without relying on external libraries.

### Two-Phase Compilation

```
SQL Text → [Tokenizer] → Tokens → [Parser] → Command Objects (AST)
```

**Phase 1: Lexical Analysis (Tokenization)**
- Input: Raw SQL string
- Output: List of Token objects
- Purpose: Break text into meaningful units (keywords, identifiers, operators, literals)

**Phase 2: Syntax Analysis (Parsing)**
- Input: List of Token objects
- Output: Command objects (AST nodes)
- Purpose: Verify syntax rules and build structured representation

### Design Philosophy

- **Hand-written**: No parser generators (yacc, ANTLR) or libraries
- **Readable**: Clear code structure over performance optimization
- **Recursive Descent**: Top-down parsing with one method per grammar rule
- **Educational**: Extensive comments explaining parsing techniques
- **Error Handling**: Detailed messages with line/column numbers

---

## Architecture

The parser is organized into three modules:

```
db_engine/parser/
├── __init__.py          # Re-exports all public symbols
├── tokens.py            # Token types and Token dataclass (105 lines)
├── ast.py               # Expression and Command AST nodes (169 lines)
└── parser.py            # Tokenizer and Parser classes (994 lines)
```

Total: ~1,268 lines of code

---

## Package Structure

### Module: `__init__.py`

Re-exports all public symbols for backward compatibility:

```python
from db_engine.parser import (
    # Token system
    TokenType, Token,

    # Expressions
    Expression, BinaryOp, UnaryOp, Literal, ColumnRef,

    # Commands
    CreateTableCommand, SelectCommand, InsertCommand, ...,

    # Main parser
    Tokenizer, Parser, parse_sql
)
```

This allows users to import directly from `db_engine.parser` without knowing the internal module structure.

---

## Token System (tokens.py)

### TokenType Enum

Defines 58 token types organized by category:

**Keywords (42 types)**
```python
SELECT, FROM, WHERE, INSERT, INTO, VALUES, CREATE, TABLE, INDEX,
DROP, DELETE, UPDATE, SET, PRIMARY, KEY, UNIQUE, NOT, NULL, AND, OR,
LIKE, EXPLAIN, ANALYZE, VACUUM, LIMIT, OFFSET, ORDER, BY, ASC, DESC,
ALTER, ADD, COLUMN, RENAME, TO, BEGIN, COMMIT, ROLLBACK, TRANSACTION,
BETWEEN, IS, TRUNCATE, AUTOINCREMENT
```

**Data Types (6 types)**
```python
INT, BIGINT, FLOAT, TEXT, BOOLEAN, TIMESTAMP
```

**Literals (4 types)**
```python
NUMBER     # Integer or float literal (e.g., 42, 3.14)
STRING     # Text literal (e.g., 'hello')
TRUE       # Boolean true
FALSE      # Boolean false
```

**Identifiers (1 type)**
```python
IDENTIFIER # Table names, column names, index names
```

**Operators (6 types)**
```python
EQ         # =
NEQ        # !=
LT         # <
GT         # >
LTE        # <=
GTE        # >=
```

**Punctuation (5 types)**
```python
LPAREN     # (
RPAREN     # )
COMMA      # ,
SEMICOLON  # ;
STAR       # *
```

**Special (1 type)**
```python
EOF        # End of input
```

### Token Dataclass

Each token stores:

```python
@dataclass
class Token:
    type: TokenType      # Token type (from enum)
    value: Any           # Actual value (e.g., 'users', 42, '=')
    position: int        # Character position in input
    line: int            # Line number (1-indexed)
    column: int          # Column number (1-indexed)
```

**Example:**
```python
# Input: "SELECT * FROM users"
Token(SELECT, 'SELECT', pos=0, line=1, column=1)
Token(STAR, '*', pos=7, line=1, column=8)
Token(FROM, 'FROM', pos=9, line=1, column=10)
Token(IDENTIFIER, 'users', pos=14, line=1, column=15)
Token(EOF, None, pos=19, line=1, column=20)
```

Position tracking enables precise error messages:
```
SyntaxError: Unexpected token '=' at line 3, column 15
```

---

## AST Nodes (ast.py)

The Abstract Syntax Tree consists of two categories: **Expressions** (for WHERE clauses) and **Commands** (for SQL statements).

### Expression Nodes

Used to represent WHERE clause conditions and value expressions.

#### BinaryOp
Binary operation with left operand, operator, and right operand.

```python
@dataclass
class BinaryOp(Expression):
    op: str           # '=', '!=', '<', '>', '<=', '>=', 'AND', 'OR', 'LIKE', 'IS'
    left: Expression
    right: Expression
```

**Example:**
```python
# age > 18
BinaryOp('>', ColumnRef('age'), Literal(18, 'INT'))

# name = 'Alice' AND age > 18
BinaryOp('AND',
    BinaryOp('=', ColumnRef('name'), Literal('Alice', 'STRING')),
    BinaryOp('>', ColumnRef('age'), Literal(18, 'INT'))
)
```

#### UnaryOp
Unary operation (currently only NOT).

```python
@dataclass
class UnaryOp(Expression):
    op: str           # 'NOT'
    operand: Expression
```

**Example:**
```python
# NOT active
UnaryOp('NOT', ColumnRef('active'))
```

#### Literal
Constant value (number, string, boolean, NULL).

```python
@dataclass
class Literal(Expression):
    value: Any        # The actual value
    datatype: str     # 'INT', 'FLOAT', 'STRING', 'BOOLEAN', 'NULL'
```

**Example:**
```python
Literal(42, 'INT')
Literal(3.14, 'FLOAT')
Literal('hello', 'STRING')
Literal(True, 'BOOLEAN')
Literal(None, 'NULL')
```

#### ColumnRef
Reference to a table column.

```python
@dataclass
class ColumnRef(Expression):
    column_name: str
```

**Example:**
```python
ColumnRef('age')
ColumnRef('email')
```

### Command Nodes

Represent parsed SQL statements. Each command corresponds to one SQL statement type.

#### DDL Commands

**CreateTableCommand**
```python
@dataclass
class CreateTableCommand:
    table_name: str
    columns: List[tuple]    # [(name, datatype, nullable, unique, autoincrement), ...]
    primary_key: List[str]  # Column names in primary key
```

**CreateIndexCommand**
```python
@dataclass
class CreateIndexCommand:
    index_name: str
    table_name: str
    columns: List[str]      # Index columns
    unique: bool            # UNIQUE index?
```

**DropTableCommand**, **DropIndexCommand**, **TruncateTableCommand**
```python
@dataclass
class DropTableCommand:
    table_name: str

@dataclass
class DropIndexCommand:
    index_name: str
    table_name: str

@dataclass
class TruncateTableCommand:
    table_name: str
```

#### DML Commands

**SelectCommand**
```python
@dataclass
class SelectCommand:
    table_name: str
    columns: List[str]                  # ['*'] or specific columns
    where: Optional[Expression]         # WHERE clause expression tree
    order_by: Optional[List[tuple]]     # [(column, 'ASC'|'DESC'), ...]
    limit: Optional[int]
    offset: Optional[int]
```

**InsertCommand**
```python
@dataclass
class InsertCommand:
    table_name: str
    columns: Optional[List[str]]  # None means all columns
    values: List[Any]             # Values to insert
```

**UpdateCommand**
```python
@dataclass
class UpdateCommand:
    table_name: str
    assignments: List[tuple]      # [(column, value_expr), ...]
    where: Optional[Expression]
```

**DeleteCommand**
```python
@dataclass
class DeleteCommand:
    table_name: str
    where: Optional[Expression]
```

#### Schema Modification Commands

**AlterTableAddColumnCommand**
```python
@dataclass
class AlterTableAddColumnCommand:
    table_name: str
    column_name: str
    datatype: str
    nullable: bool
    unique: bool
```

**AlterTableDropColumnCommand**, **AlterTableRenameColumnCommand**
```python
@dataclass
class AlterTableDropColumnCommand:
    table_name: str
    column_name: str

@dataclass
class AlterTableRenameColumnCommand:
    table_name: str
    old_column_name: str
    new_column_name: str
```

#### Utility Commands

**ExplainCommand**
```python
@dataclass
class ExplainCommand:
    command: Any  # The command to explain (SELECT, UPDATE, DELETE)
```

**AnalyzeCommand**, **VacuumCommand**
```python
@dataclass
class AnalyzeCommand:
    table_name: Optional[str]  # None means all tables

@dataclass
class VacuumCommand:
    table_name: Optional[str]  # None means all tables
```

#### Transaction Commands

```python
@dataclass
class BeginCommand:
    pass  # BEGIN [TRANSACTION]

@dataclass
class CommitCommand:
    pass  # COMMIT

@dataclass
class RollbackCommand:
    pass  # ROLLBACK
```

---

## Tokenizer (parser.py)

The `Tokenizer` class performs **lexical analysis** - converting raw SQL text into a list of tokens.

### Class Structure

```python
class Tokenizer:
    def __init__(self, sql: str):
        self.sql = sql           # Input SQL string
        self.position = 0        # Current character position
        self.line = 1            # Current line number
        self.column = 1          # Current column number
        self.tokens = []         # Accumulated tokens

    def tokenize(self) -> List[Token]:
        """Main entry point - convert SQL to tokens"""
        # Returns list of Token objects
```

### How It Works

The tokenizer scans the input character by character, identifying token boundaries:

1. **Skip whitespace** (spaces, tabs, newlines)
2. **Skip comments** (lines starting with `--`)
3. **Recognize token types**:
   - String literals: `'...'`
   - Numbers: `42`, `3.14`
   - Identifiers/keywords: `SELECT`, `users`, `age`
   - Operators: `=`, `!=`, `<=`, `>=`, `<`, `>`
   - Punctuation: `(`, `)`, `,`, `;`, `*`

### Key Methods

#### Character Navigation

```python
_current_char() -> str      # Get current character
_peek(offset=1) -> str      # Look ahead without advancing
_advance() -> str           # Move to next character, track line/column
```

#### Whitespace & Comments

```python
_skip_whitespace()          # Skip spaces, tabs, newlines
_skip_comment()             # Skip single-line comments (-- ...)
```

#### Token Recognition

```python
_read_string()              # Read 'string literal' with escape sequences
_read_number()              # Read integer or float (42, 3.14)
_read_identifier_or_keyword()  # Read identifier or check if keyword
_try_operator()             # Try to match operator or punctuation
```

### String Literals

Enclosed in single quotes with escape sequence support:

```python
'hello'           → Token(STRING, 'hello', ...)
'it\'s working'   → Token(STRING, "it's working", ...)
```

**Error handling:**
```python
'unterminated     → SyntaxError: Unterminated string literal at line X, column Y
```

### Number Literals

Integers and floating-point numbers:

```python
42       → Token(NUMBER, 42, ...)
3.14     → Token(NUMBER, 3.14, ...)
```

**Error handling:**
```python
3.14.15  → SyntaxError: Invalid number format at line X, column Y
```

### Identifiers vs Keywords

The tokenizer uses a keyword dictionary to distinguish:

```python
KEYWORDS = {
    'SELECT': TokenType.SELECT,
    'FROM': TokenType.FROM,
    'WHERE': TokenType.WHERE,
    # ... 58 total keywords
}
```

**Process:**
1. Read alphanumeric characters and underscores
2. Convert to uppercase: `Select` → `SELECT`
3. Check if in `KEYWORDS` dict
4. If found: create keyword token
5. If not: create `IDENTIFIER` token

**Examples:**
```python
SELECT   → Token(SELECT, 'SELECT', ...)
users    → Token(IDENTIFIER, 'users', ...)
age_2    → Token(IDENTIFIER, 'age_2', ...)
```

### Operators

Two-character operators are checked first:

```python
!=       → Token(NEQ, '!=', ...)
<=       → Token(LTE, '<=', ...)
>=       → Token(GTE, '>=', ...)
```

Then single-character:

```python
=        → Token(EQ, '=', ...)
<        → Token(LT, '<', ...)
>        → Token(GT, '>', ...)
```

### Line and Column Tracking

The tokenizer maintains line and column numbers for error messages:

```python
def _advance(self) -> str:
    char = self._current_char()
    self.position += 1
    if char == '\n':
        self.line += 1
        self.column = 1      # Reset column on newline
    else:
        self.column += 1     # Increment column
    return char
```

This enables precise error reporting:
```
SyntaxError: Unexpected character '@' at line 5, column 12
```

### Example Tokenization

Input:
```sql
SELECT name FROM users WHERE age > 18;
```

Output tokens:
```python
[
    Token(SELECT, 'SELECT', pos=0, line=1, column=1),
    Token(IDENTIFIER, 'name', pos=7, line=1, column=8),
    Token(FROM, 'FROM', pos=12, line=1, column=13),
    Token(IDENTIFIER, 'users', pos=17, line=1, column=18),
    Token(WHERE, 'WHERE', pos=23, line=1, column=24),
    Token(IDENTIFIER, 'age', pos=29, line=1, column=30),
    Token(GT, '>', pos=33, line=1, column=34),
    Token(NUMBER, 18, pos=35, line=1, column=36),
    Token(SEMICOLON, ';', pos=37, line=1, column=38),
    Token(EOF, None, pos=38, line=1, column=39)
]
```

---

## Parser (parser.py)

The `Parser` class performs **syntax analysis** - converting tokens into command objects (AST).

### Class Structure

```python
class Parser:
    def __init__(self, tokens: List[Token]):
        self.tokens = tokens     # Input token list
        self.position = 0        # Current token position

    def parse(self):
        """Main entry point - parse SQL command"""
        # Returns command object
```

### Recursive Descent Parsing

The parser uses **recursive descent** - a top-down parsing technique where:
- Each grammar rule becomes a method
- Methods call each other based on grammar structure
- Recursion handles nested expressions

**Grammar Example:**
```
command     → SELECT | INSERT | UPDATE | DELETE | ...
select      → SELECT columns FROM table [WHERE expr]
expr        → or_expr
or_expr     → and_expr (OR and_expr)*
and_expr    → not_expr (AND not_expr)*
not_expr    → NOT not_expr | comparison
comparison  → primary (op primary)?
primary     → LITERAL | COLUMN | ( expr )
```

**Corresponding Methods:**
```python
parse()           # Entry point - dispatch to command parsers
_parse_select()   # SELECT statement
_parse_expression()  # Expression entry point
_parse_or()       # OR expressions
_parse_and()      # AND expressions
_parse_not()      # NOT expressions
_parse_comparison()  # Comparison operators
_parse_primary()  # Literals, columns, parentheses
```

### Token Navigation Methods

```python
_current() -> Token              # Get current token
_peek(offset=1) -> Token         # Look ahead without advancing
_advance() -> Token              # Move to next token
_at_end() -> bool                # Check if at EOF
_match(*types) -> bool           # Check if current matches any type
_consume_if(type) -> bool        # Consume if matches, return True
_expect(type, msg) -> Token      # Consume or raise error
```

### Command Parsing

The `parse()` method dispatches based on the first token:

```python
def parse(self):
    token = self._current()

    if token.type == TokenType.SELECT:
        return self._parse_select()
    elif token.type == TokenType.INSERT:
        return self._parse_insert()
    elif token.type == TokenType.CREATE:
        return self._parse_create()
    # ... more command types
```

### SELECT Statement Parsing

Example flow for: `SELECT name FROM users WHERE age > 18`

```python
def _parse_select(self) -> SelectCommand:
    self._expect(TokenType.SELECT)              # Consume SELECT

    # Parse columns
    columns = []
    if self._match(TokenType.STAR):
        columns.append('*')
        self._advance()
    else:
        # Column list: name, age, email
        columns.append(self._expect(TokenType.IDENTIFIER).value)
        while self._consume_if(TokenType.COMMA):
            columns.append(self._expect(TokenType.IDENTIFIER).value)

    self._expect(TokenType.FROM)                # Consume FROM
    table_name = self._expect(TokenType.IDENTIFIER).value

    # Optional WHERE clause
    where_expr = None
    if self._consume_if(TokenType.WHERE):
        where_expr = self._parse_expression()   # Parse WHERE expression

    # Optional ORDER BY, LIMIT, OFFSET...

    return SelectCommand(table_name, columns, where_expr, ...)
```

### CREATE TABLE Parsing

Handles column definitions and constraints:

```python
def _parse_create_table(self) -> CreateTableCommand:
    self._expect(TokenType.TABLE)
    table_name = self._expect(TokenType.IDENTIFIER).value
    self._expect(TokenType.LPAREN)

    columns = []
    primary_key = []

    while not self._match(TokenType.RPAREN):
        # Check for PRIMARY KEY constraint
        if self._match(TokenType.PRIMARY):
            # Parse: PRIMARY KEY (col1, col2)
            ...
            continue

        # Parse column definition
        col_name = self._expect(TokenType.IDENTIFIER).value
        datatype = self._advance().type.name  # INT, TEXT, etc.

        # Column constraints
        nullable = True
        unique = False
        autoincrement = False

        while True:
            if self._match(TokenType.PRIMARY):
                self._advance()
                self._expect(TokenType.KEY)
                primary_key.append(col_name)
                nullable = False
            elif self._match(TokenType.NOT):
                self._advance()
                self._expect(TokenType.NULL)
                nullable = False
            elif self._match(TokenType.UNIQUE):
                self._advance()
                unique = True
            elif self._match(TokenType.AUTOINCREMENT):
                self._advance()
                autoincrement = True
            else:
                break

        columns.append((col_name, datatype, nullable, unique, autoincrement))

        if not self._consume_if(TokenType.COMMA):
            break

    self._expect(TokenType.RPAREN)

    if not primary_key:
        raise SyntaxError("Table must have a PRIMARY KEY")

    return CreateTableCommand(table_name, columns, primary_key)
```

### INSERT Statement Parsing

```python
def _parse_insert(self) -> InsertCommand:
    self._expect(TokenType.INSERT)
    self._expect(TokenType.INTO)
    table_name = self._expect(TokenType.IDENTIFIER).value

    # Optional column list: (name, age)
    columns = None
    if self._consume_if(TokenType.LPAREN):
        columns = []
        columns.append(self._expect(TokenType.IDENTIFIER).value)
        while self._consume_if(TokenType.COMMA):
            columns.append(self._expect(TokenType.IDENTIFIER).value)
        self._expect(TokenType.RPAREN)

    # VALUES clause
    self._expect(TokenType.VALUES)
    self._expect(TokenType.LPAREN)

    values = []
    values.append(self._parse_value())  # Parse value (number, string, NULL, etc.)
    while self._consume_if(TokenType.COMMA):
        values.append(self._parse_value())

    self._expect(TokenType.RPAREN)

    return InsertCommand(table_name, columns, values)
```

### Error Handling

The parser provides detailed error messages:

```python
def _expect(self, token_type: TokenType, message: str = None) -> Token:
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
```

**Example error:**
```
SyntaxError: Expected FROM after column list at line 1, column 15. Got 'WHERE' (WHERE)
```

### Optional vs Required Tokens

**Required:** Use `_expect()`
```python
self._expect(TokenType.FROM)  # Error if not present
```

**Optional:** Use `_consume_if()`
```python
if self._consume_if(TokenType.WHERE):
    where_expr = self._parse_expression()  # Only parse if present
```

**Lookahead:** Use `_match()`
```python
if self._match(TokenType.ORDER):
    order_by = self._parse_order_by()  # Check without consuming
```

---

## Expression Parsing

Expression parsing handles WHERE clause conditions with proper operator precedence.

### Operator Precedence

Precedence from lowest to highest:
1. **OR** (lowest)
2. **AND**
3. **NOT**
4. **Comparison** (`=`, `!=`, `<`, `>`, `<=`, `>=`, `LIKE`, `BETWEEN`, `IS`)
5. **Primary** (literals, columns, parentheses) (highest)

### Recursive Methods

Each precedence level has its own method that calls the next higher level:

```python
_parse_expression()   # Entry point → calls _parse_or()
_parse_or()          # OR expressions → calls _parse_and()
_parse_and()         # AND expressions → calls _parse_not()
_parse_not()         # NOT expressions → calls _parse_comparison()
_parse_comparison()  # Comparison ops → calls _parse_primary()
_parse_primary()     # Literals, columns, parentheses
```

This structure ensures correct precedence: `NOT` binds tighter than `AND`, which binds tighter than `OR`.

### OR Expressions

```python
def _parse_or(self) -> Expression:
    """Parse OR expression (lowest precedence)"""
    left = self._parse_and()

    while self._consume_if(TokenType.OR):
        right = self._parse_and()
        left = BinaryOp('OR', left, right)

    return left
```

**Example:**
```sql
age > 18 OR status = 'active'
```

Parse tree:
```
BinaryOp('OR',
    BinaryOp('>', ColumnRef('age'), Literal(18, 'INT')),
    BinaryOp('=', ColumnRef('status'), Literal('active', 'STRING'))
)
```

### AND Expressions

```python
def _parse_and(self) -> Expression:
    """Parse AND expression"""
    left = self._parse_not()

    while self._consume_if(TokenType.AND):
        right = self._parse_not()
        left = BinaryOp('AND', left, right)

    return left
```

**Example:**
```sql
age > 18 AND age < 65
```

Parse tree:
```
BinaryOp('AND',
    BinaryOp('>', ColumnRef('age'), Literal(18, 'INT')),
    BinaryOp('<', ColumnRef('age'), Literal(65, 'INT'))
)
```

### NOT Expressions

```python
def _parse_not(self) -> Expression:
    """Parse NOT expression"""
    if self._consume_if(TokenType.NOT):
        operand = self._parse_not()  # Right-associative
        return UnaryOp('NOT', operand)

    return self._parse_comparison()
```

**Example:**
```sql
NOT deleted
```

Parse tree:
```
UnaryOp('NOT', ColumnRef('deleted'))
```

**Double NOT:**
```sql
NOT NOT active
```

Parse tree:
```
UnaryOp('NOT',
    UnaryOp('NOT', ColumnRef('active'))
)
```

### Comparison Expressions

```python
def _parse_comparison(self) -> Expression:
    """Parse comparison expression"""
    left = self._parse_primary()

    # BETWEEN operator
    if self._consume_if(TokenType.BETWEEN):
        lower = self._parse_primary()
        self._expect(TokenType.AND)
        upper = self._parse_primary()
        # Transform to: (left >= lower) AND (left <= upper)
        return BinaryOp('AND',
                       BinaryOp('>=', left, lower),
                       BinaryOp('<=', left, upper))

    # IS NULL / IS NOT NULL
    if self._consume_if(TokenType.IS):
        is_negated = self._consume_if(TokenType.NOT)
        self._expect(TokenType.NULL)
        result = BinaryOp('IS', left, Literal(None, 'NULL'))
        if is_negated:
            return UnaryOp('NOT', result)
        return result

    # Comparison operators
    if self._match(TokenType.EQ, TokenType.NEQ, TokenType.LT,
                   TokenType.GT, TokenType.LTE, TokenType.GTE, TokenType.LIKE):
        op_token = self._advance()
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
```

**Examples:**

```sql
age BETWEEN 20 AND 30
```
Transformed to:
```
BinaryOp('AND',
    BinaryOp('>=', ColumnRef('age'), Literal(20, 'INT')),
    BinaryOp('<=', ColumnRef('age'), Literal(30, 'INT'))
)
```

```sql
email IS NOT NULL
```
Parse tree:
```
UnaryOp('NOT',
    BinaryOp('IS', ColumnRef('email'), Literal(None, 'NULL'))
)
```

```sql
name LIKE 'A%'
```
Parse tree:
```
BinaryOp('LIKE', ColumnRef('name'), Literal('A%', 'STRING'))
```

### Primary Expressions

```python
def _parse_primary(self) -> Expression:
    """Parse primary expression (literals, column refs, parentheses)"""

    # Parenthesized expression
    if self._consume_if(TokenType.LPAREN):
        expr = self._parse_expression()  # Recursive call for nested expr
        self._expect(TokenType.RPAREN)
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

    raise SyntaxError("Expected expression")
```

### Parentheses and Precedence

Parentheses override default precedence:

**Without parentheses:**
```sql
age > 18 OR status = 'active' AND verified = TRUE
```
Parsed as (AND binds tighter than OR):
```
age > 18 OR (status = 'active' AND verified = TRUE)
```

**With parentheses:**
```sql
(age > 18 OR status = 'active') AND verified = TRUE
```
Parsed as:
```
(age > 18 OR status = 'active') AND verified = TRUE
```

### Complex Example

Input:
```sql
(age > 18 AND age < 65) OR (status = 'admin' AND NOT deleted)
```

Parse tree:
```
BinaryOp('OR',
    BinaryOp('AND',
        BinaryOp('>', ColumnRef('age'), Literal(18, 'INT')),
        BinaryOp('<', ColumnRef('age'), Literal(65, 'INT'))
    ),
    BinaryOp('AND',
        BinaryOp('=', ColumnRef('status'), Literal('admin', 'STRING')),
        UnaryOp('NOT', ColumnRef('deleted'))
    )
)
```

### Precedence Table

| Operator | Associativity | Example |
|----------|---------------|---------|
| `OR` | Left-to-right | `a OR b OR c` → `(a OR b) OR c` |
| `AND` | Left-to-right | `a AND b AND c` → `(a AND b) AND c` |
| `NOT` | Right-to-left | `NOT NOT a` → `NOT (NOT a)` |
| `=`, `!=`, `<`, `>`, `<=`, `>=`, `LIKE` | Non-associative | `a = b` |
| `BETWEEN` | Non-associative | `a BETWEEN x AND y` |
| `IS [NOT] NULL` | Non-associative | `a IS NOT NULL` |
| Parentheses | N/A | `(expr)` - highest precedence |

---

## Usage Examples

### Basic Usage

```python
from db_engine.parser import parse_sql

# Parse a simple SELECT
sql = "SELECT * FROM users WHERE age > 18;"
command = parse_sql(sql)

# command is a SelectCommand object
print(command.table_name)  # 'users'
print(command.columns)     # ['*']
print(command.where)       # BinaryOp('>', ColumnRef('age'), Literal(18, 'INT'))
```

### CREATE TABLE Example

```python
sql = """
CREATE TABLE users (
    id INT PRIMARY KEY AUTOINCREMENT,
    email TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL,
    age INT,
    created_at TIMESTAMP
);
"""

command = parse_sql(sql)
# CreateTableCommand(
#     table_name='users',
#     columns=[
#         ('id', 'INT', False, False, True),
#         ('email', 'TEXT', False, True, False),
#         ('name', 'TEXT', False, False, False),
#         ('age', 'INT', True, False, False),
#         ('created_at', 'TIMESTAMP', True, False, False)
#     ],
#     primary_key=['id']
# )
```

### Complex WHERE Clause

```python
sql = """
SELECT name, email FROM users
WHERE (age BETWEEN 18 AND 65)
  AND (status = 'active' OR role = 'admin')
  AND email IS NOT NULL;
"""

command = parse_sql(sql)
# WHERE clause is parsed as:
# BinaryOp('AND',
#     BinaryOp('AND',
#         BinaryOp('AND',
#             BinaryOp('>=', ColumnRef('age'), Literal(18, 'INT')),
#             BinaryOp('<=', ColumnRef('age'), Literal(65, 'INT'))
#         ),
#         BinaryOp('OR',
#             BinaryOp('=', ColumnRef('status'), Literal('active', 'STRING')),
#             BinaryOp('=', ColumnRef('role'), Literal('admin', 'STRING'))
#         )
#     ),
#     UnaryOp('NOT',
#         BinaryOp('IS', ColumnRef('email'), Literal(None, 'NULL'))
#     )
# )
```

### Error Handling

```python
try:
    sql = "SELECT * FORM users;"  # Typo: FORM instead of FROM
    command = parse_sql(sql)
except SyntaxError as e:
    print(e)
    # "Expected FROM after column list at line 1, column 10. Got 'FORM' (IDENTIFIER)"
```

### Using Tokenizer Directly

```python
from db_engine.parser import Tokenizer

sql = "SELECT name FROM users WHERE age > 18;"
tokenizer = Tokenizer(sql)
tokens = tokenizer.tokenize()

for token in tokens:
    print(f"{token.type.name:15} {token.value!r:20} (line {token.line}, col {token.column})")

# Output:
# SELECT          'SELECT'             (line 1, col 1)
# IDENTIFIER      'name'               (line 1, col 8)
# FROM            'FROM'               (line 1, col 13)
# IDENTIFIER      'users'              (line 1, col 18)
# WHERE           'WHERE'              (line 1, col 24)
# IDENTIFIER      'age'                (line 1, col 30)
# GT              '>'                  (line 1, col 34)
# NUMBER          18                   (line 1, col 36)
# SEMICOLON       ';'                  (line 1, col 38)
# EOF             None                 (line 1, col 39)
```

### Using Parser Directly

```python
from db_engine.parser import Tokenizer, Parser

sql = "INSERT INTO users VALUES (1, 'alice@example.com', 'Alice', 25, NULL);"

# Step 1: Tokenize
tokenizer = Tokenizer(sql)
tokens = tokenizer.tokenize()

# Step 2: Parse
parser = Parser(tokens)
command = parser.parse()

# command is an InsertCommand object
print(command.table_name)  # 'users'
print(command.columns)     # None (means all columns)
print(command.values)      # [1, 'alice@example.com', 'Alice', 25, None]
```

### Handling Comments

```python
sql = """
-- This is a comment
SELECT name, age  -- Inline comment
FROM users
WHERE age > 18;
"""

command = parse_sql(sql)
# Comments are ignored during tokenization
# Parsed as: SELECT name, age FROM users WHERE age > 18;
```

### Multi-Statement Parsing

The parser handles one statement at a time. For multiple statements:

```python
sql_statements = """
CREATE TABLE users (id INT PRIMARY KEY, name TEXT);
INSERT INTO users VALUES (1, 'Alice');
SELECT * FROM users;
"""

# Split by semicolon (naive approach)
for stmt in sql_statements.strip().split(';'):
    stmt = stmt.strip()
    if stmt:
        try:
            command = parse_sql(stmt + ';')
            print(f"Parsed: {type(command).__name__}")
        except SyntaxError as e:
            print(f"Error: {e}")

# Output:
# Parsed: CreateTableCommand
# Parsed: InsertCommand
# Parsed: SelectCommand
```

### Accessing Expression Tree

```python
sql = "SELECT * FROM users WHERE age > 18 AND status = 'active';"
command = parse_sql(sql)

def print_expr(expr, indent=0):
    """Recursively print expression tree"""
    prefix = "  " * indent
    if isinstance(expr, BinaryOp):
        print(f"{prefix}BinaryOp({expr.op})")
        print_expr(expr.left, indent + 1)
        print_expr(expr.right, indent + 1)
    elif isinstance(expr, UnaryOp):
        print(f"{prefix}UnaryOp({expr.op})")
        print_expr(expr.operand, indent + 1)
    elif isinstance(expr, Literal):
        print(f"{prefix}Literal({expr.value}, {expr.datatype})")
    elif isinstance(expr, ColumnRef):
        print(f"{prefix}ColumnRef({expr.column_name})")

print_expr(command.where)

# Output:
# BinaryOp(AND)
#   BinaryOp(>)
#     ColumnRef(age)
#     Literal(18, INT)
#   BinaryOp(=)
#     ColumnRef(status)
#     Literal('active', STRING)
```

---

## Supported SQL Syntax Summary

### DDL (Data Definition Language)
```sql
CREATE TABLE table_name (
    column_name datatype [PRIMARY KEY] [UNIQUE] [NOT NULL] [AUTOINCREMENT],
    ...,
    PRIMARY KEY (col1, col2)
);

CREATE [UNIQUE] INDEX index_name ON table_name (col1, col2);

DROP TABLE table_name;
DROP INDEX index_name ON table_name;

TRUNCATE [TABLE] table_name;

ALTER TABLE table_name ADD [COLUMN] col_name datatype [UNIQUE] [NOT NULL];
ALTER TABLE table_name DROP [COLUMN] col_name;
ALTER TABLE table_name RENAME [COLUMN] old_name TO new_name;
```

### DML (Data Manipulation Language)
```sql
SELECT column1, column2 | *
FROM table_name
[WHERE expression]
[ORDER BY col1 [ASC|DESC], col2 [ASC|DESC], ...]
[LIMIT n]
[OFFSET n];

INSERT INTO table_name [(col1, col2, ...)] VALUES (val1, val2, ...);

UPDATE table_name SET col1 = val1, col2 = val2 [WHERE expression];

DELETE FROM table_name [WHERE expression];
```

### Utility Commands
```sql
EXPLAIN query;
ANALYZE [table_name];
VACUUM [table_name];
```

### Transaction Control
```sql
BEGIN [TRANSACTION];
COMMIT;
ROLLBACK;
```

### WHERE Clause Expressions
- Comparison: `=`, `!=`, `<`, `>`, `<=`, `>=`
- Boolean logic: `AND`, `OR`, `NOT`
- Pattern matching: `LIKE` (with `%` and `_`)
- Range: `BETWEEN lower AND upper`
- Null checks: `IS NULL`, `IS NOT NULL`
- Parentheses: `(expression)`

### Data Types
- `INT`, `BIGINT`, `FLOAT`, `TEXT`, `BOOLEAN`, `TIMESTAMP`

### Literals
- Numbers: `42`, `3.14`
- Strings: `'text'` (single quotes, escaped with `\'`)
- Booleans: `TRUE`, `FALSE`
- Null: `NULL`

---

## Testing

The parser has comprehensive test coverage in `tests/test_parser.py`:

```bash
# Run parser tests
python3 tests/test_parser.py

# Should see: 20/20 tests passing ✓
```

Test categories:
- Tokenization (keywords, identifiers, operators, literals)
- Basic commands (SELECT, INSERT, UPDATE, DELETE)
- DDL commands (CREATE TABLE/INDEX, DROP, ALTER)
- Complex WHERE clauses (AND/OR/NOT, BETWEEN, IS NULL)
- ORDER BY, LIMIT, OFFSET
- Error handling
- Comments and whitespace

---

**End of Parser Documentation**

For information on how parsed commands are executed, see the [Executor Documentation](./executor.md).
