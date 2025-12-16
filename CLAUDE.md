# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is an educational database engine built from scratch in Python, inspired by PostgreSQL's architecture. The goal is to understand database internals by implementing core components: storage layer, B-tree indexing, SQL parsing, and query execution.

**Philosophy**: Keep it simple. This is a learning tool, not a production database. Focus on clarity over performance, essential features over completeness.

## Architecture

### Storage Layer
- **Heap files** store actual table data in 8KB fixed-size pages
- Each row assigned a **ctid** (block_number, tuple_offset) - PostgreSQL-style tuple identifier
- **Tuple format with null bitmap**: Supports NULL values efficiently
  - Null bitmap: 1 bit per column (1 = NULL, 0 = not NULL)
  - Only non-NULL values are serialized after the bitmap
- File format: `tablename.dat` for heap, `tablename_indexname.idx` for indexes
- Binary serialization using Python's `struct` module for fixed-size data structures

### Indexing (B-tree)
- Single implementation: B-tree indexes only (no hash, GiST, GIN, etc.)
- Structure: Internal nodes (keys + child pointers), Leaf nodes (keys + ctid pointers)
- Fixed-size nodes (e.g., 512 bytes) stored in index files
- Index files have metadata header: magic number, root offset, node count
- **Supports composite keys** (multi-column indexes): keys stored as tuples
- Operations: insertion with splitting, single-key lookup, range queries, deletion with rebalancing
- **Uniqueness enforcement** for PRIMARY KEY and UNIQUE indexes
- Leaf node linking for efficient range scans

### Catalog System
- Metadata stored in `catalog.dat`: tables, columns, indexes
- Tracks: table_id, table_name, column definitions, index definitions
- Loaded on startup, updated on DDL operations

### SQL Support
```sql
-- CREATE TABLE with constraints
CREATE TABLE users (
    id INT PRIMARY KEY,
    email TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL,
    age INT,
    created_at TIMESTAMP
);

-- Composite primary key
CREATE TABLE orders (
    user_id INT,
    order_id INT,
    PRIMARY KEY (user_id, order_id)
);

-- INSERT
INSERT INTO users VALUES (1, 'alice@example.com', 'Alice', 25, 1704067200);
INSERT INTO users (id, name) VALUES (2, 'Bob');  -- Other columns NULL

-- SELECT with complex WHERE clauses
SELECT * FROM users WHERE age > 18;
SELECT name, email FROM users WHERE age BETWEEN 20 AND 30;
SELECT * FROM users WHERE (age > 20 AND age < 30) OR name = 'Bob';
SELECT * FROM users WHERE name LIKE 'Al%';
SELECT * FROM users WHERE email IS NOT NULL;

-- DELETE
DELETE FROM users WHERE id = 1;

-- CREATE INDEX (single or composite)
CREATE INDEX idx_users_age ON users(age);
CREATE UNIQUE INDEX idx_email ON users(email);
CREATE INDEX idx_composite ON orders(user_id, order_id);

-- DROP TABLE
DROP TABLE users;
```

**Constraint support:**
- PRIMARY KEY (mandatory for every table, automatically indexed, enforces uniqueness + NOT NULL)
- UNIQUE (enforces uniqueness via index)
- NOT NULL (column cannot be NULL)
- Composite primary keys and composite indexes supported
- No FOREIGN KEY support

### Query Execution
- **Sequential scan**: Read all pages from heap file
- **Index scan**: Use B-tree to locate specific rows by ctid
- **Cost-based query planning**: Automatically chooses between index scan and sequential scan based on:
  - Table size
  - Index availability
  - WHERE clause structure (can index be used?)
  - Estimated selectivity
- **WHERE clause evaluation**: Full boolean expression support
  - Comparison operators: `=`, `!=`, `<`, `>`, `<=`, `>=`
  - Boolean logic: `AND`, `OR`, `NOT` with proper precedence
  - Parentheses for grouping: `(age > 20 AND age < 30) OR status = 'active'`
  - Pattern matching: `LIKE` operator (`%` wildcard, `_` single char)

### Transaction Model (Phase 2 - Future)
- **Phase 1**: Auto-commit all operations (no explicit transactions)
- **Phase 2** will add:
  - BEGIN/COMMIT/ROLLBACK commands
  - File locking for single-writer enforcement (fcntl on POSIX)
  - Transaction log for rollback support
  - No MVCC initially - simple read/write locks

### REPL Interface
- Command-line interactive shell with readline support
- Meta-commands:
  - `\dt` - list all tables
  - `\di` - list all indexes
  - `\d tablename` - describe table schema
  - `\q` - quit
- Multi-line SQL input (ends with semicolon)
- Formatted table output for query results
- Error handling with clear messages

## Data Types

Supported types:
- `INT`: 4-byte signed integer (32-bit)
- `BIGINT`: 8-byte signed integer (64-bit)
- `FLOAT`: 8-byte double precision floating point
- `TEXT`: Variable-length string (max 255 chars for simplicity)
- `BOOLEAN`: 1-byte boolean
- `TIMESTAMP`: 8-byte Unix timestamp (seconds since epoch)
- **NULL support**: All columns can be NULL unless marked NOT NULL (using null bitmap in tuple serialization)

## Key Design Decisions

1. **Separate files per table/index** (PostgreSQL-style, not SQLite single-file):
   - Each table gets its own heap file: `users.dat`, `orders.dat`
   - Each index gets its own file: `users_pkey.idx`, `users_age_idx.idx`
   - Catalog stored separately: `catalog.dat`
   - **Why**: Simpler implementation, easier debugging, natural growth, clean deletion
   - **Trade-off**: More file handles vs. complexity of single-file page allocation

2. **Fixed-size pages (8KB)**: Matches PostgreSQL, simplifies addressing

3. **ctid-based indexing**: Indexes point to heap via (block, offset), not row data

4. **No WAL initially**: Durability sacrificed for simplicity

5. **Single writer**: File locking prevents concurrent writes

6. **Uniqueness via B-tree**: Primary keys enforce uniqueness during insertion

7. **Rebalancing on delete**: Properly maintain B-tree properties (borrow/merge)

## Database File Structure

Example data directory after creating tables and indexes:

```
mydb/
├── catalog.dat              # System catalog (metadata)
├── users.dat                # Heap file for 'users' table
├── users_pkey.idx           # Primary key index on users(id)
├── users_age_idx.idx        # Secondary index on users(age)
├── orders.dat               # Heap file for 'orders' table
├── orders_pkey.idx          # Primary key index on orders(id)
└── .lock                    # Lock file for single-writer enforcement
```

Each `.dat` file contains 8KB pages with table rows.
Each `.idx` file contains B-tree nodes with (key → ctid) mappings.

## Development Commands

```bash
# Run the database REPL
python main.py

# Run with existing database directory
python main.py --data-dir ./mydb

# Run tests (when implemented)
python -m pytest tests/

# Run specific test
python -m pytest tests/test_btree.py -k test_insert

# Clean data files
rm -rf mydb/*.dat mydb/*.idx mydb/.lock
```

## File Organization

```
db-engine/
├── config.py       # Configuration parameters (page size, B-tree order, etc.)
├── storage.py      # Heap file management, page I/O
├── btree.py        # B-tree index implementation
├── catalog.py      # System catalog (metadata)
├── parser.py       # SQL parser (simple regex-based)
├── executor.py     # Query execution engine
├── repl.py         # Interactive shell
├── main.py         # Entry point
└── tests/          # Unit tests
```

## Implementation Notes

### Configuration (config.py)
All system parameters centralized in `config.py`:
- Storage: `PAGE_SIZE` (8KB), `DATA_DIR`, header sizes
- B-tree: `BTREE_ORDER` (4), `NODE_SIZE` (512 bytes)
- Data types: sizes and supported types
- File naming: extensions (`.dat`, `.idx`), naming conventions
- Limits: max columns, indexes, name lengths
- Import config values throughout codebase: `from config import PAGE_SIZE, BTREE_ORDER`

### Storage Layer (storage.py)
- `HeapFile` class: manages table data files
- `Page` class: 8KB blocks with header (free space, item count)
- `insert_tuple()`: Finds page with space, appends tuple, returns ctid
- `read_tuple(ctid)`: Reads tuple by (block_number, offset)
- `delete_tuple(ctid)`: Marks tuple as deleted (tombstone)

### B-tree Index (btree.py)
- `BTreeNode` class: fixed-size serialization with `struct.pack()`
- `BTreeIndex` class: manages index file, root node, metadata
- `insert(key, ctid)`: With uniqueness check for primary keys
- `search(key)`: Returns ctid or None
- `range_query(start, end)`: Returns list of ctids
- `delete(key)`: With node rebalancing (borrow from sibling or merge)

### Catalog (catalog.py)
- Stores table schemas, column definitions, index metadata
- Loaded into memory on startup
- Persisted to `catalog.dat` after DDL operations

### Parser (parser.py)
- **Hand-written recursive descent parser** (not regex-based, not library-based)
- Two-phase parsing:
  1. **Tokenizer**: Lexical analysis - converts SQL string to tokens (keywords, identifiers, literals, operators)
  2. **Parser**: Syntax analysis - converts tokens to command objects with proper precedence
- Returns structured command objects: CreateTable, Insert, Select, Delete, CreateIndex
- Expression tree for WHERE clauses supporting boolean logic (AND/OR/NOT, parentheses)
- Better error messages than regex approach
- More educational than using external library

### Executor (executor.py)
- Orchestrates catalog, storage, and indexes
- `execute_create_table()`: Creates heap file, updates catalog, auto-creates primary key index
- `execute_insert()`: Validates constraints (PK uniqueness, NOT NULL, UNIQUE), writes to heap, updates all indexes
- `execute_select()`: Cost-based scan method selection (index or sequential), evaluates WHERE clause, projects columns
- `execute_delete()`: Removes from heap and all indexes
- `execute_create_index()`: Creates index file, populates from existing data
- `_evaluate_expression()`: Recursive WHERE clause evaluation with full boolean logic support
- `_like_match()`: Pattern matching for LIKE operator

## Excluded Features (Out of Scope)

- MVCC (multi-version concurrency control)
- Write-ahead logging (WAL)
- Crash recovery
- Advanced query optimization (we have simple cost-based planning)
- JOINs (multi-table queries)
- Aggregations (SUM, COUNT, AVG, GROUP BY, HAVING)
- Subqueries, views, triggers, stored procedures
- UPDATE statement (Phase 2)
- User authentication and permissions
- Network protocol (always local file access, no client-server)
- Replication
- Vacuum/compaction (garbage collection)
- FOREIGN KEY constraints

## Testing Strategy

Focus on unit tests for each component:
- `test_btree.py`: Insert, search, split, delete, rebalance
- `test_storage.py`: Page management, tuple insertion, ctid addressing
- `test_executor.py`: End-to-end SQL command execution
- `test_catalog.py`: Metadata persistence

## Future Extensions (When Core is Solid)

1. Add MVCC with xmin/xmax transaction IDs
2. Implement write-ahead logging for durability
3. Support composite indexes (multi-column)
4. Add JOIN operations (nested loop initially)
5. Simple query optimizer (cost-based index selection)
6. Network protocol (PostgreSQL wire protocol subset)
