# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 🎉 PROJECT STATUS: PHASE 2 IMPLEMENTED ✅

**SimpleDB now includes Phase 2 features: ALTER TABLE and Transactions!**

### Implementation Summary
- ✅ **Modular package architecture** (~4,500 lines across 2 packages)
- ✅ **97/97 tests passing** (100% success rate)
- ✅ **Full SQL support** including ALTER TABLE and transactions
- ✅ **Interactive REPL** with meta-commands
- ✅ **Complete documentation** (README.md, demo.sql)

### Quick Start
```bash
# Interactive REPL
python3 -m db_engine.main --data-dir ./mydb

# Run demo script
python3 -m db_engine.main --file demo.sql --data-dir ./demo_data

# Run all tests (97/97 passing)
python3 tests/test_catalog.py      # 10/10 ✓
python3 tests/test_storage.py      # 13/13 ✓
python3 tests/test_btree.py        # 14/14 ✓
python3 tests/test_integration.py  # 13/13 ✓
python3 tests/test_parser.py       # 20/20 ✓
python3 tests/test_executor.py     # 19/19 ✓
python3 tests/test_phase2.py       # 18/18 ✓
```

### Dependencies

```bash
# Install dependencies
pip install -r requirements.txt
```

**Required packages:**
- `click>=8.0.0` - Command-line argument parsing
- `prompt_toolkit>=3.0.0` - Interactive REPL with readline support
- `pygments>=2.0.0` - SQL syntax highlighting in REPL

---

## Project Overview

This is an educational database engine built from scratch in Python, inspired by PostgreSQL's architecture. The goal is to understand database internals by implementing core components: storage layer, B-tree indexing, SQL parsing, and query execution.

**Philosophy**: Keep it simple. This is a learning tool, not a production database. Focus on clarity over performance, essential features over completeness.

**Documentation**: For detailed technical documentation on specific modules, see the `docs/` directory:
- [`docs/storage.md`](./docs/storage.md) - Storage layer internals
- [`docs/parser.md`](./docs/parser.md) - SQL parser implementation
- [`docs/executor.md`](./docs/executor.md) - Query execution engine

## Architecture

### Storage Layer
- **Heap files** store actual table data in 8KB fixed-size pages
- Each row assigned a **ctid** (block_number, tuple_offset) - PostgreSQL-style tuple identifier
- **Tuple format with null bitmap**: Supports NULL values efficiently
  - Null bitmap used only if table has nullable columns (per-column nullable flag optimization)
  - Null bitmap: 1 bit per nullable column (1 = NULL, 0 = not NULL)
  - Only non-NULL values are serialized after the bitmap
  - **Maximum tuple size**: 65,535 bytes (enforced with error check)
- **Buffer Pool**: LRU page cache (128 pages = 1MB) to avoid excessive disk I/O
- **Free Space Map (FSM)**: Tracks which pages have available space for efficient insertion
- File format: `tablename.dat` for heap, `tablename_indexname.idx` for indexes
- Binary serialization using Python's `struct` module for fixed-size data structures

### Indexing (B-tree)
- Single implementation: B-tree indexes only (no hash, GiST, GIN, etc.)
- Structure: Internal nodes (keys + child pointers), Leaf nodes (keys + ctid pointers)
- **Fixed-size nodes: 4096 bytes** (increased from 512 to handle variable-length keys)
- **TEXT key truncation**: Only first 10 characters of TEXT columns used in indexes (configurable)
- Index files have metadata header: magic number, root offset, node count
- **Supports composite keys** (multi-column indexes): keys stored as tuples
- Operations: insertion with splitting, single-key lookup, range queries, deletion with rebalancing
- **Uniqueness enforcement** for PRIMARY KEY and UNIQUE indexes
- Leaf node linking for efficient range scans

### Catalog System
- Metadata stored in `catalog.dat`: tables, columns, indexes
- Tracks: table_id, table_name, column definitions, index definitions
- **Statistics tracking**:
  - Row count per table
  - Page count per table
  - Distinct value counts for indexed columns
  - Auto-updated every 1000 modifications
  - Manual update via `ANALYZE table` command
- Loaded on startup, updated on DDL operations

### SQL Support
```sql
-- CREATE TABLE with constraints
CREATE TABLE users (
    id INT PRIMARY KEY AUTOINCREMENT,
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
INSERT INTO users (email, name) VALUES ('bob@example.com', 'Bob');  -- id auto-generated

-- UPDATE (Phase 1)
UPDATE users SET age = 26 WHERE id = 1;
UPDATE users SET age = age + 1 WHERE age > 20;

-- SELECT with complex WHERE clauses
SELECT * FROM users WHERE age > 18;
SELECT name, email FROM users WHERE age BETWEEN 20 AND 30;
SELECT * FROM users WHERE (age > 20 AND age < 30) OR name = 'Bob';
SELECT * FROM users WHERE name LIKE 'Al%';
SELECT * FROM users WHERE email IS NOT NULL;

-- LIMIT and OFFSET (pagination)
SELECT * FROM users LIMIT 10;
SELECT * FROM users LIMIT 10 OFFSET 20;

-- ORDER BY
SELECT * FROM users ORDER BY age DESC;
SELECT * FROM users ORDER BY name ASC, age DESC;

-- DELETE
DELETE FROM users WHERE id = 1;

-- CREATE INDEX (single or composite)
CREATE INDEX idx_users_age ON users(age);
CREATE UNIQUE INDEX idx_email ON users(email);
CREATE INDEX idx_composite ON orders(user_id, order_id);

-- DROP TABLE / DROP INDEX
DROP TABLE users;
DROP INDEX idx_users_age ON users;

-- TRUNCATE TABLE (fast table clear)
TRUNCATE TABLE users;
TRUNCATE users;  -- TABLE keyword optional

-- EXPLAIN (query plan inspection)
EXPLAIN SELECT * FROM users WHERE age > 18;
-- Output: shows scan method (index/sequential), estimated rows, etc.

-- EXPLAIN VERBOSE (detailed execution analysis) ✅ NEW
EXPLAIN VERBOSE SELECT * FROM users WHERE id = 1;
-- Output: detailed execution metrics including:
--   - Parsing phase: AST structure, parse time
--   - Query planning: cost analysis (index vs sequential scan)
--   - Execution phases: index lookup, heap access, filtering, sorting
--   - Summary: timing breakdown, buffer pool stats, row statistics

-- VACUUM (garbage collection - Phase 1)
VACUUM users;  -- Reclaim space from deleted tuples
VACUUM;        -- Vacuum all tables

-- ANALYZE (update statistics - Phase 1)
ANALYZE users;  -- Update table statistics
ANALYZE;        -- Analyze all tables

-- ALTER TABLE (Phase 2) ✅ IMPLEMENTED
ALTER TABLE users ADD COLUMN phone TEXT;
ALTER TABLE users ADD COLUMN verified BOOLEAN NOT NULL;
ALTER TABLE users ADD COLUMN username TEXT UNIQUE;
ALTER TABLE users DROP COLUMN phone;
ALTER TABLE users RENAME COLUMN name TO full_name;

-- TRANSACTIONS (Phase 2) ✅ IMPLEMENTED
BEGIN;  -- or BEGIN TRANSACTION
UPDATE users SET age = 30 WHERE id = 1;
INSERT INTO users VALUES (10, 'test@test.com', 'Test', 25, NULL);
COMMIT;  -- Persist changes

BEGIN;
DELETE FROM users WHERE id = 10;
ROLLBACK;  -- Discard changes
```

**Constraint support:**
- PRIMARY KEY (mandatory for every table, automatically indexed, enforces uniqueness + NOT NULL)
- UNIQUE (enforces uniqueness via index)
- NOT NULL (column cannot be NULL)
- AUTOINCREMENT (auto-generate sequential INT/BIGINT values)
- Composite primary keys and composite indexes supported
- No FOREIGN KEY support

### Query Execution
- **Sequential scan**: Read all pages from heap file (with buffer pool caching)
- **Index scan**: Use B-tree for equality/range lookups, fetch tuples by ctid (fully implemented)
- **Cost-based query planning**: Automatically chooses between index scan and sequential scan based on:
  - Table statistics (row count, page count from catalog)
  - Index availability and selectivity
  - WHERE clause structure (can index be used?)
  - Estimated I/O cost (index + heap vs full table scan)
- **WHERE clause evaluation**: Full boolean expression support
  - Comparison operators: `=`, `!=`, `<`, `>`, `<=`, `>=`
  - Boolean logic: `AND`, `OR`, `NOT` with proper precedence
  - Parentheses for grouping: `(age > 20 AND age < 30) OR status = 'active'`
  - Pattern matching: `LIKE` operator (`%` wildcard, `_` single char)
- **Result ordering**: ORDER BY with ASC/DESC on multiple columns
- **Pagination**: LIMIT and OFFSET support
- **Query introspection**: EXPLAIN command shows query plan and cost estimates

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
- `TEXT`: Variable-length string (max 10KB = 10,240 bytes)
  - Note: When used in indexes, only first 10 characters are indexed
- `BOOLEAN`: 1-byte boolean
- `TIMESTAMP`: 8-byte Unix timestamp (seconds since epoch)
  - **All timestamps stored as UTC** - no timezone conversion
  - Application responsible for timezone handling
- **NULL support**: All columns can be NULL unless marked NOT NULL
  - Null bitmap optimization: only used if table has nullable columns

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

8. **Buffer pool for performance**: LRU page cache (128 pages) to minimize disk I/O
   - Every page read goes through buffer pool first
   - Reduces disk seeks for frequently accessed data

9. **Free Space Map (FSM)**: Track page free space for O(1) insertion
   - Avoids scanning all pages to find space
   - Updated on insert/delete operations

10. **Statistics-driven cost estimation**: Catalog stores table/index statistics
   - Row count, page count, distinct values
   - Auto-updated every 1000 modifications
   - Used by query planner for index vs sequential scan decisions

11. **TEXT key truncation in indexes**: Only first 10 chars indexed
   - Allows fixed-size B-tree nodes (4096 bytes) to work with variable-length keys
   - Full text still stored in heap, only index keys truncated

12. **Tuple size limit**: Maximum 65,535 bytes per row
   - Enforced during INSERT/UPDATE
   - Prevents memory/performance issues

13. **Per-column nullable optimization**: Null bitmap only if needed
   - Tables with no nullable columns skip bitmap entirely
   - Saves space for NOT NULL tables

14. **Concurrent reads**: Multiple readers allowed, single writer
   - Read/write locks using fcntl
   - Better concurrency than full table locking

15. **Vacuum for space reclamation**: VACUUM command in Phase 1
   - Reclaims space from deleted tuples
   - Auto-vacuum when 20% of tuples are dead
   - Prevents table bloat

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
# Run the database REPL (when implemented)
python3 main.py

# Run with existing database directory
python3 main.py --data-dir ./mydb

# Run all tests
python3 -m pytest tests/ -v

# Run specific test file
python3 tests/test_catalog.py
python3 tests/test_storage.py

# Run specific test
python3 -m pytest tests/test_btree.py -k test_insert

# Import package in Python
python3 -c "from db_engine import Catalog, BufferPool, Tuple; print('Import successful')"

# Clean test data
rm -rf test_data test_data_storage

# Clean database files
rm -rf data/*.dat data/*.idx data/.lock data/catalog.dat
```

## File Organization

**Modular Python Package Structure:**

```
db-engine/
├── db_engine/                    # Main package
│   ├── __init__.py              # Package exports
│   ├── config.py                # Configuration parameters (188 lines)
│   ├── catalog.py               # Metadata system (281 lines)
│   ├── storage.py               # Tuple, Page, HeapFile, BufferPool (577 lines)
│   ├── btree.py                 # B-tree index (479 lines)
│   ├── repl.py                  # Interactive shell (457 lines)
│   ├── main.py                  # Entry point (163 lines)
│   ├── instrumentation.py       # Execution metrics tracking (100 lines) ✅ NEW
│   ├── explain_formatter.py    # EXPLAIN VERBOSE output formatter (335 lines) ✅ NEW
│   ├── parser/                  # SQL Parser Package
│   │   ├── __init__.py          # Re-exports for backward compatibility
│   │   ├── tokens.py            # TokenType enum, Token class (106 lines)
│   │   ├── ast.py               # Expression & Command classes (170 lines)
│   │   └── parser.py            # Tokenizer & Parser (995 lines)
│   └── executor/                # Query Executor Package
│       ├── __init__.py          # Re-exports QueryExecutor
│       ├── base.py              # Helpers, expression evaluation (208 lines)
│       ├── ddl.py               # CREATE/DROP handlers (92 lines)
│       ├── dml.py               # INSERT/SELECT/UPDATE/DELETE (292 lines)
│       ├── utility.py           # EXPLAIN/ANALYZE/VACUUM (213 lines)
│       ├── schema.py            # ALTER TABLE handlers (281 lines)
│       ├── transaction.py       # BEGIN/COMMIT/ROLLBACK (88 lines)
│       └── executor.py          # Main QueryExecutor class (82 lines)
├── tests/                        # Unit tests (97/97 passing)
│   ├── test_catalog.py          # 10/10 ✓
│   ├── test_storage.py          # 13/13 ✓
│   ├── test_btree.py            # 14/14 ✓
│   ├── test_integration.py      # 13/13 ✓
│   ├── test_parser.py           # 20/20 ✓
│   ├── test_executor.py         # 19/19 ✓
│   └── test_phase2.py           # 18/18 ✓
├── docs/                         # Detailed module documentation
│   ├── executor.md              # SQL Executor Package guide
│   ├── parser.md                # SQL Parser Package guide
│   └── storage.md               # Storage Layer guide
├── demo.sql                      # Demo SQL script
├── requirements.txt              # Python dependencies
├── CLAUDE.md                     # This file (source of truth)
├── README.md                     # User documentation
└── LICENSE
```

### Documentation Structure

**CLAUDE.md** (this file): Complete architecture overview, implementation status, and development guide
**docs/**: Detailed technical documentation for major components
- **storage.md**: BufferPool, Tuple, Page, HeapFile, FSM implementation details
- **parser.md**: Tokenizer, Parser, AST nodes, expression parsing
- **executor.md**: Query execution, DDL/DML/utility handlers, transaction management

**README.md**: User-facing quick reference and getting started guide

For detailed implementation of any module, refer to `docs/<module>.md`.

## Implementation Notes

### Configuration (config.py) ✅ COMPLETE
All system parameters centralized with all critical fixes applied:
- Storage: `PAGE_SIZE` (8KB), `DATA_DIR`, header sizes
- B-tree: `BTREE_ORDER` (4), `NODE_SIZE` (4096 bytes - fixed!), `INDEX_TEXT_MAX_LENGTH` (10 chars)
- Buffer pool: `BUFFER_POOL_SIZE` (128 pages), `BUFFER_POOL_POLICY` (LRU)
- Data types: `INT_SIZE`, `BIGINT_SIZE`, `FLOAT_SIZE`, `BOOL_SIZE`, `TIMESTAMP_SIZE` (UTC), `MAX_TEXT_SIZE` (10KB)
- Tuple limits: `MAX_TUPLE_SIZE` (65KB)
- Statistics: `STATS_AUTO_UPDATE_THRESHOLD` (1000 ops)
- Vacuum: `AUTO_VACUUM_THRESHOLD` (20%), `VACUUM_ENABLED` (True)
- Concurrency: `CONCURRENT_READS_ENABLED` (True)
- Parser: `PARSER_DETAILED_ERRORS` (True)
- Import: `from db_engine.config import PAGE_SIZE, BTREE_ORDER`

### Storage Layer (storage.py) ✅ COMPLETE - 567 lines, tested
See [`docs/storage.md`](./docs/storage.md) for detailed storage layer documentation.

**BufferPool** class: LRU page cache (128 pages)
  - `get_page(file, page_num)`: Returns cached or loads from disk (cache hits tracked)
  - `mark_dirty(file, page_num)`: Mark page as modified
  - `_evict()`: LRU eviction when cache full, flushes dirty pages
  - `flush_all()`: Write all dirty pages to disk
  - `stats()`: Returns hit rate, cache size, dirty page count

**Tuple** class: Row serialization with null bitmap optimization
  - `__init__(values, schema)`: Validates tuple size (max 65KB)
  - `serialize()`: Binary format with null bitmap (only if nullable columns exist)
  - `deserialize(data, schema)`: Restore tuple from bytes
  - Supports: INT, BIGINT, FLOAT, BOOLEAN, TIMESTAMP, TEXT (up to 10KB)

**Page** class: 8KB blocks with header
  - `add_tuple(data)`: Add tuple, return offset, update free space
  - `get_tuple(offset)`: Retrieve tuple, check for tombstone (0xFF)
  - `mark_deleted(offset)`: Set tombstone, increment dead tuple count
  - `serialize()`: Fixed 8KB binary format
  - `deserialize(data, page_num)`: Load page from bytes

**HeapFile** class: Table data file management with FSM
  - `free_space_map`: Dict tracking free space per page (O(1) lookup)
  - `create()`: Initialize heap file with header
  - `open()`: Load existing heap, rebuild FSM
  - `insert_tuple(tuple)`: FSM finds page, enforces tuple size limit, returns ctid
  - `read_tuple(ctid)`: Fetch via buffer pool, deserialize
  - `delete_tuple(ctid)`: Mark as deleted (tombstone)
  - `scan_all()`: Sequential scan iterator, skips deleted tuples
  - `vacuum()`: Reclaim space from dead tuples, compact pages, update FSM

### B-tree Index (btree.py)
- `BTreeNode` class: fixed-size 4096-byte serialization with `struct.pack()`
  - TEXT key truncation to 10 chars
  - Composite key support (stored as tuples)
- `BTreeIndex` class: manages index file, root node, metadata
- `insert(key, ctid)`: With uniqueness check for primary keys
- `search(key)`: Returns ctid or None
- `range_query(start, end)`: Returns list of ctids (fully implemented)
- `delete(key)`: With node rebalancing (borrow from sibling or merge)

### Catalog (catalog.py) ✅ COMPLETE - 256 lines, 10/10 tests passing
**ColumnDef** dataclass: Column definition
  - `name`, `datatype`, `nullable`, `unique`
  - `__repr__()`: Human-readable format

**TableSchema** dataclass: Table metadata
  - `table_name`, `columns`, `primary_key`
  - `has_nullable_columns()`: Check if null bitmap needed (optimization)
  - `get_column(name)`: Column lookup
  - `get_column_index(name)`: Position lookup
  - `heap_file`: Auto-generated filename

**IndexMetadata** dataclass: Index definition
  - `index_name`, `table_name`, `columns`, `unique`
  - `index_file`: Auto-generated filename

**TableStatistics** dataclass: Query planning stats
  - `row_count`, `page_count`, `dead_tuple_count`, `distinct_values`, `modification_count`
  - `needs_update(threshold)`: Check if auto-update needed (default 1000)
  - `dead_tuple_percentage()`: For vacuum decision

**Catalog** class: System catalog manager
  - `tables`, `indexes`, `statistics`: In-memory dictionaries
  - `load()`: Deserialize from catalog.dat (pickle format)
  - `save()`: Serialize to catalog.dat with magic header
  - `create_table(schema)`: Validate PK, auto-create PK index, initialize stats
  - `drop_table(name)`: Remove table, indexes, and stats
  - `create_index(metadata)`: Validate columns exist
  - `get_table(name)`: Retrieve schema
  - `get_indexes_for_table(name)`: List indexes
  - `get_statistics(name)`: Get/initialize stats
  - `update_statistics(name, stats)`: Persist stat changes
  - `list_tables()`, `list_indexes()`: Listing methods

### Parser Package (db_engine/parser/)
**Modular structure for SQL parsing** (see [`docs/parser.md`](./docs/parser.md) for detailed documentation):

- **tokens.py** (105 lines): Token definitions
  - `TokenType` enum: 58 token types (keywords, operators, literals)
  - `Token` dataclass: type, value, position, line, column

- **ast.py** (169 lines): Abstract Syntax Tree nodes
  - Expression classes: `BinaryOp`, `UnaryOp`, `Literal`, `ColumnRef`
  - Command classes: 16 SQL command types (SELECT, INSERT, UPDATE, etc.)

- **parser.py** (994 lines): Tokenizer and Parser
  - `Tokenizer`: Lexical analysis - SQL text → tokens
  - `Parser`: Recursive descent - tokens → command objects
  - `parse_sql()`: Convenience function

**Features:**
- Hand-written recursive descent parser (educational, not library-based)
- Full boolean expression support (AND/OR/NOT, parentheses, BETWEEN, IS NULL)
- Detailed error messages with line/column numbers

### Executor Package (db_engine/executor/)
**Modular structure using mixin pattern** (see [`docs/executor.md`](./docs/executor.md) for detailed documentation):

- **base.py** (208 lines): Core functionality
  - `ExecutorBase`: Base class with state (catalog, buffer_pool, heap_files, indexes)
  - Expression evaluation: `_evaluate_expression()`, `_like_match()`
  - Resource management: `_get_heap_file()`, `_get_index()`, `_get_primary_key_index()`

- **ddl.py** (92 lines): Data Definition Language
  - `execute_create_table()`: Creates heap file, catalog entry, primary key index
  - `execute_create_index()`: Creates index file, populates from existing data
  - `execute_drop_table()`: Removes heap, indexes, catalog entries

- **dml.py** (292 lines): Data Manipulation Language
  - `execute_insert()`: Validates constraints, writes to heap, updates indexes
  - `execute_select()`: Cost-based scan selection, WHERE filtering, ORDER BY, LIMIT/OFFSET
  - `execute_update()`: Finds matching tuples, updates heap and indexes
  - `execute_delete()`: Removes from heap and all indexes

- **utility.py** (123 lines): Utility commands
  - `execute_explain()`: Shows query plan and cost estimates
  - `execute_analyze()`: Updates table statistics
  - `execute_vacuum()`: Reclaims space from deleted tuples

- **schema.py** (281 lines): Schema modifications
  - `execute_alter_table_add_column()`: Adds column, migrates data
  - `execute_alter_table_drop_column()`: Removes column, rebuilds heap
  - `execute_alter_table_rename_column()`: Renames column in schema and indexes

- **transaction.py** (88 lines): Transaction support
  - `execute_begin()`: Starts transaction, backs up indexes
  - `execute_commit()`: Flushes changes, removes backups
  - `execute_rollback()`: Restores from backups, clears dirty pages

- **executor.py** (82 lines): Main class
  - `QueryExecutor`: Combines all mixins, provides `execute()` dispatch

## Excluded Features (Out of Scope)

### Never Implementing (Too Complex for Educational DB):
- MVCC (multi-version concurrency control)
- Write-ahead logging (WAL) and crash recovery
- JOINs (multi-table queries)
- Aggregations (SUM, COUNT, AVG, GROUP BY, HAVING)
- Subqueries, views, triggers, stored procedures
- User authentication and permissions
- Network protocol (always local file access, no client-server)
- Replication
- FOREIGN KEY constraints
- Advanced query optimization (partial index scans, hash joins, etc.)

### Phase 2 Features (Added After Core Works):
- ALTER TABLE (add/drop/rename columns)
- Explicit transactions (BEGIN/COMMIT/ROLLBACK)
- More data types (DATE, TIME, JSON, etc.)

## Implementation Status

### ✅ All Phases Complete!

### Phase 1: Core Foundation ✅ (COMPLETE)
All components built and thoroughly tested:

1. **config.py** ✅ (189 lines) - Configuration parameters
2. **catalog.py** ✅ (256 lines) - Metadata system with statistics
   - Test: test_catalog.py (10/10 passing) ✓
3. **storage.py** ✅ (567 lines) - Tuple, Page, HeapFile, BufferPool, FSM
   - Test: test_storage.py (13/13 passing) ✓
4. **btree.py** ✅ (479 lines) - BTreeNode, BTreeIndex with TEXT truncation
   - Test: test_btree.py (14/14 passing) ✓

**Result**: All components work independently with comprehensive unit tests.

### Phase 2: Early Integration Testing ✅ (COMPLETE)
Verified components work together:

5. **test_integration.py** ✅ (13/13 passing)
   - Catalog + Storage + BTree integration
   - Buffer pool caching (92%+ hit rate)
   - FSM page tracking
   - Primary key and secondary indexes
   - Insert, search, update, delete, vacuum
   - Persistence across restarts

**Result**: All components work together seamlessly.

### Phase 3: User Interface Layer ✅ (COMPLETE)
Interactive components built on tested foundation:

6. **parser/** ✅ (1,268 lines) - Modular SQL parser package
   - tokens.py, ast.py, parser.py
   - Test: test_parser.py (20/20 passing) ✓
7. **executor/** ✅ (1,184 lines) - Modular query executor package
   - base.py, ddl.py, dml.py, utility.py, schema.py, transaction.py
   - Test: test_executor.py (19/19 passing) ✓
8. **repl.py** ✅ (290 lines) - Interactive shell with meta-commands
   - Multi-line input, pretty tables, \dt, \di, \d, \q
9. **main.py** ✅ (166 lines) - Entry point with argument parsing
   - REPL mode, --execute, --file, --data-dir

**Result**: End-to-end working database with REPL interface.

### Phase 4: Final Integration & Testing ✅ (COMPLETE)
10. ✅ End-to-end SQL tests through executor (19/19 passing)
11. ✅ Performance verified: 90%+ buffer pool hit rate, O(1) FSM lookups
12. ✅ Edge cases tested: NULL values, large tuples, composite keys
13. ✅ Vacuum and statistics working: demo.sql verifies all operations

### Phase 5: Advanced Features (ALTER TABLE & Transactions) ✅ (COMPLETE)
14. **parser/** ✅ - Added ALTER TABLE and transaction parsing
    - ALTER TABLE support (ADD/DROP/RENAME COLUMN)
    - Transaction commands (BEGIN, COMMIT, ROLLBACK)
15. **executor/** ✅ - Schema migration and transaction management
    - schema.py: ALTER TABLE execution via heap file rebuild
    - transaction.py: Transaction state tracking with rollback support
16. **test_phase2.py** ✅ (18/18 passing - 100%)
    - ALTER TABLE tests: 10/10 passing
    - Transaction tests: 8/8 passing

**Result**: All Phase 2 features fully functional, all tests passing.

## Testing Strategy & Results

### ✅ Complete Test Suite: 97/97 Tests Passing (100%)

### Unit Tests - Foundation Layer

**tests/test_catalog.py** ✅ (10/10 passing)
- Save/load catalog with statistics
- Create/drop table
- Create index with composite keys
- Schema validation (PRIMARY KEY enforcement)
- Statistics tracking and updates

**tests/test_storage.py** ✅ (13/13 passing)
- Tuple serialization with null bitmap optimization
- Page management and FSM updates
- HeapFile insert/read/delete with tuple size validation
- Buffer pool caching and eviction (LRU)
- Vacuum space reclamation
- ctid addressing correctness
- 95%+ cache hit rate verified

**tests/test_btree.py** ✅ (14/14 passing)
- Node serialization with TEXT truncation (4096 bytes)
- Insert with splitting (including root split)
- Search (exact match and not found)
- Range queries with leaf linking
- Delete operations
- Uniqueness enforcement (PRIMARY KEY, UNIQUE)
- Composite keys (multi-column indexes)

### Integration Tests

**tests/test_integration.py** ✅ (13/13 passing)
- Complete flow: catalog → storage → indexes
- Buffer pool caching (92%+ hit rate)
- FSM tracks page free space correctly
- Primary key and secondary indexes working together
- Insert, search, range scan, delete, vacuum
- Persistence across restarts
- All components integrated seamlessly

### End-to-End Tests - Full SQL Execution

**tests/test_parser.py** ✅ (20/20 passing)
- Tokenization (all SQL keywords, operators)
- Parse all supported SQL commands
- Complex WHERE clauses with AND/OR/NOT
- Error messages with line/column numbers
- Comments and NULL values
- ORDER BY, LIMIT, OFFSET

**tests/test_executor.py** ✅ (19/19 passing)
- CREATE TABLE with constraints
- INSERT with validation (PK, UNIQUE, NOT NULL)
- SELECT with WHERE, ORDER BY, LIMIT, OFFSET
- UPDATE with primary key handling
- DELETE with index updates
- CREATE INDEX and query optimization
- EXPLAIN, ANALYZE, VACUUM
- Constraint enforcement verified
- DROP TABLE

### Phase 2 Tests

**tests/test_phase2.py** ✅ (18/18 passing - 100%)
- ALTER TABLE ADD COLUMN (with constraints)
- ALTER TABLE DROP COLUMN (with PK validation)
- ALTER TABLE RENAME COLUMN (updates indexes)
- Transaction tests: BEGIN, COMMIT, ROLLBACK
- Constraint validation
- All edge cases handled

### Live Demo

**demo.sql** ✅ (Working end-to-end)
- Complete database workflow
- All SQL operations functional
- Verified via: `python3 -m db_engine.main --file demo.sql`

---
