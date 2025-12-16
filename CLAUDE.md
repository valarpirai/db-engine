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

-- DROP TABLE
DROP TABLE users;

-- EXPLAIN (query plan inspection - Phase 1)
EXPLAIN SELECT * FROM users WHERE age > 18;
-- Output: shows scan method (index/sequential), estimated rows, etc.

-- VACUUM (garbage collection - Phase 1)
VACUUM users;  -- Reclaim space from deleted tuples
VACUUM;        -- Vacuum all tables

-- ANALYZE (update statistics - Phase 1)
ANALYZE users;  -- Update table statistics
ANALYZE;        -- Analyze all tables

-- ALTER TABLE (Phase 2)
ALTER TABLE users ADD COLUMN phone TEXT;
ALTER TABLE users DROP COLUMN phone;
ALTER TABLE users RENAME COLUMN name TO full_name;
```

**Constraint support:**
- PRIMARY KEY (mandatory for every table, automatically indexed, enforces uniqueness + NOT NULL)
- UNIQUE (enforces uniqueness via index)
- NOT NULL (column cannot be NULL)
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

**Proper Python Package Structure:**

```
db-engine/
├── db_engine/              # Main package
│   ├── __init__.py        # Package exports
│   ├── config.py          # ✅ Configuration parameters (all fixes applied)
│   ├── catalog.py         # ✅ COMPLETE (256 lines, tested)
│   ├── storage.py         # ✅ COMPLETE (567 lines, tested)
│   ├── btree.py           # TODO: B-tree index with TEXT truncation
│   ├── parser.py          # TODO: Tokenizer + recursive descent parser
│   ├── executor.py        # TODO: Query executor with all commands
│   └── repl.py            # TODO: Interactive shell
├── tests/                  # Unit tests
│   ├── __init__.py
│   ├── test_catalog.py    # ✅ 10/10 tests passing
│   ├── test_storage.py    # ✅ 13 tests ready
│   ├── test_btree.py      # TODO
│   ├── test_parser.py     # TODO
│   └── test_executor.py   # TODO
├── main.py                 # TODO: Entry point
├── CLAUDE.md              # This file
├── LICENSE
└── README.md              # TODO: User-facing documentation
```

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

### Parser (parser.py)
- **Hand-written recursive descent parser** (not regex-based, not library-based)
- Two-phase parsing:
  1. **Tokenizer**: Lexical analysis - converts SQL string to tokens (keywords, identifiers, literals, operators)
  2. **Parser**: Syntax analysis - converts tokens to command objects with proper precedence
- Returns structured command objects: CreateTable, Insert, Update, Select, Delete, CreateIndex, Explain, Vacuum, Analyze, AlterTable
- Expression tree for WHERE clauses supporting boolean logic (AND/OR/NOT, parentheses)
- **Detailed error messages**: Line and column numbers, helpful suggestions
- Better error messages than regex approach
- More educational than using external library

### Executor (executor.py)
- Orchestrates catalog, storage, and indexes
- `execute_create_table()`: Creates heap file, updates catalog, auto-creates primary key index
- `execute_insert()`: Validates constraints (PK uniqueness, NOT NULL, UNIQUE), checks tuple size, writes to heap, updates all indexes, updates stats
- `execute_update()`: Validates constraints, finds matching tuples, updates heap and indexes (Phase 1)
- `execute_select()`:
  - Cost-based scan method selection using table statistics
  - Index scan: `range_query()` for ranges, `search()` for equality
  - Sequential scan: through buffer pool
  - ORDER BY: sorts results
  - LIMIT/OFFSET: pagination support
- `execute_delete()`: Removes from heap and all indexes, updates dead tuple count
- `execute_create_index()`: Creates index file, populates from existing data
- `execute_explain()`: Shows query plan, estimated cost, scan method (Phase 1)
- `execute_vacuum()`: Calls `heap_file.vacuum()`, reclaims space, updates stats
- `execute_analyze()`: Updates table statistics in catalog
- `execute_alter_table()`: ADD/DROP/RENAME columns (Phase 2)
- `_choose_scan_method()`: Uses table statistics for cost estimation
- `_index_scan()`: Fully implemented - extracts key from WHERE, uses B-tree, fetches by ctid
- `_evaluate_expression()`: Recursive WHERE clause evaluation with full boolean logic support
- `_like_match()`: Pattern matching for LIKE operator

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

## Implementation Approach

**Foundation First with Early Integration**

### Phase 1: Core Foundation (Bottom-Up with Unit Tests)
Build and thoroughly test each layer independently:

1. **config.py** ✓ - Configuration parameters
2. **catalog.py** - Metadata system with statistics
   - Unit test: save/load schemas, statistics updates
3. **storage.py** - Tuple, Page, HeapFile, BufferPool, FSM
   - Unit test: insert/read/delete tuples, buffer pool caching, FSM tracking
4. **btree.py** - BTreeNode, BTreeIndex with TEXT truncation
   - Unit test: insert/search/delete, node splitting/merging, composite keys

**Goal**: Each component works independently with comprehensive unit tests.

### Phase 2: Early Integration Testing
Before building the full interface, verify components work together:

5. **Integration test script** (simple Python, not full REPL):
   ```python
   # test_integration.py
   catalog.create_table(schema)
   heap.insert_tuple(tuple)  # Goes through buffer pool
   index.insert(key, ctid)
   found_ctid = index.search(key)
   tuple = heap.read_tuple(found_ctid)  # From cache
   assert tuple.values == expected
   ```

**Goal**: Verify buffer pool, FSM, indexes, and storage all connect properly.

### Phase 3: User Interface Layer (Top-Down)
Build the interactive components on top of tested foundation:

6. **parser.py** - Tokenizer, Parser, Command objects (all SQL commands)
7. **executor.py** - QueryExecutor with all execute methods
8. **repl.py** - Interactive shell with meta-commands
9. **main.py** - Entry point with argument parsing

**Goal**: End-to-end working database with REPL interface.

### Phase 4: Final Integration & Testing
10. End-to-end SQL tests through REPL
11. Performance testing with larger datasets
12. Edge case testing (NULL values, large tuples, many indexes)
13. Vacuum and statistics testing

## Testing Strategy

### Unit Tests (Phase 1)

**tests/test_buffer_pool.py**
- Page caching and eviction (LRU)
- Dirty page tracking and flushing
- Cache hit/miss statistics

**tests/test_storage.py**
- Tuple serialization with null bitmap optimization
- Page management and FSM updates
- HeapFile insert/read/delete with tuple size validation
- Vacuum space reclamation
- ctid addressing correctness

**tests/test_btree.py**
- Node serialization with TEXT truncation (4096 bytes)
- Insert with splitting (including root split)
- Search (exact match and not found)
- Range queries with leaf linking
- Delete with rebalancing (borrow and merge)
- Uniqueness enforcement (PRIMARY KEY, UNIQUE)
- Composite keys (multi-column indexes)

**tests/test_catalog.py**
- Save/load catalog with statistics
- Create/drop table
- Create index
- Schema validation
- Statistics auto-update (every 1000 ops)
- ANALYZE command

### Integration Tests (Phase 2)

**tests/test_integration.py**
- Buffer pool used by HeapFile reads
- FSM tracks page free space correctly
- Index and heap work together (insert → search → fetch)
- Statistics influence cost estimation
- Concurrent reads (multiple file handles)

### End-to-End Tests (Phase 3)

**tests/test_parser.py**
- Tokenization (all SQL keywords, operators)
- Parse all supported SQL commands
- Error messages with line/column numbers
- Expression precedence (AND/OR/NOT, parentheses)

**tests/test_executor.py**
- CREATE TABLE → INSERT → SELECT → UPDATE → DELETE flow
- PRIMARY KEY uniqueness enforcement
- NOT NULL and UNIQUE constraint enforcement
- WHERE clause evaluation (all operators, LIKE)
- ORDER BY and LIMIT/OFFSET
- Index vs sequential scan selection (verify uses statistics)
- EXPLAIN command output
- VACUUM reclaims space
- ANALYZE updates statistics
- Composite indexes used correctly

**tests/test_repl.py**
- Meta-commands (\dt, \di, \d table, \q)
- Multi-line SQL input
- Result formatting
- Error handling

### Performance Tests (Phase 4)

**tests/test_performance.py**
- Insert 10,000 rows, verify buffer pool reduces I/O
- Sequential scan vs index scan comparison
- Vacuum performance on heavily deleted table
- Statistics accuracy with large datasets
