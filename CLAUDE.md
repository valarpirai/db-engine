# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is an educational database engine built from scratch in Python, inspired by PostgreSQL's architecture. The goal is to understand database internals by implementing core components: storage layer, B-tree indexing, SQL parsing, and query execution.

**Philosophy**: Keep it simple. This is a learning tool, not a production database. Focus on clarity over performance, essential features over completeness.

## Architecture

### Storage Layer
- **Heap files** store actual table data in 8KB fixed-size pages
- Each row assigned a **ctid** (block_number, tuple_offset) - PostgreSQL-style tuple identifier
- File format: `tablename.dat` for heap, `tablename_indexname.idx` for indexes
- Binary serialization using Python's `struct` module for fixed-size data structures

### Indexing (B-tree)
- Single implementation: B-tree indexes only
- Structure: Internal nodes (keys + child pointers), Leaf nodes (keys + ctid pointers)
- Fixed-size nodes (e.g., 512 bytes) stored in index files
- Index files have metadata header: magic number, root offset, node count
- Supports: insertion with splitting, single-key lookup, range queries, deletion with rebalancing

### Catalog System
- Metadata stored in `catalog.dat`: tables, columns, indexes
- Tracks: table_id, table_name, column definitions, index definitions
- Loaded on startup, updated on DDL operations

### SQL Support (Minimal Subset)
```sql
CREATE TABLE name (col type, ...);
INSERT INTO name VALUES (...);
SELECT * FROM name WHERE col = value;
SELECT * FROM name WHERE col > value;  -- Range queries
DELETE FROM name WHERE col = value;
CREATE INDEX idx_name ON table(column);
DROP TABLE name;
```

### Query Execution
- **Sequential scan**: Read all pages from heap file
- **Index scan**: Use B-tree to locate specific rows by ctid
- **WHERE evaluation**: Simple predicates (=, <, >, <=, >=)
- No query optimizer - uses index if available for WHERE clause on indexed column

### Transaction Model
- Basic BEGIN/COMMIT/ROLLBACK support
- Single-writer model using file locking (fcntl on POSIX)
- No MVCC initially - simple read/write locks
- Rollback by discarding uncommitted changes

### REPL Interface
- Command-line interactive shell
- Meta-commands: `\dt` (list tables), `\di` (list indexes), `\q` (quit)
- SQL command execution and result display

## Data Types

Current support (keep minimal):
- `INT`: 4-byte signed integer
- `TEXT`: Variable-length string (max 255 chars for simplicity)
- `BOOLEAN`: 1-byte boolean

## Key Design Decisions

1. **Fixed-size pages (8KB)**: Matches PostgreSQL, simplifies addressing
2. **ctid-based indexing**: Indexes point to heap via (block, offset), not row data
3. **No WAL initially**: Durability sacrificed for simplicity
4. **Single writer**: File locking prevents concurrent writes
5. **Uniqueness via B-tree**: Primary keys enforce uniqueness during insertion
6. **Rebalancing on delete**: Properly maintain B-tree properties (borrow/merge)

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
rm -rf *.dat *.idx .lock
```

## File Organization

```
db-engine/
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
- Simple regex-based parsing (no full AST)
- Returns parsed command objects: CreateTable, Insert, Select, Delete
- Validates syntax but minimal semantic checking

### Executor (executor.py)
- `execute_create_table()`: Creates heap file, updates catalog
- `execute_insert()`: Parses values, writes to heap, updates indexes
- `execute_select()`: Chooses scan method (index or sequential)
- `execute_delete()`: Removes from heap and all indexes

## Excluded Features (Out of Scope)

- MVCC (multi-version concurrency control)
- Write-ahead logging (WAL)
- Crash recovery
- Query optimization/planning
- JOINs (multi-table queries)
- Aggregations (SUM, COUNT, GROUP BY)
- Subqueries, views, triggers, stored procedures
- User authentication
- Network protocol (always local file access)
- Replication
- Vacuum/compaction

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
