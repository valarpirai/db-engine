# SQL Executor Package Documentation

## Table of Contents
1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Package Structure](#package-structure)
4. [ExecutorBase - Core Functionality](#executorbase)
5. [DDL Commands (ddl.py)](#ddl-commands)
6. [DML Commands (dml.py)](#dml-commands)
7. [Utility Commands (utility.py)](#utility-commands)
8. [Schema Modifications (schema.py)](#schema-modifications)
9. [Transactions (transaction.py)](#transactions)
10. [Query Execution Flow](#query-execution-flow)
11. [Usage Examples](#usage-examples)

---

## Overview

The `db_engine/executor/` package implements **SQL command execution** - the component that actually performs database operations. It receives parsed command objects from the parser and orchestrates the catalog, storage layer, and indexes to fulfill the operation.

### Execution Pipeline

```
Parsed Command → [QueryExecutor.execute()] → [Mixin Handler] → Result
                         ↓
                  +------+------+------+------+
                  |      |      |      |      |
               Catalog Buffer HeapFile Index
                       Pool
```

**Role in Database:**
- Parser creates command objects (AST) → Executor runs them
- Coordinates: Catalog (metadata), BufferPool (caching), HeapFile (storage), BTreeIndex (indexing)
- Enforces constraints: PRIMARY KEY, UNIQUE, NOT NULL
- Implements query optimization: Cost-based index selection
- Manages transactions: BEGIN/COMMIT/ROLLBACK

### Design Philosophy

- **Mixin pattern**: Functionality split across specialized classes
- **Separation of concerns**: Each mixin handles one category of commands
- **Resource management**: Lazy loading of heap files and indexes
- **Expression evaluation**: WHERE clause filtering
- **Constraint enforcement**: Validates all SQL constraints
- **Cost-based optimization**: Chooses between index scan vs. sequential scan

---

## Architecture

The executor uses the **mixin pattern** to organize functionality:

```
QueryExecutor
    ↓ (inherits from)
    ├── ExecutorBase      (core: expression evaluation, resource management)
    ├── DDLMixin          (CREATE, DROP, TRUNCATE)
    ├── DMLMixin          (INSERT, SELECT, UPDATE, DELETE)
    ├── UtilityMixin      (EXPLAIN, ANALYZE, VACUUM)
    ├── SchemaMixin       (ALTER TABLE)
    └── TransactionMixin  (BEGIN, COMMIT, ROLLBACK)
```

**Why mixins?**
- **Modularity**: Each file handles one category (~80-290 lines each)
- **Maintainability**: Easy to find and modify specific functionality
- **Testability**: Each mixin can be tested independently
- **Clarity**: Clear separation between DDL, DML, utility, schema, transactions

### Mixin Inheritance

```python
class QueryExecutor(
    ExecutorBase,
    DDLMixin,
    DMLMixin,
    UtilityMixin,
    SchemaMixin,
    TransactionMixin
):
    """Main executor combining all functionality"""
```

All mixins share state from `ExecutorBase`:
- `self.catalog` - Metadata (tables, indexes, statistics)
- `self.buffer_pool` - LRU page cache
- `self.heap_files` - Cached HeapFile objects
- `self.indexes` - Cached BTreeIndex objects
- Transaction state (in_transaction, operations, backups)

---

## Package Structure

```
db_engine/executor/
├── __init__.py          # Re-exports QueryExecutor
├── executor.py          # Main QueryExecutor class (82 lines)
├── base.py              # ExecutorBase: helpers, expression eval (208 lines)
├── ddl.py               # DDLMixin: CREATE/DROP/TRUNCATE (92 lines)
├── dml.py               # DMLMixin: INSERT/SELECT/UPDATE/DELETE (292 lines)
├── utility.py           # UtilityMixin: EXPLAIN/ANALYZE/VACUUM (123 lines)
├── schema.py            # SchemaMixin: ALTER TABLE (281 lines)
└── transaction.py       # TransactionMixin: BEGIN/COMMIT/ROLLBACK (88 lines)
```

Total: ~1,166 lines of code (excluding executor.py dispatch logic)

### Module Breakdown

**executor.py** - Main orchestrator
- `QueryExecutor` class: Inherits from all mixins
- `execute()` method: Dispatches to appropriate handler based on command type
- 16 command types supported

**base.py** - Core functionality
- Expression evaluation: `_evaluate_expression()`, `_eval_operand()`, `_like_match()`
- Resource management: `_get_heap_file()`, `_get_index()`, `_get_primary_key_index()`
- Key extraction: `_extract_key()`, `_extract_key_from_tuple()`
- Initialization and shutdown

**ddl.py** - Data Definition Language
- `execute_create_table()`: Create table + heap file + primary key index
- `execute_create_index()`: Create secondary index and populate from data
- `execute_drop_table()`: Remove table, heap, all indexes
- `execute_drop_index()`: Remove secondary index
- `execute_truncate_table()`: Fast table clear (keep structure)

**dml.py** - Data Manipulation Language
- `execute_insert()`: Validate constraints, insert to heap + indexes, handle AUTOINCREMENT
- `execute_select()`: Cost-based scan selection, WHERE filtering, ORDER BY, LIMIT/OFFSET
- `execute_update()`: Find matching tuples, update heap and indexes
- `execute_delete()`: Remove from heap and all indexes

**utility.py** - Utility commands
- `execute_explain()`: Show query plan (index vs. sequential scan) with cost estimates
- `execute_analyze()`: Update table statistics (row count, distinct values)
- `execute_vacuum()`: Reclaim space from deleted tuples

**schema.py** - Schema modifications
- `execute_alter_table_add_column()`: Add column, migrate existing data
- `execute_alter_table_drop_column()`: Remove column, rebuild heap (no PK columns)
- `execute_alter_table_rename_column()`: Rename column in schema and indexes

**transaction.py** - Transaction support
- `execute_begin()`: Start transaction, track operations, backup indexes
- `execute_commit()`: Flush changes, remove backups, clear transaction state
- `execute_rollback()`: Restore index backups, clear dirty pages, abort transaction

---

## ExecutorBase - Core Functionality

`ExecutorBase` provides shared functionality for all mixins: expression evaluation, resource management, and helper methods.

### Initialization

```python
class ExecutorBase:
    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        self.catalog = Catalog(data_dir)
        self.catalog.load()                    # Load metadata from disk
        self.buffer_pool = BufferPool()       # LRU page cache (128 pages)
        self.heap_files: Dict[str, HeapFile] = {}      # Cached heap files
        self.indexes: Dict[str, BTreeIndex] = {}       # Cached indexes

        # Transaction state
        self.in_transaction = False
        self.transaction_operations = []
        self.transaction_index_backups = {}
```

**Key components:**
- **Catalog**: Stores table schemas, index metadata, statistics
- **Buffer Pool**: 128-page LRU cache (1MB total) for heap pages
- **Heap files**: Lazy-loaded table data files
- **Indexes**: Lazy-loaded B-tree index files
- **Transaction state**: Tracks active transaction

### Expression Evaluation

Core method for WHERE clause filtering:

```python
def _evaluate_expression(self, expr: Expression, tuple_obj: Tuple, schema: TableSchema) -> Any:
    """Evaluate WHERE expression against tuple"""
```

**Supported operations:**

**BinaryOp** - Binary operations
```python
if isinstance(expr, BinaryOp):
    left_val = self._eval_operand(expr.left, tuple_obj, schema)
    right_val = self._eval_operand(expr.right, tuple_obj, schema)

    if expr.op == '=':
        return left_val == right_val
    elif expr.op == '!=':
        return left_val != right_val
    elif expr.op == '<':
        return left_val < right_val if (left_val is not None and right_val is not None) else False
    # ... more operators
    elif expr.op == 'LIKE':
        return self._like_match(str(left_val), str(right_val))
    elif expr.op == 'AND':
        return self._evaluate_expression(expr.left, ...) and \
               self._evaluate_expression(expr.right, ...)
    elif expr.op == 'OR':
        return self._evaluate_expression(expr.left, ...) or \
               self._evaluate_expression(expr.right, ...)
    elif expr.op == 'IS':
        return left_val is None  # IS NULL check
```

**UnaryOp** - NOT operator
```python
elif isinstance(expr, UnaryOp):
    if expr.op == 'NOT':
        return not self._evaluate_expression(expr.operand, tuple_obj, schema)
```

**Literal** - Constant values
```python
elif isinstance(expr, Literal):
    return expr.value
```

**ColumnRef** - Column value from tuple
```python
elif isinstance(expr, ColumnRef):
    col_idx = schema.get_column_index(expr.column_name)
    return tuple_obj.values[col_idx]
```

**Example:**
```python
# WHERE age > 18 AND status = 'active'
expr = BinaryOp('AND',
    BinaryOp('>', ColumnRef('age'), Literal(18, 'INT')),
    BinaryOp('=', ColumnRef('status'), Literal('active', 'STRING'))
)

# Evaluate against tuple: (25, 'active', ...)
result = _evaluate_expression(expr, tuple_obj, schema)  # Returns: True
```

### LIKE Pattern Matching

```python
def _like_match(self, text: str, pattern: str) -> bool:
    """SQL LIKE pattern matching: % = wildcard, _ = single char"""
    regex_pattern = re.escape(pattern).replace('\\%', '.*').replace('\\_', '.')
    return re.match(f'^{regex_pattern}$', text) is not None
```

**Examples:**
- `'Alice' LIKE 'A%'` → True (starts with A)
- `'Bob' LIKE '_ob'` → True (3 characters, ends with 'ob')
- `'Charlie' LIKE '%lie'` → True (ends with 'lie')
- `'Dave' LIKE 'D_v_'` → True (4 characters, starts with D, 3rd is v)

### Resource Management

Lazy loading of heap files and indexes:

#### Get Heap File
```python
def _get_heap_file(self, table_name: str) -> HeapFile:
    """Get or load HeapFile for table"""
    if table_name not in self.heap_files:
        schema = self.catalog.get_table(table_name)
        file_path = os.path.join(self.data_dir, schema.heap_file)
        heap_file = HeapFile(file_path, schema, self.buffer_pool)

        if os.path.exists(file_path):
            heap_file.open()
        else:
            raise ValueError(f"Heap file for table '{table_name}' does not exist")

        self.heap_files[table_name] = heap_file  # Cache

    return self.heap_files[table_name]
```

**Why lazy loading?**
- Only load files when needed
- Reduces memory usage
- Faster startup (no upfront loading)

#### Get Index
```python
def _get_index(self, index_meta: IndexMetadata) -> BTreeIndex:
    """Get or load BTreeIndex"""
    index_key = f"{index_meta.table_name}_{index_meta.index_name}"

    if index_key not in self.indexes:
        file_path = os.path.join(self.data_dir, index_meta.index_file)
        index = BTreeIndex(file_path, index_meta.columns, index_meta.unique)

        if os.path.exists(file_path):
            index.open()
        else:
            raise ValueError(f"Index file '{index_meta.index_file}' does not exist")

        self.indexes[index_key] = index  # Cache

    return self.indexes[index_key]
```

#### Get Primary Key Index
```python
def _get_primary_key_index(self, table_name: str) -> BTreeIndex:
    """Get primary key index for table"""
    index_key = f"{table_name}_pkey"

    if index_key not in self.indexes:
        schema = self.catalog.get_table(table_name)
        file_path = os.path.join(self.data_dir, f"{table_name}_pkey.idx")
        index = BTreeIndex(file_path, schema.primary_key, unique=True)

        if os.path.exists(file_path):
            index.open()
        else:
            raise ValueError(f"Primary key index for '{table_name}' does not exist")

        self.indexes[index_key] = index  # Cache

    return self.indexes[index_key]
```

### Key Extraction

Extract key values from tuples for index operations:

```python
def _extract_key(self, values: List[Any], schema: TableSchema, key_columns: List[str]) -> Any:
    """Extract key value(s) from values list"""
    if len(key_columns) == 1:
        # Single-column key
        col_idx = schema.get_column_index(key_columns[0])
        return values[col_idx]
    else:
        # Composite key
        return tuple(values[schema.get_column_index(col)] for col in key_columns)
```

**Examples:**
```python
# Single-column primary key: id=5
key = _extract_key([5, 'Alice', 25], schema, ['id'])  # Returns: 5

# Composite primary key: (user_id=10, order_id=20)
key = _extract_key([10, 20, 'pending'], schema, ['user_id', 'order_id'])
# Returns: (10, 20)
```

### Shutdown

```python
def shutdown(self):
    """Flush all buffers and close files"""
    self.buffer_pool.flush_all()  # Write all dirty pages to disk
    self.catalog.save()            # Persist metadata changes
```

Called when database closes to ensure data consistency.

---

## DDL Commands (ddl.py)

Data Definition Language commands create and modify database schema.

### CREATE TABLE

```python
def execute_create_table(self, cmd: CreateTableCommand) -> str:
    """Execute CREATE TABLE command"""
```

**Steps:**
1. **Build column definitions** from command
2. **Validate AUTOINCREMENT** constraints (INT/BIGINT only)
3. **Create TableSchema** object
4. **Register in catalog** (also creates primary key index metadata)
5. **Create heap file** (`table_name.dat`)
6. **Create primary key index** (`table_name_pkey.idx`)
7. **Initialize autoincrement counters** (if any)

**Example:**
```sql
CREATE TABLE users (
    id INT PRIMARY KEY AUTOINCREMENT,
    email TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL
);
```

**What happens:**
- Creates `users.dat` heap file
- Creates `users_pkey.idx` B-tree index
- Registers schema in catalog
- Sets autoincrement counter for `id` to 1

### CREATE INDEX

```python
def execute_create_index(self, cmd: CreateIndexCommand) -> str:
    """Execute CREATE [UNIQUE] INDEX command"""
```

**Steps:**
1. **Validate table exists** and columns are valid
2. **Create IndexMetadata** in catalog
3. **Create B-tree index file** (`table_indexname.idx`)
4. **Populate index from existing data** (scan heap file, insert into index)

**Example:**
```sql
CREATE INDEX idx_users_email ON users(email);
```

**What happens:**
- Scans all existing rows in `users.dat`
- For each row, extracts email value and ctid
- Inserts (email, ctid) pairs into B-tree
- Result: Index ready for queries

### DROP TABLE

```python
def execute_drop_table(self, cmd: DropTableCommand) -> str:
    """Execute DROP TABLE command"""
```

**Steps:**
1. **Remove heap file** (`table.dat`)
2. **Remove all index files** (primary key + secondary indexes)
3. **Remove from catalog** (schema, statistics, index metadata)
4. **Clear from cache** (heap_files, indexes)

**Example:**
```sql
DROP TABLE users;
```

**Files deleted:**
- `users.dat`
- `users_pkey.idx`
- `users_email_idx.idx`
- Catalog entries removed

### TRUNCATE TABLE

```python
def execute_truncate_table(self, cmd: TruncateTableCommand) -> str:
    """Execute TRUNCATE TABLE command - fast table clear"""
```

**Steps:**
1. **Clear caches** (heap_files, indexes, buffer_pool pages)
2. **Delete heap file**
3. **Recreate empty heap file**
4. **Delete and recreate all index files**
5. **Reset statistics** (row_count=0, but preserve autoincrement counters)

**Example:**
```sql
TRUNCATE TABLE users;
```

**Result:**
- Table structure preserved
- All data removed
- Faster than `DELETE FROM users` (no row-by-row deletion)
- Autoincrement continues from last value

---

## DML Commands (dml.py)

Data Manipulation Language commands query and modify table data.

### INSERT

```python
def execute_insert(self, cmd: InsertCommand) -> str:
    """Execute INSERT command"""
```

**Steps:**
1. **Map values to columns** (handle partial column lists)
2. **Handle AUTOINCREMENT** (generate next value if NULL)
3. **Validate NOT NULL constraints**
4. **Check PRIMARY KEY uniqueness** (search primary key index)
5. **Check UNIQUE constraints** (for indexed columns)
6. **Create Tuple object**
7. **Insert into heap** (returns ctid)
8. **Update all indexes** (primary key + secondary indexes)
9. **Update statistics** (row_count, modification_count)

**Constraint Validation:**
```python
# NOT NULL check
for i, col in enumerate(schema.columns):
    if not col.nullable and values[i] is None:
        raise ValueError(f"Column '{col.name}' cannot be NULL")

# PRIMARY KEY uniqueness
pk_value = self._extract_key(values, schema, schema.primary_key)
if pk_index.search(pk_value) is not None:
    raise ValueError(f"Duplicate primary key: {pk_value}")
```

**AUTOINCREMENT Handling:**
```python
for i, col in enumerate(schema.columns):
    if col.autoincrement and values[i] is None:
        next_val = stats.autoincrement_counters.get(col.name, 1)
        values[i] = next_val
        stats.autoincrement_counters[col.name] = next_val + 1
```

**Example:**
```sql
-- User doesn't specify id (AUTOINCREMENT column)
INSERT INTO users (email, name) VALUES ('alice@example.com', 'Alice');
```

**What happens:**
1. id = NULL initially
2. Autoincrement fills in: id = 1 (next counter value)
3. Validates: email and name are NOT NULL ✓
4. Checks: id=1 doesn't exist in primary key index ✓
5. Creates tuple: (1, 'alice@example.com', 'Alice')
6. Inserts into `users.dat` → ctid = (0, 0)
7. Updates `users_pkey.idx`: insert (1, (0, 0))
8. Updates statistics: row_count++, autoincrement_counters[id] = 2

### SELECT

```python
def execute_select(self, cmd: SelectCommand) -> List[Dict]:
    """Execute SELECT command"""
```

**Steps:**
1. **Choose scan method** (cost-based: index vs. sequential)
2. **Scan tuples** (filter with WHERE clause)
3. **Project columns** (select specified columns)
4. **Sort results** (ORDER BY if present)
5. **Apply LIMIT/OFFSET** (pagination)

**Cost-Based Scan Selection:**
```python
def _should_use_index_scan(self, where: Expression, indexes) -> Optional[IndexMetadata]:
    """Determine if index scan is beneficial"""
    # Only use index for simple equality on indexed column
    if isinstance(where, BinaryOp) and where.op == '=' and isinstance(where.left, ColumnRef):
        col_name = where.left.column_name
        for index_meta in indexes:
            if index_meta.columns == [col_name]:
                return index_meta  # Use this index
    return None  # Use sequential scan
```

**Index Scan:**
```python
# WHERE id = 5
index = self._get_primary_key_index(table_name)
ctid = index.search(5)  # O(log N) lookup
if ctid:
    tuple_obj = heap.read_tuple(ctid)
    results.append(tuple_obj)
```

**Sequential Scan:**
```python
# WHERE age > 18
for tuple_obj, ctid in heap.scan_all():  # O(N) scan
    if self._evaluate_expression(where, tuple_obj, schema):
        results.append(tuple_obj)
```

**Sorting (ORDER BY):**
```python
# ORDER BY age DESC, name ASC
for col, direction in cmd.order_by:
    col_idx = schema.get_column_index(col)
    reverse = (direction == 'DESC')
    results.sort(key=lambda t: t.values[col_idx], reverse=reverse)
```

**Pagination (LIMIT/OFFSET):**
```python
if cmd.offset:
    results = results[cmd.offset:]  # Skip first N
if cmd.limit:
    results = results[:cmd.limit]   # Take M results
```

**Example:**
```sql
SELECT name, email FROM users WHERE age > 18 ORDER BY name LIMIT 10 OFFSET 20;
```

**Execution:**
1. Sequential scan (no index on age)
2. Filter: age > 18 for each tuple
3. Project: only name and email columns
4. Sort: by name ascending
5. Pagination: skip 20, take 10
6. Return: 10 results

### UPDATE

```python
def execute_update(self, cmd: UpdateCommand) -> str:
    """Execute UPDATE command"""
```

**Steps:**
1. **Find matching tuples** (WHERE clause filtering)
2. **For each match:**
   - Extract old primary key value
   - Apply updates to values
   - Validate constraints (NOT NULL)
   - Check for primary key changes (not allowed)
   - Delete old tuple from heap
   - Insert new tuple to heap
   - Update all indexes (delete old, insert new)
3. **Update statistics**

**Constraint Checks:**
```python
# Validate NOT NULL
for i, col in enumerate(schema.columns):
    if not col.nullable and new_values[i] is None:
        raise ValueError(f"Column '{col.name}' cannot be NULL")

# Check if primary key changed
old_pk = self._extract_key(old_values, schema, schema.primary_key)
new_pk = self._extract_key(new_values, schema, schema.primary_key)
if old_pk != new_pk:
    raise ValueError("Cannot update primary key columns")
```

**Example:**
```sql
UPDATE users SET age = 26, status = 'verified' WHERE id = 5;
```

**Execution:**
1. Index scan on id=5 (fast lookup)
2. Read tuple at ctid
3. Apply changes: age=26, status='verified'
4. Validate constraints ✓
5. Delete tuple at old ctid
6. Insert updated tuple → new ctid
7. Update indexes: delete (5, old_ctid), insert (5, new_ctid)

### DELETE

```python
def execute_delete(self, cmd: DeleteCommand) -> str:
    """Execute DELETE command"""
```

**Steps:**
1. **Find matching tuples** (WHERE clause filtering)
2. **For each match:**
   - Extract primary key value
   - Delete from heap (marks tuple as deleted with tombstone)
   - Delete from all indexes
3. **Update statistics** (modification_count++, dead_tuple_count++)
4. **Check for auto-vacuum** (if 20%+ tuples dead, trigger VACUUM)

**Example:**
```sql
DELETE FROM users WHERE age < 18;
```

**Execution:**
1. Sequential scan (no index on age)
2. For each tuple with age < 18:
   - Mark as deleted in heap (tombstone)
   - Remove from primary key index
   - Remove from all secondary indexes
3. dead_tuple_count increases
4. If > 20% dead, auto-vacuum reclaims space

---

## Utility Commands (utility.py)

Utility commands for query introspection and database maintenance.

### EXPLAIN

```python
def execute_explain(self, cmd: ExplainCommand) -> str:
    """Show query plan without executing"""
```

**Purpose:** Display how a query will be executed (index scan vs. sequential scan, cost estimates).

**Example:**
```sql
EXPLAIN SELECT * FROM users WHERE id = 5;
```

**Output:**
```
Query Plan for SELECT
Table: users
Scan Method: Index Scan using users_pkey (id = 5)
Estimated Rows: 1
Estimated Cost: 2.0 (index lookup + heap fetch)
```

### ANALYZE

```python
def execute_analyze(self, cmd: AnalyzeCommand) -> str:
    """Update table statistics"""
```

**Purpose:** Recalculate statistics (row count, distinct values) for query optimization.

**Statistics updated:**
- `row_count`: Total rows in table
- `page_count`: Total pages in heap file
- `distinct_values`: Distinct values per indexed column (for selectivity estimates)

**Example:**
```sql
ANALYZE users;  -- Update stats for users table
ANALYZE;        -- Update stats for all tables
```

### VACUUM

```python
def execute_vacuum(self, cmd: VacuumCommand) -> str:
    """Reclaim space from deleted tuples"""
```

**Purpose:** Compact heap file by removing dead tuples (marked with tombstones).

**How it works:**
1. Scan heap file, collect live tuples
2. Rewrite heap file with only live tuples
3. Rebuild all indexes (old ctids are now invalid)
4. Update statistics (dead_tuple_count = 0)

**Example:**
```sql
VACUUM users;  -- Vacuum users table
VACUUM;        -- Vacuum all tables
```

**Auto-vacuum:** Automatically triggered when 20%+ of tuples are dead.

---

## Schema Modifications (schema.py)

ALTER TABLE commands modify table structure without dropping/recreating.

### ADD COLUMN

```python
def execute_alter_table_add_column(self, cmd: AlterTableAddColumnCommand) -> str:
    """Add column to existing table"""
```

**Steps:**
1. **Add column to schema**
2. **Rebuild heap file** (add NULL or default for new column to all existing tuples)
3. **Rebuild all indexes** (old ctids are invalidated by heap rebuild)
4. **Update catalog** with new schema

**Example:**
```sql
ALTER TABLE users ADD COLUMN age INT;
ALTER TABLE users ADD COLUMN verified BOOLEAN NOT NULL;  -- Fills with FALSE
ALTER TABLE users ADD COLUMN username TEXT UNIQUE;       -- Creates index if UNIQUE
```

**What happens:**
- All existing rows get NULL (or default) for new column
- Heap file rewritten with new schema
- All indexes rebuilt with new ctids

### DROP COLUMN

```python
def execute_alter_table_drop_column(self, cmd: AlterTableDropColumnCommand) -> str:
    """Remove column from table"""
```

**Steps:**
1. **Validate**: Cannot drop primary key columns
2. **Remove column from schema**
3. **Rebuild heap file** (exclude dropped column from all tuples)
4. **Remove indexes on dropped column**
5. **Rebuild remaining indexes**
6. **Update catalog**

**Example:**
```sql
ALTER TABLE users DROP COLUMN age;
```

**Restrictions:**
- Cannot drop primary key columns
- Drops any indexes on the column

### RENAME COLUMN

```python
def execute_alter_table_rename_column(self, cmd: AlterTableRenameColumnCommand) -> str:
    """Rename column in table"""
```

**Steps:**
1. **Validate**: New name doesn't conflict
2. **Update schema** (change column name)
3. **Update indexes** (if column is in primary key or secondary indexes)
4. **Update catalog**
5. **No heap rebuild** (just metadata change)

**Example:**
```sql
ALTER TABLE users RENAME COLUMN email TO email_address;
```

**Efficiency:** Fast operation (no data movement, only metadata update).

---

## Transactions (transaction.py)

Basic transaction support with BEGIN/COMMIT/ROLLBACK.

### Transaction Model

**Current implementation:**
- Single writer (no concurrent transactions)
- Rollback via index backup + buffer pool clearing
- No Write-Ahead Log (WAL)
- No MVCC (Multi-Version Concurrency Control)

**Limitations:**
- Heap file changes cannot be rolled back (only indexes)
- Suitable for simple use cases, not production-grade

### BEGIN

```python
def execute_begin(self, cmd: BeginCommand) -> str:
    """Start transaction"""
```

**What happens:**
1. Set `in_transaction = True`
2. Clear `transaction_operations` list
3. Backup all indexes to `transaction_index_backups`

**Example:**
```sql
BEGIN;
-- All subsequent DML commands are tracked
```

### COMMIT

```python
def execute_commit(self, cmd: CommitCommand) -> str:
    """Commit transaction"""
```

**What happens:**
1. Flush buffer pool (write all dirty pages to disk)
2. Remove index backups
3. Set `in_transaction = False`
4. Clear transaction state

**Example:**
```sql
BEGIN;
INSERT INTO users VALUES (10, 'test@test.com', 'Test');
UPDATE users SET age = 30 WHERE id = 5;
COMMIT;  -- Changes persisted
```

### ROLLBACK

```python
def execute_rollback(self, cmd: RollbackCommand) -> str:
    """Rollback transaction"""
```

**What happens:**
1. Restore indexes from backups (undo all index changes)
2. Clear dirty pages from buffer pool (discard heap changes)
3. Set `in_transaction = False`
4. Clear transaction state

**Example:**
```sql
BEGIN;
DELETE FROM users WHERE id = 10;
-- Oops, that was a mistake!
ROLLBACK;  -- Changes discarded, id=10 still exists
```

**Important:** Heap changes are best-effort rollback (not guaranteed if pages already flushed).

---

## Query Execution Flow

### Complete Flow: INSERT Example

```
User: "INSERT INTO users (email, name) VALUES ('alice@example.com', 'Alice');"
  ↓
[1] Parser creates InsertCommand object
  ↓
[2] QueryExecutor.execute() dispatches to execute_insert()
  ↓
[3] DMLMixin.execute_insert():
    - Get catalog schema
    - Map values to columns (email, name) → fill id with AUTOINCREMENT
    - Validate NOT NULL constraints ✓
    - Check PRIMARY KEY uniqueness in index ✓
    - Create Tuple object: (1, 'alice@example.com', 'Alice')
  ↓
[4] HeapFile.insert_tuple():
    - Find page with free space (via FSM)
    - Serialize tuple to bytes
    - Add to page → ctid = (0, 0)
    - Update FSM
  ↓
[5] BTreeIndex.insert():
    - Primary key index: insert (1 → (0, 0))
    - Split nodes if necessary
  ↓
[6] Update Statistics:
    - row_count++
    - modification_count++
    - autoincrement_counters[id] = 2
  ↓
[7] Catalog.save() if threshold reached
  ↓
[8] Return: "Inserted 1 row"
```

### Complete Flow: SELECT Example

```
User: "SELECT name FROM users WHERE age > 18 ORDER BY name LIMIT 10;"
  ↓
[1] Parser creates SelectCommand object
  ↓
[2] QueryExecutor.execute() dispatches to execute_select()
  ↓
[3] DMLMixin.execute_select():
    - Check for index on 'age' column → None
    - Decision: Sequential scan (no usable index)
  ↓
[4] HeapFile.scan_all():
    - Iterate all pages via BufferPool
    - For each tuple:
      → Evaluate: age > 18?
      → If True: add to results
  ↓
[5] Expression Evaluation:
    - _evaluate_expression(BinaryOp('>', ColumnRef('age'), Literal(18)))
    - Extract age value from tuple
    - Compare: 25 > 18 → True ✓
  ↓
[6] Project columns:
    - Only include 'name' column in results
  ↓
[7] Sort results:
    - ORDER BY name ASC
    - Python sort: key=lambda t: t.values[name_idx]
  ↓
[8] Apply LIMIT:
    - Take first 10 results
  ↓
[9] Return: [{'name': 'Alice'}, {'name': 'Bob'}, ...]
```

---

## Usage Examples

### Basic Usage

```python
from db_engine.executor import QueryExecutor
from db_engine.parser import parse_sql

# Initialize executor
executor = QueryExecutor(data_dir='./mydb')

# Parse and execute CREATE TABLE
sql = "CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL);"
command = parse_sql(sql)
result = executor.execute(command)
print(result)  # "Table 'users' created with primary key ['id']"

# Parse and execute INSERT
sql = "INSERT INTO users VALUES (1, 'Alice');"
command = parse_sql(sql)
result = executor.execute(command)
print(result)  # "Inserted 1 row"

# Parse and execute SELECT
sql = "SELECT * FROM users;"
command = parse_sql(sql)
results = executor.execute(command)
print(results)  # [{'id': 1, 'name': 'Alice'}]

# Shutdown (flush buffers, save catalog)
executor.shutdown()
```

### WITH Constraints and AUTOINCREMENT

```python
sql = """
CREATE TABLE orders (
    id INT PRIMARY KEY AUTOINCREMENT,
    user_id INT NOT NULL,
    total FLOAT NOT NULL,
    status TEXT
);
"""
command = parse_sql(sql)
executor.execute(command)

# Insert without specifying id (auto-generated)
sql = "INSERT INTO orders (user_id, total, status) VALUES (10, 99.99, 'pending');"
command = parse_sql(sql)
executor.execute(command)  # id = 1 (auto-generated)

# Insert another
sql = "INSERT INTO orders (user_id, total, status) VALUES (10, 149.99, 'shipped');"
command = parse_sql(sql)
executor.execute(command)  # id = 2 (auto-generated)

# Query
sql = "SELECT * FROM orders WHERE user_id = 10;"
command = parse_sql(sql)
results = executor.execute(command)
# [{'id': 1, 'user_id': 10, 'total': 99.99, 'status': 'pending'},
#  {'id': 2, 'user_id': 10, 'total': 149.99, 'status': 'shipped'}]
```

### Complex WHERE Clauses

```python
sql = """
SELECT name, age FROM users
WHERE (age > 18 AND age < 65) OR role = 'admin'
ORDER BY age DESC
LIMIT 5;
"""
command = parse_sql(sql)
results = executor.execute(command)
```

### Transaction Example

```python
# Start transaction
executor.execute(parse_sql("BEGIN;"))

# Multiple operations
executor.execute(parse_sql("INSERT INTO users VALUES (10, 'temp@test.com', 'Temp');"))
executor.execute(parse_sql("UPDATE users SET status = 'inactive' WHERE id = 5;"))
executor.execute(parse_sql("DELETE FROM users WHERE age < 18;"))

# Decision point
if error_occurred:
    executor.execute(parse_sql("ROLLBACK;"))  # Undo all changes
else:
    executor.execute(parse_sql("COMMIT;"))    # Persist changes
```

### Schema Evolution

```python
# Add new column to existing table
sql = "ALTER TABLE users ADD COLUMN age INT;"
executor.execute(parse_sql(sql))

# Rename column
sql = "ALTER TABLE users RENAME COLUMN age TO user_age;"
executor.execute(parse_sql(sql))

# Drop column
sql = "ALTER TABLE users DROP COLUMN user_age;"
executor.execute(parse_sql(sql))
```

### Query Introspection with EXPLAIN

```python
sql = "EXPLAIN SELECT * FROM users WHERE id = 5;"
result = executor.execute(parse_sql(sql))
print(result)
# Output:
# Query Plan for SELECT
# Table: users
# Scan Method: Index Scan using users_pkey (id = 5)
# Estimated Rows: 1
# Estimated Cost: 2.0
```

---

**End of Executor Documentation**

For information on how SQL text is parsed into command objects, see the [Parser Documentation](./parser.md).

