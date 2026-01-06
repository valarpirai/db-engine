# SimpleDB - Educational Database Engine

A PostgreSQL-inspired database engine built from scratch in Python for educational purposes.

> **Note**: This README provides a quick overview for users. For complete documentation, architecture details, implementation notes, and development guidance, see **[CLAUDE.md](./CLAUDE.md)** which serves as the source of truth for this project.

## Features

- **Complete SQL Support**: CREATE, INSERT, SELECT, UPDATE, DELETE, ALTER TABLE, DROP INDEX, TRUNCATE
- **B-tree Indexing**: Composite keys, unique constraints, TEXT key truncation
- **Storage Layer**: 8KB pages, buffer pool (LRU cache), free space map
- **Query Planning**: Cost-based optimization (index scan vs sequential scan)
- **Transactions**: BEGIN, COMMIT, ROLLBACK with index backup/restore
- **Schema Evolution**: ALTER TABLE ADD/DROP/RENAME COLUMN
- **Auto-increment**: AUTOINCREMENT constraint for INT/BIGINT primary keys
- **Maintenance**: VACUUM, ANALYZE, EXPLAIN, TRUNCATE
- **REPL Interface**: Interactive command-line with meta-commands

## Architecture

```
REPL → Parser → Executor → Catalog/BTree → Storage → BufferPool → Disk
```

**Core modules**: catalog, storage, btree, parser/, executor/, repl, main

## Requirements

- **Python 3.9+**
- **Dependencies**: `click>=8.0.0`, `prompt_toolkit>=3.0.0`, `pygments>=2.0.0`

```bash
pip install -r requirements.txt
```

## Usage

```bash
# Interactive REPL
python -m db_engine.main --data-dir ./mydb

# Execute command or file
python -m db_engine.main --execute "SELECT * FROM users"
python -m db_engine.main --file schema.sql
```

## SQL Examples

```sql
-- DDL
CREATE TABLE users (
    id INT PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    age INT,
    email TEXT UNIQUE
);
CREATE INDEX idx_age ON users (age);
ALTER TABLE users ADD COLUMN phone TEXT;
DROP TABLE users;

-- DML
INSERT INTO users VALUES (1, 'Alice', 25, 'alice@example.com');
SELECT * FROM users WHERE (age > 20 AND age < 30) OR name LIKE 'A%'
ORDER BY age DESC LIMIT 10 OFFSET 5;
UPDATE users SET age = 26 WHERE name = 'Alice';
DELETE FROM users WHERE age < 18;

-- Transactions
BEGIN;
INSERT INTO users VALUES (2, 'Bob', 30, 'bob@example.com');
COMMIT;

-- Maintenance
EXPLAIN SELECT * FROM users WHERE age > 25;
ANALYZE users;
VACUUM users;
```

## REPL Meta-Commands

`\dt` - List tables | `\di` - List indexes | `\d <table>` - Describe table | `\q` - Quit

## Data Types & Constraints

**Types**: INT, BIGINT, FLOAT, TEXT (max 10KB), BOOLEAN, TIMESTAMP, NULL
**Constraints**: PRIMARY KEY (mandatory), UNIQUE, NOT NULL, AUTOINCREMENT, Composite keys

## Testing

```bash
python3 tests/test_*.py  # 97/97 tests passing (100%)
```

## Key Features

- **Buffer Pool**: LRU cache (128 pages) - 90%+ hit rate
- **Cost-based planning**: Automatic index vs sequential scan selection
- **Null bitmap optimization**: Used only when needed
- **~4,500 lines**: Clean, modular Python code

## Limitations

No JOINs, aggregations, subqueries, foreign keys, or concurrent writes (single-user).

## Documentation

**See [CLAUDE.md](./CLAUDE.md)** - Complete architecture, implementation details, and development guide (source of truth).

## License

Educational project - MIT License
