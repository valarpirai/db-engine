# Storage Layer Documentation

## Table of Contents
1. [Overview](#overview)
2. [Architecture](#architecture)
3. [BufferPool - LRU Page Cache](#bufferpool)
4. [Tuple - Row Serialization](#tuple)
5. [Page - 8KB Blocks](#page)
6. [HeapFile - Table Storage](#heapfile)
7. [Free Space Map (FSM)](#free-space-map)
8. [Storage Flow Examples](#storage-flow-examples)
9. [Performance Characteristics](#performance-characteristics)

---

## Overview

The storage layer (`storage.py`) implements the **physical storage** of table data. It manages how rows are serialized, organized into pages, cached in memory, and persisted to disk.

### Core Components

```
HeapFile (table data file)
    ↓
Pages (8KB blocks)
    ↓
Tuples (serialized rows)
    ↓
BufferPool (LRU cache)
    ↓
Free Space Map (FSM - efficient insertion)
```

### Responsibilities

1. **Tuple Serialization**: Convert Python objects to bytes and back
2. **Page Management**: Organize tuples into fixed-size 8KB pages
3. **Buffer Pool**: Cache frequently accessed pages in memory (LRU eviction)
4. **Heap File**: Manage table data files with page allocation
5. **Free Space Map**: Track which pages have available space (O(1) insertion)

### Design Philosophy

- **PostgreSQL-inspired**: Heap files, pages, ctid addressing, buffer pool
- **Null bitmap optimization**: Only store null bitmap if table has nullable columns
- **Fixed-size pages**: 8KB pages (standard for many databases)
- **LRU caching**: Buffer pool with 128-page capacity (1MB total)
- **FSM for efficiency**: O(1) lookup for pages with free space
- **Tombstone deletion**: Mark deleted tuples with 0xFF (VACUUM reclaims space)

---

## Architecture

### File Structure

```
table_name.dat (Heap File)
├── [File Header: 16 bytes]
│   ├── Magic: "HEAP" (4 bytes)
│   ├── Page count: 8 bytes
│   └── Reserved: 4 bytes
├── [Page 0: 8192 bytes]
│   ├── Page Header: 16 bytes
│   ├── Tuple 1
│   ├── Tuple 2
│   └── ...
├── [Page 1: 8192 bytes]
└── ...
```

### Memory Hierarchy

```
Application
    ↓
Tuple objects (Python)
    ↓
BufferPool (128 pages cache)
    ↓ (cache miss)
Page objects (deserialized)
    ↓
Heap File (disk)
```

### Class Hierarchy

```python
BufferPool
    - LRU cache for pages
    - Tracks dirty pages
    - Handles eviction

Tuple
    - Represents a row
    - Serialize/deserialize with null bitmap
    - Schema validation

Page
    - 8KB block
    - Contains multiple tuples
    - Tracks free space

HeapFile
    - Manages table data file
    - Uses BufferPool for caching
    - Free Space Map for insertion
    - Sequential scan iterator
```

---

## BufferPool - LRU Page Cache

The `BufferPool` class implements an **LRU (Least Recently Used) page cache** to minimize disk I/O by keeping frequently accessed pages in memory.

### Purpose

Without a buffer pool, every tuple read requires a disk read (~1ms). With caching:
- **Cache hit**: Page already in memory (~100ns) - **10,000x faster**
- **Cache miss**: Load from disk, cache for future use

### Class Structure

```python
class BufferPool:
    def __init__(self, size: int = 128):
        self.size = size                      # Max pages in cache (default: 128)
        self.cache = OrderedDict()            # (file_path, page_num) → Page
        self.dirty_pages = set()              # Pages modified but not written
        self.hit_count = 0                    # Statistics
        self.miss_count = 0
```

**Key attributes:**
- `cache`: OrderedDict for LRU tracking (insertion order = access order)
- `dirty_pages`: Set of modified pages needing flush
- Hit/miss counters for performance monitoring

### Core Operations

#### Get Page

```python
def get_page(self, file_path: str, page_num: int, page_loader):
    """Get page from cache or load from disk"""
    key = (file_path, page_num)

    if key in self.cache:
        # Cache HIT - move to end (most recently used)
        self.cache.move_to_end(key)
        self.hit_count += 1
        return self.cache[key]

    # Cache MISS - load from disk
    self.miss_count += 1
    page = page_loader(file_path, page_num)  # Callback to HeapFile

    # Add to cache (may trigger eviction if full)
    if len(self.cache) >= self.size:
        self._evict()

    self.cache[key] = page
    self.cache.move_to_end(key)

    return page
```

**Example:**
```python
# First access (cache miss)
page = buffer_pool.get_page('users.dat', 0, heap._read_page_direct)
# Loads from disk, adds to cache

# Second access (cache hit)
page = buffer_pool.get_page('users.dat', 0, heap._read_page_direct)
# Returns from memory, no disk I/O
```

#### Mark Dirty

```python
def mark_dirty(self, file_path: str, page_num: int):
    """Mark page as modified (needs to be written to disk)"""
    key = (file_path, page_num)
    if key in self.cache:
        self.dirty_pages.add(key)
```

**Why track dirty pages?**
- Modified pages must be written before eviction
- Deferred writes improve performance (batch I/O)
- Critical for durability

**Example:**
```python
# Modify tuple in page
page.add_tuple(tuple_data)

# Mark as dirty
buffer_pool.mark_dirty('users.dat', 0)

# Page will be written to disk:
# 1. When evicted (LRU)
# 2. When flush_all() is called (shutdown, transaction commit)
```

#### LRU Eviction

```python
def _evict(self):
    """Evict least recently used page (first item in OrderedDict)"""
    if not self.cache:
        return

    # Get LRU page (first item)
    key, page = self.cache.popitem(last=False)

    # If dirty, write to disk before evicting
    if key in self.dirty_pages:
        self._flush_page(key, page)
        self.dirty_pages.discard(key)
```

**LRU Policy:**
- OrderedDict maintains access order
- Most recent access → end of dict
- Least recent access → beginning of dict
- Evict from beginning

**Example:**
```
Cache (size=3): [Page A, Page B, Page C]
Access Page B → Cache: [Page A, Page C, Page B]  (B moved to end)
Access new Page D → Evict Page A, Cache: [Page C, Page B, Page D]
```

#### Flush All

```python
def flush_all(self):
    """Write all dirty pages to disk"""
    for key in list(self.dirty_pages):
        if key in self.cache:
            self._flush_page(key, self.cache[key])
    self.dirty_pages.clear()
```

**When called:**
- Database shutdown
- Transaction COMMIT
- Explicit fsync/checkpoint

#### Statistics

```python
def stats(self) -> dict:
    """Get cache statistics"""
    total = self.hit_count + self.miss_count
    hit_rate = self.hit_count / total if total > 0 else 0
    return {
        'size': len(self.cache),          # Current pages in cache
        'capacity': self.size,             # Max pages (128)
        'hits': self.hit_count,            # Cache hits
        'misses': self.miss_count,         # Cache misses
        'hit_rate': hit_rate,              # Hit rate (e.g., 0.92 = 92%)
        'dirty_pages': len(self.dirty_pages)  # Pages needing flush
    }
```

**Example output:**
```python
{
    'size': 64,           # 64 pages currently cached
    'capacity': 128,      # 128 page capacity
    'hits': 9200,         # 9200 cache hits
    'misses': 800,        # 800 cache misses
    'hit_rate': 0.92,     # 92% hit rate (excellent!)
    'dirty_pages': 12     # 12 pages modified
}
```

### Performance Impact

**Without buffer pool:**
```
10,000 tuple reads = 10,000 disk reads
= 10,000ms (10 seconds)
```

**With buffer pool (92% hit rate):**
```
10,000 tuple reads:
- 9,200 cache hits (memory): 9,200 × 0.0001ms = 0.92ms
- 800 cache misses (disk): 800 × 1ms = 800ms
Total: ~801ms (12x faster!)
```

### Configuration

Default: 128 pages × 8KB = **1MB cache**

Adjust in `config.py`:
```python
BUFFER_POOL_SIZE = 128  # Default
BUFFER_POOL_SIZE = 256  # 2MB cache (better for large datasets)
BUFFER_POOL_SIZE = 512  # 4MB cache (even better)
```

---

## Tuple - Row Serialization

The `Tuple` class represents a table row and handles serialization to/from bytes with **null bitmap optimization**.

### Purpose

Convert between Python objects (list of values) and binary format for disk storage:
```
Python: [1, 'Alice', 25, None]  ↔  Bytes: b'\x08\x00\x00\x00\x01Alice\x00\x00\x00\x19'
```

### Class Structure

```python
class Tuple:
    def __init__(self, values: List[Any], schema: TableSchema):
        self.values = values  # [1, 'Alice', 25, None]
        self.schema = schema  # Table schema (column types, nullable flags)

        # Validate tuple size (max 65KB)
        estimated_size = self._estimate_size()
        if estimated_size > MAX_TUPLE_SIZE:
            raise ValueError(f"Tuple size ({estimated_size}) exceeds maximum (65535)")
```

**Tuple size limit:** 65,535 bytes (enforced to prevent memory issues)

### Null Bitmap Optimization

**Problem:** Storing NULL values wastes space.

**Solution:** Use a **null bitmap** to mark which columns are NULL, then only serialize non-NULL values.

**Optimization:** Only include null bitmap if table has nullable columns!

```python
if schema.has_nullable_columns():
    # Include null bitmap
else:
    # Skip null bitmap entirely (space savings!)
```

**Example:**

Table schema:
```sql
CREATE TABLE users (
    id INT NOT NULL,          -- Not nullable
    email TEXT NOT NULL,      -- Not nullable
    age INT,                  -- Nullable
    phone TEXT                -- Nullable
);
```

Tuple: `[1, 'alice@example.com', NULL, '555-1234']`

**Null bitmap:**
```
Columns: id(not null), email(not null), age(nullable), phone(nullable)
Nullable columns: age, phone
Bitmap: [1, 0] → age is NULL (bit 0 = 1), phone is not NULL (bit 0 = 0)
Binary: 0b00000001 = 0x01
```

### Serialization Format

```
[Null Bitmap: variable bytes, only if nullable columns exist]
[Value 1: variable bytes, skip if NULL]
[Value 2: variable bytes, skip if NULL]
...
```

#### Example 1: Table with NO nullable columns

Schema: `(id INT NOT NULL, email TEXT NOT NULL)`
Tuple: `[1, 'alice@example.com']`

Serialized:
```
[No null bitmap - table has no nullable columns]
[0x01 0x00 0x00 0x00]           # id = 1 (4 bytes, INT)
[0x11 0x00]                      # text length = 17 (2 bytes)
[alice@example.com]              # text data (17 bytes)
```

Total: 4 + 2 + 17 = **23 bytes**

#### Example 2: Table WITH nullable columns

Schema: `(id INT NOT NULL, age INT, phone TEXT)`
Tuple: `[1, NULL, '555-1234']`

Serialized:
```
[0x01]                           # Null bitmap: age is NULL (bit 0 = 1)
[0x01 0x00 0x00 0x00]           # id = 1 (4 bytes)
                                 # age = NULL (skip, marked in bitmap)
[0x08 0x00]                      # phone length = 8 (2 bytes)
[555-1234]                       # phone data (8 bytes)
```

Total: 1 + 4 + 2 + 8 = **15 bytes** (saved 4 bytes by not storing NULL INT)

### Serialization Code

```python
def serialize(self) -> bytes:
    """Serialize tuple to bytes with null bitmap optimization"""
    data = b''

    # Build null bitmap only if table has nullable columns
    if self.schema.has_nullable_columns():
        nullable_cols = [col for col in self.schema.columns if col.nullable]
        bitmap_bytes = []
        current_byte = 0
        bit_position = 0

        for col in self.schema.columns:
            if col.nullable:
                col_index = self.schema.get_column_index(col.name)
                is_null = (col_index >= len(self.values) or self.values[col_index] is None)

                if is_null:
                    current_byte |= (1 << bit_position)  # Set bit to 1

                bit_position += 1
                if bit_position == 8:  # Filled one byte
                    bitmap_bytes.append(current_byte)
                    current_byte = 0
                    bit_position = 0

        # Add remaining bits if any
        if bit_position > 0:
            bitmap_bytes.append(current_byte)

        data += bytes(bitmap_bytes)

    # Serialize non-NULL values
    for i, col in enumerate(self.schema.columns):
        if i >= len(self.values) or self.values[i] is None:
            continue  # Skip NULL values

        value = self.values[i]

        if col.datatype == 'INT':
            data += struct.pack('i', int(value))  # 4 bytes, signed int
        elif col.datatype == 'BIGINT':
            data += struct.pack('q', int(value))  # 8 bytes, signed long
        elif col.datatype == 'FLOAT':
            data += struct.pack('d', float(value))  # 8 bytes, double
        elif col.datatype == 'BOOLEAN':
            data += struct.pack('?', bool(value))  # 1 byte, bool
        elif col.datatype == 'TIMESTAMP':
            data += struct.pack('q', int(value))  # 8 bytes, Unix timestamp
        elif col.datatype == 'TEXT':
            text_bytes = str(value).encode('utf-8')
            if len(text_bytes) > MAX_TEXT_SIZE:  # 10KB max
                text_bytes = text_bytes[:MAX_TEXT_SIZE]
            data += struct.pack('H', len(text_bytes))  # 2-byte length
            data += text_bytes

    return data
```

### Deserialization Code

```python
@staticmethod
def deserialize(data: bytes, schema: TableSchema) -> 'Tuple':
    """Deserialize bytes back to Tuple"""
    offset = 0
    values = []

    # Read null bitmap if table has nullable columns
    null_bitmap = []
    if schema.has_nullable_columns():
        nullable_count = sum(1 for col in schema.columns if col.nullable)
        bitmap_size = (nullable_count + 7) // 8  # Ceiling division

        for i in range(bitmap_size):
            null_bitmap.append(data[offset])
            offset += 1

    # Deserialize values
    nullable_index = 0
    for col in schema.columns:
        # Check if this column is NULL
        is_null = False
        if col.nullable and null_bitmap:
            byte_index = nullable_index // 8
            bit_index = nullable_index % 8
            is_null = (null_bitmap[byte_index] & (1 << bit_index)) != 0
            nullable_index += 1

        if is_null:
            values.append(None)
            continue

        # Deserialize non-NULL value
        if col.datatype == 'INT':
            value = struct.unpack('i', data[offset:offset+4])[0]
            offset += 4
        elif col.datatype == 'BIGINT':
            value = struct.unpack('q', data[offset:offset+8])[0]
            offset += 8
        elif col.datatype == 'FLOAT':
            value = struct.unpack('d', data[offset:offset+8])[0]
            offset += 8
        elif col.datatype == 'BOOLEAN':
            value = struct.unpack('?', data[offset:offset+1])[0]
            offset += 1
        elif col.datatype == 'TIMESTAMP':
            value = struct.unpack('q', data[offset:offset+8])[0]
            offset += 8
        elif col.datatype == 'TEXT':
            text_len = struct.unpack('H', data[offset:offset+2])[0]
            offset += 2
            text_bytes = data[offset:offset+text_len]
            value = text_bytes.decode('utf-8')
            offset += text_len

        values.append(value)

    return Tuple(values, schema)
```

### Data Type Sizes

Fixed-size types:
- `INT`: 4 bytes (32-bit signed)
- `BIGINT`: 8 bytes (64-bit signed)
- `FLOAT`: 8 bytes (double precision)
- `BOOLEAN`: 1 byte
- `TIMESTAMP`: 8 bytes (Unix timestamp, UTC)

Variable-size types:
- `TEXT`: 2 bytes (length) + UTF-8 bytes (max 10KB)

### Size Limits

- **Maximum tuple size**: 65,535 bytes
- **Maximum TEXT size**: 10,240 bytes (10KB)
- **Null bitmap size**: ⌈nullable_columns / 8⌉ bytes

**Example:** Table with 10 nullable columns:
- Null bitmap: ⌈10 / 8⌉ = 2 bytes
- Overhead per tuple: 2 bytes (only if any column is nullable)

---

## Page - 8KB Blocks

The `Page` class represents a fixed-size **8KB block** that stores multiple tuples.

### Purpose

Pages are the unit of I/O:
- Disk reads/writes operate on entire pages (not individual tuples)
- Buffer pool caches pages (not tuples)
- Fixed size simplifies addressing: page_num × 8KB = file offset

### Class Structure

```python
class Page:
    def __init__(self, page_number: int):
        self.page_number = page_number
        self.tuples = []              # List of (offset, tuple_data)
        self.free_space = 8176        # PAGE_SIZE - PAGE_HEADER_SIZE
        self.dead_tuple_count = 0     # Tombstoned tuples
```

**Key attributes:**
- `page_number`: Position in heap file (0-indexed)
- `tuples`: List of (offset, bytes) - offset is within page
- `free_space`: Remaining bytes available (starts at 8176)
- `dead_tuple_count`: Number of deleted tuples (tombstones)

### Page Layout

```
[Page Header: 16 bytes]
├── Free space: 2 bytes (uint16)
├── Tuple count: 2 bytes (uint16)
├── Dead tuple count: 2 bytes (uint16)
└── Reserved: 10 bytes

[Tuple Directory: variable size]
├── Tuple 1: offset(4 bytes) + length(4 bytes) + data(variable)
├── Tuple 2: offset(4 bytes) + length(4 bytes) + data(variable)
└── ...

[Padding to 8192 bytes total]
```

### Core Operations

#### Add Tuple

```python
def add_tuple(self, tuple_data: bytes) -> int:
    """Add tuple to page, return offset within page"""
    if not self.can_fit(len(tuple_data)):
        raise ValueError(f"Tuple doesn't fit in page")

    # Calculate offset from start of page (after header)
    offset = PAGE_HEADER_SIZE + (PAGE_SIZE - PAGE_HEADER_SIZE - self.free_space)

    self.tuples.append((offset, tuple_data))
    self.free_space -= len(tuple_data)

    return offset
```

**Example:**
```python
page = Page(page_number=0)
# free_space = 8176 bytes initially

# Add first tuple (50 bytes)
offset1 = page.add_tuple(tuple_data_1)  # Returns: 16 (after header)
# free_space = 8126 bytes

# Add second tuple (30 bytes)
offset2 = page.add_tuple(tuple_data_2)  # Returns: 66 (16 + 50)
# free_space = 8096 bytes
```

#### Get Tuple

```python
def get_tuple(self, offset: int) -> Optional[bytes]:
    """Get tuple at specific offset"""
    for tup_offset, tup_data in self.tuples:
        if tup_offset == offset:
            # Check if deleted (first byte is 0xFF for tombstone)
            if len(tup_data) > 0 and tup_data[0] == 0xFF:
                return None  # Tuple is deleted
            return tup_data
    return None
```

**Tombstone detection:**
- Deleted tuples marked with `0xFF` as first byte
- `get_tuple()` returns `None` for deleted tuples
- Space not reclaimed until VACUUM

#### Mark Deleted

```python
def mark_deleted(self, offset: int):
    """Mark tuple as deleted (tombstone)"""
    for i, (tup_offset, tup_data) in enumerate(self.tuples):
        if tup_offset == offset:
            # Mark as deleted by setting first byte to 0xFF
            self.tuples[i] = (tup_offset, b'\xFF' + tup_data[1:])
            self.dead_tuple_count += 1
            return
    raise ValueError(f"No tuple found at offset {offset}")
```

**Why tombstones?**
- Fast deletion (no data movement)
- Tuple slots preserved (stable ctids for indexes)
- Space reclaimed later by VACUUM

### Serialization

```python
def serialize(self) -> bytes:
    """Serialize page to 8KB bytes"""
    # Header (16 bytes)
    header = struct.pack('HHH', self.free_space, len(self.tuples), self.dead_tuple_count)
    header += b'\x00' * 10  # Reserved space

    # Tuple data with directory
    data = header
    for offset, tuple_data in self.tuples:
        data += struct.pack('I', offset)         # 4 bytes: offset
        data += struct.pack('I', len(tuple_data)) # 4 bytes: length
        data += tuple_data                        # Variable: data

    # Pad to PAGE_SIZE (8192 bytes)
    padding = PAGE_SIZE - len(data)
    data += b'\x00' * padding

    return data
```

### Deserialization

```python
@staticmethod
def deserialize(data: bytes, page_number: int) -> 'Page':
    """Deserialize page from bytes"""
    page = Page(page_number)

    # Read header (16 bytes)
    free_space, tuple_count, dead_tuple_count = struct.unpack('HHH', data[0:6])
    page.free_space = free_space
    page.dead_tuple_count = dead_tuple_count

    # Read tuple directory
    pos = PAGE_HEADER_SIZE
    for _ in range(tuple_count):
        # Read offset (4 bytes)
        tuple_offset = struct.unpack('I', data[pos:pos+4])[0]
        pos += 4

        # Read length (4 bytes)
        tuple_length = struct.unpack('I', data[pos:pos+4])[0]
        pos += 4

        # Read tuple data
        tuple_data = data[pos:pos+tuple_length]
        pos += tuple_length

        # Add to page
        page.tuples.append((tuple_offset, tuple_data))

    return page
```

### Page Capacity

**Maximum tuples per page:**

Depends on tuple size:
- 50-byte tuples: ~160 tuples per page
- 100-byte tuples: ~80 tuples per page
- 500-byte tuples: ~16 tuples per page

**Overhead per tuple:**
- Tuple directory: 8 bytes (offset + length)
- Actual tuple data: variable

**Example calculation:**
```
Page size: 8192 bytes
Header: 16 bytes
Available: 8176 bytes

Tuple size: 100 bytes
Directory overhead: 8 bytes per tuple
Total per tuple: 108 bytes

Max tuples: 8176 / 108 = 75 tuples
```

---

## HeapFile - Table Storage

The `HeapFile` class manages a table's data file on disk, organizing tuples into pages with efficient insertion via Free Space Map.

### Purpose

- **Persistent storage**: Stores all table data on disk
- **Page allocation**: Creates new pages as table grows
- **Fast insertion**: Uses FSM to find pages with free space in O(1)
- **Sequential scan**: Iterates all tuples for full table scans
- **VACUUM**: Reclaims space from deleted tuples

### Class Structure

```python
class HeapFile:
    def __init__(self, file_path: str, schema: TableSchema, buffer_pool: BufferPool):
        self.file_path = file_path           # e.g., 'users.dat'
        self.schema = schema                 # Table schema
        self.buffer_pool = buffer_pool       # Shared buffer pool
        self.page_count = 0                  # Number of pages in file
        self.free_space_map = {}             # page_num → free_space_bytes
```

### File Format

```
users.dat
├── [File Header: 16 bytes]
│   ├── Magic: "HEAP" (4 bytes)
│   ├── Page count: 8 bytes (uint64)
│   └── Reserved: 4 bytes
├── [Page 0: 8192 bytes]
├── [Page 1: 8192 bytes]
├── [Page 2: 8192 bytes]
└── ...
```

### Core Operations

#### Create

```python
def create(self):
    """Initialize new heap file"""
    with open(self.file_path, 'wb') as f:
        # Write file header
        header = b'HEAP'                      # Magic number
        header += struct.pack('Q', 0)         # Page count = 0
        header += b'\x00' * 4                 # Reserved
        f.write(header)

    self.page_count = 0
    self.free_space_map = {}
```

**Example:**
```python
heap = HeapFile('users.dat', schema, buffer_pool)
heap.create()
# Creates empty 16-byte file with header
```

#### Open

```python
def open(self):
    """Open existing heap file"""
    with open(self.file_path, 'rb') as f:
        # Read header
        magic = f.read(4)
        if magic != b'HEAP':
            raise ValueError(f"Invalid heap file: {self.file_path}")

        self.page_count = struct.unpack('Q', f.read(8))[0]

    # Rebuild FSM by scanning pages
    self._rebuild_fsm()
```

**Rebuilding FSM:**
```python
def _rebuild_fsm(self):
    """Rebuild free space map by scanning all pages"""
    self.free_space_map = {}
    for page_num in range(self.page_count):
        page = self._read_page_direct(self.file_path, page_num)
        self.free_space_map[page_num] = page.free_space
```

**Why rebuild FSM?**
- FSM not persisted to disk (in-memory only)
- Must be reconstructed on file open
- Real databases persist FSM for faster startup

#### Insert Tuple

```python
def insert_tuple(self, tuple: Tuple) -> (int, int):
    """Insert tuple, returns ctid (page_number, offset)"""
    tuple_data = tuple.serialize()
    tuple_size = len(tuple_data)

    # Find page with enough space using FSM (O(1) lookup)
    page_num = self._find_page_with_space(tuple_size)

    if page_num is None:
        # No page with space - create new page
        page_num = self._create_new_page()

    # Load page (through buffer pool)
    page = self._read_page(page_num)

    # Add tuple to page
    offset = page.add_tuple(tuple_data)

    # Update FSM
    self.free_space_map[page_num] = page.free_space

    # Mark page as dirty
    self.buffer_pool.mark_dirty(self.file_path, page_num)

    return (page_num, offset)  # ctid
```

**Example:**
```python
tuple = Tuple([1, 'alice@example.com', 'Alice'], schema)
ctid = heap.insert_tuple(tuple)
# Returns: (0, 16) - page 0, offset 16
```

#### Read Tuple

```python
def read_tuple(self, ctid: (int, int)) -> Optional[Tuple]:
    """Read tuple by ctid (page_number, offset)"""
    page_num, offset = ctid

    # Load page (through buffer pool for caching)
    page = self._read_page(page_num)

    # Get tuple data
    tuple_data = page.get_tuple(offset)
    if tuple_data is None:
        return None  # Deleted or not found

    # Deserialize
    return Tuple.deserialize(tuple_data, self.schema)
```

**Example:**
```python
# Read tuple at ctid (0, 16)
tuple = heap.read_tuple((0, 16))
# Returns: Tuple([1, 'alice@example.com', 'Alice'], schema)
```

#### Delete Tuple

```python
def delete_tuple(self, ctid: (int, int)):
    """Mark tuple as deleted (tombstone)"""
    page_num, offset = ctid

    # Load page
    page = self._read_page(page_num)

    # Mark as deleted
    page.mark_deleted(offset)

    # Mark page as dirty
    self.buffer_pool.mark_dirty(self.file_path, page_num)
```

**Note:** Space not reclaimed until VACUUM

#### Sequential Scan

```python
def scan_all(self):
    """Sequential scan - iterate all non-deleted tuples"""
    for page_num in range(self.page_count):
        page = self._read_page(page_num)

        for offset, tuple_data in page.tuples:
            # Skip deleted tuples (tombstones)
            if len(tuple_data) > 0 and tuple_data[0] == 0xFF:
                continue

            tuple_obj = Tuple.deserialize(tuple_data, self.schema)
            ctid = (page_num, offset)
            yield (tuple_obj, ctid)
```

**Usage:**
```python
# Scan all users
for tuple_obj, ctid in heap.scan_all():
    print(f"ctid={ctid}, values={tuple_obj.values}")

# Output:
# ctid=(0, 16), values=[1, 'alice@example.com', 'Alice']
# ctid=(0, 66), values=[2, 'bob@example.com', 'Bob']
# ...
```

#### VACUUM

```python
def vacuum(self):
    """Reclaim space from deleted tuples"""
    for page_num in range(self.page_count):
        page = self._read_page(page_num)

        if page.dead_tuple_count == 0:
            continue  # No dead tuples, skip

        # Rebuild page without dead tuples
        new_page = Page(page_num)

        for offset, tuple_data in page.tuples:
            # Skip deleted tuples (tombstones)
            if len(tuple_data) > 0 and tuple_data[0] == 0xFF:
                continue

            # Add live tuple to new page
            new_page.add_tuple(tuple_data)

        # Replace old page with compacted page
        self.buffer_pool.cache[(self.file_path, page_num)] = new_page
        self.buffer_pool.mark_dirty(self.file_path, page_num)

        # Update FSM
        self.free_space_map[page_num] = new_page.free_space

    # Flush dirty pages to disk
    self.buffer_pool.flush_all()
```

**Example:**
```
Before VACUUM:
Page 0: 100 live tuples, 20 dead tuples, 1000 bytes free

After VACUUM:
Page 0: 100 live tuples, 0 dead tuples, 2500 bytes free
(reclaimed ~1500 bytes from dead tuples)
```

### Page Management

#### Find Page with Space

```python
def _find_page_with_space(self, required_space: int) -> Optional[int]:
    """Find page with enough free space using FSM"""
    for page_num, free_space in self.free_space_map.items():
        if free_space >= required_space:
            return page_num
    return None
```

**O(1) average case** (FSM is a dict)

#### Create New Page

```python
def _create_new_page(self) -> int:
    """Create and append new page to file"""
    page_num = self.page_count
    page = Page(page_num)

    # Write empty page to file
    with open(self.file_path, 'r+b') as f:
        offset = HEAP_FILE_HEADER_SIZE + (page_num * PAGE_SIZE)
        f.seek(offset)
        f.write(page.serialize())

    # Update page count in header
    self.page_count += 1
    with open(self.file_path, 'r+b') as f:
        f.seek(4)  # After magic
        f.write(struct.pack('Q', self.page_count))

    # Update FSM
    self.free_space_map[page_num] = page.free_space

    return page_num
```

#### Read Page (with Buffer Pool)

```python
def _read_page(self, page_num: int) -> Page:
    """Read page through buffer pool"""
    return self.buffer_pool.get_page(
        self.file_path,
        page_num,
        self._read_page_direct  # Callback for cache miss
    )

def _read_page_direct(self, file_path: str, page_num: int) -> Page:
    """Read page directly from disk (used by buffer pool)"""
    with open(file_path, 'rb') as f:
        offset = HEAP_FILE_HEADER_SIZE + (page_num * PAGE_SIZE)
        f.seek(offset)
        data = f.read(PAGE_SIZE)

    return Page.deserialize(data, page_num)
```

---

## Free Space Map (FSM)

The Free Space Map is a critical optimization for efficient tuple insertion.

### Problem Without FSM

**Naive approach:** Scan all pages to find one with space
```python
# O(N) - very slow for large tables!
for page_num in range(self.page_count):
    page = read_page(page_num)
    if page.free_space >= tuple_size:
        return page_num  # Found!
```

**Performance:**
- 10,000 pages × 1ms per page read = **10 seconds per insert!**
- Unacceptable for workloads with many inserts

### Solution: FSM

**Data structure:** In-memory dict mapping page_num → free_space
```python
self.free_space_map = {
    0: 1500,    # Page 0 has 1500 bytes free
    1: 500,     # Page 1 has 500 bytes free
    2: 8000,    # Page 2 has 8000 bytes free (nearly empty)
    3: 100,     # Page 3 has 100 bytes free
}
```

**Lookup:** O(1) average case
```python
def _find_page_with_space(self, required_space: int) -> Optional[int]:
    for page_num, free_space in self.free_space_map.items():
        if free_space >= required_space:
            return page_num
    return None
```

**Performance:**
- Dict iteration over metadata: ~1μs
- **10,000x faster than scanning pages!**

### FSM Maintenance

FSM is updated on every operation that changes free space:

**After INSERT:**
```python
offset = page.add_tuple(tuple_data)
self.free_space_map[page_num] = page.free_space  # Update FSM
```

**After DELETE:**
```python
page.mark_deleted(offset)
# FSM NOT updated (tombstone doesn't reclaim space)
# Space reclaimed after VACUUM
```

**After VACUUM:**
```python
# Rebuild page without dead tuples
new_page = Page(page_num)
for tuple_data in live_tuples:
    new_page.add_tuple(tuple_data)

self.free_space_map[page_num] = new_page.free_space  # Update FSM
```

### Trade-offs

**Advantages:**
- ✅ O(1) insertion (vs. O(N) without FSM)
- ✅ Simple implementation (Python dict)
- ✅ Accurate (updated on every modification)

**Disadvantages:**
- ❌ In-memory only (not persisted)
- ❌ Must rebuild on file open (scans all pages once)
- ❌ Memory overhead: ~16 bytes per page

**Real databases (PostgreSQL):**
- FSM persisted to separate file (`.fsm`)
- Hierarchical structure for very large tables
- Approximate (not exact) free space tracking

---

## Storage Flow Examples

### Complete INSERT Flow

```
User: INSERT INTO users VALUES (1, 'alice@example.com', 'Alice');
  ↓
[1] Create Tuple object
    tuple = Tuple([1, 'alice@example.com', 'Alice'], schema)
  ↓
[2] Serialize to bytes
    tuple_data = tuple.serialize()
    # Result: b'\x01\x00\x00\x00\x11\x00alice@example.com\x05\x00Alice'
  ↓
[3] Find page with space (FSM lookup - O(1))
    page_num = heap._find_page_with_space(len(tuple_data))
    # FSM: {0: 8000, 1: 1500} → Returns: 0
  ↓
[4] Load page (via buffer pool)
    page = buffer_pool.get_page('users.dat', 0, heap._read_page_direct)
    # Cache miss → Load from disk
  ↓
[5] Add tuple to page
    offset = page.add_tuple(tuple_data)
    # Returns: 16 (after page header)
    # page.free_space: 8000 → 7965
  ↓
[6] Update FSM
    heap.free_space_map[0] = 7965
  ↓
[7] Mark page dirty
    buffer_pool.mark_dirty('users.dat', 0)
  ↓
[8] Return ctid
    return (0, 16)
```

### Complete SELECT Flow (Sequential Scan)

```
User: SELECT * FROM users WHERE age > 18;
  ↓
[1] Sequential scan all pages
    for page_num in range(heap.page_count):  # 0, 1, 2, ...
  ↓
[2] Load page (via buffer pool)
    page = buffer_pool.get_page('users.dat', page_num, ...)
    # Cache hit (90%+) → Return from memory
  ↓
[3] Iterate tuples in page
    for offset, tuple_data in page.tuples:
  ↓
[4] Check if deleted
    if tuple_data[0] == 0xFF:
        continue  # Skip tombstone
  ↓
[5] Deserialize tuple
    tuple_obj = Tuple.deserialize(tuple_data, schema)
    # Result: Tuple([1, 'alice@example.com', 'Alice', 25], schema)
  ↓
[6] Evaluate WHERE clause
    if tuple_obj.values[3] > 18:  # age > 18
        results.append(tuple_obj)  # Match!
  ↓
[7] Return results
    return results
```

### Complete DELETE Flow

```
User: DELETE FROM users WHERE id = 1;
  ↓
[1] Find tuple (index scan or sequential scan)
    ctid = (0, 16)  # Found at page 0, offset 16
  ↓
[2] Load page (via buffer pool)
    page = buffer_pool.get_page('users.dat', 0, ...)
  ↓
[3] Mark tuple deleted
    page.mark_deleted(16)
    # tuple_data[0] = 0xFF (tombstone)
    # page.dead_tuple_count++
  ↓
[4] Mark page dirty
    buffer_pool.mark_dirty('users.dat', 0)
  ↓
[5] FSM NOT updated (space not reclaimed yet)
  ↓
[6] Later: VACUUM reclaims space
    heap.vacuum()
    # Rebuilds page without tombstones
    # Updates FSM: free_space increases
```

---

## Performance Characteristics

### Time Complexity

| Operation | Without Buffer Pool | With Buffer Pool (90% hit) | Notes |
|-----------|---------------------|----------------------------|-------|
| Insert tuple | O(1) | O(1) | FSM finds page in O(1) |
| Read tuple by ctid | O(1) + disk | O(1) | Direct page + offset |
| Sequential scan | O(N) + disk | O(N) + 10% disk | N = number of tuples |
| Delete tuple | O(1) + disk | O(1) | Tombstone only |
| VACUUM | O(N) + disk | O(N) + disk | Rebuilds all pages |

### Space Complexity

**Per table:**
- Heap file: ~N × tuple_size (where N = row count)
- Buffer pool (shared): 128 pages × 8KB = 1MB
- FSM: ~16 bytes per page

**Example calculation:**

Table with 1,000,000 rows, 100-byte average tuple size:
- Tuples per page: ~75
- Total pages: 1,000,000 / 75 = ~13,334 pages
- Heap file size: 13,334 × 8KB = ~107 MB
- FSM overhead: 13,334 × 16 bytes = ~213 KB (0.2% overhead)

### Buffer Pool Hit Rate

**Typical hit rates:**
- Random access: 60-70%
- Sequential scan: 90-95%
- Repeated queries: 95-99%

**Impact on performance:**

90% hit rate example (10,000 tuple reads):
- 9,000 cache hits (memory): 9,000 × 0.0001ms = 0.9ms
- 1,000 cache misses (disk): 1,000 × 1ms = 1000ms
- **Total: ~1001ms** (vs. 10,000ms without cache = 10x faster)

### Disk I/O Patterns

**Sequential scan:**
```
Read pattern: Page 0 → Page 1 → Page 2 → ...
OS can prefetch: Good performance
```

**Random access by ctid:**
```
Read pattern: Page 47 → Page 2 → Page 193 → ...
OS cannot prefetch: Slower performance
```

**Why indexes help:**
- B-tree traversal: O(log N) pages
- Direct ctid lookup: 1 page read
- vs. Sequential scan: O(N) pages

---

**End of Storage Layer Documentation**

For information on B-tree indexes that use ctids to point to tuples, see the [B-tree Documentation](./btree.md) (to be created).

