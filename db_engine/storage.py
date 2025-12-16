"""
Storage Layer - Heap files, pages, tuples, buffer pool, and free space map

This module provides:
- BufferPool: LRU page cache to minimize disk I/O
- Tuple: Row serialization with null bitmap optimization
- Page: 8KB blocks with header
- HeapFile: Table data management with FSM for efficient insertion
"""

from typing import List, Dict, Tuple as TupleType, Optional, Any
from dataclasses import dataclass
from collections import OrderedDict
import struct
import os

from .config import (
    PAGE_SIZE, PAGE_HEADER_SIZE, HEAP_FILE_HEADER_SIZE,
    BUFFER_POOL_SIZE, MAX_TUPLE_SIZE,
    INT_SIZE, BIGINT_SIZE, FLOAT_SIZE, BOOL_SIZE, TIMESTAMP_SIZE, MAX_TEXT_SIZE
)
from .catalog import TableSchema


class BufferPool:
    """LRU page cache - minimizes disk I/O"""

    def __init__(self, size: int = BUFFER_POOL_SIZE):
        self.size = size
        self.cache: OrderedDict = OrderedDict()  # (file_path, page_num) -> Page
        self.dirty_pages: set = set()  # Track modified pages
        self.hit_count = 0
        self.miss_count = 0

    def get_page(self, file_path: str, page_num: int, page_loader):
        """Get page from cache or load from disk"""
        key = (file_path, page_num)

        if key in self.cache:
            # Cache hit - move to end (most recently used)
            self.cache.move_to_end(key)
            self.hit_count += 1
            return self.cache[key]

        # Cache miss - load from disk
        self.miss_count += 1
        page = page_loader(file_path, page_num)

        # Add to cache (may evict if full)
        if len(self.cache) >= self.size:
            self._evict()

        self.cache[key] = page
        self.cache.move_to_end(key)

        return page

    def mark_dirty(self, file_path: str, page_num: int):
        """Mark page as modified (needs to be written to disk)"""
        key = (file_path, page_num)
        if key in self.cache:
            self.dirty_pages.add(key)

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

    def _flush_page(self, key: TupleType[str, int], page):
        """Write dirty page to disk"""
        file_path, page_num = key
        # Page will have a write_to_disk method
        page.write_to_disk(file_path)

    def flush_all(self):
        """Write all dirty pages to disk"""
        for key in list(self.dirty_pages):
            if key in self.cache:
                self._flush_page(key, self.cache[key])
        self.dirty_pages.clear()

    def invalidate(self, file_path: str, page_num: int):
        """Remove page from cache"""
        key = (file_path, page_num)
        if key in self.cache:
            del self.cache[key]
        self.dirty_pages.discard(key)

    def stats(self) -> dict:
        """Get cache statistics"""
        total = self.hit_count + self.miss_count
        hit_rate = self.hit_count / total if total > 0 else 0
        return {
            'size': len(self.cache),
            'capacity': self.size,
            'hits': self.hit_count,
            'misses': self.miss_count,
            'hit_rate': hit_rate,
            'dirty_pages': len(self.dirty_pages)
        }


class Tuple:
    """Represents a table row with null bitmap optimization"""

    def __init__(self, values: List[Any], schema: TableSchema):
        self.values = values  # List of column values (None = NULL)
        self.schema = schema

        # Validate tuple size won't exceed limit
        estimated_size = self._estimate_size()
        if estimated_size > MAX_TUPLE_SIZE:
            raise ValueError(
                f"Tuple size ({estimated_size} bytes) exceeds maximum ({MAX_TUPLE_SIZE} bytes)"
            )

    def _estimate_size(self) -> int:
        """Estimate serialized tuple size"""
        size = 0

        # Null bitmap (only if table has nullable columns)
        if self.schema.has_nullable_columns():
            nullable_count = sum(1 for col in self.schema.columns if col.nullable)
            size += (nullable_count + 7) // 8  # Ceiling division

        # Values
        for i, col in enumerate(self.schema.columns):
            if i < len(self.values) and self.values[i] is not None:
                if col.datatype == 'INT':
                    size += INT_SIZE
                elif col.datatype == 'BIGINT':
                    size += BIGINT_SIZE
                elif col.datatype == 'FLOAT':
                    size += FLOAT_SIZE
                elif col.datatype == 'BOOLEAN':
                    size += BOOL_SIZE
                elif col.datatype == 'TIMESTAMP':
                    size += TIMESTAMP_SIZE
                elif col.datatype == 'TEXT':
                    text_val = str(self.values[i])
                    size += 2 + len(text_val.encode('utf-8'))  # length + data

        return size

    def serialize(self) -> bytes:
        """
        Serialize tuple to bytes with null bitmap optimization

        Format:
        [null_bitmap (if table has nullable columns): variable bytes]
        [value1: variable bytes]
        [value2: variable bytes]
        ...
        """
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
                        current_byte |= (1 << bit_position)

                    bit_position += 1
                    if bit_position == 8:
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
                continue  # Skip NULL values (marked in bitmap)

            value = self.values[i]

            if col.datatype == 'INT':
                data += struct.pack('i', int(value))
            elif col.datatype == 'BIGINT':
                data += struct.pack('q', int(value))
            elif col.datatype == 'FLOAT':
                data += struct.pack('d', float(value))
            elif col.datatype == 'BOOLEAN':
                data += struct.pack('?', bool(value))
            elif col.datatype == 'TIMESTAMP':
                data += struct.pack('q', int(value))
            elif col.datatype == 'TEXT':
                text_bytes = str(value).encode('utf-8')
                if len(text_bytes) > MAX_TEXT_SIZE:
                    text_bytes = text_bytes[:MAX_TEXT_SIZE]
                data += struct.pack('H', len(text_bytes))  # 2-byte length
                data += text_bytes

        return data

    @staticmethod
    def deserialize(data: bytes, schema: TableSchema) -> 'Tuple':
        """Deserialize bytes back to Tuple"""
        offset = 0
        values = []

        # Read null bitmap if table has nullable columns
        null_bitmap = []
        if schema.has_nullable_columns():
            nullable_count = sum(1 for col in schema.columns if col.nullable)
            bitmap_size = (nullable_count + 7) // 8

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
                value = struct.unpack('i', data[offset:offset+INT_SIZE])[0]
                offset += INT_SIZE
            elif col.datatype == 'BIGINT':
                value = struct.unpack('q', data[offset:offset+BIGINT_SIZE])[0]
                offset += BIGINT_SIZE
            elif col.datatype == 'FLOAT':
                value = struct.unpack('d', data[offset:offset+FLOAT_SIZE])[0]
                offset += FLOAT_SIZE
            elif col.datatype == 'BOOLEAN':
                value = struct.unpack('?', data[offset:offset+BOOL_SIZE])[0]
                offset += BOOL_SIZE
            elif col.datatype == 'TIMESTAMP':
                value = struct.unpack('q', data[offset:offset+TIMESTAMP_SIZE])[0]
                offset += TIMESTAMP_SIZE
            elif col.datatype == 'TEXT':
                text_len = struct.unpack('H', data[offset:offset+2])[0]
                offset += 2
                text_bytes = data[offset:offset+text_len]
                value = text_bytes.decode('utf-8')
                offset += text_len

            values.append(value)

        return Tuple(values, schema)


class Page:
    """8KB page with header and tuple storage"""

    def __init__(self, page_number: int):
        self.page_number = page_number
        self.tuples: List[TupleType[int, bytes]] = []  # (offset, tuple_data)
        self.free_space = PAGE_SIZE - PAGE_HEADER_SIZE
        self.dead_tuple_count = 0

    def can_fit(self, tuple_size: int) -> bool:
        """Check if tuple can fit in remaining space"""
        return self.free_space >= tuple_size

    def add_tuple(self, tuple_data: bytes) -> int:
        """Add tuple to page, return offset within page"""
        if not self.can_fit(len(tuple_data)):
            raise ValueError(f"Tuple ({len(tuple_data)} bytes) doesn't fit in page ({self.free_space} bytes free)")

        # Calculate offset from start of page (after header)
        offset = PAGE_HEADER_SIZE + (PAGE_SIZE - PAGE_HEADER_SIZE - self.free_space)

        self.tuples.append((offset, tuple_data))
        self.free_space -= len(tuple_data)

        return offset

    def get_tuple(self, offset: int) -> Optional[bytes]:
        """Get tuple at specific offset"""
        for tup_offset, tup_data in self.tuples:
            if tup_offset == offset:
                # Check if deleted (first byte is 0xFF for tombstone)
                if len(tup_data) > 0 and tup_data[0] == 0xFF:
                    return None  # Tuple is deleted
                return tup_data
        return None

    def mark_deleted(self, offset: int):
        """Mark tuple as deleted (tombstone)"""
        for i, (tup_offset, tup_data) in enumerate(self.tuples):
            if tup_offset == offset:
                # Mark as deleted by setting first byte to 0xFF
                self.tuples[i] = (tup_offset, b'\xFF' + tup_data[1:])
                self.dead_tuple_count += 1
                return
        raise ValueError(f"No tuple found at offset {offset}")

    def serialize(self) -> bytes:
        """
        Serialize page to 8KB bytes

        Format:
        [Page Header: 16 bytes]
        - Free space: 2 bytes
        - Tuple count: 2 bytes
        - Dead tuple count: 2 bytes
        - Reserved: 10 bytes

        [Tuple data: rest of page]
        """
        # Header
        header = struct.pack('HHH', self.free_space, len(self.tuples), self.dead_tuple_count)
        header += b'\x00' * 10  # Reserved space

        # Tuple data
        data = header
        for offset, tuple_data in self.tuples:
            data += tuple_data

        # Pad to PAGE_SIZE
        padding = PAGE_SIZE - len(data)
        data += b'\x00' * padding

        return data

    @staticmethod
    def deserialize(data: bytes, page_number: int) -> 'Page':
        """Deserialize page from bytes"""
        page = Page(page_number)

        # Read header
        free_space, tuple_count, dead_tuple_count = struct.unpack('HHH', data[0:6])
        page.free_space = free_space
        page.dead_tuple_count = dead_tuple_count

        # Read tuples
        offset = PAGE_HEADER_SIZE
        for _ in range(tuple_count):
            # Find tuple boundaries (this is simplified - in reality we'd store tuple lengths)
            # For now, we reconstruct from free space
            pass

        # TODO: Better tuple boundary detection
        # For now, this is a simplified version
        return page

    def write_to_disk(self, file_path: str):
        """Write page to disk at correct offset"""
        with open(file_path, 'r+b') as f:
            offset = HEAP_FILE_HEADER_SIZE + (self.page_number * PAGE_SIZE)
            f.seek(offset)
            f.write(self.serialize())


class HeapFile:
    """Manages table data file with Free Space Map"""

    def __init__(self, file_path: str, schema: TableSchema, buffer_pool: BufferPool):
        self.file_path = file_path
        self.schema = schema
        self.buffer_pool = buffer_pool
        self.page_count = 0
        self.free_space_map: Dict[int, int] = {}  # page_num -> free_space_bytes

    def create(self):
        """Initialize new heap file"""
        with open(self.file_path, 'wb') as f:
            # Write file header
            header = b'HEAP'  # Magic
            header += struct.pack('Q', 0)  # Page count
            header += b'\x00' * (HEAP_FILE_HEADER_SIZE - len(header))
            f.write(header)

        self.page_count = 0
        self.free_space_map = {}

    def open(self):
        """Open existing heap file"""
        if not os.path.exists(self.file_path):
            raise FileNotFoundError(f"Heap file not found: {self.file_path}")

        with open(self.file_path, 'rb') as f:
            # Read header
            magic = f.read(4)
            if magic != b'HEAP':
                raise ValueError(f"Invalid heap file: {self.file_path}")

            self.page_count = struct.unpack('Q', f.read(8))[0]

        # Rebuild FSM by scanning pages (in real DB, FSM would be persisted)
        self._rebuild_fsm()

    def _rebuild_fsm(self):
        """Rebuild free space map by scanning all pages"""
        self.free_space_map = {}
        for page_num in range(self.page_count):
            page = self._read_page_direct(page_num)
            self.free_space_map[page_num] = page.free_space

    def insert_tuple(self, tuple: Tuple) -> TupleType[int, int]:
        """
        Insert tuple into heap file using FSM
        Returns: ctid (page_number, offset)
        """
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

        return (page_num, offset)

    def read_tuple(self, ctid: TupleType[int, int]) -> Optional[Tuple]:
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

    def delete_tuple(self, ctid: TupleType[int, int]):
        """Mark tuple as deleted (tombstone)"""
        page_num, offset = ctid

        # Load page
        page = self._read_page(page_num)

        # Mark as deleted
        page.mark_deleted(offset)

        # Mark page as dirty
        self.buffer_pool.mark_dirty(self.file_path, page_num)

    def _find_page_with_space(self, required_space: int) -> Optional[int]:
        """Find page with enough free space using FSM"""
        for page_num, free_space in self.free_space_map.items():
            if free_space >= required_space:
                return page_num
        return None

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

    def _read_page(self, page_num: int) -> Page:
        """Read page through buffer pool"""
        return self.buffer_pool.get_page(self.file_path, page_num, self._read_page_direct)

    def _read_page_direct(self, file_path: str, page_num: int) -> Page:
        """Read page directly from disk (used by buffer pool)"""
        with open(file_path, 'rb') as f:
            offset = HEAP_FILE_HEADER_SIZE + (page_num * PAGE_SIZE)
            f.seek(offset)
            data = f.read(PAGE_SIZE)

        return Page.deserialize(data, page_num)

    def scan_all(self):
        """Sequential scan - iterate all non-deleted tuples"""
        for page_num in range(self.page_count):
            page = self._read_page(page_num)

            for offset, tuple_data in page.tuples:
                # Skip deleted tuples
                if len(tuple_data) > 0 and tuple_data[0] == 0xFF:
                    continue

                tuple_obj = Tuple.deserialize(tuple_data, self.schema)
                ctid = (page_num, offset)
                yield (tuple_obj, ctid)

    def vacuum(self):
        """Reclaim space from deleted tuples"""
        for page_num in range(self.page_count):
            page = self._read_page(page_num)

            if page.dead_tuple_count == 0:
                continue  # No dead tuples, skip

            # Rebuild page without dead tuples
            new_page = Page(page_num)

            for offset, tuple_data in page.tuples:
                # Skip deleted tuples
                if len(tuple_data) > 0 and tuple_data[0] == 0xFF:
                    continue

                # Add live tuple to new page
                new_page.add_tuple(tuple_data)

            # Replace old page with compacted page
            # Update cache and mark dirty
            self.buffer_pool.cache[(self.file_path, page_num)] = new_page
            self.buffer_pool.mark_dirty(self.file_path, page_num)

            # Update FSM
            self.free_space_map[page_num] = new_page.free_space

        # Flush dirty pages to disk
        self.buffer_pool.flush_all()
