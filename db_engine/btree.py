"""
B-tree Index - Composite key support with variable-length key handling

This module provides:
- BTreeNode: Fixed-size 4KB nodes with TEXT key truncation
- BTreeIndex: B-tree operations (insert, search, range query, delete)
- Uniqueness enforcement for PRIMARY KEY and UNIQUE indexes
- Composite key support (multi-column indexes)
"""

from typing import List, Tuple as TupleType, Optional, Any, Union
import struct
import os

from .config import (
    BTREE_ORDER, NODE_SIZE, INDEX_TEXT_MAX_LENGTH
)


class BTreeNode:
    """B-tree node with fixed-size serialization (4096 bytes)"""

    def __init__(self, is_leaf: bool = True):
        self.is_leaf = is_leaf
        self.keys: List[Any] = []  # Key values (int, str, or tuple for composite)
        self.values: List[Any] = []  # Leaf: ctids (page, offset); Internal: child file offsets
        self.next_leaf: int = -1  # For leaf nodes, link to next leaf (for range queries)
        self.file_offset: int = -1  # Position in index file

    def is_full(self) -> bool:
        """Check if node has reached maximum keys"""
        return len(self.keys) >= BTREE_ORDER - 1

    def is_underflow(self) -> bool:
        """Check if node has too few keys (less than half full)"""
        min_keys = (BTREE_ORDER - 1) // 2
        return len(self.keys) < min_keys

    @staticmethod
    def truncate_key(key: Any) -> Any:
        """
        Truncate TEXT keys to INDEX_TEXT_MAX_LENGTH characters
        Handles single keys and composite keys (tuples)
        """
        if isinstance(key, tuple):
            # Composite key - truncate each component
            return tuple(
                BTreeNode.truncate_key(k) for k in key
            )
        elif isinstance(key, str):
            # TEXT key - truncate to max length
            return key[:INDEX_TEXT_MAX_LENGTH]
        else:
            # INT, BIGINT, FLOAT, etc. - no truncation needed
            return key

    @staticmethod
    def compare_keys(key1: Any, key2: Any) -> int:
        """
        Compare two keys (handles composite keys)
        Returns: -1 if key1 < key2, 0 if equal, 1 if key1 > key2
        """
        # Truncate both keys before comparison
        k1 = BTreeNode.truncate_key(key1)
        k2 = BTreeNode.truncate_key(key2)

        if k1 < k2:
            return -1
        elif k1 > k2:
            return 1
        else:
            return 0

    def serialize(self) -> bytes:
        """
        Serialize node to fixed NODE_SIZE bytes (4096)

        Format:
        [is_leaf: 1 byte]
        [num_keys: 4 bytes]
        [next_leaf: 8 bytes] (only for leaf nodes, -1 for internal)
        [keys: variable, pickled for simplicity]
        [values: variable, pickled]
        [padding to NODE_SIZE]

        Note: We use pickle for key/value serialization to handle
        composite keys and variable types easily. In production, you'd
        use a more efficient binary format.
        """
        import pickle

        # Header
        data = struct.pack('?', self.is_leaf)  # 1 byte
        data += struct.pack('I', len(self.keys))  # 4 bytes
        data += struct.pack('q', self.next_leaf)  # 8 bytes

        # Serialize keys and values using pickle
        keys_data = pickle.dumps(self.keys)
        values_data = pickle.dumps(self.values)

        # Lengths
        data += struct.pack('I', len(keys_data))  # 4 bytes
        data += struct.pack('I', len(values_data))  # 4 bytes

        # Data
        data += keys_data
        data += values_data

        # Pad to NODE_SIZE
        if len(data) > NODE_SIZE:
            raise ValueError(f"Node data ({len(data)} bytes) exceeds NODE_SIZE ({NODE_SIZE} bytes)")

        padding = NODE_SIZE - len(data)
        data += b'\x00' * padding

        return data

    @staticmethod
    def deserialize(data: bytes, file_offset: int = -1) -> 'BTreeNode':
        """Deserialize node from bytes"""
        import pickle

        offset = 0

        # Header
        is_leaf = struct.unpack('?', data[offset:offset+1])[0]
        offset += 1

        num_keys = struct.unpack('I', data[offset:offset+4])[0]
        offset += 4

        next_leaf = struct.unpack('q', data[offset:offset+8])[0]
        offset += 8

        # Lengths
        keys_len = struct.unpack('I', data[offset:offset+4])[0]
        offset += 4

        values_len = struct.unpack('I', data[offset:offset+4])[0]
        offset += 4

        # Deserialize keys and values
        keys_data = data[offset:offset+keys_len]
        offset += keys_len

        values_data = data[offset:offset+values_len]
        offset += values_len

        keys = pickle.loads(keys_data)
        values = pickle.loads(values_data)

        # Build node
        node = BTreeNode(is_leaf=is_leaf)
        node.keys = keys
        node.values = values
        node.next_leaf = next_leaf
        node.file_offset = file_offset

        return node


class BTreeIndex:
    """B-tree index manager with composite key support"""

    def __init__(self, index_file: str, key_columns: List[str], unique: bool = False):
        self.index_file = index_file
        self.key_columns = key_columns  # List of column names for composite keys
        self.unique = unique
        self.root: Optional[BTreeNode] = None
        self.node_count = 0

    def create(self):
        """Initialize new index file"""
        # Create empty root node (leaf)
        self.root = BTreeNode(is_leaf=True)
        self.root.file_offset = self._allocate_offset()
        self.node_count = 1

        # Write file header
        with open(self.index_file, 'wb') as f:
            # Magic: b'BTIX'
            f.write(b'BTIX')
            # Root offset: 8 bytes
            f.write(struct.pack('q', self.root.file_offset))
            # Node count: 8 bytes
            f.write(struct.pack('q', self.node_count))
            # Unique flag: 1 byte
            f.write(struct.pack('?', self.unique))
            # Key column count: 4 bytes
            f.write(struct.pack('I', len(self.key_columns)))
            # Reserved: pad to 64 bytes
            f.write(b'\x00' * (64 - f.tell()))

            # Write root node
            f.write(self.root.serialize())

    def open(self):
        """Open existing index file"""
        if not os.path.exists(self.index_file):
            raise FileNotFoundError(f"Index file not found: {self.index_file}")

        with open(self.index_file, 'rb') as f:
            # Read header
            magic = f.read(4)
            if magic != b'BTIX':
                raise ValueError(f"Invalid index file: {self.index_file}")

            root_offset = struct.unpack('q', f.read(8))[0]
            self.node_count = struct.unpack('q', f.read(8))[0]
            self.unique = struct.unpack('?', f.read(1))[0]
            key_column_count = struct.unpack('I', f.read(4))[0]

            # Load root node
            self.root = self._read_node(root_offset)

    def _allocate_offset(self) -> int:
        """Allocate file offset for new node"""
        # Header is 64 bytes, each node is NODE_SIZE bytes
        return 64 + (self.node_count * NODE_SIZE)

    def _read_node(self, offset: int) -> BTreeNode:
        """Read node from file at given offset"""
        with open(self.index_file, 'rb') as f:
            f.seek(offset)
            data = f.read(NODE_SIZE)
            return BTreeNode.deserialize(data, file_offset=offset)

    def _write_node(self, node: BTreeNode):
        """Write node to file at its offset"""
        with open(self.index_file, 'r+b') as f:
            f.seek(node.file_offset)
            f.write(node.serialize())

    def _update_header(self):
        """Update file header (root offset and node count)"""
        with open(self.index_file, 'r+b') as f:
            f.seek(4)  # Skip magic
            f.write(struct.pack('q', self.root.file_offset))
            f.write(struct.pack('q', self.node_count))

    def search(self, key: Any) -> Optional[TupleType[int, int]]:
        """
        Search for exact key match
        Returns: ctid (page_number, offset) or None if not found
        """
        if self.root is None:
            return None

        key = BTreeNode.truncate_key(key)
        return self._search_node(self.root, key)

    def _search_node(self, node: BTreeNode, key: Any) -> Optional[TupleType[int, int]]:
        """Recursive search in node"""
        # Find position where key would be
        i = 0
        while i < len(node.keys):
            cmp = BTreeNode.compare_keys(key, node.keys[i])
            if cmp == 0:
                # Found exact match
                if node.is_leaf:
                    return node.values[i]  # Return ctid
                else:
                    # In internal node, search right subtree
                    child_offset = node.values[i + 1]
                    child = self._read_node(child_offset)
                    return self._search_node(child, key)
            elif cmp < 0:
                break
            i += 1

        # Key not found at this level
        if node.is_leaf:
            return None  # Not found
        else:
            # Search in appropriate child
            child_offset = node.values[i]
            child = self._read_node(child_offset)
            return self._search_node(child, key)

    def insert(self, key: Any, ctid: TupleType[int, int]):
        """
        Insert (key → ctid) mapping
        - If unique and key exists: raise ValueError
        - Navigate to correct leaf
        - Insert into leaf
        - Split if necessary (propagate up)
        """
        key = BTreeNode.truncate_key(key)

        # Check uniqueness constraint
        if self.unique:
            existing = self.search(key)
            if existing is not None:
                raise ValueError(f"Duplicate key violation: {key} already exists in unique index")

        # Insert into tree
        if self.root.is_full():
            # Root is full - create new root and split old root
            old_root = self.root
            new_root = BTreeNode(is_leaf=False)
            new_root.file_offset = self._allocate_offset()
            self.node_count += 1
            new_root.values.append(old_root.file_offset)

            self.root = new_root
            self._split_child(new_root, 0, old_root)
            self._update_header()

        self._insert_non_full(self.root, key, ctid)

    def _insert_non_full(self, node: BTreeNode, key: Any, ctid: TupleType[int, int]):
        """Insert into node that has space"""
        i = len(node.keys) - 1

        if node.is_leaf:
            # Insert into leaf node
            node.keys.append(None)
            node.values.append(None)

            # Shift keys/values to make room
            while i >= 0 and BTreeNode.compare_keys(key, node.keys[i]) < 0:
                node.keys[i + 1] = node.keys[i]
                node.values[i + 1] = node.values[i]
                i -= 1

            # Insert new key/value
            node.keys[i + 1] = key
            node.values[i + 1] = ctid
            self._write_node(node)
        else:
            # Find child to insert into
            while i >= 0 and BTreeNode.compare_keys(key, node.keys[i]) < 0:
                i -= 1
            i += 1

            # Load child
            child_offset = node.values[i]
            child = self._read_node(child_offset)

            # Split child if full
            if child.is_full():
                self._split_child(node, i, child)
                # After split, determine which child to insert into
                if BTreeNode.compare_keys(key, node.keys[i]) > 0:
                    i += 1
                    child_offset = node.values[i]
                    child = self._read_node(child_offset)

            # Recursively insert into child
            self._insert_non_full(child, key, ctid)

    def _split_child(self, parent: BTreeNode, child_index: int, child: BTreeNode):
        """
        Split full child node
        - Create new node (right sibling)
        - Move half the keys to new node
        - Insert median into parent
        - Update parent's children pointers
        - Update next_leaf for leaves
        """
        # Create new node (right sibling)
        new_node = BTreeNode(is_leaf=child.is_leaf)
        new_node.file_offset = self._allocate_offset()
        self.node_count += 1

        # Calculate midpoint
        mid = len(child.keys) // 2

        # Save median key before modifying arrays
        median_key = child.keys[mid]

        # Split differently for leaf vs internal nodes
        if child.is_leaf:
            # Leaf nodes: median key stays in right child
            # Left child: keys[0:mid], values[0:mid]
            # Right child: keys[mid:], values[mid:]
            # Promoted key: keys[mid]
            new_node.keys = child.keys[mid:]
            new_node.values = child.values[mid:]
            child.keys = child.keys[:mid]
            child.values = child.values[:mid]

            # Update leaf links
            new_node.next_leaf = child.next_leaf
            child.next_leaf = new_node.file_offset
        else:
            # Internal nodes: median key goes to parent only
            # Left child: keys[0:mid], values[0:mid+1]
            # Right child: keys[mid+1:], values[mid+1:]
            # Promoted key: keys[mid]
            new_node.keys = child.keys[mid + 1:]
            new_node.values = child.values[mid + 1:]
            child.keys = child.keys[:mid]
            child.values = child.values[:mid + 1]

        # Insert median into parent
        parent.keys.insert(child_index, median_key)
        parent.values.insert(child_index + 1, new_node.file_offset)

        # Write all modified nodes
        self._write_node(child)
        self._write_node(new_node)
        self._write_node(parent)

    def range_query(self, start_key: Any, end_key: Any) -> List[TupleType[int, int]]:
        """
        Range scan: start_key <= key <= end_key
        - Find first key >= start_key
        - Follow next_leaf pointers
        - Collect ctids until key > end_key
        """
        if self.root is None:
            return []

        start_key = BTreeNode.truncate_key(start_key)
        end_key = BTreeNode.truncate_key(end_key)

        # Find starting leaf node
        leaf = self._find_leaf_for_key(self.root, start_key)

        results = []

        # Traverse leaf nodes using next_leaf pointers
        while leaf is not None:
            for i, key in enumerate(leaf.keys):
                cmp_start = BTreeNode.compare_keys(key, start_key)
                cmp_end = BTreeNode.compare_keys(key, end_key)

                if cmp_start >= 0 and cmp_end <= 0:
                    # Key is in range
                    results.append(leaf.values[i])
                elif cmp_end > 0:
                    # Past end of range
                    return results

            # Move to next leaf
            if leaf.next_leaf != -1:
                leaf = self._read_node(leaf.next_leaf)
            else:
                break

        return results

    def _find_leaf_for_key(self, node: BTreeNode, key: Any) -> BTreeNode:
        """Find leaf node where key would be located"""
        if node.is_leaf:
            return node

        # Find child to descend into
        i = 0
        while i < len(node.keys) and BTreeNode.compare_keys(key, node.keys[i]) >= 0:
            i += 1

        child_offset = node.values[i]
        child = self._read_node(child_offset)
        return self._find_leaf_for_key(child, key)

    def delete(self, key: Any):
        """
        Delete key from index
        Simplified version: removes key from leaf, no rebalancing
        (Full rebalancing with borrow/merge can be added in Phase 2)
        """
        key = BTreeNode.truncate_key(key)

        # Find leaf containing key
        leaf = self._find_leaf_for_key(self.root, key)

        # Find key in leaf
        for i, leaf_key in enumerate(leaf.keys):
            if BTreeNode.compare_keys(leaf_key, key) == 0:
                # Found key - remove it
                del leaf.keys[i]
                del leaf.values[i]
                self._write_node(leaf)
                return

        # Key not found - that's okay, deletion is idempotent
        pass
