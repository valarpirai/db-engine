"""
Instrumentation module for tracking query execution metrics.

Provides detailed timing, storage access, and performance statistics
for EXPLAIN VERBOSE output.
"""

import time
from typing import List, Tuple, Dict, Any, Optional
from dataclasses import dataclass, field


class Timer:
    """Context manager for timing operations in milliseconds"""

    def __enter__(self):
        self.start = time.perf_counter()
        return self

    def __exit__(self, *args):
        self.elapsed = (time.perf_counter() - self.start) * 1000  # Convert to ms


@dataclass
class BTreeNodeVisit:
    """Record of a B-tree node visit during traversal"""
    offset: int  # File offset of the node
    is_leaf: bool
    keys: List[Any]
    comparison_result: Optional[str] = None  # e.g., "1 < 2 → left"
    found: bool = False


@dataclass
class PageAccess:
    """Record of a heap page access"""
    page_num: int
    cache_hit: bool  # True if page was in buffer pool
    free_space: int
    live_tuples: int
    dead_tuples: int


@dataclass
class TupleDetails:
    """Detailed information about a tuple"""
    ctid: Tuple[int, int]
    offset_in_page: int
    header_size: int
    null_bitmap_size: int
    data_size: int
    total_size: int
    column_sizes: Dict[str, int]  # column_name → bytes


@dataclass
class ExecutionMetrics:
    """Comprehensive metrics for query execution"""

    # Timing breakdown (in milliseconds)
    parse_time: float = 0.0
    plan_time: float = 0.0
    index_lookup_time: float = 0.0
    heap_access_time: float = 0.0
    filter_time: float = 0.0
    sort_time: float = 0.0
    projection_time: float = 0.0

    # B-tree traversal details
    btree_nodes_visited: List[BTreeNodeVisit] = field(default_factory=list)
    btree_comparisons: int = 0
    btree_depth: int = 0

    # Storage access details
    pages_accessed: List[PageAccess] = field(default_factory=list)
    tuples_fetched: List[TupleDetails] = field(default_factory=list)

    # Row statistics
    rows_scanned: int = 0
    rows_filtered: int = 0
    rows_returned: int = 0
    estimated_rows: int = 0

    # Memory and buffer statistics
    memory_used: int = 0
    buffer_pool_hits: int = 0
    buffer_pool_misses: int = 0

    # Query planning details
    scan_method: str = ""  # "INDEX" or "SEQUENTIAL"
    index_used: Optional[str] = None
    index_scan_cost: float = 0.0
    sequential_scan_cost: float = 0.0

    def total_time(self) -> float:
        """Calculate total execution time"""
        return (self.parse_time + self.plan_time + self.index_lookup_time +
                self.heap_access_time + self.filter_time + self.sort_time +
                self.projection_time)
