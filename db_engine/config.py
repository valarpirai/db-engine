"""
Configuration file for the database engine.
Contains all tunable parameters for storage, indexing, and system behavior.
"""

# ============================================================================
# Storage Configuration
# ============================================================================

# Page size for heap files (matches PostgreSQL standard)
PAGE_SIZE = 8192  # 8KB per page

# Default data directory for all database files
DATA_DIR = './data'

# Heap file header size (metadata at start of each .dat file)
HEAP_FILE_HEADER_SIZE = 32  # bytes

# Page header size within each page
PAGE_HEADER_SIZE = 16  # bytes (free_space, item_count, flags)

# ============================================================================
# Buffer Pool Configuration
# ============================================================================

# Buffer pool size (number of pages to cache in memory)
BUFFER_POOL_SIZE = 128  # 128 pages = 1MB cache (128 * 8KB)

# Buffer pool eviction policy
BUFFER_POOL_POLICY = 'LRU'  # Least Recently Used

# ============================================================================
# B-tree Index Configuration
# ============================================================================

# B-tree order (max children per internal node)
BTREE_ORDER = 4  # ORDER-1 = max keys per node

# Fixed size for each B-tree node (increased to handle variable-length keys)
NODE_SIZE = 4096  # bytes (was 512, increased to support TEXT keys)

# Index key truncation for TEXT columns
INDEX_TEXT_MAX_LENGTH = 10  # Only first 10 chars of TEXT used in indexes

# Index file header size (metadata at start of each .idx file)
INDEX_FILE_HEADER_SIZE = 32  # bytes (magic, root_offset, node_count)

# Magic number for index files (validation)
INDEX_MAGIC = b'BTIX'

# ============================================================================
# Data Type Configuration
# ============================================================================

# Fixed-size data types
INT_SIZE = 4        # 4 bytes (32-bit signed integer)
BIGINT_SIZE = 8     # 8 bytes (64-bit signed integer)
FLOAT_SIZE = 8      # 8 bytes (double precision)
BOOL_SIZE = 1       # 1 byte
TIMESTAMP_SIZE = 8  # 8 bytes (Unix timestamp as 64-bit integer)

# Variable-size data types
MAX_TEXT_SIZE = 10240  # Maximum string length (10KB)

# Supported data types
SUPPORTED_TYPES = ['INT', 'BIGINT', 'FLOAT', 'TEXT', 'BOOLEAN', 'TIMESTAMP']

# NULL support
NULL_BITMAP_ENABLED = True  # Use null bitmap in tuple serialization (only if nullable columns exist)

# Tuple size limits
MAX_TUPLE_SIZE = 65535  # Maximum tuple size in bytes (64KB - 1)

# ============================================================================
# Catalog Configuration
# ============================================================================

# System catalog file name
CATALOG_FILE = 'catalog.dat'

# Magic number for catalog file
CATALOG_MAGIC = b'CTLG'

# ============================================================================
# Statistics Configuration
# ============================================================================

# Auto-update statistics frequency (number of INSERT/DELETE operations)
STATS_AUTO_UPDATE_THRESHOLD = 1000  # Update stats every 1000 modifications

# Statistics tracked per table
STATS_TRACK_ROW_COUNT = True
STATS_TRACK_PAGE_COUNT = True
STATS_TRACK_DISTINCT_VALUES = True  # For indexed columns

# ============================================================================
# Timestamp Configuration
# ============================================================================

# All timestamps stored as UTC (Unix epoch seconds)
TIMESTAMP_TIMEZONE = 'UTC'  # All timestamps are in UTC

# ============================================================================
# Concurrency Configuration
# ============================================================================

# Lock file for single-writer enforcement
LOCK_FILE = '.lock'

# Lock acquisition timeout (seconds)
LOCK_TIMEOUT = 30

# Support concurrent reads (multiple readers, single writer)
CONCURRENT_READS_ENABLED = True

# ============================================================================
# Transaction Configuration
# ============================================================================

# Transaction states
TXN_STATE_ACTIVE = 'ACTIVE'
TXN_STATE_COMMITTED = 'COMMITTED'
TXN_STATE_ABORTED = 'ABORTED'

# ============================================================================
# File Naming Conventions
# ============================================================================

# Heap file extension
HEAP_FILE_EXT = '.dat'

# Index file extension
INDEX_FILE_EXT = '.idx'

# Primary key index suffix
PKEY_SUFFIX = '_pkey'

# ============================================================================
# System Limits
# ============================================================================

# Maximum number of columns per table
MAX_COLUMNS = 32

# Maximum number of indexes per table
MAX_INDEXES = 16

# Maximum table name length
MAX_TABLE_NAME_LEN = 64

# Maximum column name length
MAX_COLUMN_NAME_LEN = 64

# Maximum index name length
MAX_INDEX_NAME_LEN = 64

# ============================================================================
# Vacuum/Garbage Collection Configuration
# ============================================================================

# Auto-vacuum threshold (percentage of dead tuples)
AUTO_VACUUM_THRESHOLD = 20  # Vacuum when 20% of tuples are dead

# Vacuum reclaims space from deleted tuples
VACUUM_ENABLED = True

# ============================================================================
# Parser Configuration
# ============================================================================

# Parser error messages include line and column numbers
PARSER_DETAILED_ERRORS = True

# Maximum SQL statement length
MAX_SQL_LENGTH = 65536  # 64KB

# ============================================================================
# Debug and Logging
# ============================================================================

# Enable debug mode
DEBUG = False

# Log file location
LOG_FILE = 'db_engine.log'

# Verbose output
VERBOSE = False
