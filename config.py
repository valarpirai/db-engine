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
# B-tree Index Configuration
# ============================================================================

# B-tree order (max children per internal node)
BTREE_ORDER = 4  # ORDER-1 = max keys per node

# Fixed size for each B-tree node
NODE_SIZE = 512  # bytes

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
MAX_TEXT_SIZE = 255  # Maximum string length (for simplicity)

# Supported data types
SUPPORTED_TYPES = ['INT', 'BIGINT', 'FLOAT', 'TEXT', 'BOOLEAN', 'TIMESTAMP']

# NULL support
NULL_BITMAP_ENABLED = True  # Use null bitmap in tuple serialization

# ============================================================================
# Catalog Configuration
# ============================================================================

# System catalog file name
CATALOG_FILE = 'catalog.dat'

# Magic number for catalog file
CATALOG_MAGIC = b'CTLG'

# ============================================================================
# Concurrency Configuration
# ============================================================================

# Lock file for single-writer enforcement
LOCK_FILE = '.lock'

# Lock acquisition timeout (seconds)
LOCK_TIMEOUT = 30

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
# Debug and Logging
# ============================================================================

# Enable debug mode
DEBUG = False

# Log file location
LOG_FILE = 'db_engine.log'

# Verbose output
VERBOSE = False
