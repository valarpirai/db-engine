"""
Transaction command handlers.

This module provides:
- TransactionMixin: Handlers for BEGIN, COMMIT, ROLLBACK
"""

import os
import shutil

from ..storage import BufferPool
from ..parser import BeginCommand, CommitCommand, RollbackCommand


class TransactionMixin:
    """Mixin class for transaction commands"""

    def execute_begin(self, cmd: BeginCommand) -> str:
        """Execute BEGIN TRANSACTION"""
        if self.in_transaction:
            raise ValueError("Already in a transaction. Use COMMIT or ROLLBACK first.")

        # Flush buffer pool to disk to create a clean snapshot for rollback
        self.buffer_pool.flush_all()

        self.in_transaction = True
        self.transaction_operations = []
        self.transaction_index_backups = {}

        # Back up all index files for rollback support
        # (B-tree writes directly to disk, so we need file-level backups)
        for idx_name, idx_meta in self.catalog.indexes.items():
            idx_file = os.path.join(self.data_dir, idx_meta.index_file)
            if os.path.exists(idx_file):
                backup_file = idx_file + '.txn_backup'
                shutil.copy2(idx_file, backup_file)
                self.transaction_index_backups[idx_file] = backup_file

        return "Transaction started"

    def execute_commit(self, cmd: CommitCommand) -> str:
        """Execute COMMIT"""
        if not self.in_transaction:
            raise ValueError("No active transaction to commit")

        # Flush all changes to disk
        self.buffer_pool.flush_all()
        self.catalog.save()

        # Remove index backup files (transaction successful)
        for idx_file, backup_file in self.transaction_index_backups.items():
            if os.path.exists(backup_file):
                os.remove(backup_file)

        # Clear transaction state
        self.in_transaction = False
        self.transaction_operations = []
        self.transaction_index_backups = {}

        return "Transaction committed"

    def execute_rollback(self, cmd: RollbackCommand) -> str:
        """Execute ROLLBACK"""
        if not self.in_transaction:
            raise ValueError("No active transaction to rollback")

        # Clear buffer pool (discard dirty pages)
        self.buffer_pool = BufferPool()

        # Restore index files from backups (B-tree writes directly to disk)
        for idx_file, backup_file in self.transaction_index_backups.items():
            if os.path.exists(backup_file):
                shutil.copy2(backup_file, idx_file)
                os.remove(backup_file)

        # Reload catalog from disk
        self.catalog.load()

        # Reopen all heap files and indexes
        self.heap_files = {}
        self.indexes = {}

        # Clear transaction state
        self.in_transaction = False
        self.transaction_operations = []
        self.transaction_index_backups = {}

        return "Transaction rolled back"
