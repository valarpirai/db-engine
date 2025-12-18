# Missing Features Analysis

**Analysis Date**: 2025-12-18
**Database Version**: Phase 2 Implementation (94/97 tests passing)

This document identifies gaps between the CLAUDE.md specification and the actual implementation.

---

## Critical: Features Documented But Not Working

### 1. BETWEEN Operator 🔥 SILENTLY BROKEN - DATA CORRECTNESS ISSUE
**Documented in CLAUDE.md**:
```sql
SELECT name, email FROM users WHERE age BETWEEN 20 AND 30;
```

**Status**: **CRITICAL BUG** - Parses without error but returns WRONG RESULTS

**Issue**:
- Tokenizer recognizes BETWEEN keyword
- Parser does NOT handle BETWEEN correctly
- WHERE clause becomes just `ColumnRef('age')` instead of a range expression
- Result: Returns ALL rows instead of filtering by range

**Test Case**:
```sql
-- Data: rows with age 25, 30, 35
SELECT * FROM test WHERE age BETWEEN 28 AND 32;
-- Expected: 1 row (age=30)
-- Actual: 3 rows (25, 30, 35) ❌ WRONG!
```

**Root Cause**: Parser treats BETWEEN as end of expression, not as an operator.

**Impact**: **SEVERE** - Users get incorrect query results without any error message. Silent data corruption in results.

**Fix Required**: Implement proper BETWEEN parsing in `_parse_comparison()` method.

---

### 2. IS NULL / IS NOT NULL 🔥 SILENTLY BROKEN - DATA CORRECTNESS ISSUE
**Documented in CLAUDE.md**:
```sql
SELECT * FROM users WHERE email IS NOT NULL;
```

**Status**: **CRITICAL BUG** - Parses without error but returns WRONG RESULTS

**Issue**:
- Tokenizer recognizes IS, NULL keywords
- Parser does NOT handle IS NULL correctly
- WHERE clause becomes just `ColumnRef('email')` instead of NULL check expression
- Result: Returns ALL non-NULL rows instead of filtering correctly

**Test Case**:
```sql
-- Data: 3 rows, row 2 has NULL name
SELECT * FROM test WHERE name IS NULL;
-- Expected: 1 row (id=2 with NULL name)
-- Actual: 2 rows (id=1 and id=3 with non-NULL names) ❌ WRONG!
```

**Impact**: **SEVERE** - Users get completely inverted query results. NULL checks return non-NULL values!

**Fix Required**: Implement IS NULL / IS NOT NULL in `_parse_comparison()` method.

---

### 3. UPDATE with Arithmetic Expressions ❌ BROKEN
**Documented in CLAUDE.md**:
```sql
UPDATE users SET age = age + 1 WHERE age > 20;
```

**Status**: Documented as Phase 1 feature but **NOT IMPLEMENTED**

**Issue**: Parser only supports literals in UPDATE assignments, not expressions.
- Line 999 in parser.py: `value_expr = self._parse_primary()`
- `_parse_primary()` only handles literals and column references, not binary operations

**Impact**: Users cannot do arithmetic updates like incrementing values.

**Fix Required**: Parse full expressions in UPDATE SET clause with `_parse_expression()` instead of `_parse_primary()`.

---

## Phase 2 Features: Partially Implemented

### 2. More Data Types ❌ NOT IMPLEMENTED
**Documented in CLAUDE.md** (Line 489):
> Phase 2 Features (Added After Core Works):
> - More data types (DATE, TIME, JSON, etc.)

**Status**: Listed as Phase 2 feature but **NOT IMPLEMENTED**

**Currently Supported**:
- INT, BIGINT, FLOAT, TEXT, BOOLEAN, TIMESTAMP

**Missing**:
- DATE type
- TIME type
- JSON type
- No type for date/time without timestamp component

**Impact**: Users cannot store date-only or time-only data efficiently.

### 3. File Locking ❌ NOT IMPLEMENTED
**Documented in CLAUDE.md** (Line 189):
> - File locking for single-writer enforcement (fcntl on POSIX)

**Status**: Mentioned in Phase 2 transaction spec but **NOT IMPLEMENTED**

**Current Implementation**: No file locking at all
- Multiple processes could write simultaneously (data corruption risk)
- No fcntl, flock, or any locking mechanism

**Impact**: Concurrent writes can corrupt the database files.

**Fix Required**: Implement file locking in HeapFile and Catalog operations.

### 4. Transaction Log ❌ NOT IMPLEMENTED
**Documented in CLAUDE.md** (Line 190):
> - Transaction log for rollback support

**Status**: Mentioned in Phase 2 spec but **NOT IMPLEMENTED**

**Current Implementation**:
- `transaction_operations` list exists but is never populated
- ROLLBACK works by reloading entire database from disk
- No WAL (Write-Ahead Log)
- No undo log

**Impact**:
- ROLLBACK is inefficient (reloads everything)
- Cannot do partial rollbacks
- Lost work if crash happens during ROLLBACK

---

## Missing SQL Commands

### 5. DROP INDEX ❌ NOT IMPLEMENTED
**Status**: Basic operation missing

**Current**: Only DROP TABLE is supported
**Missing**: DROP INDEX command

**Workaround**: Users must manually delete .idx files

**Fix Required**:
- Add DROP INDEX parsing
- Implement execute_drop_index() method

### 6. TRUNCATE TABLE ❌ NOT IMPLEMENTED
**Status**: Efficiency feature missing

**Current**: Must use `DELETE FROM table;` (slow for large tables)
**Missing**: TRUNCATE TABLE (fast reset)

**Impact**: Slow to clear large tables

---

## Parser Limitations

### 7. Arithmetic Operators in Tokenizer ❌ INCOMPLETE
**Issue**: Plus (+), Minus (-), Multiply (*), Divide (/) not tokenized

**Current**: Only supports these operators in WHERE clauses via LIKE and comparison
**Missing**: Cannot parse arithmetic expressions anywhere

**Affected Operations**:
- UPDATE with expressions
- SELECT with computed columns (not supported anyway)
- WHERE with computed values

**Example Failures**:
```sql
UPDATE users SET age = age + 1;        -- ❌ Fails
UPDATE users SET score = score * 2;    -- ❌ Fails
```

---

## Known Issues with Implemented Features

### 8. ALTER TABLE Limitations ⚠️ PARTIAL
**Status**: Implemented but with significant limitations

**Issues**:
1. **Indexes are deleted** after ADD/DROP COLUMN
   - Users must manually recreate indexes with CREATE INDEX
   - Primary key index is preserved, but secondary indexes are removed

2. **3 failing tests** (test_phase2.py):
   - test_add_column: Index rebuilding fails with schema mismatch
   - test_drop_column: Similar index rebuilding issues
   - test_begin_rollback: Rollback with concurrent schema changes fails

**Impact**: ALTER TABLE works for simple cases but fails with complex schemas.

### 9. Transaction Isolation ⚠️ WEAK
**Status**: Implemented but very basic

**Issues**:
1. **No isolation levels** - essentially READ UNCOMMITTED
2. **No read/write locks** during transaction
3. **Dirty reads possible** - concurrent reads see uncommitted data
4. **No MVCC** - only single version of each row

**Example Problem**:
```sql
-- Session 1
BEGIN;
UPDATE users SET balance = 1000 WHERE id = 1;
-- Balance is 1000 in memory, not yet committed

-- Session 2 (different process)
SELECT balance FROM users WHERE id = 1;
-- Sees old value from disk, or new value if dirty page shared
```

---

## Documentation Issues

### 10. CLAUDE.md Inconsistencies
**Issues**:

1. **Line 10**: Claims "~4,200 lines" but actual count is lower
   ```bash
   wc -l db_engine/*.py  # Shows ~3,800 lines
   ```

2. **Line 103**: Shows example that doesn't work
   ```sql
   UPDATE users SET age = age + 1 WHERE age > 20;  -- BROKEN
   ```

3. **Line 493**: Claims "All Phases Complete!" but Phase 2 is only partially complete
   - File locking: Missing
   - Transaction log: Missing
   - More data types: Missing

---

## Summary

### 🔥 CRITICAL DATA CORRECTNESS BUGS:
1. **BETWEEN operator returns ALL rows** instead of filtering (silently wrong)
2. **IS NULL returns non-NULL rows** (completely inverted results)
3. **IS NOT NULL also broken** (returns wrong rows)

These bugs are **SEVERE** because:
- No error is raised (silent failure)
- Users get completely wrong results
- Documented as working in CLAUDE.md
- May be used in production code

### Critical Issues (Block Production Use):
1. 🔥 **BETWEEN silently broken** (wrong results, no error)
2. 🔥 **IS NULL/IS NOT NULL silently broken** (inverted results)
3. ❌ No file locking (concurrent write corruption risk)
4. ❌ UPDATE expressions not working despite documentation
5. ⚠️ Transaction isolation very weak

### Missing Phase 2 Features:
1. ❌ DATE, TIME, JSON datatypes
2. ❌ Transaction log/WAL
3. ❌ File locking for concurrency
4. ❌ DROP INDEX command

### Implementation Gaps:
1. ❌ Arithmetic operators in parser
2. ⚠️ ALTER TABLE index rebuilding (3 test failures)
3. ❌ TRUNCATE TABLE

### Documentation Fixes Needed:
1. Remove/comment out `UPDATE age = age + 1` example (doesn't work)
2. Update "All Phases Complete" status to be more accurate
3. Document known limitations more prominently
4. Update line counts

---

## Recommendations

### 🔥 IMMEDIATE (Critical Data Correctness Bugs):
1. **FIX BETWEEN operator** - Currently returns all rows, not filtered results
   - Remove from SQL examples in CLAUDE.md until fixed
   - Add warning in documentation
   - Implement proper BETWEEN parsing
2. **FIX IS NULL / IS NOT NULL** - Currently returns inverted/wrong results
   - Remove from SQL examples in CLAUDE.md until fixed
   - Add warning in documentation
   - Implement proper NULL check parsing
3. **Add regression tests** for these bugs to prevent reoccurrence

### High Priority (Security/Data Integrity):
1. **Implement file locking** - Critical for multi-process safety
2. **Fix or remove** broken UPDATE expression example from docs
3. **Document transaction isolation** level (currently READ UNCOMMITTED)

### Medium Priority (Functionality):
1. **Implement UPDATE expressions** - Match documented behavior
2. **Fix ALTER TABLE** index rebuilding (resolve 3 test failures)
3. **Add DROP INDEX** command

### Low Priority (Nice to Have):
1. Add DATE/TIME/JSON types
2. Implement proper transaction log
3. Add TRUNCATE TABLE
4. Improve transaction isolation

---

## Test Coverage Analysis

**Current**: 94/97 tests passing (97%)

**Failing Tests**:
1. `test_add_column` - Index rebuilding after schema change
2. `test_drop_column` - Index rebuilding after schema change
3. `test_begin_rollback` - Rollback with schema changes

**Missing Test Coverage**:
1. Concurrent write scenarios (file locking)
2. UPDATE with expressions
3. Transaction isolation levels
4. DROP INDEX operations
5. Complex schema migrations

---

## Conclusion

The database engine has **CRITICAL DATA CORRECTNESS BUGS** that must be fixed immediately:

🔥 **CRITICAL BUGS (Silent Data Corruption)**:
- **BETWEEN returns ALL rows** instead of range (e.g., "age BETWEEN 28 AND 32" returns ages 25, 30, 35)
- **IS NULL returns non-NULL values** (completely inverted logic)
- **IS NOT NULL also broken** (returns wrong subset)
- These bugs are **SILENT** - no error raised, just wrong results
- **Documented as working** in CLAUDE.md, users will trust them

**Impact**: Any query using BETWEEN or IS NULL will return incorrect results without warning.

---

✅ **Working Well**:
- Core storage layer (pages, tuples, buffer pool)
- B-tree indexing
- Basic SQL (SELECT with =, <, >, INSERT, DELETE)
- ALTER TABLE RENAME COLUMN
- Simple transactions (BEGIN/COMMIT)
- REPL interface
- LIKE operator
- ORDER BY, LIMIT, OFFSET

❌ **Critical Issues**:
1. 🔥 **BETWEEN operator** (wrong results)
2. 🔥 **IS NULL/IS NOT NULL** (inverted results)
3. ❌ **File locking missing** (concurrent write corruption risk)
4. ❌ **UPDATE expressions** (documented but broken)
5. ⚠️ **ALTER TABLE** (3 test failures with indexes)

⚠️ **Incomplete Phase 2**:
- Only 2/5 Phase 2 features fully implemented
- Transactions work but are very basic (no isolation, no WAL)
- Missing planned datatypes (DATE, TIME, JSON)
- No file locking as specified

**Overall Assessment**:
- Phase 1: ⚠️ **Major bugs in WHERE clause parsing** (BETWEEN, IS NULL)
- Phase 2: ⚠️ 40% Complete (ALTER TABLE + basic transactions only)
- Documented features: 🔥 **Multiple broken examples in CLAUDE.md**
- Test coverage: ❌ **Missing tests for BETWEEN and IS NULL**

**URGENT Actions Required**:
1. 🔥 **Immediately fix or remove** BETWEEN and IS NULL from documentation
2. 🔥 **Add failing tests** for these bugs
3. 🔥 **Fix parser** to handle BETWEEN and IS NULL correctly
4. ❌ **Add file locking** before multi-user use
5. ❌ **Update CLAUDE.md** to reflect actual capabilities

**Current Status**: ⚠️ **NOT SAFE FOR PRODUCTION** due to silent data correctness bugs. Suitable only for single-user educational use with simplified queries (avoid BETWEEN and IS NULL).

**Recommendation**: Fix critical parsing bugs before any production use. Update documentation to match implementation. Consider Phase 2 incomplete until file locking and proper NULL handling are implemented.
