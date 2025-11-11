# PR #5 Documentation Updates Needed for PR #4 Schema

**Date**: 2025-11-11
**Status**: PR #5 documentation is outdated - reflects old VARCHAR schema
**Blocking**: PR #4 must merge first, then update PR #5

---

## Executive Summary

PR #5 adds excellent user documentation, but it documents the **old VARCHAR-based schema**. After PR #4 merges (INTEGER row_id schema), these docs will be incorrect and confusing for users.

**Impact**: Medium - Code examples will work (API unchanged), but schema documentation is wrong.

**Action Required**: Update 2 files after PR #4 merges:
1. `docs/user-guide.md` - Schema definition (HIGH priority)
2. `docs/getting-started.md` - Add note about internal conversion (MEDIUM priority)

---

## Files Requiring Updates

### 1. `docs/user-guide.md` - **CRITICAL**

**Location**: Lines 108-120 (Schema section)

**Current (WRONG)**:
```sql
CREATE TABLE node(
    pid VARCHAR PRIMARY KEY,
    tcreated INTEGER,
    tmodified INTEGER,
    otype VARCHAR,
    label VARCHAR,
    s VARCHAR,
    p VARCHAR,
    o VARCHAR[],
    n VARCHAR,
    description VARCHAR,
    altids VARCHAR[],
    ...
);
```

**Should be (PR #4 schema)**:
```sql
CREATE TABLE node(
    row_id INTEGER PRIMARY KEY DEFAULT nextval('row_id_sequence'),
    pid VARCHAR UNIQUE NOT NULL,
    tcreated INTEGER DEFAULT(CAST(epoch(current_timestamp) AS INTEGER)),
    tmodified INTEGER DEFAULT(CAST(epoch(current_timestamp) AS INTEGER)),
    otype VARCHAR,
    label VARCHAR,
    s INTEGER DEFAULT(NULL),                -- ← Changed from VARCHAR
    p VARCHAR DEFAULT(NULL),
    o INTEGER[] DEFAULT([]),                -- ← Changed from VARCHAR[]
    n VARCHAR DEFAULT(NULL),
    description VARCHAR DEFAULT(NULL),
    altids VARCHAR[] DEFAULT(NULL),
    ...
);
```

**Additional changes needed in user-guide.md**:
- Line 110: Add `row_id INTEGER PRIMARY KEY` as first field
- Line 111: Change `pid VARCHAR PRIMARY KEY` → `pid VARCHAR UNIQUE NOT NULL` (no longer primary key!)
- Line 112: `s VARCHAR` → `s INTEGER`
- Line 114: `o VARCHAR[]` → `o INTEGER[]`
- Lines 203-208: Update type mapping table

**Type Mapping Table Update** (Lines 203-208):

Current row:
```markdown
| `str` | VARCHAR | Text data |
```

Should add:
```markdown
| `str` | VARCHAR | Text data (or INTEGER for s/o edge references) |
```

### 2. `docs/getting-started.md` - **RECOMMENDED**

**Location**: After line 170 (Edge creation example)

**Current**:
```python
# Create edges to represent relationships
works_on = Edge(s="person_001", p="works_on", o=["project_001"])
```

**Add explanatory note**:
```markdown
> **Note**: The API accepts PIDs (strings) like `"person_001"`, but internally
> PQG stores these as INTEGER row_id references for performance. The conversion
> happens automatically - you don't need to manage row_ids manually.
```

**Rationale**: Clarifies the disconnect between what users see (PIDs) and what's stored (integers).

### 3. `docs/tutorials/` - **NO CHANGES NEEDED** ✅

**Reason**: All tutorial code examples use `Edge(s="pid", o=["pid"])` with string PIDs, which is **correct** because:
- PR #4's conversion layer handles PID → row_id internally
- API surface unchanged - users still pass PIDs
- Examples will work without modification

**Files checked**:
- ✅ `docs/tutorials/01-basic-usage.md` - Uses string PIDs (correct)
- ✅ `docs/tutorials/02-complex-objects.md` - No schema references
- ✅ `docs/tutorials/03-querying.md` - Uses string PIDs in Edge creation (correct)
- ✅ `docs/tutorials/04-visualization.md` - No schema references

### 4. `docs/cli-reference.md` - **NO CHANGES NEEDED** ✅

**Reason**: Documents CLI commands, not internal schema. No schema-specific examples found.

---

## Priority Matrix

| File | Section | Priority | Effort | Impact if Wrong |
|------|---------|----------|--------|-----------------|
| `user-guide.md` | Schema definition (L108-120) | 🔴 HIGH | 10 min | Users see wrong schema, confusion |
| `user-guide.md` | Type mapping table (L203-208) | 🟡 MEDIUM | 5 min | Minor clarification needed |
| `getting-started.md` | Edge creation note (after L170) | 🟡 MEDIUM | 5 min | Helpful context, not critical |

**Total effort estimate**: 20 minutes of editing

---

## Specific Line-by-Line Changes

### File: `docs/user-guide.md`

#### Change 1: Schema Definition (Lines 108-120)

```diff
 CREATE TABLE node(
-    pid VARCHAR PRIMARY KEY,
+    row_id INTEGER PRIMARY KEY DEFAULT nextval('row_id_sequence'),
+    pid VARCHAR UNIQUE NOT NULL,
     tcreated INTEGER DEFAULT(CAST(epoch(current_timestamp) AS INTEGER)),
     tmodified INTEGER DEFAULT(CAST(epoch(current_timestamp) AS INTEGER)),
     otype VARCHAR,
     label VARCHAR,
-    s VARCHAR DEFAULT(NULL),
+    s INTEGER DEFAULT(NULL),
     p VARCHAR DEFAULT(NULL),
-    o VARCHAR[] DEFAULT([]),
+    o INTEGER[] DEFAULT([]),
     n VARCHAR DEFAULT(NULL),
     description VARCHAR DEFAULT(NULL),
     altids VARCHAR[] DEFAULT(NULL),
```

#### Change 2: Field Descriptions Table (Add after schema)

Add new section explaining the dual nature of `s` and `o`:

```markdown
### Understanding Edge References

The `s` (subject) and `o` (object) fields in edges work differently than other fields:

- **API Level**: You provide string PIDs (e.g., `"node_001"`)
- **Storage Level**: PQG converts these to INTEGER row_id references
- **Retrieval**: Automatic conversion back to PIDs when querying

**Why?** INTEGER joins are 2-5x faster than VARCHAR joins for large graphs.

**Example**:
```python
# You write:
edge = Edge(s="node_001", p="relates_to", o=["node_002"])

# PQG stores (internally):
# s = 1 (row_id of node_001)
# o = [2] (row_id of node_002)

# You get back:
retrieved = pqg.getEdge(pid=edge.pid)
assert retrieved.s == "node_001"  # Converted back to PID
```
```

#### Change 3: Type Mapping Table (Line 203)

```diff
 | Python Type | SQL Type | Description |
 |-------------|----------|-------------|
-| `str` | VARCHAR | Text data |
+| `str` | VARCHAR | Text data (PIDs, labels, descriptions) |
+| `int` | INTEGER | Numeric data, row_id references |
 | `int` | INTEGER | Numeric data |
```

### File: `docs/getting-started.md`

#### Change 1: Add Note After Edge Example (After Line 170)

```diff
 # Create edges to represent relationships
 works_on = Edge(s="person_001", p="works_on", o=["project_001"])
 located_in = Edge(s="project_001", p="located_in", o=["loc_001"])
+
+> **💡 Performance Tip**: While you provide PIDs as strings, PQG internally
+> stores these as INTEGER row_id references for 2-5x faster graph traversals.
+> The conversion is automatic and transparent.
```

---

## Testing After Updates

After making these changes, verify:

1. **Schema accuracy**: Compare `docs/user-guide.md` schema with actual PR #4 schema in `pqg/pqg_singletable.py:initialize()`
2. **Code examples still work**: Run tutorial code to ensure PID-based API still functions
3. **Consistency**: Search for any remaining "VARCHAR" references in edge context:
   ```bash
   grep -rn "s.*VARCHAR\|o.*VARCHAR" docs/
   ```

---

## Recommended Workflow

1. **Wait for PR #4 to merge** (don't update PR #5 yet - will cause conflicts)
2. **After PR #4 merges**:
   - Pull latest main
   - Rebase PR #5 branch on main
   - Apply changes listed above
   - Test all code examples
   - Update PR #5 with new commit
3. **Update PR description** to note: "Updated to reflect PR #4 schema changes"

---

## Additional Documentation Gaps (Future Work)

Consider adding these sections to PR #5 docs (not urgent):

1. **Migration Guide** (`docs/migration.md`)
   - How to detect old vs new schema
   - SQL migration script
   - Rollback instructions

2. **Performance Guide** (`docs/performance.md`)
   - Benchmark results (VARCHAR vs INTEGER joins)
   - Index size comparison
   - Memory usage improvements

3. **Troubleshooting** (`docs/troubleshooting.md`)
   - "PID not found" errors (referential integrity)
   - Schema version mismatch
   - External entity references

---

## Summary

**Files to update**: 2
**Lines to change**: ~15
**Effort**: 20 minutes
**Timing**: After PR #4 merges

**Key message for users**: "The API uses PIDs (you see strings), but storage uses integers (for speed). This is transparent - you don't need to manage row_ids."

---

**Prepared by**: Claude Code (Sonnet 4.5)
**Assessment Date**: 2025-11-11
**Related**: PR4_TEST_RESULTS.md
