# PR #6 Action Plan: Addressing AI Review Recommendations

**Generated**: 2025-12-12
**Review Sources**: Gemini 2.5, OpenAI Codex (gpt-5.1-codex-max)
**PR**: https://github.com/isamplesorg/pqg/pull/6

## Executive Summary

The PR introduces solid schema validation infrastructure and efficient SQL-based conversion. However, there are critical mismatches between the theoretical edge type definitions (`edge_types.py`) and the physical implementation (`sql_converter.py`). These must be resolved before merge.

---

## High Priority Issues

### Issue 1: Wrong Location Column for SamplingSite (Wide Format)

**Location**: `pqg/sql_converter.py:1387-1389`

**Current Code**:
```sql
[se.p__sample_location]::INTEGER[] as p__sample_location,
NULL::INTEGER[] as p__sampling_site,
NULL::INTEGER[] as p__site_location
```

**Expected** (per `edge_types.py:50`):
- `SITE_LOCATION` = `SamplingSite__site_location__GeospatialCoordLocation`
- Sites should use `p__site_location`, not `p__sample_location`

**Fix**:
```sql
NULL::INTEGER[] as p__sample_location,
NULL::INTEGER[] as p__sampling_site,
[se.p__site_location]::INTEGER[] as p__site_location
```

**Also update** the `site_edges` temp table to use `p__site_location` column name.

---

### Issue 2: Wrong Predicate for Site→Location Edge (Narrow Format)

**Location**: `pqg/sql_converter.py:911-940`

**Current Code**:
```sql
CREATE TEMP TABLE edge_sample_location AS
...
'sample_location' as p,  -- WRONG predicate!
```

**Expected** (per `edge_types.py:50`):
- `SITE_LOCATION` uses predicate `site_location`, not `sample_location`

**Fix**:
```sql
CREATE TEMP TABLE edge_site_location AS
...
'site_location' as p,  -- Correct predicate for SamplingSite -> Location
```

---

### Issue 3: Missing Event→Location Edge (Narrow Format)

**Location**: After `edge_sampling_site` (around line 909)

**Expected** (per `edge_types.py:46`):
- `EVENT_SAMPLE_LOCATION` = `SamplingEvent__sample_location__GeospatialCoordLocation`

**Current State**: No edge creates `SamplingEvent -> sample_location -> Location`

**Fix**: Add new edge table after `edge_sampling_site`:
```sql
-- Edge: Event -> sample_location -> Location (full 40-column schema)
CREATE TEMP TABLE edge_event_location AS
SELECT
    -- Core identification
    {edge_ss_max} + row_number() OVER () as row_id,
    s.sample_identifier || '_edge_event_location' as pid,
    NULL::INTEGER as tcreated,
    NULL::INTEGER as tmodified,
    '_edge_' as otype,
    -- Edge columns
    evt.row_id as s,
    'sample_location' as p,  -- Correct predicate for Event -> Location
    [loc.row_id]::INTEGER[] as o,
    -- Graph metadata
    s.source_collection as n,
    NULL::VARCHAR[] as altids,
    NULL::GEOMETRY as geometry,
    -- Entity columns (all NULL for edges)
    {null_cols}
FROM source s
JOIN pid_lookup evt ON evt.pid = s.sample_identifier || '_event'
JOIN pid_lookup loc ON loc.pid = s.sample_identifier || '_location'
WHERE s.produced_by IS NOT NULL
  AND s.sample_location_latitude IS NOT NULL
```

**Also update**: Wide format to populate `p__sample_location` for SamplingEvent rows.

---

## Medium Priority Issues

### Issue 4: Missing MaterialSampleCuration Nodes

**Impact**: `MSR_CURATION` and `CURATION_RESPONSIBILITY` edge types cannot be represented

**Current State**: Converter flattens curation data directly onto MaterialSampleRecord

**Decision Required**:
- Option A: Extract `MaterialSampleCuration` as separate nodes (matches edge_types.py)
- Option B: Remove `MSR_CURATION` and `CURATION_RESPONSIBILITY` from edge_types.py (simplify model)

**Recommendation**: Option A if curation needs to be queried independently; Option B for simpler UI

---

### Issue 5: Missing Wide Schema Columns

**Location**: `pqg/schemas/wide.py`

**Missing Columns** (implied by edge_types.py):
- `p__curation` - MaterialSampleRecord → MaterialSampleCuration
- `p__related_resource` - MaterialSampleRecord → SampleRelation

**Fix**: Add to `RELATIONSHIP_COLUMNS` in `wide.py`:
```python
ColumnSpec(
    name="p__curation",
    arrow_type=pa.list_(pa.int64()),
    nullable=True,
    required=False,
    description="MaterialSampleRecord curation MaterialSampleCuration",
),
ColumnSpec(
    name="p__related_resource",
    arrow_type=pa.list_(pa.int64()),
    nullable=True,
    required=False,
    description="MaterialSampleRecord related_resource SampleRelation",
),
```

---

### Issue 6: Schema Validation Ignores Nullable Flag

**Location**: `pqg/schemas/base.py:60-72`

**Current State**: `ColumnSpec.validate()` only checks presence and type compatibility

**Fix**: Add nullability validation:
```python
def validate(self, actual_field: Optional[pa.Field]) -> List[str]:
    errors = []
    # ... existing checks ...

    # Check nullability
    if not self.nullable and actual_field.nullable:
        errors.append(
            f"Column '{self.name}' should not be nullable but parquet schema allows nulls"
        )

    return errors
```

---

## Low Priority Issues

### Issue 7: Export Format Not Detected

**Location**: `pqg/schemas/base.py:150-190` (`get_schema_from_parquet`)

**Fix**: Add export format detection based on presence of `produced_by` STRUCT column or absence of `otype` column.

---

### Issue 8: Hardcoded SQL Strings

**Recommendation**: Consider generating NULL column lists from `NARROW_COLUMNS` definition to stay in sync:
```python
null_cols = ", ".join(
    f"NULL::{sql_type} as {col_name}"
    for col_name, sql_type in ENTITY_COLUMNS
)
```

---

## Implementation Order

1. **Fix Issue 1 + 2 + 3** (location edges) - These are correctness bugs
2. **Fix Issue 6** (nullable validation) - Quick win for validation strictness
3. **Fix Issue 5** (missing wide columns) - Schema completeness
4. **Decide on Issue 4** (MaterialSampleCuration) - Needs design decision
5. **Fix Issue 7 + 8** (export detection, SQL generation) - Nice to have

---

## Testing Plan

After fixes, run:
```bash
# Unit tests
uv run pytest tests/test_schemas.py -v
uv run pytest tests/test_typed_edges.py -v
uv run pytest tests/test_format_equivalence.py -v

# Integration test with real data
uv run python scripts/sql_convert_isamples.py \
    /path/to/export.parquet \
    /tmp/narrow_test.parquet \
    --format narrow

uv run python scripts/sql_convert_isamples.py \
    /path/to/export.parquet \
    /tmp/wide_test.parquet \
    --format wide

# Validate output
uv run python -c "
from pqg.schemas import validate_parquet, NARROW_SCHEMA, WIDE_SCHEMA
print('Narrow errors:', validate_parquet('/tmp/narrow_test.parquet', NARROW_SCHEMA))
print('Wide errors:', validate_parquet('/tmp/wide_test.parquet', WIDE_SCHEMA))
"

# Verify typed edges work
uv run python -c "
from pqg.typed_edges import TypedEdgeQueries
from pqg.edge_types import ISamplesEdgeType
import duckdb

con = duckdb.connect()
con.execute(\"CREATE TABLE pqg AS SELECT * FROM read_parquet('/tmp/narrow_test.parquet')\")
teq = TypedEdgeQueries(con)

# Should now return results for all location edge types
for et in [ISamplesEdgeType.EVENT_SAMPLE_LOCATION, ISamplesEdgeType.SITE_LOCATION]:
    count = sum(1 for _ in teq.get_edges_by_type(et, limit=100))
    print(f'{et.name}: {count} edges')
"
```

---

## Summary Table

| Issue | Priority | Effort | File(s) |
|-------|----------|--------|---------|
| Wrong p__sample_location for sites | High | Small | sql_converter.py:1387 |
| Wrong predicate for site→location | High | Small | sql_converter.py:923 |
| Missing event→location edge | High | Medium | sql_converter.py (new section) |
| Missing MaterialSampleCuration | Medium | Large | sql_converter.py, wide.py |
| Missing p__curation, p__related_resource | Medium | Small | wide.py |
| Nullable validation | Medium | Small | base.py |
| Export format detection | Low | Small | base.py |
| Hardcoded SQL | Low | Medium | sql_converter.py |
