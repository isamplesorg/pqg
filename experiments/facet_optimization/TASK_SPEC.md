# Facet Metadata Optimization Task

**Issue:** https://github.com/isamplesorg/pqg/issues/18
**Goal:** Generate pre-computed facet summary tables for instant dashboard queries

## Data Source

```python
PARQUET_URL = "https://pub-a18234d962364c22a50c787b7ca09fa5.r2.dev/isamples_202601_wide.parquet"
```

- ~280 MB, ~20M rows
- Contains samples from OPENCONTEXT, SESAR, GEOME, SMITHSONIAN

## Schema (Relevant Columns)

| Column | Type | Description |
|--------|------|-------------|
| `row_id` | INTEGER | Unique identifier |
| `otype` | VARCHAR | Entity type - `'MaterialSampleRecord'` for samples |
| `n` | VARCHAR | Source: OPENCONTEXT, SESAR, GEOME, SMITHSONIAN |
| `label` | VARCHAR | Human-readable name |
| `p__has_material_category` | INTEGER[] | Array of row_ids pointing to IdentifiedConcept |
| `p__has_context_category` | INTEGER[] | Array of row_ids pointing to IdentifiedConcept |
| `p__has_sample_object_type` | INTEGER[] | Array of row_ids pointing to IdentifiedConcept |

For IdentifiedConcept rows (otype = 'IdentifiedConcept'):
| Column | Type | Description |
|--------|------|-------------|
| `row_id` | INTEGER | Unique identifier (referenced by p__* arrays) |
| `label` | VARCHAR | Concept label (e.g., "Rock", "Earth interior") |
| `scheme_name` | VARCHAR | Vocabulary name |

## Task 1: Baseline Benchmark

Measure current facet query performance.

```python
import duckdb
import time

con = duckdb.connect()
PARQUET_URL = "https://pub-a18234d962364c22a50c787b7ca09fa5.r2.dev/isamples_202601_wide.parquet"

# Query 1: Source facet counts
SOURCE_FACET = f"""
SELECT n as source, COUNT(*) as count
FROM read_parquet('{PARQUET_URL}')
WHERE otype = 'MaterialSampleRecord'
GROUP BY n
ORDER BY count DESC
"""

# Query 2: Material category facet (requires join)
MATERIAL_FACET = f"""
WITH samples AS (
    SELECT row_id, UNNEST(p__has_material_category) as material_id
    FROM read_parquet('{PARQUET_URL}')
    WHERE otype = 'MaterialSampleRecord'
      AND p__has_material_category IS NOT NULL
),
concepts AS (
    SELECT row_id, label
    FROM read_parquet('{PARQUET_URL}')
    WHERE otype = 'IdentifiedConcept'
)
SELECT c.label as material, COUNT(*) as count
FROM samples s
JOIN concepts c ON c.row_id = s.material_id
GROUP BY c.label
ORDER BY count DESC
LIMIT 50
"""

# Query 3: Entity type counts (quick sanity check)
OTYPE_COUNTS = f"""
SELECT otype, COUNT(*) as count
FROM read_parquet('{PARQUET_URL}')
GROUP BY otype
ORDER BY count DESC
"""
```

**Measure:** Execute each query 3 times, report median time in milliseconds.

## Task 2: Generate Source Facet Summary

Simple aggregation - should be tiny file.

```python
OUTPUT_PATH = "/tmp/facet_source_counts.parquet"

query = f"""
COPY (
    SELECT
        'source' as facet_type,
        n as facet_value,
        COUNT(*) as count
    FROM read_parquet('{PARQUET_URL}')
    WHERE otype = 'MaterialSampleRecord'
    GROUP BY n
    ORDER BY count DESC
) TO '{OUTPUT_PATH}' (FORMAT PARQUET);
"""
con.execute(query)
```

## Task 3: Generate Material Category Facet Summary

Requires joining through the relationship arrays.

```python
OUTPUT_PATH = "/tmp/facet_material_counts.parquet"

query = f"""
COPY (
    WITH samples AS (
        SELECT UNNEST(p__has_material_category) as material_id
        FROM read_parquet('{PARQUET_URL}')
        WHERE otype = 'MaterialSampleRecord'
          AND p__has_material_category IS NOT NULL
    ),
    concepts AS (
        SELECT row_id, label, scheme_name
        FROM read_parquet('{PARQUET_URL}')
        WHERE otype = 'IdentifiedConcept'
    )
    SELECT
        'material' as facet_type,
        c.label as facet_value,
        c.scheme_name as scheme,
        COUNT(*) as count
    FROM samples s
    JOIN concepts c ON c.row_id = s.material_id
    GROUP BY c.label, c.scheme_name
    ORDER BY count DESC
) TO '{OUTPUT_PATH}' (FORMAT PARQUET);
"""
con.execute(query)
```

## Task 4: Generate Context Category Facet Summary

```python
OUTPUT_PATH = "/tmp/facet_context_counts.parquet"

query = f"""
COPY (
    WITH samples AS (
        SELECT UNNEST(p__has_context_category) as context_id
        FROM read_parquet('{PARQUET_URL}')
        WHERE otype = 'MaterialSampleRecord'
          AND p__has_context_category IS NOT NULL
    ),
    concepts AS (
        SELECT row_id, label, scheme_name
        FROM read_parquet('{PARQUET_URL}')
        WHERE otype = 'IdentifiedConcept'
    )
    SELECT
        'context' as facet_type,
        c.label as facet_value,
        c.scheme_name as scheme,
        COUNT(*) as count
    FROM samples s
    JOIN concepts c ON c.row_id = s.context_id
    GROUP BY c.label, c.scheme_name
    ORDER BY count DESC
) TO '{OUTPUT_PATH}' (FORMAT PARQUET);
"""
con.execute(query)
```

## Task 5: Generate Combined Facet Summary

All facets in one file for easy loading.

```python
OUTPUT_PATH = "/tmp/facet_summaries_all.parquet"

query = f"""
COPY (
    -- Source facet
    SELECT 'source' as facet_type, n as facet_value, NULL as scheme, COUNT(*) as count
    FROM read_parquet('{PARQUET_URL}')
    WHERE otype = 'MaterialSampleRecord'
    GROUP BY n

    UNION ALL

    -- Material facet
    SELECT 'material' as facet_type, c.label as facet_value, c.scheme_name as scheme, COUNT(*) as count
    FROM (
        SELECT UNNEST(p__has_material_category) as material_id
        FROM read_parquet('{PARQUET_URL}')
        WHERE otype = 'MaterialSampleRecord' AND p__has_material_category IS NOT NULL
    ) s
    JOIN (SELECT row_id, label, scheme_name FROM read_parquet('{PARQUET_URL}') WHERE otype = 'IdentifiedConcept') c
    ON c.row_id = s.material_id
    GROUP BY c.label, c.scheme_name

    UNION ALL

    -- Context facet
    SELECT 'context' as facet_type, c.label as facet_value, c.scheme_name as scheme, COUNT(*) as count
    FROM (
        SELECT UNNEST(p__has_context_category) as context_id
        FROM read_parquet('{PARQUET_URL}')
        WHERE otype = 'MaterialSampleRecord' AND p__has_context_category IS NOT NULL
    ) s
    JOIN (SELECT row_id, label, scheme_name FROM read_parquet('{PARQUET_URL}') WHERE otype = 'IdentifiedConcept') c
    ON c.row_id = s.context_id
    GROUP BY c.label, c.scheme_name

    UNION ALL

    -- Object type facet
    SELECT 'object_type' as facet_type, c.label as facet_value, c.scheme_name as scheme, COUNT(*) as count
    FROM (
        SELECT UNNEST(p__has_sample_object_type) as type_id
        FROM read_parquet('{PARQUET_URL}')
        WHERE otype = 'MaterialSampleRecord' AND p__has_sample_object_type IS NOT NULL
    ) s
    JOIN (SELECT row_id, label, scheme_name FROM read_parquet('{PARQUET_URL}') WHERE otype = 'IdentifiedConcept') c
    ON c.row_id = s.type_id
    GROUP BY c.label, c.scheme_name
) TO '{OUTPUT_PATH}' (FORMAT PARQUET);
"""
con.execute(query)
```

## Task 6: Generate Cross-Facet Summary (Source × Material)

For "how many Rock samples from OPENCONTEXT?"

```python
OUTPUT_PATH = "/tmp/facet_source_material_cross.parquet"

query = f"""
COPY (
    SELECT
        s.source,
        c.label as material,
        COUNT(*) as count
    FROM (
        SELECT n as source, UNNEST(p__has_material_category) as material_id
        FROM read_parquet('{PARQUET_URL}')
        WHERE otype = 'MaterialSampleRecord' AND p__has_material_category IS NOT NULL
    ) s
    JOIN (SELECT row_id, label FROM read_parquet('{PARQUET_URL}') WHERE otype = 'IdentifiedConcept') c
    ON c.row_id = s.material_id
    GROUP BY s.source, c.label
    HAVING COUNT(*) > 100  -- Filter out tiny combinations
    ORDER BY count DESC
) TO '{OUTPUT_PATH}' (FORMAT PARQUET);
"""
con.execute(query)
```

## Task 7: Benchmark Summary Table Queries

Compare querying summary tables vs full data.

```python
# Load summary and query
SUMMARY_PATH = "/tmp/facet_summaries_all.parquet"

# This should be nearly instant
FAST_SOURCE_FACET = f"""
SELECT facet_value, count
FROM read_parquet('{SUMMARY_PATH}')
WHERE facet_type = 'source'
ORDER BY count DESC
"""

FAST_MATERIAL_FACET = f"""
SELECT facet_value, count
FROM read_parquet('{SUMMARY_PATH}')
WHERE facet_type = 'material'
ORDER BY count DESC
"""
```

## Expected Output

Generate a JSON results file:

```json
{
  "baseline": {
    "source_facet_ms": 2345,
    "material_facet_ms": 5678,
    "context_facet_ms": 4567,
    "otype_counts_ms": 1234
  },
  "with_summary": {
    "source_facet_ms": 5,
    "material_facet_ms": 8,
    "context_facet_ms": 7
  },
  "speedup": {
    "source": 469,
    "material": 710
  },
  "summary_files": {
    "facet_summaries_all.parquet": {
      "size_bytes": 12345,
      "row_count": 234
    },
    "facet_source_material_cross.parquet": {
      "size_bytes": 45678,
      "row_count": 1234
    }
  },
  "facet_counts": {
    "source": {
      "SESAR": 3100000,
      "OPENCONTEXT": 1200000,
      "GEOME": 1500000,
      "SMITHSONIAN": 900000
    },
    "material_top10": ["Rock", "ite", "..."],
    "context_top10": ["Earth interior", "..."]
  }
}
```

## Output Files

Save to `experiments/facet_optimization/results/`:
- `benchmark_results.json` - The JSON above
- `facet_summaries_all.parquet` - Combined facet counts
- `facet_source_material_cross.parquet` - Cross-tab counts
- `benchmark_log.txt` - Full execution log
