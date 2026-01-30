# H3 Geospatial Optimization Task

**Issue:** https://github.com/isamplesorg/pqg/issues/17
**Goal:** Add H3 index columns to iSamples parquet and benchmark speedup

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
| `otype` | VARCHAR | Entity type - filter to `'MaterialSampleRecord'` for samples |
| `latitude` | DOUBLE | WGS84 latitude (nullable) |
| `longitude` | DOUBLE | WGS84 longitude (nullable) |
| `n` | VARCHAR | Source: OPENCONTEXT, SESAR, GEOME, SMITHSONIAN |
| `label` | VARCHAR | Human-readable name |

## Environment Setup

```python
import duckdb

# Install and load H3 extension
con = duckdb.connect()
con.execute("INSTALL h3; LOAD h3;")
```

## Task 1: Baseline Benchmark

Measure current geospatial query performance.

```python
PARQUET_URL = "https://pub-a18234d962364c22a50c787b7ca09fa5.r2.dev/isamples_202601_wide.parquet"

# Query 1: Bounding box - Western US
BBOX_QUERY = f"""
SELECT COUNT(*) as cnt
FROM read_parquet('{PARQUET_URL}')
WHERE otype = 'MaterialSampleRecord'
  AND latitude BETWEEN 32 AND 42
  AND longitude BETWEEN -125 AND -110
"""

# Query 2: Bounding box with facet
BBOX_FACET_QUERY = f"""
SELECT n as source, COUNT(*) as cnt
FROM read_parquet('{PARQUET_URL}')
WHERE otype = 'MaterialSampleRecord'
  AND latitude BETWEEN 32 AND 42
  AND longitude BETWEEN -125 AND -110
GROUP BY n
"""

# Query 3: Point radius (approximate - 1 degree ≈ 111km)
# San Francisco area, ~50km radius
RADIUS_QUERY = f"""
SELECT COUNT(*) as cnt
FROM read_parquet('{PARQUET_URL}')
WHERE otype = 'MaterialSampleRecord'
  AND latitude BETWEEN 37.3 AND 38.1
  AND longitude BETWEEN -122.8 AND -122.0
"""
```

**Measure:** Execute each query 3 times, report median time in milliseconds.

## Task 2: Generate H3-Enhanced Parquet

Add H3 columns at resolutions 4, 6, and 8.

```python
import duckdb
import time

con = duckdb.connect()
con.execute("INSTALL h3; LOAD h3;")

PARQUET_URL = "https://pub-a18234d962364c22a50c787b7ca09fa5.r2.dev/isamples_202601_wide.parquet"
OUTPUT_PATH = "/tmp/isamples_wide_h3.parquet"

# Generate with H3 columns
query = f"""
COPY (
    SELECT *,
        CASE WHEN latitude IS NOT NULL AND longitude IS NOT NULL
             THEN h3_latlng_to_cell(latitude, longitude, 4) END as h3_res4,
        CASE WHEN latitude IS NOT NULL AND longitude IS NOT NULL
             THEN h3_latlng_to_cell(latitude, longitude, 6) END as h3_res6,
        CASE WHEN latitude IS NOT NULL AND longitude IS NOT NULL
             THEN h3_latlng_to_cell(latitude, longitude, 8) END as h3_res8
    FROM read_parquet('{PARQUET_URL}')
) TO '{OUTPUT_PATH}' (FORMAT PARQUET, COMPRESSION ZSTD);
"""

start = time.time()
con.execute(query)
elapsed = time.time() - start
print(f"Generated in {elapsed:.1f}s")
```

**Report:**
- Original file size (MB)
- New file size with H3 (MB)
- Size increase percentage
- Row count with valid H3 (non-null lat/lon)

## Task 3: H3 Benchmark

Re-run equivalent queries using H3 filters.

```python
OUTPUT_PATH = "/tmp/isamples_wide_h3.parquet"

# Get H3 cells covering the Western US bbox at res 4
# (In practice, use h3 library to get these)
# For now, query to find the cells:
FIND_CELLS = f"""
SELECT DISTINCT h3_res4
FROM read_parquet('{OUTPUT_PATH}')
WHERE latitude BETWEEN 32 AND 42
  AND longitude BETWEEN -125 AND -110
  AND h3_res4 IS NOT NULL
"""

# Then filter by H3 cell instead of lat/lon
# This should be faster because H3 is an integer column with good stats
H3_BBOX_QUERY = f"""
SELECT COUNT(*) as cnt
FROM read_parquet('{OUTPUT_PATH}')
WHERE otype = 'MaterialSampleRecord'
  AND h3_res4 IN (SELECT DISTINCT h3_res4
                   FROM read_parquet('{OUTPUT_PATH}')
                   WHERE latitude BETWEEN 32 AND 42
                     AND longitude BETWEEN -125 AND -110)
"""

# For aggregation by location (clustering for map display)
H3_CLUSTER_QUERY = f"""
SELECT h3_res6, COUNT(*) as cnt,
       AVG(latitude) as center_lat,
       AVG(longitude) as center_lon
FROM read_parquet('{OUTPUT_PATH}')
WHERE otype = 'MaterialSampleRecord'
  AND h3_res4 IN (...cells from above...)
GROUP BY h3_res6
"""
```

## Task 4: Resolution Analysis

Determine optimal H3 resolutions.

```python
# Count distinct cells at each resolution
RESOLUTION_STATS = f"""
SELECT
    COUNT(DISTINCT h3_res4) as unique_res4,
    COUNT(DISTINCT h3_res6) as unique_res6,
    COUNT(DISTINCT h3_res8) as unique_res8,
    COUNT(*) as total_rows,
    COUNT(h3_res4) as rows_with_h3
FROM read_parquet('{OUTPUT_PATH}')
WHERE otype = 'MaterialSampleRecord'
"""
```

**Report:** Unique cells per resolution, average points per cell.

## Expected Output

Generate a JSON results file:

```json
{
  "baseline": {
    "bbox_query_ms": 1234,
    "bbox_facet_ms": 1456,
    "radius_query_ms": 1123
  },
  "with_h3": {
    "bbox_query_ms": 234,
    "bbox_facet_ms": 345,
    "cluster_query_ms": 456
  },
  "speedup": {
    "bbox": 5.3,
    "facet": 4.2
  },
  "file_size": {
    "original_mb": 282,
    "with_h3_mb": 310,
    "increase_pct": 9.9
  },
  "h3_stats": {
    "rows_with_coords": 5400000,
    "unique_res4_cells": 1234,
    "unique_res6_cells": 45678,
    "unique_res8_cells": 234567
  }
}
```

## Output Files

Save to `experiments/h3_optimization/results/`:
- `benchmark_results.json` - The JSON above
- `isamples_wide_h3.parquet` - Enhanced parquet (or note if too large to include)
- `benchmark_log.txt` - Full execution log
