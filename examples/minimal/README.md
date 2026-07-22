# Minimal PQG Example Data

This directory contains small, hand-crafted example datasets to help understand the iSamples PQG format. The same data is represented in JSON, CSV, and all three parquet formats (export, narrow, wide).

## Dataset Overview

**Domain**: Geological rock samples from Mount Rainier volcanic monitoring project

**Entities**:
- 3 MaterialSampleRecords (samples)
- 3 SamplingEvents (collection/preparation events)
- 2 GeospatialCoordLocations (coordinates)
- 1 SamplingSite (Mount Rainier Summit Area)
- 1 Agent (Jane Smith, collector)

**Relationships demonstrated**:
- Sample → produced_by → SamplingEvent (how samples are created)
- Sample → derivedFrom → Sample (parent/child relationship)
- SamplingEvent → sample_location → GeospatialCoordLocation
- SamplingEvent → sampling_site → SamplingSite
- SamplingSite → site_location → GeospatialCoordLocation

## File Structure

```
minimal/
├── json/
│   ├── 1_sample.json       # Single sample (simplest case)
│   └── 3_samples.json      # Three related samples
├── csv/
│   ├── samples.csv         # MaterialSampleRecords
│   ├── events.csv          # SamplingEvents
│   ├── locations.csv       # GeospatialCoordLocations
│   ├── sites.csv           # SamplingSites
│   ├── agents.csv          # Agents
│   └── edges.csv           # Relationships (for narrow format)
└── parquet/
    ├── minimal_export.parquet    # Export format (3 rows, nested)
    ├── minimal_narrow.parquet    # Narrow format (21 rows, with edges)
    └── minimal_wide.parquet      # Wide format (10 rows, p__* columns)
```

## The Three Parquet Formats

### Export Format (`minimal_export.parquet`)
- **3 rows** - one per sample
- Sample-centric with nested structs for related entities
- Best for: Simple queries on sample properties
- Coordinates pre-extracted to `sample_location_latitude/longitude`

### Narrow Format (`minimal_narrow.parquet`)
- **21 rows** - 10 entities + 11 edge rows
- Graph-normalized with explicit `_edge_` rows
- Columns `s` (subject), `p` (predicate), `o` (object array)
- Best for: Graph traversal, flexible relationship queries

### Wide Format (`minimal_wide.parquet`)
- **10 rows** - one per entity (no edge rows)
- Relationships stored as `p__*` columns with row_id arrays
- Best for: Fast entity queries, smaller file size, analytical queries

## Example Queries

### Query 1: Find all samples (works in all formats)

**Export format:**
```sql
SELECT sample_identifier, label
FROM read_parquet('parquet/minimal_export.parquet')
```

**Wide format:**
```sql
SELECT pid, label
FROM read_parquet('parquet/minimal_wide.parquet')
WHERE otype = 'MaterialSampleRecord'
```

**Narrow format:**
```sql
SELECT pid, label
FROM read_parquet('parquet/minimal_narrow.parquet')
WHERE otype = 'MaterialSampleRecord'
```

### Query 2: Find samples with their locations

**Wide format (uses p__* columns):**
```sql
SELECT
    s.pid as sample,
    s.label,
    loc.latitude,
    loc.longitude
FROM read_parquet('parquet/minimal_wide.parquet') s
JOIN read_parquet('parquet/minimal_wide.parquet') e
    ON e.otype = 'SamplingEvent'
    AND list_contains(s.p__produced_by, e.row_id)
JOIN read_parquet('parquet/minimal_wide.parquet') loc
    ON loc.otype = 'GeospatialCoordLocation'
    AND list_contains(e.p__sample_location, loc.row_id)
WHERE s.otype = 'MaterialSampleRecord'
```

**Narrow format (uses edge rows):**
```sql
SELECT
    s.pid as sample,
    s.label,
    loc.latitude,
    loc.longitude
FROM read_parquet('parquet/minimal_narrow.parquet') s
JOIN read_parquet('parquet/minimal_narrow.parquet') e1
    ON e1.otype = '_edge_'
    AND e1.s = s.row_id
    AND e1.p = 'produced_by'
JOIN read_parquet('parquet/minimal_narrow.parquet') ev
    ON ev.otype = 'SamplingEvent'
    AND list_contains(e1.o, ev.row_id)
JOIN read_parquet('parquet/minimal_narrow.parquet') e2
    ON e2.otype = '_edge_'
    AND e2.s = ev.row_id
    AND e2.p = 'sample_location'
JOIN read_parquet('parquet/minimal_narrow.parquet') loc
    ON loc.otype = 'GeospatialCoordLocation'
    AND list_contains(e2.o, loc.row_id)
WHERE s.otype = 'MaterialSampleRecord'
```

### Query 3: Count entities by type

```sql
SELECT otype, COUNT(*) as count
FROM read_parquet('parquet/minimal_wide.parquet')
GROUP BY otype
ORDER BY count DESC
```

Expected output:
```
MaterialSampleRecord     3
SamplingEvent           3
GeospatialCoordLocation 2
SamplingSite            1
Agent                   1
```

## JSON Schema Validation

The JSON files validate against the iSamples Core 1.0 schema:

```python
import json
from jsonschema import validate

# Load schema (from isamplesorg-metadata repo)
with open('path/to/iSamplesSchemaCore1.0.json') as f:
    schema = json.load(f)

# Load and validate
with open('json/1_sample.json') as f:
    sample = json.load(f)

validate(instance=sample, schema=schema)  # Raises if invalid
```

## Entity Relationship Diagram

```
MaterialSampleRecord ──produced_by──► SamplingEvent ──sample_location──► GeospatialCoordLocation
        │                                   │
        │                                   └──sampling_site──► SamplingSite ──site_location──► GeospatialCoordLocation
        │
        ├──registrant──► Agent
        │
        └──derivedFrom──► MaterialSampleRecord (parent sample)
```

## Size Comparison

| Format | Rows | File Size | Notes |
|--------|------|-----------|-------|
| Export | 3 | 1.7 KB | Nested structs, sample-centric |
| Narrow | 21 | 4.8 KB | Explicit edge rows |
| Wide | 10 | 5.0 KB | p__* columns |

In production datasets:
- Wide is typically 60-70% smaller than narrow
- Export is smallest but less flexible for complex queries

## See Also

- [PQG Specification](../../docs/PQG_SPECIFICATION.md) - Full format specification
- [Edge Types](../../pqg/edge_types.py) - All 14 iSamples edge types
- [Schema Definitions](../../pqg/schemas/) - Python schema validators
