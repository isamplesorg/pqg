# PQG: Property Query Graph Format Specification

**Version**: 0.3.0
**Status**: Draft
**Authors**: iSamples Project Team
**Date**: December 2025

---

## For the Impatient

**What is PQG?** A way to store connected data (like "Sample → collected at → Location") in Parquet files that you can query with SQL.

**Why should I care?** If you have data with relationships (samples, events, people, places), PQG lets you:
- Store it in a single file (no database server needed)
- Query it in your browser (via DuckDB-WASM)
- Archive it on Zenodo (it's just Parquet)
- Share it with colleagues (they don't need special software)

**Quick example:**
```sql
-- Find all rock samples from California
SELECT s.label, loc.latitude, loc.longitude
FROM pqg WHERE otype = 'MaterialSampleRecord' s
JOIN pqg WHERE otype = '_edge_' AND p = 'produced_by' e1 ON e1.s = s.row_id
JOIN pqg WHERE otype = 'SamplingEvent' evt ON e1.o[1] = evt.row_id
JOIN pqg WHERE otype = '_edge_' AND p = 'sample_location' e2 ON e2.s = evt.row_id
JOIN pqg WHERE otype = 'GeospatialCoordLocation' loc ON e2.o[1] = loc.row_id
WHERE s.label LIKE '%rock%' AND loc.latitude BETWEEN 32 AND 42
```

---

## Table of Contents

1. [Introduction](#1-introduction)
2. [Core Concepts](#2-core-concepts)
3. [The Three Formats](#3-the-three-formats)
4. [Schema Reference](#4-schema-reference)
5. [Edge Types](#5-edge-types)
6. [Validation](#6-validation)
7. [Conversion](#7-conversion)
8. [Implementation Guide](#8-implementation-guide)
9. [FAQ](#9-faq)

---

## 1. Introduction

### 1.1 The Problem

Scientific data is inherently connected:
- A **sample** was collected during an **event**
- The **event** happened at a **location**
- A **person** was responsible for the **event**
- The **sample** is made of certain **materials**

Traditional approaches have tradeoffs:

| Approach | Pros | Cons |
|----------|------|------|
| **Flat CSV** | Simple, universal | Loses relationships, redundant data |
| **Relational DB** | Preserves structure | Requires server, not portable |
| **JSON-LD** | Semantic web compatible | Slow queries, large files |
| **Neo4j/Graph DB** | Native graph queries | Requires server, vendor lock-in |

### 1.2 The PQG Solution

PQG stores property graphs in Parquet files:

```
┌─────────────────────────────────────────────────────────────┐
│                    Single Parquet File                       │
├─────────────────────────────────────────────────────────────┤
│  Entities (nodes):  Sample, Event, Location, Person, ...    │
│  Relationships (edges): collected_at, responsible_for, ...  │
│  Properties: label, description, coordinates, ...           │
└─────────────────────────────────────────────────────────────┘
```

**Benefits:**
- **Portable**: One file, works anywhere
- **Queryable**: Standard SQL (DuckDB, Spark, Pandas)
- **Archivable**: Perfect for Zenodo, Dataverse
- **Efficient**: Columnar compression, HTTP range requests
- **Browser-ready**: DuckDB-WASM runs in your browser

### 1.3 Design Philosophy

1. **Parquet-native**: No extensions, just standard Parquet
2. **SQL-first**: Designed for SQL queries, not special APIs
3. **Self-describing**: Schema embedded in file metadata
4. **Lossless**: Can round-trip to/from other graph formats
5. **Practical**: Optimized for real scientific workflows

---

## 2. Core Concepts

### 2.1 Nodes and Edges in a Single Table

Unlike traditional graph databases, PQG stores everything in one table:

```
┌────────┬─────────────────────┬───────────────────────┬─────┐
│ row_id │ otype               │ (entity columns...)   │ ... │
├────────┼─────────────────────┼───────────────────────┼─────┤
│ 1      │ MaterialSampleRecord│ label="Rock ABC"      │     │
│ 2      │ SamplingEvent       │ result_time="2024"    │     │
│ 3      │ GeospatialCoordLocation │ lat=37.5, lon=-122│     │
│ 4      │ _edge_              │ s=1, p="produced_by", o=[2] │
│ 5      │ _edge_              │ s=2, p="sample_location", o=[3] │
└────────┴─────────────────────┴───────────────────────┴─────┘
```

**Key insight**: Edges are just rows with `otype = '_edge_'` and special columns `s`, `p`, `o`.

### 2.2 The SPO Triple

Every edge encodes a relationship:

- **S** (Subject): The source node's `row_id`
- **P** (Predicate): The relationship name (string)
- **O** (Object): Array of target node `row_id`s

Example: "Sample 1 was produced by Event 2"
```
s=1, p="produced_by", o=[2]
```

### 2.3 Entity Types (otype)

The `otype` column identifies what kind of thing each row represents:

| otype | Description |
|-------|-------------|
| `MaterialSampleRecord` | A physical sample |
| `SamplingEvent` | When/how a sample was collected |
| `SamplingSite` | A named location |
| `GeospatialCoordLocation` | Lat/lon coordinates |
| `Agent` | A person or organization |
| `IdentifiedConcept` | A controlled vocabulary term |
| `MaterialSampleCuration` | Curation/storage information |
| `SampleRelation` | Links between samples |
| `_edge_` | A relationship (not an entity) |

### 2.4 The row_id / pid Distinction

- **row_id**: Integer, internal to this file (fast JOINs)
- **pid**: String, globally unique identifier (portable)

Use `row_id` for queries within a file. Use `pid` when referencing data across files or systems.

---

## 3. The Three Formats

PQG defines three serialization formats for different use cases:

### 3.1 Narrow Format (Full Graph)

**Best for**: Archival, graph algorithms, data curation

```
Rows: Entities + Edge rows (many rows)
Columns: 40 (includes s, p, o)
File size: Largest
Query complexity: Requires JOINs
```

The narrow format is the canonical representation. Every relationship is an explicit row.

**Example query** (find samples and their locations):
```sql
SELECT s.label, loc.latitude, loc.longitude
FROM pqg s
JOIN pqg e ON e.s = s.row_id AND e.p = 'produced_by'
JOIN pqg evt ON e.o[1] = evt.row_id
JOIN pqg e2 ON e2.s = evt.row_id AND e2.p = 'sample_location'
JOIN pqg loc ON e2.o[1] = loc.row_id
WHERE s.otype = 'MaterialSampleRecord'
```

### 3.2 Wide Format (Denormalized Entities)

**Best for**: Analytics, dashboards, moderate complexity

```
Rows: Entities only (no edge rows)
Columns: 49 (entity columns + p__* relationship arrays)
File size: Medium
Query complexity: Moderate (array operations)
```

Relationships are stored as `p__*` columns containing arrays of `row_id`s:

```
┌────────┬─────────────────────┬────────────────┬─────────────────┐
│ row_id │ otype               │ p__produced_by │ p__sample_location │
├────────┼─────────────────────┼────────────────┼─────────────────┤
│ 1      │ MaterialSampleRecord│ [2]            │ NULL            │
│ 2      │ SamplingEvent       │ NULL           │ [3]             │
│ 3      │ GeospatialCoordLocation │ NULL       │ NULL            │
└────────┴─────────────────────┴────────────────┴─────────────────┘
```

**Example query**:
```sql
SELECT s.label, loc.latitude, loc.longitude
FROM pqg s
JOIN pqg evt ON evt.row_id = s.p__produced_by[1]
JOIN pqg loc ON loc.row_id = evt.p__sample_location[1]
WHERE s.otype = 'MaterialSampleRecord'
```

### 3.3 Export Format (Sample-Centric)

**Best for**: End-user queries, web UIs, simple analytics

```
Rows: One per sample (most compact)
Columns: 19 (including nested STRUCTs)
File size: Smallest
Query complexity: Simplest (no JOINs for basic queries)
```

Relationships are nested STRUCTs:

```sql
SELECT
  sample_identifier,
  label,
  produced_by.sampling_site.sample_location.latitude,
  produced_by.sampling_site.sample_location.longitude
FROM export_pqg
WHERE label LIKE '%rock%'
```

### 3.4 Format Comparison

| Aspect | Narrow | Wide | Export |
|--------|--------|------|--------|
| **Rows** | 92M | 19.5M | 6.7M |
| **Columns** | 40 | 49 | 19 |
| **File Size** | 725MB | 290MB | 300MB |
| **Map query** | 16ms | 7ms | 41ms |
| **Facet query** | ~3s | ~2s | ~500ms |
| **Graph traversal** | Native | Moderate | Limited |
| **Archival fidelity** | Full | High | Lossy |

**Recommendation**:
- Archive in **Narrow** (lossless)
- Analyze in **Wide** (balanced)
- Serve UI from **Export** (fastest)

---

## 4. Schema Reference

### 4.1 Core Columns (All Formats)

| Column | Type | Required | Description |
|--------|------|----------|-------------|
| `row_id` | INT32/INT64 | Yes | Internal row identifier |
| `pid` | STRING | Yes | Persistent global identifier |
| `tcreated` | INT32 | No | Unix timestamp created |
| `tmodified` | INT32 | No | Unix timestamp modified |
| `otype` | STRING | Yes | Entity type or `_edge_` |

### 4.2 Edge Columns (Narrow Only)

| Column | Type | Description |
|--------|------|-------------|
| `s` | INT32 | Subject row_id |
| `p` | STRING | Predicate name |
| `o` | INT32[] | Object row_id(s) |

### 4.3 Graph Metadata

| Column | Type | Description |
|--------|------|-------------|
| `n` | STRING | Named graph (source collection) |
| `altids` | STRING[] | Alternate identifiers |
| `geometry` | BINARY | WKB geometry |

### 4.4 Entity-Specific Columns

Each entity type uses relevant columns:

**MaterialSampleRecord**: `sample_identifier`, `label`, `description`, `sampling_purpose`

**SamplingEvent**: `result_time`, `has_feature_of_interest`

**GeospatialCoordLocation**: `latitude`, `longitude`, `elevation`

**Agent**: `name`, `role`, `affiliation`

**SamplingSite**: `place_name`, `label`, `description`

**IdentifiedConcept**: `scheme_uri`, `scheme_name`, `label`

**MaterialSampleCuration**: `curation_location`, `access_constraints`

### 4.5 Relationship Columns (Wide Only)

| Column | Type | Description |
|--------|------|-------------|
| `p__produced_by` | INT32[] | Sample → Event |
| `p__sample_location` | INT32[] | Event → Location |
| `p__sampling_site` | INT32[] | Event → Site |
| `p__site_location` | INT32[] | Site → Location |
| `p__responsibility` | INT32[] | Event/Curation → Agent |
| `p__registrant` | INT32[] | Sample → Agent |
| `p__has_material_category` | INT32[] | Sample → Concept |
| `p__has_context_category` | INT32[] | Sample → Concept |
| `p__has_sample_object_type` | INT32[] | Sample → Concept |
| `p__keywords` | INT32[] | Sample → Concept |
| `p__curation` | INT32[] | Sample → Curation |
| `p__related_resource` | INT32[] | Sample → Relation |

---

## 5. Edge Types

PQG defines 14 typed edge patterns for iSamples:

### 5.1 Sample Relationships

| Edge Type | Subject | Predicate | Object |
|-----------|---------|-----------|--------|
| `MSR_PRODUCED_BY` | MaterialSampleRecord | produced_by | SamplingEvent |
| `MSR_REGISTRANT` | MaterialSampleRecord | registrant | Agent |
| `MSR_CURATION` | MaterialSampleRecord | curation | MaterialSampleCuration |
| `MSR_RELATED_RESOURCE` | MaterialSampleRecord | related_resource | SampleRelation |
| `MSR_HAS_MATERIAL_CATEGORY` | MaterialSampleRecord | has_material_category | IdentifiedConcept |
| `MSR_HAS_CONTEXT_CATEGORY` | MaterialSampleRecord | has_context_category | IdentifiedConcept |
| `MSR_HAS_SAMPLE_OBJECT_TYPE` | MaterialSampleRecord | has_sample_object_type | IdentifiedConcept |
| `MSR_KEYWORDS` | MaterialSampleRecord | keywords | IdentifiedConcept |

### 5.2 Event Relationships

| Edge Type | Subject | Predicate | Object |
|-----------|---------|-----------|--------|
| `EVENT_SAMPLE_LOCATION` | SamplingEvent | sample_location | GeospatialCoordLocation |
| `EVENT_SAMPLING_SITE` | SamplingEvent | sampling_site | SamplingSite |
| `EVENT_RESPONSIBILITY` | SamplingEvent | responsibility | Agent |
| `EVENT_HAS_CONTEXT_CATEGORY` | SamplingEvent | has_context_category | IdentifiedConcept |

### 5.3 Site and Curation Relationships

| Edge Type | Subject | Predicate | Object |
|-----------|---------|-----------|--------|
| `SITE_LOCATION` | SamplingSite | site_location | GeospatialCoordLocation |
| `CURATION_RESPONSIBILITY` | MaterialSampleCuration | responsibility | Agent |

### 5.4 Validation Rules

Each edge type has constraints:

```python
{
    "subject_type": "MaterialSampleRecord",
    "predicate": "produced_by",
    "object_type": "SamplingEvent",
    "multivalued": False,  # Only one producing event
    "required": False      # Some samples lack provenance
}
```

---

## 6. Validation

### 6.1 Schema Validation

```python
from pqg.schemas import NARROW_SCHEMA, validate_parquet

errors = validate_parquet('my_file.parquet', NARROW_SCHEMA)
if errors:
    print("Validation failed:", errors)
```

### 6.2 What Gets Checked

1. **Required columns present**: All core columns exist
2. **Type compatibility**: Columns have expected types
3. **Forbidden columns absent**: Format-specific exclusions
4. **Valid otype values**: Only known entity types
5. **Nullable constraints**: Non-nullable columns checked

### 6.3 Format Detection

```python
from pqg.schemas import get_schema_from_parquet, SchemaFormat

schema, format = get_schema_from_parquet('unknown_file.parquet')
if format == SchemaFormat.NARROW:
    print("This is a narrow format file")
```

---

## 7. Conversion

### 7.1 Export → Narrow/Wide

```python
from pqg.sql_converter import convert_isamples_sql

# To narrow format (archival)
convert_isamples_sql('export.parquet', 'narrow.parquet', wide=False)

# To wide format (analytics)
convert_isamples_sql('export.parquet', 'wide.parquet', wide=True)
```

### 7.2 Conversion Options

```python
convert_isamples_sql(
    input_parquet='export.parquet',
    output_parquet='output.parquet',
    wide=False,              # True for wide format
    dedupe_sites=True,       # Merge duplicate locations
    site_precision=5,        # Decimal places for deduplication
    verbose=True             # Print progress
)
```

### 7.3 Performance

The SQL-based converter uses DuckDB for 50-100x speedup over row-by-row processing:

| Dataset | Rows | Conversion Time |
|---------|------|-----------------|
| 1K samples | 10K | 2s |
| 100K samples | 1M | 30s |
| 6.7M samples | 92M | ~10min |

---

## 8. Implementation Guide

### 8.1 Reading PQG Files

**Python (DuckDB)**:
```python
import duckdb
con = duckdb.connect()
df = con.execute("""
    SELECT * FROM read_parquet('samples.parquet')
    WHERE otype = 'MaterialSampleRecord'
    LIMIT 100
""").df()
```

**JavaScript (DuckDB-WASM)**:
```javascript
const db = await duckdb.Database.create(':memory:');
const conn = await db.connect();
const result = await conn.query(`
    SELECT * FROM read_parquet('https://example.com/samples.parquet')
    WHERE otype = 'MaterialSampleRecord'
`);
```

**R**:
```r
library(arrow)
library(dplyr)
read_parquet("samples.parquet") %>%
  filter(otype == "MaterialSampleRecord")
```

### 8.2 Graph Traversal Patterns

**Find all samples at a location**:
```sql
WITH RECURSIVE sample_path AS (
    SELECT row_id, pid, otype
    FROM pqg WHERE otype = 'GeospatialCoordLocation'
      AND latitude BETWEEN 37 AND 38

    UNION ALL

    SELECT e.s as row_id, n.pid, n.otype
    FROM sample_path sp
    JOIN pqg e ON e.o[1] = sp.row_id AND e.otype = '_edge_'
    JOIN pqg n ON n.row_id = e.s
)
SELECT * FROM sample_path WHERE otype = 'MaterialSampleRecord'
```

### 8.3 Creating PQG Files

Use the reference converter or implement the schema yourself:

```python
import pyarrow as pa
import pyarrow.parquet as pq
from pqg.schemas import NARROW_SCHEMA

# Build your data
table = pa.table({
    'row_id': [...],
    'pid': [...],
    'otype': [...],
    # ... all 40 columns
})

# Validate
errors = NARROW_SCHEMA.validate_schema(table.schema)
if errors:
    raise ValueError(errors)

# Write
pq.write_table(table, 'output.parquet', compression='zstd')
```

---

## 9. FAQ

### For Data Curators

**Q: How is this different from a CSV?**

A: PQG preserves relationships between records. Instead of repeating "California" in every row, you have one Location entity and many edges pointing to it.

**Q: Can I open this in Excel?**

A: Not directly, but you can query it with DuckDB and export to CSV:
```bash
duckdb -c "COPY (SELECT * FROM 'data.parquet' WHERE otype='MaterialSampleRecord') TO 'samples.csv'"
```

**Q: What if I just want a flat table?**

A: Use the Export format or query the Wide format—both give you one row per sample with relationships embedded.

### For Developers

**Q: Why Parquet instead of JSON-LD?**

A: Performance. Parquet enables columnar scans, predicate pushdown, and HTTP range requests. A 300MB Parquet file can answer queries in milliseconds; the equivalent JSON-LD would take seconds.

**Q: Why a single table instead of multiple?**

A: Simplicity and portability. One file = one dataset. JOINs work within the file using row_id.

**Q: How do I handle updates?**

A: PQG is designed for archival/analytical workloads, not OLTP. For updates, regenerate the file or use a database and export to PQG periodically.

### For Graph Database Users

**Q: Can I import this into Neo4j?**

A: Yes. Narrow format maps directly to nodes + relationships:
- Rows with `otype != '_edge_'` → Nodes
- Rows with `otype = '_edge_'` → Relationships

**Q: What about RDF/SPARQL?**

A: PQG is property-graph oriented, not RDF. However, the `pid` column provides IRIs, and edges can be mapped to RDF triples.

---

## Appendix A: Full Column List (Narrow Format)

```
row_id, pid, tcreated, tmodified, otype,
s, p, o, n, altids, geometry,
authorized_by, has_feature_of_interest, affiliation,
sampling_purpose, complies_with, project,
alternate_identifiers, relationship, elevation,
sample_identifier, dc_rights, result_time,
contact_information, latitude, target, role,
scheme_uri, is_part_of, scheme_name, name,
longitude, obfuscated, curation_location,
last_modified_time, access_constraints, place_name,
description, label, thumbnail_url
```

## Appendix B: Reference Implementation

- **Repository**: https://github.com/isamplesorg/pqg
- **Schemas**: `pqg/schemas/narrow.py`, `wide.py`, `export.py`
- **Converter**: `pqg/sql_converter.py`
- **Validation**: `pqg/schemas/base.py`
- **Edge Types**: `pqg/edge_types.py`

## Appendix C: Related Standards

- [GeoParquet](https://geoparquet.org/) - Geospatial data in Parquet
- [Apache Arrow](https://arrow.apache.org/) - Columnar format specification
- [LinkML](https://linkml.io/) - Linked data modeling language
- [iSamples](https://isamplesorg.github.io/) - Physical sample metadata standard
