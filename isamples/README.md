---
title: iSamples Property Graph
---

# iSamples Property Graph in Parquet

This document describes the parquet representation of iSamples material sample records.

The basic pattern is that rows in the parquet file represent nodes and edges. Each node entry corresponds with an instance of a class in the iSamples model. Columns are the union of columns across all the objects.

The iSamples parquet files are structured with a metadata section (stored in the parquet key-value metadata section), and the data section:

```
KV Metadata
  pqg_version
  pqg_primary_key
  pqg_node_types
  pqg_edge_fields
  pqg_literal_fields
Data
  entry 1
  entry 2
  ...
```

## KV Metadata

| Key | Value |
| -- | -- |
| `pqg_version` | "0.2.0" |
| `pqg_primary_key` | "pid" |
| `pqg_node_types` | JSON dictionary of node types |
| `pqg_edge_fields` | JSON list of edge fields in edge entries |
| `pqg_literal_fields` | JSON list of all literal value fields |


### `pqg_node_types`

```json
{
  "Agent": {
    "name": "name VARCHAR DEFAULT NULL",
    "affiliation": "affiliation VARCHAR DEFAULT NULL",
    "contact_information": "contact_information VARCHAR DEFAULT NULL",
    "role": "role VARCHAR DEFAULT NULL",
    "label": "label VARCHAR DEFAULT NULL",
    "description": "description VARCHAR DEFAULT NULL"
  },
  "IdentifiedConcept": {
    "label": "label VARCHAR DEFAULT NULL",
    "scheme_name": "scheme_name VARCHAR DEFAULT NULL",
    "scheme_uri": "scheme_uri VARCHAR DEFAULT NULL",
    "description": "description VARCHAR DEFAULT NULL"
  },
  "GeospatialCoordLocation": {
    "elevation": "elevation VARCHAR DEFAULT NULL",
    "latitude": "latitude DOUBLE DEFAULT NULL",
    "longitude": "longitude DOUBLE DEFAULT NULL",
    "obfuscated": "obfuscated BOOLEAN ",
    "label": "label VARCHAR DEFAULT NULL",
    "description": "description VARCHAR DEFAULT NULL"
  },
  "SamplingSite": {
    "description": "description VARCHAR DEFAULT NULL",
    "label": "label VARCHAR DEFAULT NULL",
    "place_name": "place_name VARCHAR[]",
    "is_part_of": "is_part_of VARCHAR[]"
  },
  "SamplingEvent": {
    "label": "label VARCHAR DEFAULT NULL",
    "description": "description VARCHAR DEFAULT NULL",
    "has_feature_of_interest": "has_feature_of_interest VARCHAR DEFAULT NULL",
    "project": "project VARCHAR DEFAULT NULL",
    "result_time": "result_time VARCHAR DEFAULT NULL",
    "authorized_by": "authorized_by VARCHAR[]"
  },
  "MaterialSampleCuration": {
    "access_constraints": "access_constraints VARCHAR[]",
    "curation_location": "curation_location VARCHAR DEFAULT NULL",
    "description": "description VARCHAR DEFAULT NULL",
    "label": "label VARCHAR DEFAULT NULL"
  },
  "SampleRelation": {
    "description": "description VARCHAR DEFAULT NULL",
    "label": "label VARCHAR DEFAULT NULL",
    "relationship": "relationship VARCHAR DEFAULT NULL",
    "target": "target VARCHAR DEFAULT NULL"
  },
  "MaterialSampleRecord": {
    "label": "label VARCHAR DEFAULT NULL",
    "last_modified_time": "last_modified_time VARCHAR DEFAULT NULL",
    "description": "description VARCHAR DEFAULT NULL",
    "sample_identifier": "sample_identifier VARCHAR DEFAULT NULL",
    "alternate_identifiers": "alternate_identifiers VARCHAR[]",
    "sampling_purpose": "sampling_purpose VARCHAR DEFAULT NULL",
    "complies_with": "complies_with VARCHAR[]",
    "dc_rights": "dc_rights VARCHAR DEFAULT NULL"
  }
}
```

### `pqg_edge_fields`

```json
[
  "pid",
  "otype",
  "s",
  "p",
  "o",
  "n",
  "altids",
  "geometry"
]
```

### `pqg_literal_fields`

```json
[
  "authorized_by",
  "has_feature_of_interest",
  "affiliation",
  "sampling_purpose",
  "complies_with",
  "project",
  "alternate_identifiers",
  "relationship",
  "elevation",
  "sample_identifier",
  "dc_rights",
  "result_time",
  "contact_information",
  "latitude",
  "target",
  "role",
  "scheme_uri",
  "is_part_of",
  "scheme_name",
  "name",
  "longitude",
  "obfuscated",
  "curation_location",
  "last_modified_time",
  "access_constraints",
  "place_name",
  "description",
  "label",
  "pid",
  "otype",
  "s",
  "p",
  "o",
  "n",
  "altids",
  "geometry"
]
```

These properties are set during the creation of the parquet file.



## Data

The full schema for the data portion is:

```sql
CREATE TABLE node(
    row_id INTEGER PRIMARY KEY DEFAULT nextval('row_id_sequence'),
    pid VARCHAR UNIQUE NOT NULL,
    tcreated INTEGER DEFAULT(CAST(epoch(current_timestamp) AS INTEGER)),
    tmodified INTEGER DEFAULT(CAST(epoch(current_timestamp) AS INTEGER)),
    otype VARCHAR,
    s INTEGER DEFAULT(NULL),
    p VARCHAR DEFAULT(NULL),
    o INTEGER[] DEFAULT(NULL),
    n VARCHAR DEFAULT(NULL),
    altids VARCHAR[] DEFAULT(NULL),
    geometry GEOMETRY DEFAULT(NULL),
    authorized_by VARCHAR[],
    has_feature_of_interest VARCHAR DEFAULT(NULL),
    affiliation VARCHAR DEFAULT(NULL),
    sampling_purpose VARCHAR DEFAULT(NULL),
    complies_with VARCHAR[],
    project VARCHAR DEFAULT(NULL),
    alternate_identifiers VARCHAR[],
    relationship VARCHAR DEFAULT(NULL),
    elevation VARCHAR DEFAULT(NULL),
    sample_identifier VARCHAR DEFAULT(NULL),
    dc_rights VARCHAR DEFAULT(NULL),
    result_time VARCHAR DEFAULT(NULL),
    contact_information VARCHAR DEFAULT(NULL),
    latitude DOUBLE DEFAULT(NULL),
    target VARCHAR DEFAULT(NULL),
    "role" VARCHAR DEFAULT(NULL),
    scheme_uri VARCHAR DEFAULT(NULL),
    is_part_of VARCHAR[],
    scheme_name VARCHAR DEFAULT(NULL),
    "name" VARCHAR DEFAULT(NULL),
    longitude DOUBLE DEFAULT(NULL),
    obfuscated BOOLEAN,
    curation_location VARCHAR DEFAULT(NULL),
    last_modified_time VARCHAR DEFAULT(NULL),
    access_constraints VARCHAR[],
    place_name VARCHAR[],
    description VARCHAR DEFAULT(NULL),
    "label" VARCHAR DEFAULT(NULL),
    thumbnail_url VARCHAR DEFAULT(NULL)
);
```

| Field | Description |
| -- | -- |
| `row_id` | Auto-incrementing integer primary key for performance. |
| `pid` | Unique identifier for the row. This is the globally unique identifier used externally. |
| `tcreated` | Timestmap indicating the time the record in the files was created, not the time that the record was created in the content management system. |
| `tmodified` | Timestmap indicating the time the record was modified in this file. |
| `otype` | Type of object represented by the row. Edges are always called "_edge_". |
| `s` | The subject of the triple statement between two objects (integer row_id reference). |
| `p` | The predicate of the triple statement. |
| `o` | The object or target of the triple statement (array of integer row_id references). |
| `n` | Optional name of the graph the statement exists in. |
| `thumbnail_url` | Optional URL to a thumbnail image for the record. |

The possible values of `otype` are defined by the isamples linkml schema, and are:

```
│ Agent                   │
│ MaterialSampleRecord    │
│ SamplingEvent           │
│ GeospatialCoordLocation │
│ SamplingSite            │
│ IdentifiedConcept       │
│ MaterialSampleCuration  │
│ _edge_                  │
```

The `_edge_` rows provide the relationships between objects. For example, the iSamples metadata model identifies that the [`MaterialSampleRecord`](https://isamplesorg.github.io/metadata/MaterialSampleRecord.html) has a property [`registrant`](https://isamplesorg.github.io/metadata/registrant.html), the values of which are instances of [`Agent`](https://isamplesorg.github.io/metadata/Agent.html).

```mermaid
erDiagram
    MaterialSampleRecord {
        pid msr_1
    }
    Agent {
        pid agent_1
    }
    MaterialSampleRecord }o--o| Agent : registrant
```

Results in the following records (note: s and o now store row_id integers, not PIDs):

| row_id | pid | otype | s | p | o | ...|
| -- | -- | -- | -- | -- | -- | -- |
| 1 | `msr_1` | `MaterialSampleRecord` | null | null | null | |
| 2 | `agent_1` | `Agent` | null | null | null | |
| 3 | `45ef21` | `_edge_` | 1 | `registrant` | [2] | |

Note: In the edge row, the `s` field contains the row_id (1) of the MaterialSampleRecord, and the `o` field contains an array with the row_id (2) of the Agent. The API continues to use PIDs externally for compatibility, but internally uses row_ids for improved performance.

---

## Wide Format Schema

The **wide format** is an alternative serialization optimized for query performance. Instead of storing relationships as separate `_edge_` rows, relationships are stored as `p__*` columns directly on entity rows.

### Key Differences from Narrow Format

| Aspect | Narrow Format | Wide Format |
|--------|---------------|-------------|
| Edge storage | Separate `_edge_` rows | `p__*` columns on entities |
| Columns `s`, `p`, `o` | Present (for edges) | **NOT present** |
| Row count | Higher (includes edge rows) | Lower (entities only) |
| File size | Larger | ~60% smaller |
| Query complexity | More joins | Fewer joins |
| Use case | Full graph operations | Fast analytical queries |

### Wide Format Schema

```sql
CREATE TABLE node_wide(
    -- Core identification (same as narrow)
    row_id INTEGER PRIMARY KEY,
    pid VARCHAR UNIQUE NOT NULL,
    tcreated INTEGER,
    tmodified INTEGER,
    otype VARCHAR,
    n VARCHAR,
    altids VARCHAR[],
    geometry BLOB,

    -- Entity-specific fields (same as narrow)
    authorized_by VARCHAR[],
    has_feature_of_interest VARCHAR,
    affiliation VARCHAR,
    sampling_purpose VARCHAR,
    complies_with VARCHAR[],
    project VARCHAR,
    alternate_identifiers VARCHAR[],
    relationship VARCHAR,
    elevation VARCHAR,
    sample_identifier VARCHAR,
    dc_rights VARCHAR,
    result_time VARCHAR,
    contact_information VARCHAR,
    latitude DOUBLE,
    target VARCHAR,
    "role" VARCHAR,
    scheme_uri VARCHAR,
    is_part_of VARCHAR[],
    scheme_name VARCHAR,
    "name" VARCHAR,
    longitude DOUBLE,
    obfuscated BOOLEAN,
    curation_location VARCHAR,
    last_modified_time VARCHAR,
    access_constraints VARCHAR[],
    place_name VARCHAR[],
    description VARCHAR,
    "label" VARCHAR,
    thumbnail_url VARCHAR,

    -- Relationship columns (WIDE FORMAT ONLY)
    -- These replace the _edge_ rows from narrow format
    p__has_context_category INTEGER[],
    p__has_material_category INTEGER[],
    p__has_sample_object_type INTEGER[],
    p__keywords INTEGER[],
    p__produced_by INTEGER[],
    p__registrant INTEGER[],
    p__responsibility INTEGER[],
    p__sample_location INTEGER[],
    p__sampling_site INTEGER[],
    p__site_location INTEGER[]
);
```

### Relationship Columns (`p__*`)

The `p__*` columns store the same relationship information as `_edge_` rows, but in denormalized form:

| Column | Source Entity | Target Entity | Description |
|--------|---------------|---------------|-------------|
| `p__produced_by` | MaterialSampleRecord | SamplingEvent | Sample was produced by this event |
| `p__has_sample_object_type` | MaterialSampleRecord | IdentifiedConcept | Type of sample object |
| `p__has_material_category` | MaterialSampleRecord | IdentifiedConcept | Material category |
| `p__has_context_category` | MaterialSampleRecord | IdentifiedConcept | Context category |
| `p__keywords` | MaterialSampleRecord | IdentifiedConcept | Keywords |
| `p__registrant` | MaterialSampleRecord | Agent | Who registered the sample |
| `p__sampling_site` | SamplingEvent | SamplingSite | Where sampling occurred |
| `p__sample_location` | SamplingEvent | GeospatialCoordLocation | Exact coordinates |
| `p__responsibility` | SamplingEvent | Agent | Responsible party |
| `p__site_location` | SamplingSite | GeospatialCoordLocation | Site coordinates |

### Wide Format `otype` Values

Unlike narrow format, wide format does **NOT** include `_edge_` as an otype:

```
│ Agent                   │
│ MaterialSampleRecord    │
│ SamplingEvent           │
│ GeospatialCoordLocation │
│ SamplingSite            │
│ IdentifiedConcept       │
│ MaterialSampleCuration  │
```

### Example: Narrow vs Wide

**Narrow format** (3 rows):
| row_id | pid | otype | s | p | o |
|--------|-----|-------|---|---|---|
| 1 | `msr_1` | MaterialSampleRecord | null | null | null |
| 2 | `agent_1` | Agent | null | null | null |
| 3 | `edge_1` | _edge_ | 1 | registrant | [2] |

**Wide format** (2 rows):
| row_id | pid | otype | p__registrant |
|--------|-----|-------|---------------|
| 1 | `msr_1` | MaterialSampleRecord | [2] |
| 2 | `agent_1` | Agent | null |

### Query Pattern Comparison

**Narrow format query** (find sample's registrant):
```sql
SELECT agent.*
FROM samples
JOIN edges ON edges.s = samples.row_id AND edges.p = 'registrant'
JOIN agents ON agents.row_id = ANY(edges.o)
WHERE samples.pid = 'msr_1';
```

**Wide format query** (same result):
```sql
SELECT agents.*
FROM samples
JOIN agents ON agents.row_id = ANY(samples.p__registrant)
WHERE samples.pid = 'msr_1';
```

### When to Use Each Format

- **Narrow**: Graph algorithms, relationship exploration, schema flexibility
- **Wide**: Analytical queries, dashboards, browser-based analysis (DuckDB-WASM)
