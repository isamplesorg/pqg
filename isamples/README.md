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
