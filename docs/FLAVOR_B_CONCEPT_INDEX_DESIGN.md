# Flavor B Concept Index Design

Date: 2026-06-01

Scope: read-only investigation for iSamples Explorer issue #256. No frontend or deployment changes were made.

## Summary

The proposed Flavor B access pattern is sound: build a sample-centric projection
from PQG concept edges so the Explorer can semi-join `pid` against
`concept_uri` without graph joins at query time.

However, the four acceptance-test external URIs do not currently resolve to
`IdentifiedConcept` nodes in the queried PQG files:

- `https://vocab.getty.edu/aat/300387149`
- `https://purl.obolibrary.org/obo/UBERON_0000979`
- `https://purl.obolibrary.org/obo/UBERON_0000981`
- `https://vocab.getty.edu/aat/300263796`

I checked:

- remote wide: `https://data.isamples.org/current/wide.parquet`
- remote narrow: `https://pub-a18234d962364c22a50c787b7ca09fa5.r2.dev/isamples_202512_narrow.parquet`
- local wide cache: `~/Data/iSample/pqg_refining/isamples_202604_wide.parquet`
- local narrow cache: `~/Data/iSample/pqg_refining/zenodo_narrow_2025-12-12.parquet`
- local Flavor A facets mirror: `isamplesorg.github.io/docs/data/isamples_202601_sample_facets_v2.parquet`

The exact URI lookup returns no concept row for all four URIs. A fragment scan
over `pid`, `scheme_uri`, `description`, and `altids` in the local narrow cache
also returns zero rows for the target identifiers.

## Schema Evidence

Narrow PQG has 40 columns. It stores graph edges as rows with:

- `otype = '_edge_'`
- `s BIGINT`
- `p VARCHAR`
- `o INTEGER[]`

Wide PQG has 49 columns. It omits edge rows and stores relationships on entity
rows as `p__*` arrays of target `row_id` values:

- `p__has_context_category BIGINT[]`
- `p__has_material_category BIGINT[]`
- `p__has_sample_object_type BIGINT[]`
- `p__keywords BIGINT[]`
- `p__produced_by INTEGER[]`
- other non-concept relation arrays

The row counts in the local narrow and wide caches align:

| otype | wide rows | narrow entity rows |
|---|---:|---:|
| MaterialSampleRecord | 6,680,932 | 6,680,932 |
| SamplingEvent | 6,354,171 | 6,354,171 |
| GeospatialCoordLocation | 5,980,282 | 5,980,282 |
| MaterialSampleCuration | 720,254 | 720,254 |
| SampleRelation | 501,579 | 501,579 |
| SamplingSite | 386,160 | 386,160 |
| IdentifiedConcept | 55,893 | 55,893 |
| Agent | 50,087 | 50,087 |
| _edge_ | n/a | 80,657,822 |

For concept-bearing edges, narrow confirms the actual path:

| subject_type | predicate | object_type | edge rows |
|---|---|---|---:|
| MaterialSampleRecord | keywords | IdentifiedConcept | 15,055,284 |
| MaterialSampleRecord | has_material_category | IdentifiedConcept | 8,303,906 |
| MaterialSampleRecord | has_sample_object_type | IdentifiedConcept | 7,649,826 |
| MaterialSampleRecord | has_context_category | IdentifiedConcept | 6,533,692 |

So for the current PQG data, concept search should be built from direct
`MaterialSampleRecord -> IdentifiedConcept` paths:

- narrow: `sample row_id -> _edge_.s`, `p IN (...)`, `_edge_.o[] -> concept row_id`
- wide: `sample.p__keywords`, `sample.p__has_context_category`,
  `sample.p__has_material_category`, `sample.p__has_sample_object_type`

I found no `SamplingEvent -> has_context_category -> IdentifiedConcept` rows in
the wide count (`event.has_context_category = 0`), so the event-context multi-hop
path is not carrying these concept links in the current data.

## Acceptance URI Results

| term | external URI | IdentifiedConcept node? | concept label | exact graph sample pids | free-text count |
|---|---|---:|---|---:|---:|
| bucchero | `https://vocab.getty.edu/aat/300387149` | no | n/a | 0 | 2,693 |
| tibia | `https://purl.obolibrary.org/obo/UBERON_0000979` | no | n/a | 0 | 16,577 |
| femur | `https://purl.obolibrary.org/obo/UBERON_0000981` | no | n/a | 0 | 13,388 |
| whorls | `https://vocab.getty.edu/aat/300263796` | no | n/a | 0 | 1,473 |

The free-text counts are real in the local `sample_facets_v2` mirror when
searching across `label + description + place_name`. They do not imply linked
concept URI presence. For example, many `tibia` hits are lexical matches in
description text, not a graph edge to `UBERON_0000979`.

Flavor A correctly returns zero for the four external URIs in
`material/context/object_type`, because those columns contain iSamples-vocab
facet URI strings rather than arbitrary external concept URIs.

## Projection Design

Create a new flat parquet, one row per sample/concept/relation/source:

| column | type | notes |
|---|---|---|
| `pid` | `VARCHAR` | material sample pid; same join key as `sample_facets_v2.pid` |
| `source` | `VARCHAR` | sample source graph/provider, copied from sample `n` or normalized from pid/source policy |
| `concept_uri` | `VARCHAR` | `IdentifiedConcept.pid`; exact URI search key |
| `concept_label` | `VARCHAR` | quoted PQG `label` aliased as `concept_label` during build |
| `relation_type` | `VARCHAR` | one of `keywords`, `has_context_category`, `has_material_category`, `has_sample_object_type` |
| `concept_row_id` | `BIGINT` | optional debug/provenance field; can be omitted from frontend copy |
| `concept_scheme_uri` | `VARCHAR` | optional, currently often null for keyword concepts |
| `concept_scheme_name` | `VARCHAR` | optional, currently often null for keyword concepts |

Recommended filename:

`isamples_YYYYMM_sample_concepts.parquet`

Recommended publish location:

- build artifact in `pqg`
- publish beside current Explorer parquet artifacts under R2/data.isamples.org,
  for example `https://data.isamples.org/current/sample_concepts.parquet`
- mirror into `isamplesorg.github.io/docs/data/` only if the file size is small
  enough for repository delivery; otherwise keep it as a remote data artifact
  like `wide.parquet`

## Build Approach

Use DuckDB over wide PQG for the production build. Wide is the better source for
this projection because it already materializes edge targets as arrays on sample
rows and avoids scanning 80.7M narrow edge rows.

Sketch:

```sql
INSTALL httpfs;
LOAD httpfs;

CREATE OR REPLACE VIEW wide AS
SELECT *
FROM read_parquet('https://data.isamples.org/current/wide.parquet');

COPY (
  WITH sample_concepts AS (
    SELECT
      s.pid,
      s.n AS source,
      'keywords' AS relation_type,
      c.row_id AS concept_row_id,
      c.pid AS concept_uri,
      c."label" AS concept_label,
      c.scheme_uri AS concept_scheme_uri,
      c.scheme_name AS concept_scheme_name
    FROM wide s
    JOIN wide c
      ON c.otype = 'IdentifiedConcept'
     AND list_contains(s.p__keywords, c.row_id)
    WHERE s.otype = 'MaterialSampleRecord'

    UNION ALL
    SELECT s.pid, s.n, 'has_context_category', c.row_id, c.pid,
           c."label", c.scheme_uri, c.scheme_name
    FROM wide s
    JOIN wide c
      ON c.otype = 'IdentifiedConcept'
     AND list_contains(s.p__has_context_category, c.row_id)
    WHERE s.otype = 'MaterialSampleRecord'

    UNION ALL
    SELECT s.pid, s.n, 'has_material_category', c.row_id, c.pid,
           c."label", c.scheme_uri, c.scheme_name
    FROM wide s
    JOIN wide c
      ON c.otype = 'IdentifiedConcept'
     AND list_contains(s.p__has_material_category, c.row_id)
    WHERE s.otype = 'MaterialSampleRecord'

    UNION ALL
    SELECT s.pid, s.n, 'has_sample_object_type', c.row_id, c.pid,
           c."label", c.scheme_uri, c.scheme_name
    FROM wide s
    JOIN wide c
      ON c.otype = 'IdentifiedConcept'
     AND list_contains(s.p__has_sample_object_type, c.row_id)
    WHERE s.otype = 'MaterialSampleRecord'
  )
  SELECT DISTINCT *
  FROM sample_concepts
  WHERE concept_uri IS NOT NULL
) TO 'isamples_YYYYMM_sample_concepts.parquet'
  (FORMAT PARQUET, COMPRESSION ZSTD);
```

If build time is high, use the narrow edge table as the build source instead:
filter `_edge_` rows to the four concept predicates, unnest `o`, join subjects
and concepts by `row_id`, then write the same output schema. Narrow is more
canonical but larger; wide is the pragmatic build input.

## Row Estimate

Current wide array-length counts:

| relation_type | samples with values | projection rows |
|---|---:|---:|
| `keywords` | 6,523,490 | 15,055,284 |
| `has_context_category` | 6,532,832 | 6,533,692 |
| `has_material_category` | 6,672,599 | 8,303,906 |
| `has_sample_object_type` | 6,680,932 | 7,649,826 |
| `event.has_context_category` | 0 | 0 |

Expected projection size before exact duplicate removal:

`15,055,284 + 6,533,692 + 8,303,906 + 7,649,826 = 37,542,708` rows.

This is large but still a narrow, two-key lookup table. It should compress well
because `concept_uri`, `concept_label`, and `relation_type` are highly repeated.

## Separate File vs UNION

Recommendation: ship a separate `sample_concepts` parquet first, not a UNIONed
replacement for `sample_facets_v2`.

Reasons:

- Flavor A facets are compact and stable for `material/context/object_type`.
- Flavor B is much wider: about 37.5M relation rows before dedupe.
- Keeping the files separate lets the frontend load Flavor B only when the user
  searches by explicit concept URI or when described-by search is enabled.
- It avoids changing current facet semantics while Eric/Andrea decide which
  relations count as "described by."

The frontend can treat Flavor B like a sidecar:

```sql
SELECT DISTINCT f.*
FROM sample_facets_v2 f
SEMI JOIN sample_concepts c
  ON c.pid = f.pid
WHERE c.concept_uri = ?
  AND c.relation_type IN (...);
```

Later, if usage proves that described-by search should always be available, a
view or materialized UNION can combine iSamples-vocab facet URIs and external
concept URIs into one logical concept table.

## Open Questions for Eric

1. Exact URI only, or broader expansion?
   - Should `described-by=https://vocab.getty.edu/aat/300387149` match only that
     exact concept URI, or also narrower/broader Getty AAT terms?
   - Same question for UBERON anatomy hierarchy.

2. Which relations count as "described by"?
   - Minimal: `keywords`
   - Broader: `keywords`, `has_context_category`, `has_material_category`,
     `has_sample_object_type`
   - Do iSamples-vocab category fields and arbitrary external keyword concepts
     need separate UI labels or ranking?

3. Where should the four acceptance-test external URI nodes come from?
   - They are not present in the current queried PQG files as exact
     `IdentifiedConcept.pid` values.
   - If they are expected from OpenContext linked-data enrichment, the PQG build
     needs to preserve those URIs instead of only lexical keyword concepts.

4. Should label search remain a fallback?
   - The text hits are abundant, but they are lexical approximations. They should
     not be presented as exact linked-data URI matches unless the graph contains
     the URI edge.
