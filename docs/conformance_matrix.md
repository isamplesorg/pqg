---
title: "PQG Conformance Matrix"
subtitle: "Which shipped parquet files cover which QUERY_SPEC dimensions"
date: 2026-04-24
version: 0.1
---

## 1. Purpose

[QUERY_SPEC.md](https://github.com/isamplesorg/isamplesorg.github.io/blob/main/query-spec.qmd)
§2 defines the canonical query vocabulary for iSamples — the set of
dimensions that every substrate (DuckDB-WASM, Python/Ibis, Solr) is
expected to bind. This document answers the follow-up question: **for
each of those dimensions, which shipped parquet files actually carry
it, and in what form?**

The goal is to make the bind between spec and substrate verifiable.
Anyone proposing to implement a query dimension can check this table
and know whether the data is there or whether the pipeline needs to
grow first.

Schemas below were verified by `DESCRIBE SELECT *` against live R2
URLs (and local copies where available) on 2026-04-24. If the files
are regenerated with schema changes, this document needs a refresh.

## 2. Shipped parquet files surveyed

| Short name | File | Size | Columns | Row grain |
|---|---|---|---|---|
| `wide` | `isamples_202604_wide.parquet` | 282 MB | 49 | entity (row per sample/event/site/etc) |
| `wide_h3` | `isamples_YYYYMM_wide_h3.parquet` | 292 MB | 52 | same as wide + `h3_res{4,6,8}` |
| `narrow` | `isamples_202512_narrow.parquet` | 820 MB | 40 | graph-normalized (s/p/o + typed cols) |
| `lite` | `isamples_202601_samples_map_lite.parquet` | 60 MB | 9 | sample (display projection) |
| `sample_facets_v2` | `isamples_202601_sample_facets_v2.parquet` | 63 MB | 8 | sample (denormalized facet URIs) |
| `facet_summaries` | `isamples_202601_facet_summaries.parquet` | 2 KB | 4 | (facet_type, facet_value) |
| `facet_cross_filter` | `isamples_202601_facet_cross_filter.parquet` | 6 KB | 7 | (filter, facet_type, facet_value) |
| `h3_summary` | `isamples_202601_h3_summary_res{4,6,8}.parquet` | ≤2.4 MB | 7 | H3 cell |

## 3. Conformance matrix

**Legend**: ✅ = present with matching name · 🔄 = present but renamed
(alias given) · ⚠️ = derivable but not direct (cast or join needed)
· ❌ = absent · — = not applicable to this row grain (e.g., aggregate
files don't carry per-sample identity fields).

### 3.1 Identity and provenance (QUERY_SPEC §2.1)

| Dimension | wide | wide_h3 | narrow | lite | sample_facets_v2 | facet_summaries | facet_cross_filter | h3_summary |
|---|---|---|---|---|---|---|---|---|
| `pid` | ✅ | ✅ | ✅ | ✅ | ✅ | — | — | — |
| `source` | 🔄 `n` | 🔄 `n` | 🔄 `n` | ✅ | ✅ | — (`facet_type='source'`) | — (`filter_source`) | 🔄 `dominant_source` |
| `label` | ✅ | ✅ | ✅ | ✅ | ✅ | — | — | — |
| `description` | ✅ | ✅ | ✅ | ❌ | ✅ | — | — | — |
| `registrant` | ⚠️ edge `p__registrant` → Agent | same | ⚠️ (via s/p/o) | ❌ | ❌ | — | — | — |
| `sourceUpdatedTime` | 🔄 `last_modified_time` (VARCHAR) ⚠️ cast | same | same | ❌ | ❌ | — | — | — |

Note: `wide` also ships a `tmodified: INTEGER` (unix epoch), easier to
filter/sort than `last_modified_time`. QUERY_SPEC v0.2 should pick one.

### 3.2 Classification (QUERY_SPEC §2.2)

| Dimension | wide | wide_h3 | narrow | lite | sample_facets_v2 | facet_summaries | facet_cross_filter | h3_summary |
|---|---|---|---|---|---|---|---|---|
| `material` | 🔄 `p__has_material_category` ⚠️ edge→IdentifiedConcept | same | 🔄 (via s/p/o) | ❌ | ✅ (URI string) | ✅ `facet_type='material'` | ✅ | ❌ |
| `context` | 🔄 `p__has_context_category` ⚠️ edge | same | 🔄 | ❌ | ✅ | ✅ | ✅ | ❌ |
| `specimen` | 🔄 `p__has_sample_object_type` ⚠️ edge, naming drift | same | 🔄 | ❌ | 🔄 `object_type` | ✅ `facet_type='object_type'` | ✅ | ❌ |
| `keywords` | 🔄 `p__keywords` ⚠️ edge | same | 🔄 | ❌ | ❌ | ❌ | ❌ | ❌ |
| `informalClassification` | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |

**Naming drift flagged**: QUERY_SPEC calls it `specimen`
(`hasSpecimenCategory`); every shipped file calls it `object_type`
(`hasSampleObjectType`). This needs resolution in v0.2.

**`informalClassification` is a ghost**: the spec names it but no file
carries it. Either drop from spec or add to pipeline.

### 3.3 Sampling event and site (QUERY_SPEC §2.3)

| Dimension | wide | wide_h3 | narrow | lite | sample_facets_v2 | facet_summaries | facet_cross_filter | h3_summary |
|---|---|---|---|---|---|---|---|---|
| `resultTime` | 🔄 `result_time` (VARCHAR) ⚠️ cast | same | same | ✅ (VARCHAR) | ❌ | ❌ | ❌ | ❌ |
| `resultTimeRange` | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `samplingPurpose` | 🔄 `sampling_purpose` | same | same | ❌ | ❌ | — | — | — |
| `featureOfInterest` | 🔄 `has_feature_of_interest` | same | same | ❌ | ❌ | — | — | — |
| `responsibility` | 🔄 `p__responsibility` ⚠️ edge→Agent | same | 🔄 | ❌ | ❌ | — | — | — |
| `siteLabel` | ⚠️ same file, different `otype='SamplingSite'` row | same | ⚠️ | ❌ | ❌ | — | — | — |
| `siteDescription` | ⚠️ as above | same | ⚠️ | ❌ | ❌ | — | — | — |
| `placeName` | ✅ `place_name` (VARCHAR[]) | same | same | ✅ | 🔄 `place_name` (VARCHAR) | — | — | — |
| `elevation` | ✅ (VARCHAR) ⚠️ cast | same | same | ❌ | ❌ | — | — | — |

**Answers QUERY_SPEC §7 Q2**: yes, `result_time` IS already in `lite`
(as `VARCHAR`). The spec's §2.4 binding table row "`time BETWEEN …` |
TBD — `producedBy_resultTime` not yet in lite parquet" should be
updated to a real DuckDB binding (with a cast).

**`resultTimeRange` is a ghost**: Solr carried it via the `date_range`
plugin; no parquet file has an interval type. Recommend dropping from
spec (§2.3) or changing to "derived from (resultTime, resultTime)" if
useful.

### 3.4 Spatial (QUERY_SPEC §2.4)

| Dimension | wide | wide_h3 | narrow | lite | sample_facets_v2 | facet_summaries | facet_cross_filter | h3_summary |
|---|---|---|---|---|---|---|---|---|
| `latitude` | ✅ | ✅ | ✅ | ✅ | ❌ | — | — | 🔄 `center_lat` |
| `longitude` | ✅ | ✅ | ✅ | ✅ | ❌ | — | — | 🔄 `center_lng` |
| `bbox` | ⚠️ derived (`lat BETWEEN … AND lon BETWEEN …`) | same | same | ⚠️ same | ❌ | — | — | ⚠️ centroid |
| `h3[res4]` | ❌ | ✅ | ❌ | ❌ | ❌ | — | — | ✅ (when `resolution=4`) |
| `h3[res6]` | ❌ | ✅ | ❌ | ❌ | ❌ | — | — | ✅ (when `resolution=6`) |
| `h3[res8]` | ❌ | ✅ | ❌ | ✅ (+ `h3_res8_hex`) | ❌ | — | — | ✅ |

**H3 filtering at res 4 or 6 requires `wide_h3` or `h3_summary`** —
plain `wide` and `narrow` do not carry H3 columns. This has been a
source of confusion.

### 3.5 Curation (QUERY_SPEC §2.5)

| Dimension | wide | wide_h3 | narrow | lite | sample_facets_v2 | facet_summaries | facet_cross_filter | h3_summary |
|---|---|---|---|---|---|---|---|---|
| `curationLocation` | ✅ `curation_location` | same | same | ❌ | ❌ | — | — | — |
| `curationResponsibility` | ⚠️ edge `p__curation` → MaterialSampleCuration | same | ⚠️ via s/p/o | ❌ | ❌ | — | — | — |
| `curationAccessConstraints` | 🔄 `access_constraints` (VARCHAR[]) | same | same | ❌ | ❌ | — | — | — |

## 4. Observations

- **Only `wide` (and its `wide_h3` sibling) and `narrow` are complete
  per-sample substrates.** Every other file is a projection or
  aggregate for a specific use case. Queries that touch multiple
  dimensions of §2.1–§2.5 should target `wide` / `wide_h3`.

- **`lite` is a pure-display projection.** It carries pid, label,
  source, lat/lng, place_name, result_time, and h3_res8 — nothing
  classification-related. Don't try to filter material/context/specimen
  on `lite`.

- **`sample_facets_v2` is the right file for facet-URI filtering**
  (material/context/object_type), not the facet caches. The caches
  carry *counts* keyed by value — not the sample-level membership.

- **The `p__*` columns in `wide` are edge references (arrays of
  row_ids), not denormalized values.** To filter on, say, material
  label "Rock", you need either a JOIN back to the IdentifiedConcept
  rows in the same wide file, OR use `sample_facets_v2` where the
  facet URI is already denormalized as a string.

- **Specimen vs object_type naming drift** is the single biggest
  mismatch between spec and data: three different places (spec, wide,
  sample_facets_v2) use three subtly different names for the same
  vocabulary.

- **Ghosts in the spec**: `informalClassification` and `resultTimeRange`
  exist in QUERY_SPEC.md §2 but no shipped parquet carries them. They
  were in the Solr schema; neither was migrated to the parquet
  pipeline.

- **Ghosts in the data**: the `wide` parquet ships `thumbnail_url` as a
  plain column — this isn't in QUERY_SPEC §2 yet but is core to the
  sample-card projection (§4.2 already mentions it). Spec should
  acknowledge.

## 5. Implications for QUERY_SPEC.md v0.2

1. **Rename or alias**: resolve `specimen` ↔ `object_type`. Either
   rename the spec dimension to `objectType` / `hasSampleObjectType`
   (match the data), or rename the data columns (match the spec).
   Former is cheaper; latter is more semantically clean.
2. **Drop ghosts**: remove `informalClassification` and
   `resultTimeRange` from §2 unless the pipeline will add them.
3. **Document `thumbnail_url`** in §2.1 as an optional-but-shipped
   field (see §4.2 sample card).
4. **Clarify time on lite**: §5.1 binding table for `time BETWEEN …`
   should be updated to show the DuckDB cast, not "TBD".
5. **Pick one modified-time field**: `last_modified_time` (VARCHAR)
   vs `tmodified` (INTEGER epoch) both exist. Spec says
   `sourceUpdatedTime` (instant); pick the epoch column, alias it.
6. **Document the h3 tier column convention**: `wide` does NOT carry
   h3 columns; `wide_h3` and `h3_summary_*` do. This is not currently
   obvious from the spec.

## 6. Implications for shipped files

1. **Consider moving `thumbnail_url` to a sidecar** (the endorsed
   pattern per the sidecar rollout issue). Today it lives in `wide`
   but is populated only for OpenContext; a sidecar per source makes
   the provenance explicit and lets the pipeline grow coverage
   incrementally.
2. **Add `registrant` and `responsibility` as denormalized strings
   somewhere** (perhaps to `lite` or a new "sample card" projection)
   so the sample card can render them without a multi-hop edge join.
3. **Consider adding `object_type` to `lite`** if specimen-type
   filters land in the web Explorer (QUERY_SPEC §7 Q1).

---

Last updated: 2026-04-24 by Claude (reviewed by Raymond Yee)
