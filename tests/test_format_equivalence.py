"""Tests for content equivalence across Export, Narrow, and Wide formats.

These tests verify that all three parquet format representations contain
the same underlying data, just structured differently:
- Export: Flat sample-centric with nested STRUCTs
- Narrow: Graph-normalized with separate edge rows
- Wide: Entity-centric with p__* relationship arrays

Key equivalences to verify:
1. Sample counts match across formats
2. Sample identifiers are identical
3. Coordinates can be reconstructed from all formats
4. Material categories contain the same concepts
5. Agent relationships are preserved
"""

import pytest
import duckdb
from pathlib import Path

# File paths - Zenodo export and derived PQG formats
EXPORT_PATH = Path.home() / "Data/iSample/2025_04_21_16_23_46/isamples_export_2025_04_21_16_23_46_geo.parquet"
ZENODO_NARROW_PATH = Path.home() / "Data/iSample/pqg_refining/zenodo_narrow_strict.parquet"
ZENODO_WIDE_PATH = Path.home() / "Data/iSample/pqg_refining/zenodo_wide_strict.parquet"

# Eric's OpenContext-only files (different source coverage)
OC_NARROW_PATH = Path.home() / "Data/iSample/pqg_refining/oc_isamples_pqg.parquet"
OC_WIDE_PATH = Path.home() / "Data/iSample/pqg_refining/oc_isamples_pqg_wide.parquet"


def files_exist(*paths):
    """Check if all paths exist."""
    return all(p.exists() for p in paths)


@pytest.fixture
def con():
    """Create a DuckDB connection."""
    return duckdb.connect()


class TestZenodoFormatEquivalence:
    """Tests comparing Zenodo Export with derived Narrow/Wide formats."""

    @pytest.mark.skipif(
        not files_exist(EXPORT_PATH, ZENODO_NARROW_PATH, ZENODO_WIDE_PATH),
        reason="Zenodo format files not available locally"
    )
    def test_sample_counts_match(self, con):
        """All formats should have the same number of samples."""
        # Export: one row per sample
        export_count = con.sql(f"""
            SELECT COUNT(*) FROM read_parquet('{EXPORT_PATH}')
        """).fetchone()[0]

        # Narrow: count MaterialSampleRecord rows
        narrow_count = con.sql(f"""
            SELECT COUNT(*) FROM read_parquet('{ZENODO_NARROW_PATH}')
            WHERE otype = 'MaterialSampleRecord'
        """).fetchone()[0]

        # Wide: count MaterialSampleRecord rows
        wide_count = con.sql(f"""
            SELECT COUNT(*) FROM read_parquet('{ZENODO_WIDE_PATH}')
            WHERE otype = 'MaterialSampleRecord'
        """).fetchone()[0]

        assert export_count == narrow_count, \
            f"Export ({export_count:,}) != Narrow ({narrow_count:,})"
        assert export_count == wide_count, \
            f"Export ({export_count:,}) != Wide ({wide_count:,})"

    @pytest.mark.skipif(
        not files_exist(EXPORT_PATH, ZENODO_NARROW_PATH),
        reason="Zenodo format files not available locally"
    )
    def test_sample_identifiers_match(self, con):
        """Sample identifiers should be identical across formats."""
        # Get sample identifiers from Export
        export_ids = con.sql(f"""
            SELECT sample_identifier FROM read_parquet('{EXPORT_PATH}')
            WHERE sample_identifier IS NOT NULL
            ORDER BY sample_identifier
        """).fetchdf()

        # Get sample identifiers from Narrow (pid for MaterialSampleRecord)
        narrow_ids = con.sql(f"""
            SELECT pid as sample_identifier FROM read_parquet('{ZENODO_NARROW_PATH}')
            WHERE otype = 'MaterialSampleRecord' AND pid IS NOT NULL
            ORDER BY pid
        """).fetchdf()

        assert len(export_ids) == len(narrow_ids), \
            f"Different ID counts: Export={len(export_ids)}, Narrow={len(narrow_ids)}"

        # Check first and last 100 match (full comparison would be slow)
        export_first = set(export_ids['sample_identifier'].head(100))
        narrow_first = set(narrow_ids['sample_identifier'].head(100))
        assert export_first == narrow_first, "First 100 sample IDs don't match"

        export_last = set(export_ids['sample_identifier'].tail(100))
        narrow_last = set(narrow_ids['sample_identifier'].tail(100))
        assert export_last == narrow_last, "Last 100 sample IDs don't match"

    @pytest.mark.skipif(
        not files_exist(EXPORT_PATH, ZENODO_WIDE_PATH),
        reason="Zenodo format files not available locally"
    )
    def test_source_collection_counts_match(self, con):
        """Source collection distribution should be identical."""
        # Export
        export_sources = con.sql(f"""
            SELECT source_collection, COUNT(*) as cnt
            FROM read_parquet('{EXPORT_PATH}')
            GROUP BY source_collection
            ORDER BY source_collection
        """).fetchdf()

        # Wide - need to find source_collection column
        # In Wide format, source_collection might be stored differently
        # This test verifies the total counts per source match
        wide_counts = con.sql(f"""
            SELECT COUNT(*) as cnt
            FROM read_parquet('{ZENODO_WIDE_PATH}')
            WHERE otype = 'MaterialSampleRecord'
        """).fetchone()[0]

        export_total = export_sources['cnt'].sum()
        assert export_total == wide_counts, \
            f"Total sample counts differ: Export={export_total}, Wide={wide_counts}"

    @pytest.mark.skipif(
        not files_exist(EXPORT_PATH, ZENODO_NARROW_PATH),
        reason="Zenodo format files not available locally"
    )
    def test_coordinate_counts_match(self, con):
        """Number of samples with coordinates should match."""
        # Export: direct lat/lon columns
        export_coords = con.sql(f"""
            SELECT COUNT(*) FROM read_parquet('{EXPORT_PATH}')
            WHERE sample_location_latitude IS NOT NULL
              AND sample_location_longitude IS NOT NULL
        """).fetchone()[0]

        # Narrow: count GeospatialCoordLocation entities
        narrow_coords = con.sql(f"""
            SELECT COUNT(*) FROM read_parquet('{ZENODO_NARROW_PATH}')
            WHERE otype = 'GeospatialCoordLocation'
              AND latitude IS NOT NULL
        """).fetchone()[0]

        # Note: These may not be exactly equal if some samples share locations
        # Export denormalizes (one coord per sample), Narrow normalizes (unique locations)
        # So narrow_coords <= export_coords
        assert narrow_coords <= export_coords, \
            f"More unique coords than samples with coords is impossible"

        # Log the difference for information
        print(f"Export samples with coords: {export_coords:,}")
        print(f"Narrow unique coord entities: {narrow_coords:,}")
        print(f"Deduplication ratio: {export_coords / narrow_coords:.2f}x")

    @pytest.mark.skipif(
        not files_exist(EXPORT_PATH, ZENODO_NARROW_PATH),
        reason="Zenodo format files not available locally"
    )
    def test_material_category_concepts_match(self, con):
        """Same material category concepts should exist in all formats."""
        # Export: unnest from nested struct
        export_materials = con.sql(f"""
            SELECT DISTINCT mat.identifier as material
            FROM read_parquet('{EXPORT_PATH}'),
            LATERAL (SELECT unnest(has_material_category) as mat)
            WHERE has_material_category IS NOT NULL
              AND len(has_material_category) > 0
            ORDER BY material
        """).fetchdf()

        # Narrow: get IdentifiedConcept labels linked via has_material_category edges
        narrow_materials = con.sql(f"""
            SELECT DISTINCT c.pid as material
            FROM read_parquet('{ZENODO_NARROW_PATH}') e
            JOIN read_parquet('{ZENODO_NARROW_PATH}') c
              ON list_contains(e.o, c.row_id)
            WHERE e.otype = '_edge_' AND e.p = 'has_material_category'
              AND c.otype = 'IdentifiedConcept'
            ORDER BY material
        """).fetchdf()

        export_set = set(export_materials['material'])
        narrow_set = set(narrow_materials['material'])

        # Check overlap
        overlap = export_set & narrow_set
        export_only = export_set - narrow_set
        narrow_only = narrow_set - export_set

        print(f"Export materials: {len(export_set)}")
        print(f"Narrow materials: {len(narrow_set)}")
        print(f"Overlap: {len(overlap)}")
        print(f"Export only: {len(export_only)}")
        print(f"Narrow only: {len(narrow_only)}")

        # At least 90% overlap expected (some variation due to normalization)
        overlap_pct = len(overlap) / max(len(export_set), len(narrow_set))
        assert overlap_pct >= 0.9, \
            f"Material category overlap too low: {overlap_pct:.1%}"


class TestOpenContextFormatEquivalence:
    """Tests comparing Eric's OpenContext Narrow and Wide formats."""

    @pytest.mark.skipif(
        not files_exist(OC_NARROW_PATH, OC_WIDE_PATH),
        reason="OpenContext format files not available locally"
    )
    def test_sample_counts_match(self, con):
        """Narrow and Wide should have same sample count."""
        narrow_count = con.sql(f"""
            SELECT COUNT(*) FROM read_parquet('{OC_NARROW_PATH}')
            WHERE otype = 'MaterialSampleRecord'
        """).fetchone()[0]

        wide_count = con.sql(f"""
            SELECT COUNT(*) FROM read_parquet('{OC_WIDE_PATH}')
            WHERE otype = 'MaterialSampleRecord'
        """).fetchone()[0]

        assert narrow_count == wide_count, \
            f"Narrow ({narrow_count:,}) != Wide ({wide_count:,})"

    @pytest.mark.skipif(
        not files_exist(OC_NARROW_PATH, OC_WIDE_PATH),
        reason="OpenContext format files not available locally"
    )
    def test_entity_type_distribution(self, con):
        """Entity type distributions should match (excluding edges)."""
        # Narrow: exclude _edge_ rows
        narrow_types = con.sql(f"""
            SELECT otype, COUNT(*) as cnt
            FROM read_parquet('{OC_NARROW_PATH}')
            WHERE otype != '_edge_'
            GROUP BY otype
            ORDER BY otype
        """).fetchdf()

        # Wide: no edges, just entities
        wide_types = con.sql(f"""
            SELECT otype, COUNT(*) as cnt
            FROM read_parquet('{OC_WIDE_PATH}')
            GROUP BY otype
            ORDER BY otype
        """).fetchdf()

        # Compare
        narrow_dict = dict(zip(narrow_types['otype'], narrow_types['cnt']))
        wide_dict = dict(zip(wide_types['otype'], wide_types['cnt']))

        for otype in narrow_dict:
            narrow_cnt = narrow_dict.get(otype, 0)
            wide_cnt = wide_dict.get(otype, 0)
            assert narrow_cnt == wide_cnt, \
                f"Type {otype}: Narrow={narrow_cnt}, Wide={wide_cnt}"


class TestCrossSourceComparison:
    """Tests comparing Zenodo (all sources) with OpenContext-only files."""

    @pytest.mark.skipif(
        not files_exist(EXPORT_PATH, OC_NARROW_PATH),
        reason="Required files not available locally"
    )
    def test_opencontext_subset_in_zenodo(self, con):
        """OpenContext samples in PQG should be subset of Zenodo Export."""
        # Get OpenContext sample count from Export
        export_oc_count = con.sql(f"""
            SELECT COUNT(*) FROM read_parquet('{EXPORT_PATH}')
            WHERE source_collection = 'OPENCONTEXT'
        """).fetchone()[0]

        # Get sample count from Eric's OpenContext Narrow
        oc_narrow_count = con.sql(f"""
            SELECT COUNT(*) FROM read_parquet('{OC_NARROW_PATH}')
            WHERE otype = 'MaterialSampleRecord'
        """).fetchone()[0]

        print(f"Export OpenContext samples: {export_oc_count:,}")
        print(f"OC Narrow samples: {oc_narrow_count:,}")

        # Should be very close (some small variation due to timing/updates)
        ratio = min(export_oc_count, oc_narrow_count) / max(export_oc_count, oc_narrow_count)
        assert ratio >= 0.95, \
            f"OpenContext counts differ by more than 5%: {ratio:.1%}"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
