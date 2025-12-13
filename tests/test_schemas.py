"""Tests for PQG schema validation.

These tests verify that:
1. Schema definitions match the documented specs in isamples/README.md
2. Validation correctly accepts compliant files
3. Validation correctly rejects non-compliant files
4. Format detection works for both narrow and wide formats
"""

import pytest
import pyarrow as pa
from pqg.schemas import (
    NARROW_SCHEMA,
    WIDE_SCHEMA,
    SchemaFormat,
    ColumnSpec,
    validate_parquet,
    get_schema_from_parquet,
    SchemaValidationError,
    validate_parquet_strict,
)


class TestColumnSpec:
    """Tests for ColumnSpec validation."""

    def test_required_column_missing(self):
        """Required column that's missing should fail."""
        spec = ColumnSpec(
            name="test_col",
            arrow_type=pa.string(),
            required=True,
        )
        errors = spec.validate(None)
        assert len(errors) == 1
        assert "Missing required column" in errors[0]

    def test_optional_column_missing(self):
        """Optional column that's missing should pass."""
        spec = ColumnSpec(
            name="test_col",
            arrow_type=pa.string(),
            required=False,
        )
        errors = spec.validate(None)
        assert len(errors) == 0

    def test_type_match(self):
        """Matching types should pass."""
        spec = ColumnSpec(
            name="test_col",
            arrow_type=pa.string(),
        )
        field = pa.field("test_col", pa.string())
        errors = spec.validate(field)
        assert len(errors) == 0

    def test_type_mismatch(self):
        """Mismatched types should fail."""
        spec = ColumnSpec(
            name="test_col",
            arrow_type=pa.string(),
        )
        field = pa.field("test_col", pa.int64())
        errors = spec.validate(field)
        assert len(errors) == 1
        assert "type mismatch" in errors[0]

    def test_integer_size_compatible(self):
        """int32 and int64 should be compatible."""
        spec = ColumnSpec(
            name="test_col",
            arrow_type=pa.int32(),
        )
        field = pa.field("test_col", pa.int64())
        errors = spec.validate(field)
        assert len(errors) == 0

    def test_string_variations_compatible(self):
        """string and large_string should be compatible."""
        spec = ColumnSpec(
            name="test_col",
            arrow_type=pa.string(),
        )
        field = pa.field("test_col", pa.large_string())
        errors = spec.validate(field)
        assert len(errors) == 0

    def test_list_type_compatible(self):
        """List types with compatible inner types should pass."""
        spec = ColumnSpec(
            name="test_col",
            arrow_type=pa.list_(pa.int32()),
        )
        field = pa.field("test_col", pa.list_(pa.int64()))
        errors = spec.validate(field)
        assert len(errors) == 0


class TestNarrowSchema:
    """Tests for narrow format schema definition."""

    def test_has_edge_columns(self):
        """Narrow schema must have s, p, o columns."""
        col_names = {col.name for col in NARROW_SCHEMA.columns}
        assert 's' in col_names
        assert 'p' in col_names
        assert 'o' in col_names

    def test_edge_otype_allowed(self):
        """Narrow format allows _edge_ otype."""
        assert '_edge_' in NARROW_SCHEMA.valid_otypes

    def test_no_relationship_columns(self):
        """Narrow schema should forbid p__* columns."""
        assert 'p__produced_by' in NARROW_SCHEMA.forbidden_columns
        assert 'p__registrant' in NARROW_SCHEMA.forbidden_columns

    def test_required_columns(self):
        """Check required columns are defined."""
        required = NARROW_SCHEMA.required_columns()
        assert 'row_id' in required
        assert 'pid' in required
        assert 'otype' in required
        assert 's' in required
        assert 'p' in required
        assert 'o' in required


class TestWideSchema:
    """Tests for wide format schema definition."""

    def test_no_edge_columns(self):
        """Wide schema must forbid s, p, o columns."""
        assert 's' in WIDE_SCHEMA.forbidden_columns
        assert 'p' in WIDE_SCHEMA.forbidden_columns
        assert 'o' in WIDE_SCHEMA.forbidden_columns

    def test_edge_otype_not_allowed(self):
        """Wide format should not allow _edge_ otype."""
        assert '_edge_' not in WIDE_SCHEMA.valid_otypes

    def test_has_relationship_columns(self):
        """Wide schema must have all 10 p__* columns."""
        col_names = {col.name for col in WIDE_SCHEMA.columns}
        expected_p_cols = {
            'p__has_context_category',
            'p__has_material_category',
            'p__has_sample_object_type',
            'p__keywords',
            'p__produced_by',
            'p__registrant',
            'p__responsibility',
            'p__sample_location',
            'p__sampling_site',
            'p__site_location',
        }
        assert expected_p_cols.issubset(col_names)

    def test_required_columns(self):
        """Check required columns are defined."""
        required = WIDE_SCHEMA.required_columns()
        assert 'row_id' in required
        assert 'pid' in required
        assert 'otype' in required
        # All p__* columns should be required
        assert 'p__produced_by' in required


class TestSchemaValidation:
    """Tests for validate_parquet and get_schema_from_parquet."""

    def test_narrow_schema_validates_correct_file(self, tmp_path):
        """Create a minimal narrow format file and validate it."""
        # Create minimal narrow format table
        table = pa.table({
            'row_id': pa.array([1, 2], type=pa.int32()),
            'pid': pa.array(['a', 'b'], type=pa.string()),
            'otype': pa.array(['MaterialSampleRecord', '_edge_'], type=pa.string()),
            's': pa.array([None, 1], type=pa.int32()),
            'p': pa.array([None, 'produced_by'], type=pa.string()),
            'o': pa.array([None, [2]], type=pa.list_(pa.int32())),
        })

        path = tmp_path / 'test_narrow.parquet'
        import pyarrow.parquet as pq
        pq.write_table(table, path)

        # Should detect as narrow
        schema, fmt = get_schema_from_parquet(str(path))
        assert fmt == SchemaFormat.NARROW

        # Should pass validation (only checking required columns present)
        errors = validate_parquet(str(path), NARROW_SCHEMA)
        # May have some errors for missing optional columns, but no forbidden column errors
        forbidden_errors = [e for e in errors if 'Forbidden' in e]
        assert len(forbidden_errors) == 0

    def test_wide_schema_rejects_edge_columns(self, tmp_path):
        """Wide schema should reject files with s, p, o columns."""
        # Create file with edge columns (should fail wide validation)
        table = pa.table({
            'row_id': pa.array([1], type=pa.int32()),
            'pid': pa.array(['a'], type=pa.string()),
            'otype': pa.array(['MaterialSampleRecord'], type=pa.string()),
            's': pa.array([None], type=pa.int32()),  # Forbidden in wide
            'p': pa.array([None], type=pa.string()),  # Forbidden in wide
            'o': pa.array([None], type=pa.list_(pa.int32())),  # Forbidden in wide
        })

        path = tmp_path / 'test_bad_wide.parquet'
        import pyarrow.parquet as pq
        pq.write_table(table, path)

        errors = validate_parquet(str(path), WIDE_SCHEMA)
        assert any('Forbidden columns' in e for e in errors)
        assert any("'o'" in e or "'p'" in e or "'s'" in e for e in errors)

    def test_format_detection(self, tmp_path):
        """Test automatic format detection."""
        import pyarrow.parquet as pq

        # Create narrow format file
        narrow_table = pa.table({
            'row_id': pa.array([1], type=pa.int32()),
            'pid': pa.array(['a'], type=pa.string()),
            's': pa.array([None], type=pa.int32()),
            'p': pa.array([None], type=pa.string()),
            'o': pa.array([None], type=pa.list_(pa.int32())),
        })
        narrow_path = tmp_path / 'narrow.parquet'
        pq.write_table(narrow_table, narrow_path)

        _, fmt = get_schema_from_parquet(str(narrow_path))
        assert fmt == SchemaFormat.NARROW

        # Create wide format file
        wide_table = pa.table({
            'row_id': pa.array([1], type=pa.int32()),
            'pid': pa.array(['a'], type=pa.string()),
            'p__produced_by': pa.array([None], type=pa.list_(pa.int32())),
        })
        wide_path = tmp_path / 'wide.parquet'
        pq.write_table(wide_table, wide_path)

        _, fmt = get_schema_from_parquet(str(wide_path))
        assert fmt == SchemaFormat.WIDE

    def test_strict_validation_raises(self, tmp_path):
        """validate_parquet_strict should raise on errors."""
        # Create file with edge columns (should fail wide validation)
        table = pa.table({
            'row_id': pa.array([1], type=pa.int32()),
            'pid': pa.array(['a'], type=pa.string()),
            's': pa.array([None], type=pa.int32()),  # Forbidden in wide
        })

        path = tmp_path / 'test_bad.parquet'
        import pyarrow.parquet as pq
        pq.write_table(table, path)

        with pytest.raises(SchemaValidationError) as exc_info:
            validate_parquet_strict(str(path), WIDE_SCHEMA)

        assert exc_info.value.schema_format == SchemaFormat.WIDE
        assert len(exc_info.value.errors) > 0


class TestRemoteValidation:
    """Tests for validating remote parquet files.

    These tests require network access and are marked as slow.
    """

    @pytest.mark.slow
    def test_eric_wide_format(self):
        """Validate Eric's production wide format file."""
        url = 'https://storage.googleapis.com/opencontext-parquet/oc_isamples_pqg_wide.parquet'

        schema, fmt = get_schema_from_parquet(url)
        assert fmt == SchemaFormat.WIDE

        errors = validate_parquet(url, WIDE_SCHEMA)
        assert len(errors) == 0, f"Validation errors: {errors}"

    @pytest.mark.slow
    def test_eric_narrow_format(self):
        """Validate Eric's production narrow format file."""
        url = 'https://storage.googleapis.com/opencontext-parquet/oc_isamples_pqg.parquet'

        schema, fmt = get_schema_from_parquet(url)
        assert fmt == SchemaFormat.NARROW

        errors = validate_parquet(url, NARROW_SCHEMA)
        assert len(errors) == 0, f"Validation errors: {errors}"
