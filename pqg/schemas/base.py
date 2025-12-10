"""Base schema definitions and validation utilities for PQG parquet formats.

This module provides the foundation for schema validation, including:
- ColumnSpec: Definition of expected column properties
- PQGSchema: Base class for schema definitions
- validate_parquet(): Validate a parquet file against a schema
- get_schema_from_parquet(): Detect which schema a parquet file uses
"""

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import List, Dict, Optional, Set, Tuple, Any
import pyarrow as pa
import pyarrow.parquet as pq


class SchemaFormat(Enum):
    """PQG serialization formats."""
    NARROW = "narrow"
    WIDE = "wide"
    EXPORT = "export"
    UNKNOWN = "unknown"


class SchemaValidationError(Exception):
    """Raised when a parquet file fails schema validation."""

    def __init__(self, errors: List[str], schema_format: SchemaFormat):
        self.errors = errors
        self.schema_format = schema_format
        super().__init__(f"Schema validation failed for {schema_format.value}: {errors}")


@dataclass
class ColumnSpec:
    """Specification for a single column in a PQG schema.

    Attributes:
        name: Column name
        arrow_type: Expected PyArrow data type
        nullable: Whether NULL values are allowed
        required: Whether the column must be present
        description: Human-readable description
    """
    name: str
    arrow_type: pa.DataType
    nullable: bool = True
    required: bool = True
    description: str = ""

    def validate(self, actual_field: Optional[pa.Field]) -> List[str]:
        """Validate an actual PyArrow field against this spec.

        Returns:
            List of validation error messages (empty if valid)
        """
        errors = []

        if actual_field is None:
            if self.required:
                errors.append(f"Missing required column: {self.name}")
            return errors

        # Check type compatibility
        if not self._types_compatible(actual_field.type, self.arrow_type):
            errors.append(
                f"Column '{self.name}' type mismatch: "
                f"expected {self.arrow_type}, got {actual_field.type}"
            )

        return errors

    def _types_compatible(self, actual: pa.DataType, expected: pa.DataType) -> bool:
        """Check if actual type is compatible with expected type.

        Handles common variations like:
        - BLOB vs binary
        - large_string vs string
        - list vs large_list
        """
        # Exact match
        if actual.equals(expected):
            return True

        # Binary variations (BLOB, binary, large_binary)
        if pa.types.is_binary(actual) and pa.types.is_binary(expected):
            return True
        if pa.types.is_large_binary(actual) and pa.types.is_binary(expected):
            return True
        if pa.types.is_binary(actual) and pa.types.is_large_binary(expected):
            return True

        # String variations (string, large_string)
        if pa.types.is_string(actual) and pa.types.is_string(expected):
            return True
        if pa.types.is_large_string(actual) and pa.types.is_string(expected):
            return True
        if pa.types.is_string(actual) and pa.types.is_large_string(expected):
            return True

        # List variations - check inner types
        if pa.types.is_list(actual) or pa.types.is_large_list(actual):
            if pa.types.is_list(expected) or pa.types.is_large_list(expected):
                actual_inner = actual.value_type
                expected_inner = expected.value_type
                return self._types_compatible(actual_inner, expected_inner)

        # Integer size variations (int32 vs int64)
        if pa.types.is_integer(actual) and pa.types.is_integer(expected):
            # Allow wider integers (int64 is compatible with int32 expectation)
            return True

        # Float variations
        if pa.types.is_floating(actual) and pa.types.is_floating(expected):
            return True

        # Timestamp variations (different precisions: us, ns, ms, s)
        if pa.types.is_timestamp(actual) and pa.types.is_timestamp(expected):
            # Allow different precisions, but check timezone compatibility
            actual_tz = actual.tz
            expected_tz = expected.tz
            # Both have tz, both don't have tz, or expected doesn't care
            if actual_tz == expected_tz:
                return True
            if expected_tz is None:
                return True  # Expected doesn't require specific tz
            if actual_tz is not None and expected_tz is not None:
                return True  # Both have tz (may be different representations)
            return False

        return False


@dataclass
class PQGSchema:
    """Base class for PQG schema definitions.

    Subclasses define NARROW_SCHEMA and WIDE_SCHEMA with their
    specific column specifications.
    """
    name: str
    format: SchemaFormat
    version: str
    columns: List[ColumnSpec] = field(default_factory=list)
    description: str = ""

    # Columns that MUST NOT be present
    forbidden_columns: Set[str] = field(default_factory=set)

    # Valid otype values for this format
    valid_otypes: Set[str] = field(default_factory=set)

    def get_column(self, name: str) -> Optional[ColumnSpec]:
        """Get column spec by name."""
        for col in self.columns:
            if col.name == name:
                return col
        return None

    def required_columns(self) -> List[str]:
        """Get list of required column names."""
        return [col.name for col in self.columns if col.required]

    def optional_columns(self) -> List[str]:
        """Get list of optional column names."""
        return [col.name for col in self.columns if not col.required]

    def to_pyarrow_schema(self) -> pa.Schema:
        """Convert to PyArrow schema for parquet writing."""
        fields = []
        for col in self.columns:
            fields.append(pa.field(col.name, col.arrow_type, nullable=col.nullable))
        return pa.schema(fields)

    def validate_schema(self, actual_schema: pa.Schema) -> List[str]:
        """Validate a PyArrow schema against this spec.

        Returns:
            List of validation error messages (empty if valid)
        """
        errors = []

        # Check for forbidden columns
        actual_names = set(actual_schema.names)
        forbidden_present = self.forbidden_columns & actual_names
        if forbidden_present:
            errors.append(
                f"Forbidden columns present for {self.format.value} format: "
                f"{sorted(forbidden_present)}"
            )

        # Check each expected column
        for col_spec in self.columns:
            try:
                actual_field = actual_schema.field(col_spec.name)
            except KeyError:
                actual_field = None

            col_errors = col_spec.validate(actual_field)
            errors.extend(col_errors)

        return errors


def _get_schema_via_duckdb(parquet_path: str) -> pa.Schema:
    """Get PyArrow schema using DuckDB (handles HTTP URLs)."""
    import duckdb
    con = duckdb.connect()

    # DuckDB can read remote parquet files and return Arrow schema
    result = con.execute(f"""
        SELECT * FROM read_parquet('{parquet_path}') LIMIT 0
    """).fetch_arrow_table()
    return result.schema


def _is_remote_path(path: str) -> bool:
    """Check if path is a remote URL."""
    return path.startswith('http://') or path.startswith('https://') or path.startswith('gs://')


def get_schema_from_parquet(parquet_path: str) -> Tuple[pa.Schema, SchemaFormat]:
    """Detect which schema format a parquet file uses.

    Detection logic:
    - If 's', 'p', 'o' columns present AND '_edge_' otype exists → NARROW
    - If p__* columns present AND no 's', 'p', 'o' → WIDE
    - Otherwise → UNKNOWN

    Args:
        parquet_path: Path to parquet file (local path or HTTP/GCS URL)

    Returns:
        Tuple of (PyArrow schema, detected format)
    """
    # Use DuckDB for remote files, PyArrow for local
    if _is_remote_path(parquet_path):
        schema = _get_schema_via_duckdb(parquet_path)
    else:
        pf = pq.ParquetFile(parquet_path)
        schema = pf.schema_arrow

    column_names = set(schema.names)

    # Check for edge columns (narrow format indicators)
    has_edge_columns = {'s', 'p', 'o'}.issubset(column_names)

    # Check for p__* columns (wide format indicators)
    has_relationship_columns = any(
        name.startswith('p__') for name in column_names
    )

    if has_edge_columns and not has_relationship_columns:
        return schema, SchemaFormat.NARROW
    elif has_relationship_columns and not has_edge_columns:
        return schema, SchemaFormat.WIDE
    elif has_edge_columns and has_relationship_columns:
        # Both present - this shouldn't happen
        return schema, SchemaFormat.UNKNOWN
    else:
        return schema, SchemaFormat.UNKNOWN


def validate_parquet(
    parquet_path: str,
    expected_schema: PQGSchema,
    check_data: bool = False
) -> List[str]:
    """Validate a parquet file against a PQG schema.

    Args:
        parquet_path: Path to parquet file (local or remote URL)
        expected_schema: Schema to validate against
        check_data: If True, also validate otype values (slower)

    Returns:
        List of validation error messages (empty if valid)

    Raises:
        FileNotFoundError: If local parquet file doesn't exist
    """
    # Only check file existence for local paths
    if not _is_remote_path(parquet_path):
        path = Path(parquet_path)
        if not path.exists():
            raise FileNotFoundError(f"Parquet file not found: {parquet_path}")

    errors = []

    # Read schema - use DuckDB for remote, PyArrow for local
    if _is_remote_path(parquet_path):
        actual_schema = _get_schema_via_duckdb(parquet_path)
    else:
        pf = pq.ParquetFile(parquet_path)
        actual_schema = pf.schema_arrow

    # Validate schema structure
    schema_errors = expected_schema.validate_schema(actual_schema)
    errors.extend(schema_errors)

    # Optionally validate data content
    if check_data and expected_schema.valid_otypes:
        import duckdb
        con = duckdb.connect()

        # Check otype values
        otypes = con.execute(f"""
            SELECT DISTINCT otype
            FROM read_parquet('{parquet_path}')
            WHERE otype IS NOT NULL
        """).fetchall()

        actual_otypes = {row[0] for row in otypes}
        invalid_otypes = actual_otypes - expected_schema.valid_otypes

        if invalid_otypes:
            errors.append(
                f"Invalid otype values for {expected_schema.format.value} format: "
                f"{sorted(invalid_otypes)}"
            )

    return errors


def validate_parquet_strict(
    parquet_path: str,
    expected_schema: PQGSchema,
    check_data: bool = False
) -> None:
    """Validate parquet file and raise exception if invalid.

    Args:
        parquet_path: Path to parquet file
        expected_schema: Schema to validate against
        check_data: If True, also validate otype values (slower)

    Raises:
        SchemaValidationError: If validation fails
        FileNotFoundError: If parquet file doesn't exist
    """
    errors = validate_parquet(parquet_path, expected_schema, check_data)
    if errors:
        raise SchemaValidationError(errors, expected_schema.format)
