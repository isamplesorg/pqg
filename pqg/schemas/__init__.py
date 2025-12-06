"""PQG Schema Definitions.

This module provides canonical schema definitions for PQG parquet formats.
All converters should validate output against these schemas.

Two serialization formats are supported:
- NARROW: Entities + edge rows (normalized, more flexible)
- WIDE: Entities with p__* columns (denormalized, faster queries)

Usage:
    from pqg.schemas import NARROW_SCHEMA, WIDE_SCHEMA, validate_parquet

    # Check if a file matches the expected schema
    errors = validate_parquet('output.parquet', WIDE_SCHEMA)
    if errors:
        raise ValueError(f"Schema validation failed: {errors}")

    # Auto-detect format and validate
    schema, format = get_schema_from_parquet('unknown.parquet')
    print(f"Detected format: {format.value}")

    # Strict validation (raises on error)
    validate_parquet_strict('output.parquet', WIDE_SCHEMA)
"""

from .base import (
    PQGSchema,
    ColumnSpec,
    SchemaFormat,
    SchemaValidationError,
    validate_parquet,
    validate_parquet_strict,
    get_schema_from_parquet,
)
from .narrow import NARROW_SCHEMA, NarrowSchemaValidator
from .wide import WIDE_SCHEMA, WideSchemaValidator

__all__ = [
    # Schema definitions
    "NARROW_SCHEMA",
    "WIDE_SCHEMA",
    # Format enum
    "SchemaFormat",
    # Validators
    "NarrowSchemaValidator",
    "WideSchemaValidator",
    # Base classes and utilities
    "PQGSchema",
    "ColumnSpec",
    "SchemaValidationError",
    "validate_parquet",
    "validate_parquet_strict",
    "get_schema_from_parquet",
]
