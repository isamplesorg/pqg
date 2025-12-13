"""Narrow format schema definition for PQG parquet files.

The narrow format stores relationships as separate _edge_ rows with
s (subject), p (predicate), and o (object) columns.

Reference: isamples/README.md "Data" section
"""

import pyarrow as pa
from .base import PQGSchema, ColumnSpec, SchemaFormat


# Core identification columns (shared with wide)
CORE_COLUMNS = [
    ColumnSpec(
        name="row_id",
        arrow_type=pa.int32(),  # Can also be int64
        nullable=True,  # Many parquet writers create nullable schemas by default
        required=True,
        description="Auto-incrementing integer primary key for performance"
    ),
    ColumnSpec(
        name="pid",
        arrow_type=pa.string(),
        nullable=True,  # Many parquet writers create nullable schemas by default
        required=True,
        description="Globally unique identifier (externally visible)"
    ),
    ColumnSpec(
        name="tcreated",
        arrow_type=pa.int32(),
        nullable=True,
        required=True,
        description="Unix timestamp when record was created in this file"
    ),
    ColumnSpec(
        name="tmodified",
        arrow_type=pa.int32(),
        nullable=True,
        required=True,
        description="Unix timestamp when record was last modified"
    ),
    ColumnSpec(
        name="otype",
        arrow_type=pa.string(),
        nullable=True,
        required=True,
        description="Type of object (entity class name or '_edge_')"
    ),
]

# Edge columns (NARROW FORMAT ONLY)
EDGE_COLUMNS = [
    ColumnSpec(
        name="s",
        arrow_type=pa.int32(),  # row_id of subject
        nullable=True,
        required=True,
        description="Subject row_id (for edge rows only)"
    ),
    ColumnSpec(
        name="p",
        arrow_type=pa.string(),
        nullable=True,
        required=True,
        description="Predicate/relationship name (for edge rows only)"
    ),
    ColumnSpec(
        name="o",
        arrow_type=pa.list_(pa.int32()),  # Array of object row_ids
        nullable=True,
        required=True,
        description="Object row_ids array (for edge rows only)"
    ),
]

# Graph metadata columns
GRAPH_COLUMNS = [
    ColumnSpec(
        name="n",
        arrow_type=pa.string(),
        nullable=True,
        required=True,
        description="Named graph identifier"
    ),
    ColumnSpec(
        name="altids",
        arrow_type=pa.list_(pa.string()),
        nullable=True,
        required=True,
        description="Alternate identifiers"
    ),
    ColumnSpec(
        name="geometry",
        arrow_type=pa.binary(),  # Can be GEOMETRY or BLOB
        nullable=True,
        required=True,
        description="Spatial geometry (WKB format)"
    ),
]

# Entity-specific columns (from iSamples LinkML schema) - ALL REQUIRED for strict schema
ENTITY_COLUMNS = [
    # SamplingEvent fields
    ColumnSpec(name="authorized_by", arrow_type=pa.list_(pa.string()), required=True),
    ColumnSpec(name="has_feature_of_interest", arrow_type=pa.string(), required=True),
    ColumnSpec(name="project", arrow_type=pa.string(), required=True),
    ColumnSpec(name="result_time", arrow_type=pa.string(), required=True),

    # Agent fields
    ColumnSpec(name="affiliation", arrow_type=pa.string(), required=True),
    ColumnSpec(name="contact_information", arrow_type=pa.string(), required=True),
    ColumnSpec(name="name", arrow_type=pa.string(), required=True),
    ColumnSpec(name="role", arrow_type=pa.string(), required=True),

    # MaterialSampleRecord fields
    ColumnSpec(name="sampling_purpose", arrow_type=pa.string(), required=True),
    ColumnSpec(name="complies_with", arrow_type=pa.list_(pa.string()), required=True),
    ColumnSpec(name="alternate_identifiers", arrow_type=pa.list_(pa.string()), required=True),
    ColumnSpec(name="sample_identifier", arrow_type=pa.string(), required=True),
    ColumnSpec(name="dc_rights", arrow_type=pa.string(), required=True),
    ColumnSpec(name="last_modified_time", arrow_type=pa.string(), required=True),

    # SampleRelation fields
    ColumnSpec(name="relationship", arrow_type=pa.string(), required=True),
    ColumnSpec(name="target", arrow_type=pa.string(), required=True),

    # GeospatialCoordLocation fields
    ColumnSpec(name="elevation", arrow_type=pa.string(), required=True),
    ColumnSpec(name="latitude", arrow_type=pa.float64(), required=True),
    ColumnSpec(name="longitude", arrow_type=pa.float64(), required=True),
    ColumnSpec(name="obfuscated", arrow_type=pa.bool_(), required=True),

    # IdentifiedConcept fields
    ColumnSpec(name="scheme_uri", arrow_type=pa.string(), required=True),
    ColumnSpec(name="scheme_name", arrow_type=pa.string(), required=True),

    # SamplingSite fields
    ColumnSpec(name="is_part_of", arrow_type=pa.list_(pa.string()), required=True),
    ColumnSpec(name="place_name", arrow_type=pa.list_(pa.string()), required=True),

    # MaterialSampleCuration fields
    ColumnSpec(name="curation_location", arrow_type=pa.string(), required=True),
    ColumnSpec(name="access_constraints", arrow_type=pa.list_(pa.string()), required=True),

    # Common fields
    ColumnSpec(name="description", arrow_type=pa.string(), required=True),
    ColumnSpec(name="label", arrow_type=pa.string(), required=True),

    # OpenContext extension
    ColumnSpec(name="thumbnail_url", arrow_type=pa.string(), required=True),
]


# Valid otype values for narrow format (includes _edge_)
NARROW_OTYPES = {
    "Agent",
    "MaterialSampleRecord",
    "SamplingEvent",
    "GeospatialCoordLocation",
    "SamplingSite",
    "IdentifiedConcept",
    "MaterialSampleCuration",
    "SampleRelation",
    "_edge_",
}


# Columns that should NOT be in narrow format
NARROW_FORBIDDEN = {
    "p__has_context_category",
    "p__has_material_category",
    "p__has_sample_object_type",
    "p__keywords",
    "p__produced_by",
    "p__registrant",
    "p__responsibility",
    "p__sample_location",
    "p__sampling_site",
    "p__site_location",
}


NARROW_SCHEMA = PQGSchema(
    name="PQG Narrow Format",
    format=SchemaFormat.NARROW,
    version="0.2.0",
    columns=CORE_COLUMNS + EDGE_COLUMNS + GRAPH_COLUMNS + ENTITY_COLUMNS,
    forbidden_columns=NARROW_FORBIDDEN,
    valid_otypes=NARROW_OTYPES,
    description=(
        "Normalized format with separate _edge_ rows for relationships. "
        "Use for graph algorithms and relationship exploration."
    ),
)


class NarrowSchemaValidator:
    """Validator for narrow format parquet files."""

    @staticmethod
    def validate(parquet_path: str, check_data: bool = False) -> list[str]:
        """Validate a parquet file against narrow schema.

        Args:
            parquet_path: Path to parquet file
            check_data: If True, validate otype values (slower)

        Returns:
            List of validation errors (empty if valid)
        """
        from .base import validate_parquet
        return validate_parquet(parquet_path, NARROW_SCHEMA, check_data)

    @staticmethod
    def get_schema() -> PQGSchema:
        """Get the narrow schema definition."""
        return NARROW_SCHEMA
