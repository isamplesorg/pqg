"""Wide format schema definition for PQG parquet files.

The wide format stores relationships as p__* columns directly on entity rows,
eliminating the need for separate _edge_ rows.

Reference: isamples/README.md "Wide Format Schema" section
"""

import pyarrow as pa
from .base import PQGSchema, ColumnSpec, SchemaFormat


# Core identification columns (shared with narrow, minus edge columns)
CORE_COLUMNS = [
    ColumnSpec(
        name="row_id",
        arrow_type=pa.int32(),  # Can also be int64
        nullable=False,
        required=True,
        description="Auto-incrementing integer primary key for performance"
    ),
    ColumnSpec(
        name="pid",
        arrow_type=pa.string(),
        nullable=False,
        required=True,
        description="Globally unique identifier (externally visible)"
    ),
    ColumnSpec(
        name="tcreated",
        arrow_type=pa.int32(),
        nullable=True,
        required=False,
        description="Unix timestamp when record was created in this file"
    ),
    ColumnSpec(
        name="tmodified",
        arrow_type=pa.int32(),
        nullable=True,
        required=False,
        description="Unix timestamp when record was last modified"
    ),
    ColumnSpec(
        name="otype",
        arrow_type=pa.string(),
        nullable=True,
        required=True,
        description="Type of object (entity class name, never '_edge_')"
    ),
]

# Graph metadata columns (same as narrow)
GRAPH_COLUMNS = [
    ColumnSpec(
        name="n",
        arrow_type=pa.string(),
        nullable=True,
        required=False,
        description="Named graph identifier"
    ),
    ColumnSpec(
        name="altids",
        arrow_type=pa.list_(pa.string()),
        nullable=True,
        required=False,
        description="Alternate identifiers"
    ),
    ColumnSpec(
        name="geometry",
        arrow_type=pa.binary(),  # Can be GEOMETRY or BLOB
        nullable=True,
        required=False,
        description="Spatial geometry (WKB format)"
    ),
]

# Entity-specific columns (same as narrow)
ENTITY_COLUMNS = [
    # SamplingEvent fields
    ColumnSpec(name="authorized_by", arrow_type=pa.list_(pa.string()), required=False),
    ColumnSpec(name="has_feature_of_interest", arrow_type=pa.string(), required=False),
    ColumnSpec(name="project", arrow_type=pa.string(), required=False),
    ColumnSpec(name="result_time", arrow_type=pa.string(), required=False),

    # Agent fields
    ColumnSpec(name="affiliation", arrow_type=pa.string(), required=False),
    ColumnSpec(name="contact_information", arrow_type=pa.string(), required=False),
    ColumnSpec(name="name", arrow_type=pa.string(), required=False),
    ColumnSpec(name="role", arrow_type=pa.string(), required=False),

    # MaterialSampleRecord fields
    ColumnSpec(name="sampling_purpose", arrow_type=pa.string(), required=False),
    ColumnSpec(name="complies_with", arrow_type=pa.list_(pa.string()), required=False),
    ColumnSpec(name="alternate_identifiers", arrow_type=pa.list_(pa.string()), required=False),
    ColumnSpec(name="sample_identifier", arrow_type=pa.string(), required=False),
    ColumnSpec(name="dc_rights", arrow_type=pa.string(), required=False),
    ColumnSpec(name="last_modified_time", arrow_type=pa.string(), required=False),

    # SampleRelation fields
    ColumnSpec(name="relationship", arrow_type=pa.string(), required=False),
    ColumnSpec(name="target", arrow_type=pa.string(), required=False),

    # GeospatialCoordLocation fields
    ColumnSpec(name="elevation", arrow_type=pa.string(), required=False),
    ColumnSpec(name="latitude", arrow_type=pa.float64(), required=False),
    ColumnSpec(name="longitude", arrow_type=pa.float64(), required=False),
    ColumnSpec(name="obfuscated", arrow_type=pa.bool_(), required=False),

    # IdentifiedConcept fields
    ColumnSpec(name="scheme_uri", arrow_type=pa.string(), required=False),
    ColumnSpec(name="scheme_name", arrow_type=pa.string(), required=False),

    # SamplingSite fields
    ColumnSpec(name="is_part_of", arrow_type=pa.list_(pa.string()), required=False),
    ColumnSpec(name="place_name", arrow_type=pa.list_(pa.string()), required=False),

    # MaterialSampleCuration fields
    ColumnSpec(name="curation_location", arrow_type=pa.string(), required=False),
    ColumnSpec(name="access_constraints", arrow_type=pa.list_(pa.string()), required=False),

    # Common fields
    ColumnSpec(name="description", arrow_type=pa.string(), required=False),
    ColumnSpec(name="label", arrow_type=pa.string(), required=False),

    # OpenContext extension
    ColumnSpec(name="thumbnail_url", arrow_type=pa.string(), required=False),
]


# Relationship columns (WIDE FORMAT ONLY)
# These replace _edge_ rows from narrow format
RELATIONSHIP_COLUMNS = [
    ColumnSpec(
        name="p__has_context_category",
        arrow_type=pa.list_(pa.int32()),
        nullable=True,
        required=True,
        description="Context category concepts (MaterialSampleRecord → IdentifiedConcept)"
    ),
    ColumnSpec(
        name="p__has_material_category",
        arrow_type=pa.list_(pa.int32()),
        nullable=True,
        required=True,
        description="Material category concepts (MaterialSampleRecord → IdentifiedConcept)"
    ),
    ColumnSpec(
        name="p__has_sample_object_type",
        arrow_type=pa.list_(pa.int32()),
        nullable=True,
        required=True,
        description="Sample object type concepts (MaterialSampleRecord → IdentifiedConcept)"
    ),
    ColumnSpec(
        name="p__keywords",
        arrow_type=pa.list_(pa.int32()),
        nullable=True,
        required=True,
        description="Keyword concepts (MaterialSampleRecord → IdentifiedConcept)"
    ),
    ColumnSpec(
        name="p__produced_by",
        arrow_type=pa.list_(pa.int32()),
        nullable=True,
        required=True,
        description="Producing event (MaterialSampleRecord → SamplingEvent)"
    ),
    ColumnSpec(
        name="p__registrant",
        arrow_type=pa.list_(pa.int32()),
        nullable=True,
        required=True,
        description="Registrant agent (MaterialSampleRecord → Agent)"
    ),
    ColumnSpec(
        name="p__responsibility",
        arrow_type=pa.list_(pa.int32()),
        nullable=True,
        required=True,
        description="Responsible agent (SamplingEvent → Agent)"
    ),
    ColumnSpec(
        name="p__sample_location",
        arrow_type=pa.list_(pa.int32()),
        nullable=True,
        required=True,
        description="Sample coordinates (SamplingEvent → GeospatialCoordLocation)"
    ),
    ColumnSpec(
        name="p__sampling_site",
        arrow_type=pa.list_(pa.int32()),
        nullable=True,
        required=True,
        description="Sampling site (SamplingEvent → SamplingSite)"
    ),
    ColumnSpec(
        name="p__site_location",
        arrow_type=pa.list_(pa.int32()),
        nullable=True,
        required=True,
        description="Site coordinates (SamplingSite → GeospatialCoordLocation)"
    ),
]


# Valid otype values for wide format (NO _edge_)
WIDE_OTYPES = {
    "Agent",
    "MaterialSampleRecord",
    "SamplingEvent",
    "GeospatialCoordLocation",
    "SamplingSite",
    "IdentifiedConcept",
    "MaterialSampleCuration",
    "SampleRelation",
}


# Columns that should NOT be in wide format
WIDE_FORBIDDEN = {
    "s",   # Subject column (narrow edge format)
    "p",   # Predicate column (narrow edge format)
    "o",   # Object column (narrow edge format)
}


WIDE_SCHEMA = PQGSchema(
    name="PQG Wide Format",
    format=SchemaFormat.WIDE,
    version="0.2.0",
    columns=CORE_COLUMNS + GRAPH_COLUMNS + ENTITY_COLUMNS + RELATIONSHIP_COLUMNS,
    forbidden_columns=WIDE_FORBIDDEN,
    valid_otypes=WIDE_OTYPES,
    description=(
        "Denormalized format with p__* columns for relationships. "
        "Use for analytical queries, dashboards, and browser-based analysis."
    ),
)


class WideSchemaValidator:
    """Validator for wide format parquet files."""

    @staticmethod
    def validate(parquet_path: str, check_data: bool = False) -> list[str]:
        """Validate a parquet file against wide schema.

        Args:
            parquet_path: Path to parquet file
            check_data: If True, validate otype values (slower)

        Returns:
            List of validation errors (empty if valid)
        """
        from .base import validate_parquet
        return validate_parquet(parquet_path, WIDE_SCHEMA, check_data)

    @staticmethod
    def get_schema() -> PQGSchema:
        """Get the wide schema definition."""
        return WIDE_SCHEMA
