"""Export format schema definition for iSamples parquet files.

The export format is a flat, sample-centric schema with nested STRUCTs
for relationships. This is the format produced by the iSamples Central
export API and archived on Zenodo.

Key characteristics:
- One row per sample (no separate entity rows)
- Nested STRUCTs for relationships (produced_by, curation, etc.)
- Pre-extracted coordinates at top level (sample_location_latitude/longitude)
- Fastest format for UI queries (no JOINs needed)

Reference: https://zenodo.org/records/15278211
"""

import pyarrow as pa
from .base import PQGSchema, ColumnSpec, SchemaFormat


# Define nested struct types for complex fields

# Simple identifier wrapper: STRUCT(identifier VARCHAR)
IDENTIFIER_STRUCT = pa.struct([
    pa.field("identifier", pa.string())
])

# Keyword wrapper: STRUCT(keyword VARCHAR)
KEYWORD_STRUCT = pa.struct([
    pa.field("keyword", pa.string())
])

# Agent/responsibility: STRUCT(name VARCHAR, role VARCHAR)
RESPONSIBILITY_STRUCT = pa.struct([
    pa.field("name", pa.string()),
    pa.field("role", pa.string())
])

# Registrant: STRUCT(name VARCHAR)
REGISTRANT_STRUCT = pa.struct([
    pa.field("name", pa.string())
])

# Related resource: STRUCT(target VARCHAR)
RELATED_RESOURCE_STRUCT = pa.struct([
    pa.field("target", pa.string())
])

# Sample location (nested in sampling_site): STRUCT(elevation DOUBLE, latitude DOUBLE, longitude DOUBLE)
SAMPLE_LOCATION_STRUCT = pa.struct([
    pa.field("elevation", pa.float64()),
    pa.field("latitude", pa.float64()),
    pa.field("longitude", pa.float64())
])

# Sampling site (nested in produced_by): STRUCT(description, label, place_name[], sample_location)
SAMPLING_SITE_STRUCT = pa.struct([
    pa.field("description", pa.string()),
    pa.field("label", pa.string()),
    pa.field("place_name", pa.list_(pa.string())),
    pa.field("sample_location", SAMPLE_LOCATION_STRUCT)
])

# Produced by (SamplingEvent): Complex nested struct
PRODUCED_BY_STRUCT = pa.struct([
    pa.field("description", pa.string()),
    pa.field("has_feature_of_interest", pa.string()),
    pa.field("identifier", pa.string()),
    pa.field("label", pa.string()),
    pa.field("responsibility", pa.list_(RESPONSIBILITY_STRUCT)),
    pa.field("result_time", pa.string()),
    pa.field("sampling_site", SAMPLING_SITE_STRUCT)
])

# Curation: STRUCT with nested responsibility
CURATION_STRUCT = pa.struct([
    pa.field("access_constraints", pa.list_(pa.string())),
    pa.field("curation_location", pa.string()),
    pa.field("description", pa.string()),
    pa.field("label", pa.string()),
    pa.field("responsibility", pa.list_(RESPONSIBILITY_STRUCT))
])


# Export format columns
EXPORT_COLUMNS = [
    # Core identification
    ColumnSpec(
        name="sample_identifier",
        arrow_type=pa.string(),
        nullable=False,
        required=True,
        description="Unique identifier for the sample (e.g., ark:/21547/...)"
    ),
    ColumnSpec(
        name="@id",
        arrow_type=pa.string(),
        nullable=True,
        required=True,
        description="JSON-LD identifier (usually same as sample_identifier)"
    ),
    ColumnSpec(
        name="label",
        arrow_type=pa.string(),
        nullable=True,
        required=True,
        description="Human-readable label for the sample"
    ),
    ColumnSpec(
        name="description",
        arrow_type=pa.string(),
        nullable=True,
        required=True,
        description="Description of the sample"
    ),
    ColumnSpec(
        name="source_collection",
        arrow_type=pa.string(),
        nullable=True,
        required=True,
        description="Source collection (SESAR, OPENCONTEXT, GEOME, SMITHSONIAN)"
    ),

    # Classification (nested struct arrays)
    ColumnSpec(
        name="has_sample_object_type",
        arrow_type=pa.list_(IDENTIFIER_STRUCT),
        nullable=True,
        required=True,
        description="Sample object type concepts (array of identifier structs)"
    ),
    ColumnSpec(
        name="has_material_category",
        arrow_type=pa.list_(IDENTIFIER_STRUCT),
        nullable=True,
        required=True,
        description="Material category concepts (array of identifier structs)"
    ),
    ColumnSpec(
        name="has_context_category",
        arrow_type=pa.list_(IDENTIFIER_STRUCT),
        nullable=True,
        required=True,
        description="Context category concepts (array of identifier structs)"
    ),
    ColumnSpec(
        name="informal_classification",
        arrow_type=pa.list_(pa.string()),
        nullable=True,
        required=True,
        description="Informal classification strings"
    ),
    ColumnSpec(
        name="keywords",
        arrow_type=pa.list_(KEYWORD_STRUCT),
        nullable=True,
        required=True,
        description="Keywords (array of keyword structs)"
    ),

    # Relationships (nested structs)
    ColumnSpec(
        name="produced_by",
        arrow_type=PRODUCED_BY_STRUCT,
        nullable=True,
        required=True,
        description="SamplingEvent with nested site, location, and responsibility"
    ),
    ColumnSpec(
        name="last_modified_time",
        arrow_type=pa.timestamp('us', tz='UTC'),
        nullable=True,
        required=True,
        description="Last modification timestamp"
    ),
    ColumnSpec(
        name="curation",
        arrow_type=CURATION_STRUCT,
        nullable=True,
        required=True,
        description="Curation information with nested responsibility"
    ),
    ColumnSpec(
        name="registrant",
        arrow_type=REGISTRANT_STRUCT,
        nullable=True,
        required=True,
        description="Registrant agent (name only)"
    ),
    ColumnSpec(
        name="related_resource",
        arrow_type=pa.list_(RELATED_RESOURCE_STRUCT),
        nullable=True,
        required=True,
        description="Related resources (array of target structs)"
    ),
    ColumnSpec(
        name="sampling_purpose",
        arrow_type=pa.list_(pa.string()),
        nullable=True,
        required=True,
        description="Purpose(s) for sampling"
    ),

    # Pre-extracted coordinates (for fast queries)
    ColumnSpec(
        name="sample_location_longitude",
        arrow_type=pa.float64(),
        nullable=True,
        required=True,
        description="Longitude extracted from produced_by.sampling_site.sample_location"
    ),
    ColumnSpec(
        name="sample_location_latitude",
        arrow_type=pa.float64(),
        nullable=True,
        required=True,
        description="Latitude extracted from produced_by.sampling_site.sample_location"
    ),

    # Geometry
    ColumnSpec(
        name="geometry",
        arrow_type=pa.binary(),
        nullable=True,
        required=True,
        description="Spatial geometry (WKB format)"
    ),
]


# Valid source_collection values
EXPORT_SOURCE_COLLECTIONS = {
    "SESAR",
    "OPENCONTEXT",
    "GEOME",
    "SMITHSONIAN",
}


# Columns that should NOT be in export format (PQG-specific)
EXPORT_FORBIDDEN = {
    "row_id",      # PQG internal
    "pid",         # PQG internal (use sample_identifier instead)
    "tcreated",    # PQG internal
    "tmodified",   # PQG internal
    "otype",       # PQG internal (all rows are samples)
    "s", "p", "o", # PQG narrow edge columns
    "n",           # PQG graph column
    "altids",      # PQG graph column
}


EXPORT_SCHEMA = PQGSchema(
    name="iSamples Export Format",
    format=SchemaFormat.EXPORT,
    version="1.0.0",
    columns=EXPORT_COLUMNS,
    forbidden_columns=EXPORT_FORBIDDEN,
    valid_otypes=set(),  # No otype column in export format
    description=(
        "Flat, sample-centric format with nested STRUCTs for relationships. "
        "Optimized for fast UI queries - no JOINs needed. "
        "One row per sample with pre-extracted coordinates."
    ),
)


class ExportSchemaValidator:
    """Validator for export format parquet files."""

    @staticmethod
    def validate(parquet_path: str, check_data: bool = False) -> list[str]:
        """Validate a parquet file against export schema.

        Args:
            parquet_path: Path to parquet file
            check_data: If True, validate source_collection values (slower)

        Returns:
            List of validation errors (empty if valid)
        """
        from .base import validate_parquet
        errors = validate_parquet(parquet_path, EXPORT_SCHEMA, check_data=False)

        # Custom validation for source_collection if check_data is True
        if check_data:
            import duckdb
            con = duckdb.connect()

            sources = con.execute(f"""
                SELECT DISTINCT source_collection
                FROM read_parquet('{parquet_path}')
                WHERE source_collection IS NOT NULL
            """).fetchall()

            actual_sources = {row[0] for row in sources}
            invalid_sources = actual_sources - EXPORT_SOURCE_COLLECTIONS

            if invalid_sources:
                errors.append(
                    f"Invalid source_collection values: {sorted(invalid_sources)}"
                )

        return errors

    @staticmethod
    def get_schema() -> PQGSchema:
        """Get the export schema definition."""
        return EXPORT_SCHEMA
