"""SQL column definitions for PQG entity tables.

This module defines the full 40-column schema that all entity tables must have
to support UNION ALL operations. Each entity type fills in relevant columns
and leaves others as NULL.

The column order matches Eric's OpenContext parquet schema.
"""

# Full column list for NARROW format (40 columns)
# Order matches Eric's oc_isamples_pqg.parquet schema
NARROW_COLUMNS = [
    # Core identification
    ("row_id", "BIGINT"),
    ("pid", "VARCHAR"),
    ("tcreated", "INTEGER"),
    ("tmodified", "INTEGER"),
    ("otype", "VARCHAR"),
    # Edge columns (NULL for entity rows, populated for _edge_ rows)
    ("s", "INTEGER"),
    ("p", "VARCHAR"),
    ("o", "INTEGER[]"),
    # Graph metadata
    ("n", "VARCHAR"),
    ("altids", "VARCHAR[]"),
    ("geometry", "GEOMETRY"),
    # Entity-specific columns (from iSamples LinkML schema)
    ("authorized_by", "VARCHAR[]"),
    ("has_feature_of_interest", "VARCHAR"),
    ("affiliation", "VARCHAR"),
    ("sampling_purpose", "VARCHAR"),
    ("complies_with", "VARCHAR[]"),
    ("project", "VARCHAR"),
    ("alternate_identifiers", "VARCHAR[]"),
    ("relationship", "VARCHAR"),
    ("elevation", "VARCHAR"),
    ("sample_identifier", "VARCHAR"),
    ("dc_rights", "VARCHAR"),
    ("result_time", "VARCHAR"),
    ("contact_information", "VARCHAR"),
    ("latitude", "DOUBLE"),
    ("target", "VARCHAR"),
    ("role", "VARCHAR"),
    ("scheme_uri", "VARCHAR"),
    ("is_part_of", "VARCHAR[]"),
    ("scheme_name", "VARCHAR"),
    ("name", "VARCHAR"),
    ("longitude", "DOUBLE"),
    ("obfuscated", "BOOLEAN"),
    ("curation_location", "VARCHAR"),
    ("last_modified_time", "VARCHAR"),
    ("access_constraints", "VARCHAR[]"),
    ("place_name", "VARCHAR[]"),
    ("description", "VARCHAR"),
    ("label", "VARCHAR"),
    ("thumbnail_url", "VARCHAR"),
]

# Additional columns for WIDE format (10 p__* columns)
WIDE_RELATIONSHIP_COLUMNS = [
    ("p__has_context_category", "INTEGER[]"),
    ("p__has_material_category", "INTEGER[]"),
    ("p__has_sample_object_type", "INTEGER[]"),
    ("p__keywords", "INTEGER[]"),
    ("p__produced_by", "INTEGER[]"),
    ("p__registrant", "INTEGER[]"),
    ("p__responsibility", "INTEGER[]"),
    ("p__sample_location", "INTEGER[]"),
    ("p__sampling_site", "INTEGER[]"),
    ("p__site_location", "INTEGER[]"),
]

# Full column list for WIDE format (47 columns = 40 - 3 edge + 10 p__)
# Wide format removes s, p, o columns and adds p__* columns
WIDE_COLUMNS = [col for col in NARROW_COLUMNS if col[0] not in ('s', 'p', 'o')] + WIDE_RELATIONSHIP_COLUMNS


def get_null_columns_sql(columns: list, exclude: set = None) -> str:
    """Generate NULL column expressions for columns not being populated.

    Args:
        columns: List of (name, type) tuples
        exclude: Set of column names to exclude (they will be populated separately)

    Returns:
        SQL fragment with NULL::type AS name for each column
    """
    exclude = exclude or set()
    parts = []
    for name, dtype in columns:
        if name not in exclude:
            parts.append(f"NULL::{dtype} AS {name}")
    return ",\n            ".join(parts)


def get_column_names(columns: list) -> list:
    """Get just the column names from a column list."""
    return [col[0] for col in columns]


def get_select_columns_sql(columns: list) -> str:
    """Generate SELECT column list for final output."""
    return ", ".join(get_column_names(columns))


# Entity type to relevant columns mapping
ENTITY_COLUMNS = {
    "MaterialSampleRecord": {
        "label", "description", "sample_identifier", "sampling_purpose",
        "alternate_identifiers", "complies_with", "dc_rights", "last_modified_time"
    },
    "SamplingEvent": {
        "label", "description", "result_time", "has_feature_of_interest",
        "project", "authorized_by"
    },
    "SamplingSite": {
        "label", "description", "place_name", "is_part_of"
    },
    "GeospatialCoordLocation": {
        "label", "description", "latitude", "longitude", "elevation", "obfuscated"
    },
    "IdentifiedConcept": {
        "label", "description", "scheme_name", "scheme_uri"
    },
    "Agent": {
        "label", "description", "name", "affiliation", "contact_information", "role"
    },
    "MaterialSampleCuration": {
        "label", "description", "curation_location", "access_constraints"
    },
    "SampleRelation": {
        "label", "description", "relationship", "target"
    },
}
