#!/usr/bin/env python3
"""
Analyze iSamples LinkML schema to extract the 14 theoretical edge types.
An edge type is defined as a (subject_type, predicate, object_type) triple
where both subject and object are entity classes (not primitives).
"""

import yaml
import urllib.request
from typing import Set, Tuple

# URL to the live iSamples LinkML schema
SCHEMA_URL = "https://raw.githubusercontent.com/isamplesorg/metadata/refs/heads/main/src/schemas/isamples_core.yaml"

# Entity classes in iSamples schema (not primitive types)
ENTITY_CLASSES = {
    'MaterialSampleRecord',
    'SamplingEvent',
    'SamplingSite',
    'GeospatialCoordLocation',
    'Agent',
    'IdentifiedConcept',
    'MaterialSampleCuration',
    'SampleRelation'
}

def fetch_schema() -> dict:
    """Fetch and parse the LinkML schema from GitHub."""
    print(f"Fetching schema from: {SCHEMA_URL}")
    with urllib.request.urlopen(SCHEMA_URL) as response:
        schema_content = response.read().decode('utf-8')
    return yaml.safe_load(schema_content)

def extract_edge_types(schema: dict) -> Set[Tuple[str, str, str]]:
    """
    Extract all edge types from the schema.

    Returns:
        Set of (subject_type, predicate, object_type) tuples
    """
    edge_types = set()
    classes = schema.get('classes', {})
    slots_def = schema.get('slots', {})

    for class_name, class_def in classes.items():
        # Skip if not an entity class
        if class_name not in ENTITY_CLASSES:
            continue

        slots = class_def.get('slots', [])

        for slot_name in slots:
            # Look up the slot definition
            slot_def = slots_def.get(slot_name, {})
            slot_range = slot_def.get('range')

            # Check if the range is an entity class (creating an edge)
            if slot_range in ENTITY_CLASSES:
                edge_types.add((class_name, slot_name, slot_range))

    return edge_types

def main():
    print("=" * 70)
    print("SYSTEMATIC ANALYSIS: iSamples Edge Types")
    print("=" * 70)

    # Fetch and parse schema
    schema = fetch_schema()

    print(f"\n✅ Schema version: {schema.get('version', 'unknown')}")
    print(f"📋 Entity classes: {sorted(ENTITY_CLASSES)}")

    # Extract edge types
    edge_types = extract_edge_types(schema)

    print(f"\n🔗 Found {len(edge_types)} edge types (entity→entity relationships):\n")

    # Sort by subject type for readability
    sorted_edges = sorted(edge_types, key=lambda x: (x[0], x[1]))

    for i, (subject, predicate, obj) in enumerate(sorted_edges, 1):
        # Check if multivalued
        slot_def = schema.get('slots', {}).get(predicate, {})
        multivalued = slot_def.get('multivalued', False)
        cardinality = " [*]" if multivalued else " [1]"

        print(f"{i:2d}. {subject:30s} --{predicate:30s}--> {obj:30s}{cardinality}")

    print(f"\n📊 SUMMARY:")
    print(f"   Total edge types: {len(edge_types)}")

    # Group by subject type
    by_subject = {}
    for subj, pred, obj in edge_types:
        by_subject.setdefault(subj, []).append((pred, obj))

    print(f"\n   By subject type:")
    for subj in sorted(by_subject.keys()):
        print(f"     {subj}: {len(by_subject[subj])} edge types")

    # Export for use in code generation
    print("\n\n" + "=" * 70)
    print("PYTHON CODE GENERATION")
    print("=" * 70)

    print("\n# Edge type enum:")
    print("from enum import Enum\n")
    print("class ISamplesEdgeType(Enum):")
    print('    """The 14 theoretical edge types in iSamples schema."""')

    for i, (subject, predicate, obj) in enumerate(sorted_edges, 1):
        # Create enum name from predicate
        enum_name = predicate.upper()
        # Create a descriptive value
        enum_value = f"{subject}__{predicate}__{obj}"
        print(f'    {enum_name} = "{enum_value}"')

    print("\n\n# Edge type validation mapping:")
    print("EDGE_TYPE_CONSTRAINTS = {")
    for subject, predicate, obj in sorted_edges:
        enum_value = f"{subject}__{predicate}__{obj}"
        print(f'    "{enum_value}": {{')
        print(f'        "subject_type": "{subject}",')
        print(f'        "predicate": "{predicate}",')
        print(f'        "object_type": "{obj}",')
        print(f'    }},')
    print("}")

if __name__ == "__main__":
    main()
