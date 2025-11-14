#!/usr/bin/env python3
"""
Demonstration of typed edge support for PQG.

This script shows how to use the 14 specialized edge types without
modifying the PQG schema.
"""

import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import dataclasses
import pqg


# Define iSamples-compatible dataclasses
@dataclasses.dataclass(kw_only=True)
class Agent(pqg.Base):
    """An agent (person or organization)"""
    affiliation: pqg.OptionalStr = None


@dataclasses.dataclass(kw_only=True)
class GeospatialCoordLocation(pqg.Base):
    """Geographic location"""
    longitude: float = 0.0
    latitude: float = 0.0


@dataclasses.dataclass(kw_only=True)
class SamplingSite(pqg.Base):
    """Location where sampling occurred"""
    place_name: pqg.OptionalStr = None


@dataclasses.dataclass(kw_only=True)
class SamplingEvent(pqg.Base):
    """Event of sample collection"""
    result_time: pqg.OptionalStr = None


@dataclasses.dataclass(kw_only=True)
class MaterialSampleRecord(pqg.Base):
    """A material sample"""
    sample_identifier: pqg.OptionalStr = None


@dataclasses.dataclass(kw_only=True)
class IdentifiedConcept(pqg.Base):
    """A concept with an identifier"""
    scheme_name: pqg.OptionalStr = None


def main():
    print("=" * 70)
    print("TYPED EDGE SUPPORT FOR PQG - DEMONSTRATION")
    print("=" * 70)

    # Part 1: Show the 14 edge types
    print("\n📋 Part 1: The 14 iSamples Edge Types\n")
    print(f"Total edge types defined: {len(list(pqg.ISamplesEdgeType))}\n")

    for i, edge_type in enumerate(pqg.ISamplesEdgeType, 1):
        print(f"{i:2d}. {edge_type.name:35s} → {edge_type}")

    # Part 2: Edge type utilities
    print("\n\n🔍 Part 2: Edge Type Inference\n")

    # Find edge type from SPO triple
    et = pqg.ISamplesEdgeType.from_spo(
        "MaterialSampleRecord", "produced_by", "SamplingEvent"
    )
    print(f"Edge type for (MaterialSampleRecord, produced_by, SamplingEvent):")
    print(f"  → {et.name if et else 'None'}")

    # Find all edge types using a predicate
    responsibility_types = pqg.ISamplesEdgeType.from_predicate("responsibility")
    print(f"\nEdge types using 'responsibility' predicate: {len(responsibility_types)}")
    for et in responsibility_types:
        print(f"  → {et}")

    # Get edge types by subject
    msr_types = pqg.get_edge_types_by_subject("MaterialSampleRecord")
    print(f"\nEdge types from MaterialSampleRecord: {len(msr_types)}")
    for et in msr_types:
        print(f"  → {et.predicate:30s} to {et.object_type}")

    # Get edge types by object
    agent_types = pqg.get_edge_types_by_object("Agent")
    print(f"\nEdge types pointing to Agent: {len(agent_types)}")
    for et in agent_types:
        print(f"  → {et.subject_type:30s} via {et.predicate}")

    # Part 3: Create a graph and use typed edges
    print("\n\n🔨 Part 3: Creating a Graph with Typed Edges\n")

    # Create an in-memory database
    conn = pqg.PQG.connect(":memory:")
    graph = pqg.PQG(conn)

    # Initialize with our classes
    graph.initialize(classes=[
        MaterialSampleRecord,
        SamplingEvent,
        SamplingSite,
        Agent,
        GeospatialCoordLocation,
        IdentifiedConcept,
    ])

    print("✓ Created in-memory PQG database")
    print("✓ Initialized with iSamples classes")

    # Create some nodes
    agent = Agent(
        pid="agent_001",
        label="Dr. Jane Smith",
        affiliation="University of Example"
    )
    graph.addNode(agent)
    print(f"\n✓ Added Agent: {agent.label}")

    location = GeospatialCoordLocation(
        pid="loc_001",
        longitude=-122.4194,
        latitude=37.7749,
    )
    graph.addNode(location)
    print(f"✓ Added Location: ({location.latitude}, {location.longitude})")

    site = SamplingSite(
        pid="site_001",
        label="San Francisco Bay",
        place_name="SF Bay Area"
    )
    graph.addNode(site)
    print(f"✓ Added Sampling Site: {site.label}")

    event = SamplingEvent(
        pid="event_001",
        label="Bay Water Sampling 2025",
        result_time="2025-01-15"
    )
    graph.addNode(event)
    print(f"✓ Added Sampling Event: {event.label}")

    sample = MaterialSampleRecord(
        pid="sample_001",
        label="Bay Water Sample #001",
        sample_identifier="SFBAY-2025-001"
    )
    graph.addNode(sample)
    print(f"✓ Added Sample: {sample.label}")

    keyword = IdentifiedConcept(
        pid="keyword_001",
        label="Marine water",
        scheme_name="GCMD Keywords"
    )
    graph.addNode(keyword)
    print(f"✓ Added Keyword: {keyword.label}")

    # Create typed edge generator
    generator = pqg.TypedEdgeGenerator(graph)
    print("\n✓ Created TypedEdgeGenerator")

    # Add typed edges
    print("\nAdding typed edges:")

    edge_pid = generator.add_msr_produced_by("sample_001", "event_001")
    print(f"  1. Sample --produced_by--> Event (edge: {edge_pid[:20]}...)")

    edge_pid = generator.add_msr_registrant("sample_001", "agent_001")
    print(f"  2. Sample --registrant--> Agent (edge: {edge_pid[:20]}...)")

    edge_pid = generator.add_event_sampling_site("event_001", "site_001")
    print(f"  3. Event --sampling_site--> Site (edge: {edge_pid[:20]}...)")

    edge_pid = generator.add_site_location("site_001", "loc_001")
    print(f"  4. Site --site_location--> Location (edge: {edge_pid[:20]}...)")

    edge_pid = generator.add_event_responsibility("event_001", ["agent_001"])
    print(f"  5. Event --responsibility--> Agent (edge: {edge_pid[:20]}...)")

    edge_pid = generator.add_msr_keywords("sample_001", ["keyword_001"])
    print(f"  6. Sample --keywords--> Keyword (edge: {edge_pid[:20]}...)")

    # Part 4: Query typed edges
    print("\n\n🔎 Part 4: Querying Typed Edges\n")

    queries = pqg.TypedEdgeQueries(graph)

    # Get all MSR_PRODUCED_BY edges
    print("Query: All 'produced_by' edges")
    for s, p, o_list, n, et in queries.get_edges_by_type(pqg.ISamplesEdgeType.MSR_PRODUCED_BY):
        print(f"  → {s} --{p}--> {o_list} (type: {et.name})")

    # Get all edges from MaterialSampleRecord
    print("\nQuery: All edges from MaterialSampleRecord nodes")
    count = 0
    for s, p, o, et in queries.get_edges_by_subject_type("MaterialSampleRecord"):
        print(f"  → {s} --{p}--> {o} (type: {et.name})")
        count += 1
    print(f"  Total: {count} edges")

    # Get all edges to Agent
    print("\nQuery: All edges pointing to Agent nodes")
    count = 0
    for s, p, o, et in queries.get_edges_by_object_type("Agent"):
        print(f"  → {s} --{p}--> {o} (type: {et.name})")
        count += 1
    print(f"  Total: {count} edges")

    # Get typed relations for a specific subject
    print("\nQuery: All typed relations from 'sample_001'")
    for s, p, o, et in queries.get_typed_relations(subject="sample_001"):
        if et:
            print(f"  → {s} --{p}--> {o} (type: {et.name})")
        else:
            print(f"  → {s} --{p}--> {o} (type: unrecognized)")

    # Part 5: Validation
    print("\n\n✅ Part 5: Edge Validation\n")

    # Validate a correct edge
    is_valid, error = queries.validate_edge(
        "sample_001", "produced_by", "event_001",
        expected_type=pqg.ISamplesEdgeType.MSR_PRODUCED_BY
    )
    print(f"Validate: sample_001 --produced_by--> event_001")
    print(f"  Result: {'✓ VALID' if is_valid else '✗ INVALID'}")
    if error:
        print(f"  Error: {error}")

    # Try to validate an incorrect edge
    is_valid, error = queries.validate_edge(
        "sample_001", "produced_by", "agent_001",  # Wrong: Agent instead of SamplingEvent
        expected_type=pqg.ISamplesEdgeType.MSR_PRODUCED_BY
    )
    print(f"\nValidate: sample_001 --produced_by--> agent_001 (incorrect)")
    print(f"  Result: {'✓ VALID' if is_valid else '✗ INVALID'}")
    if error:
        print(f"  Error: {error}")

    # Part 6: Statistics
    print("\n\n📊 Part 6: Edge Type Statistics\n")

    stats = queries.get_edge_type_statistics()
    print(f"Edge types in use: {len(stats)}\n")
    for edge_type, count in stats:
        print(f"  {edge_type.name:35s} → {count:3d} edge(s)")

    print("\n\n" + "=" * 70)
    print("✓ DEMONSTRATION COMPLETE")
    print("=" * 70)
    print("\nKey Features:")
    print("  • 14 typed edge patterns from iSamples schema")
    print("  • NO schema changes to PQG")
    print("  • Edge types inferred from node otipes")
    print("  • Specialized query methods by type")
    print("  • Generation helpers with validation")
    print("  • Full backward compatibility")
    print()

    conn.close()


if __name__ == "__main__":
    main()
