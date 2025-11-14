"""
Tests for typed edge query and generation support.
"""

import pytest
import pqg
import dataclasses


@dataclasses.dataclass(kw_only=True)
class Agent(pqg.Base):
    """An agent (person or organization)"""
    affiliation: pqg.OptionalStr = None


@dataclasses.dataclass(kw_only=True)
class GeospatialCoordLocation(pqg.Base):
    """Geographic location"""
    longitude: float = None
    latitude: float = None


@dataclasses.dataclass(kw_only=True)
class SamplingSite(pqg.Base):
    """Location where sampling occurred"""
    pass


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


def test_edge_type_enum():
    """Test the ISamplesEdgeType enum"""
    # Check that we have 14 edge types
    all_types = list(pqg.ISamplesEdgeType)
    assert len(all_types) == 14, f"Expected 14 edge types, found {len(all_types)}"

    # Check specific edge types
    assert pqg.ISamplesEdgeType.MSR_PRODUCED_BY.subject_type == "MaterialSampleRecord"
    assert pqg.ISamplesEdgeType.MSR_PRODUCED_BY.predicate == "produced_by"
    assert pqg.ISamplesEdgeType.MSR_PRODUCED_BY.object_type == "SamplingEvent"

    # Check triple representation
    assert pqg.ISamplesEdgeType.MSR_PRODUCED_BY.as_triple == (
        "MaterialSampleRecord", "produced_by", "SamplingEvent"
    )


def test_edge_type_from_spo():
    """Test finding edge types from SPO triples"""
    result = pqg.ISamplesEdgeType.from_spo(
        "MaterialSampleRecord", "produced_by", "SamplingEvent"
    )
    assert result == pqg.ISamplesEdgeType.MSR_PRODUCED_BY

    # Non-existent pattern
    result = pqg.ISamplesEdgeType.from_spo(
        "Foo", "bar", "Baz"
    )
    assert result is None


def test_edge_type_from_predicate():
    """Test finding edge types by predicate"""
    # 'responsibility' appears in 2 contexts
    results = pqg.ISamplesEdgeType.from_predicate("responsibility")
    assert len(results) == 2

    predicates = {r.predicate for r in results}
    assert "responsibility" in predicates


def test_get_edge_types_by_subject():
    """Test getting edge types by subject type"""
    # MaterialSampleRecord has 8 edge types
    types = pqg.get_edge_types_by_subject("MaterialSampleRecord")
    assert len(types) == 8

    # All should have MaterialSampleRecord as subject
    for et in types:
        assert et.subject_type == "MaterialSampleRecord"


def test_get_edge_types_by_object():
    """Test getting edge types by object type"""
    # Agent appears as object in 3 edge types
    types = pqg.get_edge_types_by_object("Agent")
    assert len(types) == 3

    # All should have Agent as object
    for et in types:
        assert et.object_type == "Agent"


def test_infer_edge_type():
    """Test edge type inference"""
    result = pqg.infer_edge_type(
        "SamplingEvent", "sampling_site", "SamplingSite"
    )
    assert result == pqg.ISamplesEdgeType.EVENT_SAMPLING_SITE


def test_typed_edge_generator(tmp_path):
    """Test TypedEdgeGenerator with a real graph"""
    # Create a test database
    db_path = tmp_path / "test_typed_edges.db"
    conn = pqg.PQG.connect(str(db_path))
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

    # Create some nodes
    agent = Agent(pid="agent_001", label="Dr. Smith")
    graph.addNode(agent)

    site = SamplingSite(pid="site_001", label="Test Site")
    graph.addNode(site)

    event = SamplingEvent(pid="event_001", label="Sampling Event 1")
    graph.addNode(event)

    sample = MaterialSampleRecord(pid="sample_001", label="Sample 1")
    graph.addNode(sample)

    # Create typed edge generator
    generator = pqg.TypedEdgeGenerator(graph)

    # Add a typed edge: MaterialSampleRecord -> produced_by -> SamplingEvent
    edge_pid = generator.add_msr_produced_by("sample_001", "event_001")
    assert edge_pid is not None

    # Add another: SamplingEvent -> sampling_site -> SamplingSite
    edge_pid = generator.add_event_sampling_site("event_001", "site_001")
    assert edge_pid is not None

    # Add: SamplingEvent -> responsibility -> Agent
    edge_pid = generator.add_event_responsibility("event_001", ["agent_001"])
    assert edge_pid is not None

    # Verify edges were created
    relations = list(graph.getRelations(subject="sample_001"))
    assert len(relations) == 1
    assert relations[0][1] == "produced_by"

    conn.close()


def test_typed_edge_queries(tmp_path):
    """Test TypedEdgeQueries with a real graph"""
    # Create a test database
    db_path = tmp_path / "test_typed_queries.db"
    conn = pqg.PQG.connect(str(db_path))
    graph = pqg.PQG(conn)

    # Initialize
    graph.initialize(classes=[
        MaterialSampleRecord,
        SamplingEvent,
        Agent,
    ])

    # Create nodes
    agent = Agent(pid="agent_001", label="Dr. Smith")
    graph.addNode(agent)

    event = SamplingEvent(pid="event_001", label="Event 1")
    graph.addNode(event)

    sample = MaterialSampleRecord(pid="sample_001", label="Sample 1")
    graph.addNode(sample)

    # Add edges using generator
    generator = pqg.TypedEdgeGenerator(graph)
    generator.add_msr_produced_by("sample_001", "event_001")
    generator.add_msr_registrant("sample_001", "agent_001")
    generator.add_event_responsibility("event_001", ["agent_001"])

    # Query typed edges
    queries = pqg.TypedEdgeQueries(graph)

    # Get all MSR_PRODUCED_BY edges
    produced_by_edges = list(queries.get_edges_by_type(pqg.ISamplesEdgeType.MSR_PRODUCED_BY))
    assert len(produced_by_edges) == 1
    s, p, o_list, n, et = produced_by_edges[0]
    assert s == "sample_001"
    assert p == "produced_by"
    assert o_list == ["event_001"]
    assert et == pqg.ISamplesEdgeType.MSR_PRODUCED_BY

    # Get all edges from MaterialSampleRecord
    msr_edges = list(queries.get_edges_by_subject_type("MaterialSampleRecord"))
    assert len(msr_edges) == 2  # produced_by + registrant

    # Get all edges to Agent
    agent_edges = list(queries.get_edges_by_object_type("Agent"))
    assert len(agent_edges) == 2  # registrant + responsibility

    # Get typed relations
    typed_relations = list(queries.get_typed_relations(subject="sample_001"))
    assert len(typed_relations) == 2  # produced_by + registrant

    # Validate an edge
    is_valid, error = queries.validate_edge(
        "sample_001", "produced_by", "event_001",
        expected_type=pqg.ISamplesEdgeType.MSR_PRODUCED_BY
    )
    assert is_valid
    assert error is None

    # Get statistics
    stats = queries.get_edge_type_statistics()
    assert len(stats) > 0

    conn.close()


def test_edge_validation(tmp_path):
    """Test edge validation with invalid types"""
    db_path = tmp_path / "test_validation.db"
    conn = pqg.PQG.connect(str(db_path))
    graph = pqg.PQG(conn)

    graph.initialize(classes=[
        MaterialSampleRecord,
        SamplingEvent,
        Agent,
    ])

    # Create nodes
    agent = Agent(pid="agent_001", label="Dr. Smith")
    graph.addNode(agent)

    sample = MaterialSampleRecord(pid="sample_001", label="Sample 1")
    graph.addNode(sample)

    # Try to create an invalid edge (MaterialSampleRecord -> produced_by -> Agent)
    # This should fail because produced_by requires SamplingEvent, not Agent
    generator = pqg.TypedEdgeGenerator(graph)

    with pytest.raises(ValueError, match="Edge validation failed"):
        generator.add_typed_edge(
            "sample_001",
            "produced_by",
            ["agent_001"],
            expected_type=pqg.ISamplesEdgeType.MSR_PRODUCED_BY,
            validate=True
        )

    conn.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
