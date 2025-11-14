# Typed Edge Support for PQG

## Overview

PQG now includes specialized query and generation support for **14 theoretical edge types** derived from the iSamples LinkML schema. This feature provides type-safe edge operations while maintaining full backward compatibility with existing PQG databases.

**Key Benefits:**
- ✅ No schema changes required
- ✅ Edge types inferred dynamically from node types
- ✅ Type-safe query and generation methods
- ✅ Validation against iSamples schema
- ✅ Full backward compatibility

## The 14 Edge Types

Based on the iSamples LinkML schema, PQG supports 14 entity-to-entity relationship patterns:

### MaterialSampleRecord Edges (8 types)

1. **MSR_CURATION**: `MaterialSampleRecord → curation → MaterialSampleCuration`
2. **MSR_HAS_CONTEXT_CATEGORY**: `MaterialSampleRecord → has_context_category → IdentifiedConcept` (multivalued)
3. **MSR_HAS_MATERIAL_CATEGORY**: `MaterialSampleRecord → has_material_category → IdentifiedConcept` (multivalued)
4. **MSR_HAS_SAMPLE_OBJECT_TYPE**: `MaterialSampleRecord → has_sample_object_type → IdentifiedConcept` (multivalued)
5. **MSR_KEYWORDS**: `MaterialSampleRecord → keywords → IdentifiedConcept` (multivalued)
6. **MSR_PRODUCED_BY**: `MaterialSampleRecord → produced_by → SamplingEvent`
7. **MSR_REGISTRANT**: `MaterialSampleRecord → registrant → Agent`
8. **MSR_RELATED_RESOURCE**: `MaterialSampleRecord → related_resource → SampleRelation` (multivalued)

### SamplingEvent Edges (4 types)

9. **EVENT_HAS_CONTEXT_CATEGORY**: `SamplingEvent → has_context_category → IdentifiedConcept` (multivalued)
10. **EVENT_RESPONSIBILITY**: `SamplingEvent → responsibility → Agent` (multivalued)
11. **EVENT_SAMPLE_LOCATION**: `SamplingEvent → sample_location → GeospatialCoordLocation`
12. **EVENT_SAMPLING_SITE**: `SamplingEvent → sampling_site → SamplingSite`

### Other Edges (2 types)

13. **SITE_LOCATION**: `SamplingSite → site_location → GeospatialCoordLocation`
14. **CURATION_RESPONSIBILITY**: `MaterialSampleCuration → responsibility → Agent` (multivalued)

## Usage

### Import Typed Edge Support

```python
import pqg

# Access the edge type enum
from pqg import ISamplesEdgeType

# Access query and generation classes
from pqg import TypedEdgeQueries, TypedEdgeGenerator
```

### Edge Type Enum

The `ISamplesEdgeType` enum provides access to all 14 types:

```python
# Check all available types
all_types = list(pqg.ISamplesEdgeType)
print(f"Total edge types: {len(all_types)}")  # 14

# Access specific type
et = pqg.ISamplesEdgeType.MSR_PRODUCED_BY

# Get type properties
print(et.subject_type)  # "MaterialSampleRecord"
print(et.predicate)     # "produced_by"
print(et.object_type)   # "SamplingEvent"
print(et.as_triple)     # ("MaterialSampleRecord", "produced_by", "SamplingEvent")
```

### Edge Type Inference

Find edge types from SPO components:

```python
# Find by subject-predicate-object triple
et = pqg.ISamplesEdgeType.from_spo(
    "MaterialSampleRecord",
    "produced_by",
    "SamplingEvent"
)
# Returns: ISamplesEdgeType.MSR_PRODUCED_BY

# Find all edge types using a predicate
responsibility_types = pqg.ISamplesEdgeType.from_predicate("responsibility")
# Returns: [CURATION_RESPONSIBILITY, EVENT_RESPONSIBILITY]

# Get edge types by subject type
msr_types = pqg.get_edge_types_by_subject("MaterialSampleRecord")
# Returns: all 8 MaterialSampleRecord edge types

# Get edge types by object type
agent_types = pqg.get_edge_types_by_object("Agent")
# Returns: [MSR_REGISTRANT, EVENT_RESPONSIBILITY, CURATION_RESPONSIBILITY]
```

### Typed Edge Generation

The `TypedEdgeGenerator` class provides type-safe edge creation with automatic validation:

```python
# Create graph
graph = pqg.PQG(connection)
graph.initialize(classes=[MaterialSampleRecord, SamplingEvent, Agent, ...])

# Create nodes
sample = MaterialSampleRecord(pid="sample_001", label="Sample 1")
graph.addNode(sample)

event = SamplingEvent(pid="event_001", label="Sampling Event 1")
graph.addNode(event)

agent = Agent(pid="agent_001", label="Dr. Smith")
graph.addNode(agent)

# Create typed edge generator
generator = pqg.TypedEdgeGenerator(graph)

# Add typed edges (with automatic validation)
generator.add_msr_produced_by("sample_001", "event_001")
generator.add_msr_registrant("sample_001", "agent_001")
generator.add_event_responsibility("event_001", ["agent_001"])

# Or use the generic method with explicit type
generator.add_typed_edge(
    "sample_001",
    "produced_by",
    ["event_001"],
    expected_type=pqg.ISamplesEdgeType.MSR_PRODUCED_BY,
    validate=True  # Validates against iSamples schema
)
```

### Helper Methods

Each of the 14 edge types has a dedicated helper method:

```python
# MaterialSampleRecord edges
generator.add_msr_curation(msr_pid, curation_pid)
generator.add_msr_has_context_category(msr_pid, [concept_pid1, concept_pid2])
generator.add_msr_has_material_category(msr_pid, [concept_pid])
generator.add_msr_has_sample_object_type(msr_pid, [concept_pid])
generator.add_msr_keywords(msr_pid, [keyword_pid1, keyword_pid2])
generator.add_msr_produced_by(msr_pid, event_pid)
generator.add_msr_registrant(msr_pid, agent_pid)
generator.add_msr_related_resource(msr_pid, [relation_pid])

# SamplingEvent edges
generator.add_event_has_context_category(event_pid, [concept_pid])
generator.add_event_responsibility(event_pid, [agent_pid1, agent_pid2])
generator.add_event_sample_location(event_pid, location_pid)
generator.add_event_sampling_site(event_pid, site_pid)

# Other edges
generator.add_site_location(site_pid, location_pid)
generator.add_curation_responsibility(curation_pid, [agent_pid])
```

### Typed Edge Queries

The `TypedEdgeQueries` class provides specialized query methods:

```python
# Create query interface
queries = pqg.TypedEdgeQueries(graph)

# Get all edges of a specific type
for s, p, o_list, n, et in queries.get_edges_by_type(pqg.ISamplesEdgeType.MSR_PRODUCED_BY):
    print(f"{s} --{p}--> {o_list}")
    # edge_type (et) is the inferred ISamplesEdgeType

# Get all edges from nodes of a specific type
for s, p, o, et in queries.get_edges_by_subject_type("MaterialSampleRecord"):
    print(f"{s} --{p}--> {o} (type: {et.name})")

# Get all edges to nodes of a specific type
for s, p, o, et in queries.get_edges_by_object_type("Agent"):
    print(f"{s} --{p}--> {o} (type: {et.name})")

# Get typed relations (like PQG.getRelations but with types)
for s, p, o, et in queries.get_typed_relations(subject="sample_001"):
    if et:
        print(f"{s} --{p}--> {o} (type: {et.name})")
    else:
        print(f"{s} --{p}--> {o} (type: unrecognized)")

# Filter by edge type
for s, p, o, et in queries.get_typed_relations(
    edge_type=pqg.ISamplesEdgeType.MSR_PRODUCED_BY
):
    print(f"{s} --{p}--> {o}")
```

### Edge Validation

Validate edges against iSamples schema constraints:

```python
queries = pqg.TypedEdgeQueries(graph)

# Validate an edge
is_valid, error = queries.validate_edge(
    "sample_001",
    "produced_by",
    "event_001",
    expected_type=pqg.ISamplesEdgeType.MSR_PRODUCED_BY
)

if is_valid:
    print("✓ Edge is valid")
else:
    print(f"✗ Edge is invalid: {error}")

# Infer and validate automatically
inferred_type = queries.infer_edge_type_from_pids(
    "sample_001",
    "produced_by",
    "event_001"
)
print(f"Inferred type: {inferred_type.name if inferred_type else 'unknown'}")
```

### Statistics

Get edge type usage statistics:

```python
queries = pqg.TypedEdgeQueries(graph)

stats = queries.get_edge_type_statistics()
for edge_type, count in stats:
    print(f"{edge_type.name}: {count} edges")
```

## How It Works

### No Schema Changes

Unlike traditional typed graph databases, PQG's typed edge support does NOT modify the underlying schema. The edge type is inferred dynamically at query time by:

1. Looking up the subject node's `otype`
2. Getting the edge's predicate
3. Looking up the object node's `otype`
4. Matching the (subject_otype, predicate, object_otype) triple to one of the 14 types

This approach:
- ✅ Maintains backward compatibility with existing databases
- ✅ Works with any PQG database (new or existing)
- ✅ Adds zero storage overhead
- ✅ Preserves PQG's flexible schema

### Type Inference Performance

Edge type inference requires node lookups, which may add overhead for large-scale queries. For performance-critical applications:

- Use `get_edges_by_type()` which filters efficiently
- Cache `TypedEdgeQueries` instances
- Consider adding indexes on `otype` if not already present

## Example: Complete Workflow

```python
import pqg
import dataclasses

# Define classes
@dataclasses.dataclass(kw_only=True)
class Agent(pqg.Base):
    affiliation: pqg.OptionalStr = None

@dataclasses.dataclass(kw_only=True)
class SamplingEvent(pqg.Base):
    result_time: pqg.OptionalStr = None

@dataclasses.dataclass(kw_only=True)
class MaterialSampleRecord(pqg.Base):
    sample_identifier: pqg.OptionalStr = None

# Create graph
conn = pqg.PQG.connect(":memory:")
graph = pqg.PQG(conn)
graph.initialize(classes=[MaterialSampleRecord, SamplingEvent, Agent])

# Add nodes
agent = Agent(pid="agent_001", label="Dr. Smith")
graph.addNode(agent)

event = SamplingEvent(pid="event_001", label="Field Collection")
graph.addNode(event)

sample = MaterialSampleRecord(pid="sample_001", label="Rock Sample")
graph.addNode(sample)

# Add typed edges with validation
generator = pqg.TypedEdgeGenerator(graph)
generator.add_msr_produced_by("sample_001", "event_001")
generator.add_msr_registrant("sample_001", "agent_001")
generator.add_event_responsibility("event_001", ["agent_001"])

# Query typed edges
queries = pqg.TypedEdgeQueries(graph)

print("Sample relationships:")
for s, p, o, et in queries.get_typed_relations(subject="sample_001"):
    print(f"  {s} --{p}--> {o} (type: {et.name if et else 'unknown'})")

# Statistics
print("\nEdge type usage:")
for edge_type, count in queries.get_edge_type_statistics():
    print(f"  {edge_type.name}: {count}")

conn.close()
```

## API Reference

### Classes

#### `ISamplesEdgeType` (Enum)

Enumeration of 14 edge types.

**Properties:**
- `subject_type`: Expected subject node otype
- `predicate`: Edge predicate name
- `object_type`: Expected object node otype
- `as_triple`: Tuple of (subject_type, predicate, object_type)

**Methods:**
- `from_spo(subject_type, predicate, object_type)`: Find edge type from SPO triple
- `from_predicate(predicate)`: Find all edge types using a predicate

#### `TypedEdgeGenerator`

Helper class for creating typed edges with validation.

**Constructor:**
```python
TypedEdgeGenerator(pqg_instance: PQG)
```

**Methods:**
- `add_typed_edge(subject_pid, predicate, object_pids, expected_type=None, named_graph=None, validate=True)`: Add edge with validation
- `add_msr_*()`: Specialized methods for each edge type

#### `TypedEdgeQueries`

Query interface for typed edges.

**Constructor:**
```python
TypedEdgeQueries(pqg_instance: PQG)
```

**Methods:**
- `infer_edge_type_from_pids(subject_pid, predicate, object_pid)`: Infer type from PIDs
- `get_edges_by_type(edge_type, limit=None)`: Get edges matching a type
- `get_edges_by_subject_type(subject_type, limit=None)`: Get edges from subject type
- `get_edges_by_object_type(object_type, limit=None)`: Get edges to object type
- `get_typed_relations(subject=None, edge_type=None, object_node=None, maxrows=0)`: Get typed relations
- `validate_edge(subject_pid, predicate, object_pid, expected_type=None)`: Validate edge
- `get_edge_type_statistics()`: Get usage statistics

### Functions

- `infer_edge_type(subject_otype, predicate, object_otype)`: Infer edge type from types
- `validate_edge_type(edge_type_str, subject_otype, predicate, object_otype)`: Validate edge type
- `get_edge_types_by_subject(subject_type)`: Get types by subject
- `get_edge_types_by_object(object_type)`: Get types by object

## See Also

- [iSamples LinkML Schema](https://github.com/isamplesorg/metadata/blob/main/src/schemas/isamples_core.yaml)
- [PQG User Guide](user-guide.md)
- [Example Script](../examples/typed_edges_demo.py)
- [Test Suite](../tests/test_typed_edges.py)
