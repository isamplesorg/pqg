# PQG User Guide

Complete reference guide for using PQG (Property Graph in DuckDB).

## Table of Contents

1. [Introduction](#introduction)
2. [Core Concepts](#core-concepts)
3. [Installation](#installation)
4. [Data Model](#data-model)
5. [Working with Graphs](#working-with-graphs)
6. [Querying](#querying)
7. [Storage and Export](#storage-and-export)
8. [Performance](#performance)
9. [Best Practices](#best-practices)
10. [API Reference](#api-reference)
11. [Troubleshooting](#troubleshooting)

## Introduction

### What is PQG?

PQG (Property Graph in DuckDB) is a Python library that implements a property graph database using DuckDB as its storage backend. It occupies a unique position between full-featured property graph databases like Neo4j and fully decomposed graph representations like RDF stores.

### Key Features

- **Single Table Design**: All nodes and edges stored in one table
- **DuckDB Backend**: Fast, embedded, zero-configuration database
- **Python Native**: Work with Python dataclasses
- **Automatic Decomposition**: Complex objects automatically split into nodes and edges
- **Spatial Support**: Built-in geographic data support
- **Multiple Export Formats**: Parquet, GeoJSON, Graphviz
- **Type-Safe**: Leverages Python type hints

### When to Use PQG

**Good Use Cases:**
- Knowledge graphs
- Research data management
- Connected data exploration
- Data lineage tracking
- Spatial data with relationships
- Prototyping graph applications

**Not Ideal For:**
- Real-time transactional workloads
- Distributed graph processing
- Very large graphs (> 100M nodes) without partitioning
- Applications requiring ACID transactions at scale

## Core Concepts

### Property Graphs

A property graph consists of:

- **Nodes (Vertices)**: Entities or things
- **Edges (Relations)**: Connections between nodes
- **Properties**: Key-value pairs on nodes and edges
- **Labels/Types**: Categories for nodes and edges

### The PQG Model

#### Nodes

Every node has these base properties:

| Property | Type | Description |
|----------|------|-------------|
| `pid` | string | Unique identifier |
| `tcreated` | integer | Creation timestamp (Unix epoch) |
| `tmodified` | integer | Modification timestamp |
| `otype` | string | Object type (class name) |
| `label` | string | Human-readable label |
| `description` | string | Longer description |
| `altids` | list[string] | Alternative identifiers |

Plus any custom properties defined in your dataclass.

#### Edges

Edges are special nodes with `otype="_edge_"` and these additional properties:

| Property | Type | Description |
|----------|------|-------------|
| `s` | string | Subject (source node PID) |
| `p` | string | Predicate (relationship type) |
| `o` | list[string] | Object (target node PIDs) |
| `n` | string | Named graph (optional) |

#### Triple Pattern

Edges follow the RDF-style Subject-Predicate-Object (SPO) pattern:

```
book_001 --authored_by--> author_001
   ↓           ↓                ↓
Subject   Predicate        Object
```

### Single Table Design

All data (nodes and edges) lives in one table:

```sql
CREATE TABLE node (
    row_id INTEGER PRIMARY KEY DEFAULT nextval('row_id_sequence'),
    pid VARCHAR UNIQUE NOT NULL,
    tcreated INTEGER DEFAULT(CAST(epoch(current_timestamp) AS INTEGER)),
    tmodified INTEGER DEFAULT(CAST(epoch(current_timestamp) AS INTEGER)),
    otype VARCHAR,
    -- Edge fields
    s INTEGER,
    p VARCHAR,
    o INTEGER[],
    n VARCHAR,
    -- Base fields
    label VARCHAR,
    description VARCHAR,
    altids VARCHAR[],
    -- Custom fields from registered types
    title VARCHAR,
    name VARCHAR,
    year INTEGER,
    ...
);
```

> **Note**: While the API uses PIDs (strings) for edge references, PQG internally stores these as INTEGER `row_id` references for 2-5x faster joins. The conversion is automatic and transparent.

This design enables:
- Simple queries without complex joins
- Efficient storage in columnar format (Parquet)
- Easy schema evolution

## Installation

### Requirements

- Python >= 3.11
- pip or uv package manager

### Using pip

```bash
git clone https://github.com/isamplesorg/pqg.git
cd pqg
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

### Using uv

```bash
git clone https://github.com/isamplesorg/pqg.git
cd pqg
uv sync
```

### Verifying Installation

```bash
# Check CLI
pqg --help

# Check Python import
python -c "import pqg; print('PQG imported successfully')"
```

### Dependencies

Core dependencies:
- `duckdb` - Database backend
- `mashumaro` - Serialization
- Standard library modules

Optional for visualization:
- `graphviz` - Graph visualization
- `matplotlib` - Statistical plots
- `networkx` - Network analysis

## Data Model

### Defining Node Types

Use Python dataclasses that inherit from `pqg.Base`:

```python
from dataclasses import dataclass
from typing import Optional, List
from pqg import Base

@dataclass
class Person(Base):
    """A person node type"""
    name: Optional[str] = None
    age: Optional[int] = None
    email: Optional[str] = None
```

### Supported Field Types

| Python Type | DuckDB Type | Notes |
|-------------|-------------|-------|
| `str` | VARCHAR | Text data, PIDs |
| `int` | INTEGER | Whole numbers, row_id references |
| `float` | DOUBLE | Decimals |
| `bool` | BOOLEAN | True/False |
| `datetime` | TIMESTAMPTZ | Timestamps |
| `List[str]` | VARCHAR[] | String arrays |
| `List[int]` | INTEGER[] | Integer arrays (used for `o` field edges) |
| `List[float]` | DOUBLE[] | Float arrays |
| Dataclass | N/A | Decomposed to separate nodes |

### Optional vs Required Fields

Always use `Optional` with defaults for flexibility:

```python
# Good
@dataclass
class Book(Base):
    title: Optional[str] = None
    year: Optional[int] = None

# Problematic - may cause issues if fields missing
@dataclass
class Book(Base):
    title: str  # No default!
    year: int   # No default!
```

### Field Naming

Follow Python conventions:

```python
@dataclass
class Sample(Base):
    sample_id: Optional[str] = None  # snake_case
    collected_date: Optional[str] = None
    collection_method: Optional[str] = None
```

Avoid SQL reserved words:
- Use `sample_desc` not `description` (unless inherited from Base)
- Use `item_order` not `order`
- Use `item_type` not `type`

## Working with Graphs

### Creating a Graph

```python
import duckdb
from pqg import PQG

# In-memory database
db = duckdb.connect()

# Persistent database
db = duckdb.connect("my_graph.duckdb")

# Create PQG instance
graph = PQG(db, source="my_graph")
```

### Registering Types

Before adding nodes, register your custom types:

```python
graph.registerType(Person)
graph.registerType(Book)
graph.registerType(Organization)

# Initialize schema
graph.initialize()
```

### Adding Nodes

#### Simple Nodes

```python
person = Person(
    pid="person_001",
    label="Alice Smith",
    name="Alice Smith",
    age=30,
    email="alice@example.com"
)

graph.addNode(person)
db.commit()
```

#### Auto-Generated PIDs

If you don't provide a PID, one is generated:

```python
person = Person(name="Bob")
graph.addNode(person)
# person.pid is now something like "anon_a1b2c3d4"
```

#### Batch Adding

```python
people = [
    Person(pid=f"person_{i}", name=f"Person {i}")
    for i in range(100)
]

for person in people:
    graph.addNode(person)

db.commit()  # Commit once at the end
```

### Adding Edges

#### Manual Edge Creation

```python
from pqg import Edge

edge = Edge(
    s="person_001",      # Source
    p="knows",           # Relationship type
    o=["person_002"]     # Target(s) - always a list
)

graph.addEdge(edge)
db.commit()
```

#### Multiple Targets

```python
# One person knows multiple people
edge = Edge(
    s="person_001",
    p="knows",
    o=["person_002", "person_003", "person_004"]
)

graph.addEdge(edge)
```

#### Automatic Edge Creation

When a field contains PIDs, edges are created automatically:

```python
@dataclass
class Book(Base):
    title: Optional[str] = None
    author: Optional[str] = None  # Will create edge

book = Book(
    pid="book_001",
    title="Example Book",
    author="author_001"  # Creates: book_001 --author--> author_001
)

graph.addNode(book)
```

For lists:

```python
@dataclass
class Book(Base):
    title: Optional[str] = None
    authors: Optional[List[str]] = None  # Multiple edges

book = Book(
    pid="book_001",
    title="Example Book",
    authors=["author_001", "author_002"]
)

graph.addNode(book)
# Creates two edges with predicate "authors"
```

### Updating Nodes

To update, retrieve, modify, and re-add:

```python
# Get existing node
person_data = graph.getNode("person_001")

# Create updated instance
person = Person(**person_data)
person.age = 31
person.email = "newemail@example.com"

# Update in graph
graph.addNode(person)
db.commit()
```

### Deleting Nodes

PQG doesn't have a built-in delete method. Use SQL directly:

```python
# Delete a specific node
db.execute("DELETE FROM node WHERE pid = ?", ["person_001"])

# Delete all nodes of a type
db.execute("DELETE FROM node WHERE otype = ?", ["Person"])

db.commit()
```

**Warning**: This doesn't cascade. Edges referencing deleted nodes will remain.

## Querying

### Basic Queries

#### Get Node by ID

```python
node_data = graph.getNode("person_001")

if node_data:
    print(f"Name: {node_data['name']}")
    print(f"Age: {node_data['age']}")
else:
    print("Node not found")
```

#### Get All IDs of a Type

```python
# Get all person IDs
person_ids = list(graph.getIds(otype="Person"))

# Limit results
sample_ids = list(graph.getIds(otype="Person", maxrows=10))
```

#### Get All IDs

```python
# Get all node IDs (no type filter)
all_ids = list(graph.getIds())
```

### Relationship Queries

#### All Relationships

```python
for subject, predicate, obj in graph.getRelations():
    print(f"{subject} --{predicate}--> {obj}")
```

#### Filter by Subject

```python
# What relationships does person_001 have?
for s, p, o in graph.getRelations(subject="person_001"):
    print(f"{s} --{p}--> {o}")
```

#### Filter by Predicate

```python
# All "knows" relationships
for s, p, o in graph.getRelations(predicate="knows"):
    print(f"{s} knows {o}")
```

#### Filter by Object

```python
# What points to person_001?
for s, p, o in graph.getRelations(obj="person_001"):
    print(f"{s} --{p}--> {o}")
```

#### Combine Filters

```python
# Specific relationship
relations = list(graph.getRelations(
    subject="person_001",
    predicate="knows",
    obj="person_002"
))

if relations:
    print("person_001 knows person_002")
```

### Aggregation

#### Count by Type

```python
for otype, count in graph.objectCounts():
    print(f"{otype}: {count}")
```

#### Count by Predicate

```python
for predicate, count in graph.predicateCounts():
    print(f"{predicate}: {count} relationships")
```

### Traversal

#### Breadth-First

```python
for s, p, o, depth in graph.breadthFirstTraversal("person_001"):
    indent = "  " * depth
    print(f"{indent}{s} --{p}--> {o} (depth: {depth})")
```

#### Find Roots

```python
# What nodes reference this one?
roots = graph.getRootsForPid("person_001")
print(f"Referenced by: {roots}")
```

#### Get Contributing Nodes

```python
# All PIDs that contribute to a complex node
node_ids = graph.getNodeIds("person_001")
print(f"Contributing PIDs: {node_ids}")
```

### Advanced Queries

#### Multi-Hop Traversal

```python
def find_friends_of_friends(graph, person_id):
    """Find friends of friends"""
    friends = set()
    fof = set()

    # First hop: direct friends
    for _, _, friend_id in graph.getRelations(subject=person_id, predicate="friend"):
        friends.add(friend_id)

        # Second hop: friends of friends
        for _, _, fof_id in graph.getRelations(subject=friend_id, predicate="friend"):
            if fof_id != person_id:  # Exclude original person
                fof.add(fof_id)

    # Remove direct friends from FOF
    fof -= friends
    fof.discard(person_id)

    return friends, fof
```

#### Path Finding

```python
def find_path(graph, start, end, max_depth=5):
    """Find path between two nodes"""
    from collections import deque

    queue = deque([(start, [start])])
    visited = set([start])

    while queue:
        current, path = queue.popleft()

        if len(path) > max_depth:
            continue

        if current == end:
            return path

        for _, _, neighbor in graph.getRelations(subject=current):
            if neighbor not in visited:
                visited.add(neighbor)
                queue.append((neighbor, path + [neighbor]))

    return None  # No path found
```

#### Custom SQL Queries

For complex queries, use SQL directly:

```python
# Find all people over 30 in New York
result = db.execute("""
    SELECT pid, name, age
    FROM node
    WHERE otype = 'Person'
      AND age > 30
      AND city = 'New York'
    ORDER BY age DESC
""").fetchall()

for pid, name, age in result:
    print(f"{name} ({age}): {pid}")
```

## Storage and Export

### Parquet Export

```python
import pathlib

# Export to Parquet
output_path = pathlib.Path("my_graph.parquet")
graph.asParquet(output_path)
```

#### With Grouping

For large graphs, split into multiple files:

```python
# Create multiple Parquet files
graph.asParquet(
    pathlib.Path("my_graph.parquet"),
    group_size=10000  # 10,000 rows per file
)

# Creates:
# my_graph-0.parquet
# my_graph-1.parquet
# my_graph-2.parquet
# ...
```

### Loading from Parquet

```python
import duckdb
from pqg import PQG

# Create new database
db = duckdb.connect()

# Load from Parquet
graph = PQG(db, source="my_graph.parquet")

# Load metadata
graph.loadMetadata()

# Now you can query
print(graph.objectCounts())
```

### GeoJSON Export

For spatial data:

```bash
# Using CLI
pqg geo my_graph.parquet > output.geojson
```

Or programmatically (requires implementing custom export).

### Graphviz Export

```python
# Generate DOT format
dot_lines = graph.toGraphviz()

# Save to file
with open("graph.dot", "w") as f:
    f.write("\n".join(dot_lines))
```

Render:

```bash
dot -Tpng graph.dot -o graph.png
dot -Tsvg graph.dot -o graph.svg
dot -Tpdf graph.dot -o graph.pdf
```

### Database Export

Export to DuckDB file:

```python
# Your graph is already in DuckDB
# Just use a file-based connection

db = duckdb.connect("my_graph.duckdb")
graph = PQG(db, source="my_graph")

# ... add data ...

db.commit()
db.close()

# Later, load it
db = duckdb.connect("my_graph.duckdb")
graph = PQG(db, source="my_graph")
graph.loadMetadata()
```

## Performance

### Best Practices

#### 1. Batch Operations

```python
# Slow: commit after each node
for person in people:
    graph.addNode(person)
    db.commit()  # Too many commits!

# Fast: commit once
for person in people:
    graph.addNode(person)
db.commit()  # Single commit
```

#### 2. Use Indexes

```python
# Add indexes for frequently queried fields
db.execute("CREATE INDEX idx_otype ON node(otype)")
db.execute("CREATE INDEX idx_s ON node(s)")
db.execute("CREATE INDEX idx_p ON node(p)")
```

#### 3. Filter Early

```python
# Less efficient
all_people = list(graph.getIds(otype="Person"))
filtered = [p for p in all_people if graph.getNode(p)['age'] > 30]

# More efficient - use SQL
result = db.execute("""
    SELECT pid FROM node
    WHERE otype = 'Person' AND age > 30
""").fetchall()
filtered = [row[0] for row in result]
```

#### 4. Limit Results

```python
# For exploration, don't load everything
sample = list(graph.getIds(otype="Person", maxrows=100))
```

#### 5. Cache Frequently Accessed Nodes

```python
node_cache = {}

def get_cached(graph, pid):
    if pid not in node_cache:
        node_cache[pid] = graph.getNode(pid)
    return node_cache[pid]
```

### Memory Management

For large graphs:

```python
# Use file-based DuckDB instead of in-memory
db = duckdb.connect("large_graph.duckdb")

# Process in chunks
chunk_size = 1000
for i in range(0, len(large_dataset), chunk_size):
    chunk = large_dataset[i:i+chunk_size]
    for item in chunk:
        graph.addNode(item)
    db.commit()
    print(f"Processed {i+chunk_size} items")
```

### Profiling

```python
import time

start = time.time()

# Your operation
for i in range(1000):
    graph.addNode(Person(name=f"Person {i}"))
db.commit()

elapsed = time.time() - start
print(f"Added 1000 nodes in {elapsed:.2f} seconds")
print(f"Rate: {1000/elapsed:.0f} nodes/second")
```

## Best Practices

### Data Modeling

#### Use Meaningful PIDs

```python
# Good - descriptive and unique
person = Person(pid="person_alice_smith_001")
book = Book(pid="isbn_978-0-123456-78-9")

# Okay - simple and unique
person = Person(pid="p001")

# Avoid - hard to debug
person = Person(pid="x")
```

#### Always Provide Labels

```python
# Good
person = Person(
    pid="p001",
    label="Alice Smith",  # Human-readable
    name="Alice Smith"
)

# Missing label makes debugging harder
person = Person(
    pid="p001",
    name="Alice Smith"
)
```

#### Document Your Schema

```python
@dataclass
class Sample(Base):
    """
    Represents a physical sample collected in the field.

    Attributes:
        sample_id: Unique identifier from collection system
        collected_date: ISO 8601 date string
        collector: PID of Person who collected the sample
        location: PID of Location where collected
    """
    sample_id: Optional[str] = None
    collected_date: Optional[str] = None
    collector: Optional[str] = None
    location: Optional[str] = None
```

### Code Organization

#### Separate Concerns

```python
# models.py - Define your types
@dataclass
class Person(Base):
    name: Optional[str] = None

# database.py - Database operations
def create_graph():
    db = duckdb.connect()
    graph = PQG(db, source="app")
    graph.registerType(Person)
    graph.initialize()
    return db, graph

# queries.py - Query functions
def get_person_by_name(graph, name):
    # Implementation
    pass

# main.py - Application logic
def main():
    db, graph = create_graph()
    # Use the graph
```

#### Create Helper Functions

```python
def add_person(graph, db, name, age, email):
    """Helper to add a person"""
    person = Person(
        pid=f"person_{name.lower().replace(' ', '_')}",
        label=name,
        name=name,
        age=age,
        email=email
    )
    graph.addNode(person)
    db.commit()
    return person.pid

def link_friendship(graph, db, person1_id, person2_id):
    """Create bidirectional friendship"""
    edge1 = Edge(s=person1_id, p="friend", o=[person2_id])
    edge2 = Edge(s=person2_id, p="friend", o=[person1_id])
    graph.addEdge(edge1)
    graph.addEdge(edge2)
    db.commit()
```

### Testing

```python
import pytest
import duckdb
from pqg import PQG

@pytest.fixture
def graph():
    """Create a test graph"""
    db = duckdb.connect()
    graph = PQG(db, source="test")
    graph.registerType(Person)
    graph.initialize()
    return db, graph

def test_add_person(graph):
    db, graph = graph

    person = Person(pid="test_001", name="Test Person")
    graph.addNode(person)
    db.commit()

    retrieved = graph.getNode("test_001")
    assert retrieved['name'] == "Test Person"

def test_relationship(graph):
    db, graph = graph

    p1 = Person(pid="p1", name="Alice")
    p2 = Person(pid="p2", name="Bob")

    graph.addNode(p1)
    graph.addNode(p2)

    edge = Edge(s="p1", p="knows", o=["p2"])
    graph.addEdge(edge)
    db.commit()

    relations = list(graph.getRelations(subject="p1", predicate="knows"))
    assert len(relations) == 1
    assert relations[0][2] == "p2"
```

## API Reference

### PQG Class

#### `__init__(dbinstance, source, primary_key_field="pid")`

Initialize a PQG instance.

**Parameters:**
- `dbinstance`: DuckDB connection
- `source`: Name or path identifier
- `primary_key_field`: Primary key column name (default: "pid")

#### `registerType(cls)`

Register a dataclass type with the graph.

**Parameters:**
- `cls`: Dataclass that inherits from `Base`

#### `initialize(classes=None)`

Create database schema.

**Parameters:**
- `classes`: Optional list of classes to register

#### `addNode(obj)`

Add a node to the graph.

**Parameters:**
- `obj`: Instance of a registered dataclass

#### `addEdge(edge)`

Add an edge to the graph.

**Parameters:**
- `edge`: Instance of `Edge`

#### `getNode(pid, max_depth=0)`

Retrieve a node.

**Parameters:**
- `pid`: Node identifier
- `max_depth`: Depth for expansion (default: 0)

**Returns:** Dictionary of node properties or None

#### `getIds(otype=None, maxrows=100)`

Get node identifiers.

**Parameters:**
- `otype`: Filter by type (optional)
- `maxrows`: Maximum results (default: 100)

**Returns:** Generator of (pid, otype) tuples

#### `getRelations(subject=None, predicate=None, obj=None)`

Query relationships.

**Parameters:**
- `subject`: Filter by subject (optional)
- `predicate`: Filter by predicate (optional)
- `obj`: Filter by object (optional)

**Returns:** Generator of (subject, predicate, object) tuples

#### `objectCounts()`

Count nodes by type.

**Returns:** Generator of (otype, count) tuples

#### `predicateCounts()`

Count relationships by predicate.

**Returns:** Generator of (predicate, count) tuples

#### `breadthFirstTraversal(pid)`

Traverse graph breadth-first.

**Parameters:**
- `pid`: Starting node

**Returns:** Generator of (subject, predicate, object, depth) tuples

#### `getRootsForPid(pid)`

Find nodes that reference a given node.

**Parameters:**
- `pid`: Target node

**Returns:** List of PIDs

#### `asParquet(dest_base_name, group_size=0)`

Export to Parquet.

**Parameters:**
- `dest_base_name`: Output path
- `group_size`: Rows per file (0 = single file)

#### `toGraphviz(nlights=None, elights=None)`

Generate Graphviz DOT format.

**Parameters:**
- `nlights`: List of node PIDs to highlight
- `elights`: List of edge PIDs to highlight

**Returns:** List of DOT format strings

### Base Class

Base class for all nodes.

**Fields:**
- `pid`: Optional[str] - Unique identifier
- `label`: Optional[str] - Human-readable label
- `description`: Optional[str] - Description
- `altids`: Optional[List[str]] - Alternative IDs
- `s`: Optional[str] - Subject (for edges)
- `p`: Optional[str] - Predicate (for edges)
- `o`: Optional[List[str]] - Objects (for edges)
- `n`: Optional[str] - Named graph (for edges)

### Edge Class

Represents a relationship.

**Constructor:**
```python
Edge(s, p, o, n=None, label=None, description=None)
```

## Troubleshooting

### Common Issues

#### Issue: "No such table: node"

**Cause:** Schema not initialized.

**Solution:**
```python
graph.initialize()
```

#### Issue: "No such column"

**Cause:** Type not registered before initialize.

**Solution:**
```python
graph.registerType(MyClass)  # Before initialize!
graph.initialize()
```

#### Issue: "Node not found"

**Cause:** Wrong PID or node not committed.

**Solution:**
```python
# Make sure you committed
db.commit()

# Check if node exists
node = graph.getNode(pid)
if node is None:
    print(f"Node {pid} does not exist")
```

#### Issue: Empty results from getIds()

**Cause:** No nodes of that type, or maxrows too small.

**Solution:**
```python
# Increase maxrows
ids = list(graph.getIds(otype="Person", maxrows=1000))

# Check object counts
print(dict(graph.objectCounts()))
```

#### Issue: "Module not found: pqg"

**Cause:** PQG not installed.

**Solution:**
```bash
cd pqg_directory
pip install -e .
```

### Getting Help

- Check the [tutorials](tutorials/) for examples
- Read [CLI Reference](cli-reference.md) for command-line usage
- Report issues on [GitHub](https://github.com/isamplesorg/pqg/issues)

## Further Reading

- [Getting Started Guide](getting-started.md)
- [Tutorial 1: Basic Usage](tutorials/01-basic-usage.md)
- [Tutorial 2: Complex Objects](tutorials/02-complex-objects.md)
- [Tutorial 3: Querying](tutorials/03-querying.md)
- [Tutorial 4: Visualization](tutorials/04-visualization.md)
- [CLI Reference](cli-reference.md)
