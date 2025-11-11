# Property Graph in DuckDB (PQG)

A Python library for building and querying property graphs using DuckDB.

## What is PQG?

PQG provides a middle ground between full-featured property graph databases (like Neo4j) and fully decomposed RDF stores. It uses a single-table design backed by DuckDB, making it fast, lightweight, and easy to use.

**Key Features:**

- **Simple & Fast**: Single table design with DuckDB backend
- **Python Native**: Work with Python dataclasses - no query language needed
- **Automatic Decomposition**: Complex nested objects automatically become nodes and edges
- **Spatial Support**: Built-in geographic data handling and GeoJSON export
- **Multiple Formats**: Export to Parquet, Graphviz, GeoJSON
- **Type-Safe**: Leverages Python type hints for data validation

## Quick Start

```python
import duckdb
from pqg import PQG, Base, Edge
from dataclasses import dataclass
from typing import Optional

# Define your data model
@dataclass
class Person(Base):
    name: Optional[str] = None
    age: Optional[int] = None

# Create a graph
db = duckdb.connect()
graph = PQG(db, source="my_graph")
graph.registerType(Person)
graph.initialize()

# Add nodes
alice = Person(pid="alice", name="Alice", age=30)
bob = Person(pid="bob", name="Bob", age=25)
graph.addNode(alice)
graph.addNode(bob)

# Add relationships
friendship = Edge(s="alice", p="knows", o=["bob"])
graph.addEdge(friendship)
db.commit()

# Query
for subject, predicate, obj in graph.getRelations():
    print(f"{subject} {predicate} {obj}")
```

## Documentation

### For New Users

- **[Getting Started Guide](docs/getting-started.md)** - Installation and basic concepts
- **[Tutorial 1: Basic Usage](docs/tutorials/01-basic-usage.md)** - Creating nodes and edges
- **[Tutorial 2: Complex Objects](docs/tutorials/02-complex-objects.md)** - Working with nested data
- **[Tutorial 3: Querying](docs/tutorials/03-querying.md)** - Powerful graph queries
- **[Tutorial 4: Visualization](docs/tutorials/04-visualization.md)** - Creating visualizations

### Reference Documentation

- **[User Guide](docs/user-guide.md)** - Comprehensive reference for all features
- **[CLI Reference](docs/cli-reference.md)** - Command-line tool documentation
- **[iSamples Format](isamples/README.md)** - Domain-specific implementation

## How PQG Works

### Nodes (Entities)

Every node has these properties:

- `row_id` - Auto-incrementing integer primary key
- `pid` - Unique identifier (globally unique but not primary key)
- `otype` - Type/class of the node
- `label` - Human-readable name
- `description` - Longer description
- `altids` - Alternative identifiers
- Plus any custom properties you define

### Edges (Relationships)

Edges follow the Subject-Predicate-Object pattern:

- `s` - Subject row_id (integer reference to source node)
- `p` - Predicate (relationship type)
- `o` - Object row_ids (integer array referencing target nodes)
- `n` - Named graph (optional)

> **Note**: The API accepts PIDs (strings) when creating edges, but internally PQG uses integer row_id references for performance. This conversion is automatic and transparent.

### Single Table Design

All data lives in one table, making queries fast and storage efficient:

```sql
CREATE TABLE node (
    row_id INTEGER PRIMARY KEY DEFAULT nextval('row_id_sequence'),
    pid VARCHAR UNIQUE NOT NULL,
    otype VARCHAR,
    label VARCHAR,
    -- Edge fields
    s INTEGER,
    p VARCHAR,
    o INTEGER[],
    -- Your custom fields
    name VARCHAR,
    age INTEGER,
    ...
);
```

### Automatic Decomposition

When you add complex nested objects, PQG automatically:

1. Separates them into individual nodes
2. Creates edges to represent relationships
3. Uses property names as relationship types

This means you work with natural Python objects while PQG handles the graph structure!


## Installation

### Option 1: Using venv

```bash
git clone https://github.com/isamplesorg/pqg.git
cd pqg
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
pip install -e .
pqg --help
```

### Option 2: Using uv (faster)

```bash
git clone https://github.com/isamplesorg/pqg.git
cd pqg
uv sync
uv run pqg --help
```

**Requirements:** Python >= 3.11

## Common Use Cases

### Knowledge Graphs

Build interconnected knowledge bases with typed entities and relationships:

```python
@dataclass
class Concept(Base):
    term: Optional[str] = None
    definition: Optional[str] = None

concept1 = Concept(pid="ml", term="Machine Learning")
concept2 = Concept(pid="ai", term="Artificial Intelligence")

# Link concepts
graph.addEdge(Edge(s="ml", p="part_of", o=["ai"]))
```

### Research Data Management

Track samples, experiments, and publications:

```python
@dataclass
class Sample(Base):
    sample_id: Optional[str] = None
    collection_date: Optional[str] = None
    collector: Optional[str] = None  # PID of Person (not Person object)

@dataclass
class Publication(Base):
    title: Optional[str] = None
    doi: Optional[str] = None
```

### Data Lineage

Track how data flows through transformations:

```python
@dataclass
class Dataset(Base):
    name: Optional[str] = None
    version: Optional[str] = None

# Show lineage
graph.addEdge(Edge(s="processed_data", p="derived_from", o=["raw_data"]))
```

### Spatial Data Networks

Work with geographic data and relationships:

```python
@dataclass
class Location(Base):
    name: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None

# Export to GeoJSON
graph.asParquet(Path("locations.parquet"))
# Then: pqg geo locations.parquet > map.geojson
```

## Command-Line Tools

PQG includes CLI tools for exploring graphs:

```bash
# List node types
pqg types my_graph.parquet

# List relationship types
pqg predicates my_graph.parquet

# View a specific node
pqg node my_graph.parquet node_001

# View with all connections
pqg node my_graph.parquet node_001 --expand

# Show as tree
pqg tree my_graph.parquet node_001

# Export spatial data
pqg geo my_graph.parquet > output.geojson
```

See the [CLI Reference](docs/cli-reference.md) for complete documentation.

## Why PQG?

**vs. Neo4j / Full Graph Databases:**
- Lighter weight, no server required
- Easier deployment and maintenance
- File-based storage (Parquet)
- Perfect for analytics and data science

**vs. RDF / Triple Stores:**
- More intuitive Python API
- Better performance for analytical queries
- Simpler data model
- Native support for complex datatypes

**vs. Relational Databases:**
- Natural representation of connected data
- Easier to query relationships
- Automatic handling of nested objects
- Better for exploratory analysis

## Performance

PQG is designed for analytical workloads:

- **Fast reads**: Columnar storage with DuckDB
- **Efficient storage**: Parquet compression
- **Scalable**: Handle millions of nodes/edges
- **In-memory or on-disk**: Flexible deployment

For production applications requiring high transaction throughput, consider a dedicated graph database.

## Contributing

Contributions are welcome! Please:

1. Check existing issues or create a new one
2. Fork the repository
3. Create a feature branch
4. Make your changes with tests
5. Submit a pull request

## License

See the LICENSE file for details.

## Citation

If you use PQG in your research, please cite:

```bibtex
@software{pqg,
  title = {PQG: Property Graph in DuckDB},
  author = {iSamples Contributors},
  url = {https://github.com/isamplesorg/pqg},
  year = {2024}
}
```

## Acknowledgments

PQG is developed as part of the [iSamples](https://isamplesorg.github.io/) project.

## Support

- **Documentation**: [docs/](docs/)
- **Issues**: [GitHub Issues](https://github.com/isamplesorg/pqg/issues)
- **Discussions**: [GitHub Discussions](https://github.com/isamplesorg/pqg/discussions)
