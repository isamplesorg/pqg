# CLI Reference

PQG provides a command-line interface for exploring and working with property graphs stored in Parquet files. This reference covers all available commands and options.

## Table of Contents

- [Installation](#installation)
- [Basic Usage](#basic-usage)
- [Global Options](#global-options)
- [Commands](#commands)
  - [entries](#entries)
  - [node](#node)
  - [refs](#refs)
  - [tree](#tree)
  - [types](#types)
  - [predicates](#predicates)
  - [metadata](#metadata)
  - [geo](#geo)
- [Examples](#examples)
- [Tips and Tricks](#tips-and-tricks)

## Installation

After installing PQG, the `pqg` command becomes available:

```bash
# Check installation
pqg --help

# Check version
python -c "import pqg; print(pqg.__version__)"
```

## Basic Usage

```bash
pqg [GLOBAL_OPTIONS] COMMAND [ARGS] [COMMAND_OPTIONS]
```

### Pattern

```
pqg --verbosity INFO entries my_graph.parquet --otype Book
│   └─ Global Option  └─ Command  └─ Required Arg  └─ Command Option
```

## Global Options

These options must come **before** the command name.

### `--verbosity`, `-V`

Set the logging level.

**Values**: `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`

**Default**: `WARNING`

```bash
# Show detailed debug information
pqg -V DEBUG entries graph.parquet

# Show only errors
pqg --verbosity ERROR types graph.parquet

# Standard info messages
pqg -V INFO node graph.parquet node_001
```

**Usage Tips**:
- Use `DEBUG` when troubleshooting issues
- Use `INFO` for normal operation with progress messages
- Use `WARNING` (default) for quiet operation
- Use `ERROR` to suppress all but critical messages

## Commands

---

### `entries`

List node identifiers and their types in the graph.

#### Syntax

```bash
pqg entries STORE [OPTIONS]
```

#### Arguments

| Argument | Required | Description |
|----------|----------|-------------|
| `STORE` | Yes | Path to Parquet file or database |

#### Options

| Option | Short | Type | Default | Description |
|--------|-------|------|---------|-------------|
| `--otype` | `-o` | string | `None` | Filter by object type |
| `--maxrows` | `-m` | integer | `100` | Maximum rows to return |

#### Examples

```bash
# List all entries (first 100)
pqg entries library.parquet

# List only books
pqg entries library.parquet --otype Book

# List only books, show up to 10
pqg entries library.parquet -o Book -m 10

# List all authors
pqg entries library.parquet --otype Author --maxrows 50
```

#### Output Format

```
PID                  OTYPE
────────────────────────────────────
book_001            Book
book_002            Book
author_001          Author
```

#### Use Cases

- Quick overview of graph contents
- Finding PIDs for use with other commands
- Checking what types exist in the graph
- Validating data import

---

### `node`

Retrieve and display a single node or node with all its relationships.

#### Syntax

```bash
pqg node STORE PID [OPTIONS]
```

#### Arguments

| Argument | Required | Description |
|----------|----------|-------------|
| `STORE` | Yes | Path to Parquet file or database |
| `PID` | Yes | Unique identifier of the node |

#### Options

| Option | Short | Type | Default | Description |
|--------|-------|------|---------|-------------|
| `--expand` | `-e` | flag | `False` | Include all connected nodes |

#### Examples

```bash
# Get a single node
pqg node library.parquet book_001

# Get a node with all its connections
pqg node library.parquet book_001 --expand

# Expanded view (short form)
pqg node library.parquet author_001 -e
```

#### Output Format

**Basic node**:
```json
{
  "pid": "book_001",
  "otype": "Book",
  "title": "Foundation",
  "year": 1951,
  "label": "Foundation"
}
```

**Expanded node** (includes connected nodes):
```json
{
  "pid": "book_001",
  "otype": "Book",
  "title": "Foundation",
  "year": 1951,
  "label": "Foundation",
  "_related": {
    "authored_by": [
      {
        "pid": "author_001",
        "name": "Isaac Asimov",
        ...
      }
    ]
  }
}
```

#### Use Cases

- Inspecting node properties
- Debugging data issues
- Understanding node relationships
- Extracting specific records

---

### `refs`

Find all nodes that reference a given node (recursive parent lookup).

#### Syntax

```bash
pqg refs STORE PID
```

#### Arguments

| Argument | Required | Description |
|----------|----------|-------------|
| `STORE` | Yes | Path to Parquet file or database |
| `PID` | Yes | Unique identifier of the node |

#### Examples

```bash
# Find what references an author
pqg refs library.parquet author_001

# Find what references a book
pqg refs library.parquet book_001
```

#### Output Format

```json
[
  "book_001",
  "book_002",
  "review_001"
]
```

#### Use Cases

- Finding all usages of a node
- Identifying dependencies
- Impact analysis before deletion
- Understanding graph structure

---

### `tree`

Display all nodes referenced by a given node as a tree structure.

#### Syntax

```bash
pqg tree STORE PID
```

#### Arguments

| Argument | Required | Description |
|----------|----------|-------------|
| `STORE` | Yes | Path to Parquet file or database |
| `PID` | Yes | Unique identifier of the root node |

#### Examples

```bash
# Show tree structure from a project
pqg tree research.parquet project_001

# Show book and all its connections
pqg tree library.parquet book_001
```

#### Output Format

```
book_001 (Book)
├── author_001 (Author)
│   └── org_001 (Organization)
└── publisher_001 (Publisher)
    └── location_001 (Location)
```

#### Use Cases

- Visualizing hierarchical structures
- Understanding complex object composition
- Exploring graph neighborhoods
- Documentation and reporting

---

### `types`

List all object types in the graph with their counts.

#### Syntax

```bash
pqg types STORE
```

#### Arguments

| Argument | Required | Description |
|----------|----------|-------------|
| `STORE` | Yes | Path to Parquet file or database |

#### Examples

```bash
# Show all types
pqg types library.parquet

# With verbose output
pqg -V INFO types library.parquet
```

#### Output Format

```
Object Types
─────────────────────
Author          3
Book           15
Review          8
_edge_         32
```

#### Use Cases

- Understanding graph composition
- Validating data imports
- Planning queries
- Generating statistics

---

### `predicates`

List all relationship types (predicates) in the graph with their counts.

#### Syntax

```bash
pqg predicates STORE
```

#### Arguments

| Argument | Required | Description |
|----------|----------|-------------|
| `STORE` | Yes | Path to Parquet file or database |

#### Examples

```bash
# Show all predicates
pqg predicates library.parquet

# With detailed logging
pqg --verbosity DEBUG predicates library.parquet
```

#### Output Format

```
Predicates
─────────────────────
authored_by      15
published_by     15
reviews           8
cited_by         12
```

#### Use Cases

- Understanding relationships in the graph
- Finding relationship types for queries
- Validating graph structure
- Documenting graph schema

---

### `metadata`

Display key-value metadata from a Parquet file.

#### Syntax

```bash
pqg metadata STORE
```

#### Arguments

| Argument | Required | Description |
|----------|----------|-------------|
| `STORE` | Yes | Path to Parquet file |

#### Examples

```bash
# Show metadata
pqg metadata library.parquet

# Pretty-print JSON output
pqg metadata library.parquet | jq .
```

#### Output Format

```json
{
  "pqg_version": "0.2.0",
  "pqg_primary_key": "pid",
  "pqg_node_types": {
    "Author": {
      "name": "name VARCHAR DEFAULT NULL",
      "birth_year": "birth_year INTEGER DEFAULT NULL"
    },
    "Book": {
      "title": "title VARCHAR DEFAULT NULL",
      "year": "year INTEGER DEFAULT NULL"
    }
  },
  "pqg_edge_fields": ["pid", "otype", "s", "p", "o", "n"],
  "pqg_literal_fields": ["name", "title", "year", ...]
}
```

#### Use Cases

- Understanding graph schema
- Checking PQG version used to create file
- Debugging import/export issues
- Documentation

---

### `geo`

Export nodes with geometry as a GeoJSON FeatureCollection.

#### Syntax

```bash
pqg geo STORE
```

#### Arguments

| Argument | Required | Description |
|----------|----------|-------------|
| `STORE` | Yes | Path to Parquet file or database |

#### Examples

```bash
# Export to GeoJSON
pqg geo locations.parquet > locations.geojson

# View in browser with pretty-printing
pqg geo locations.parquet | jq . > locations_pretty.geojson

# Count features
pqg geo locations.parquet | jq '.features | length'
```

#### Output Format

```json
{
  "type": "FeatureCollection",
  "features": [
    {
      "type": "Feature",
      "id": "location_001",
      "geometry": {
        "type": "Point",
        "coordinates": [-74.0060, 40.7128]
      },
      "properties": {
        "name": "New York",
        "population": 8000000
      }
    }
  ]
}
```

#### Use Cases

- Exporting spatial data for GIS applications
- Creating web maps
- Spatial analysis
- Integration with mapping libraries

---

## Examples

### Example 1: Exploring a New Graph

```bash
# Get overview
pqg types library.parquet
pqg predicates library.parquet

# List first few entries
pqg entries library.parquet --maxrows 5

# Examine a specific node
pqg node library.parquet book_001

# See its relationships
pqg tree library.parquet book_001
```

### Example 2: Finding Specific Data

```bash
# Find all books
pqg entries library.parquet --otype Book -m 1000 > books.txt

# Get details for each book (using shell loop)
while read pid _; do
  pqg node library.parquet "$pid" >> book_details.json
done < books.txt
```

### Example 3: Analyzing Graph Structure

```bash
# Count node types
pqg types library.parquet

# Count relationship types
pqg predicates library.parquet

# Check metadata
pqg metadata library.parquet | jq '.pqg_node_types | keys'
```

### Example 4: Exporting Data

```bash
# Export spatial data
pqg geo samples.parquet > samples.geojson

# Open in QGIS or other GIS software
qgis samples.geojson
```

### Example 5: Pipeline Processing

```bash
# Get all authors, extract names, sort, count
pqg entries library.parquet --otype Author --maxrows 1000 | \
  cut -f1 | \
  while read pid; do
    pqg node library.parquet "$pid" | jq -r '.name'
  done | \
  sort | \
  uniq -c | \
  sort -rn
```

## Tips and Tricks

### Tip 1: Use Shell Redirection

```bash
# Save output to file
pqg types library.parquet > types.txt

# Append to file
pqg predicates library.parquet >> analysis.txt

# Pipe to other tools
pqg entries library.parquet | grep "Book"
```

### Tip 2: Combine with jq

```bash
# Pretty-print JSON
pqg node library.parquet book_001 | jq .

# Extract specific field
pqg node library.parquet book_001 | jq -r '.title'

# Filter by property
pqg entries library.parquet --otype Book | \
  cut -f1 | \
  while read pid; do
    pqg node library.parquet "$pid"
  done | \
  jq 'select(.year > 1960)'
```

### Tip 3: Create Aliases

Add to your `.bashrc` or `.zshrc`:

```bash
# Quick aliases
alias pqt='pqg types'
alias pqp='pqg predicates'
alias pqn='pqg node'
alias pqe='pqg entries'

# Use them
pqt library.parquet
pqn library.parquet book_001
```

### Tip 4: Debugging with Verbosity

```bash
# When something doesn't work
pqg -V DEBUG node library.parquet problematic_id

# See what's happening
pqg --verbosity INFO entries large_graph.parquet
```

### Tip 5: Scripting Common Tasks

Create a script `explore_graph.sh`:

```bash
#!/bin/bash

GRAPH=$1

echo "=== Graph Overview ==="
pqg types "$GRAPH"

echo ""
echo "=== Relationships ==="
pqg predicates "$GRAPH"

echo ""
echo "=== Sample Entries ==="
pqg entries "$GRAPH" --maxrows 5
```

Use it:

```bash
chmod +x explore_graph.sh
./explore_graph.sh library.parquet
```

### Tip 6: Working with Large Graphs

```bash
# Sample data instead of loading everything
pqg entries huge_graph.parquet --maxrows 100 > sample_ids.txt

# Process in batches
split -l 10 sample_ids.txt batch_

for batch in batch_*; do
  while read pid _; do
    pqg node huge_graph.parquet "$pid"
  done < "$batch" > "output_${batch}.json"
done
```

## Command Cheat Sheet

| Task | Command |
|------|---------|
| List all node types | `pqg types STORE` |
| List all relationship types | `pqg predicates STORE` |
| List nodes of type X | `pqg entries STORE -o X` |
| Get node details | `pqg node STORE PID` |
| Get node with relationships | `pqg node STORE PID -e` |
| Show node tree | `pqg tree STORE PID` |
| Find node references | `pqg refs STORE PID` |
| Show metadata | `pqg metadata STORE` |
| Export to GeoJSON | `pqg geo STORE > output.geojson` |
| Debug command | `pqg -V DEBUG COMMAND ...` |

## Common Issues

### Issue: "No such file or directory"

```bash
# Wrong
pqg types library  # Missing .parquet extension

# Right
pqg types library.parquet
```

### Issue: "No node found with pid"

```bash
# Check if node exists
pqg entries graph.parquet | grep "node_id"

# Use correct PID
pqg node graph.parquet correct_pid
```

### Issue: Empty output

```bash
# Add verbosity to see what's happening
pqg -V INFO types empty_graph.parquet

# Check metadata
pqg metadata empty_graph.parquet
```

### Issue: Command not found

```bash
# Make sure PQG is installed
pip list | grep pqg

# Reinstall if needed
pip install -e .

# Check PATH
which pqg
```

## Related Documentation

- [Getting Started](getting-started.md) - Installation and basic concepts
- [User Guide](user-guide.md) - Comprehensive PQG usage guide
- [Tutorials](tutorials/) - Step-by-step learning materials

## Contributing

Found an issue with the CLI? Report it on [GitHub](https://github.com/isamplesorg/pqg/issues).
