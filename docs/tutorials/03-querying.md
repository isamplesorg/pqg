# Tutorial 3: Querying the Graph

Mastering graph queries is essential for getting the most out of PQG. This tutorial covers all the different ways to query, filter, and traverse your graph data.

## What You'll Learn

- Basic node and edge queries
- Pattern matching with `getRelations()`
- Graph traversal techniques
- Aggregation and counting
- Finding paths and root nodes
- Performance tips

## Prerequisites

- Completed [Tutorial 1](01-basic-usage.md) and [Tutorial 2](02-complex-objects.md)
- A graph with some data (we'll use the library example from Tutorial 1)
- 25-30 minutes

## Setup

We'll use an extended version of the library graph:

```python
import duckdb
from dataclasses import dataclass
from typing import Optional, List
from pqg import PQG, Base, Edge

@dataclass
class Author(Base):
    name: Optional[str] = None
    birth_year: Optional[int] = None
    nationality: Optional[str] = None

@dataclass
class Book(Base):
    title: Optional[str] = None
    year: Optional[int] = None
    genre: Optional[str] = None
    pages: Optional[int] = None

@dataclass
class Review(Base):
    reviewer_name: Optional[str] = None
    rating: Optional[int] = None  # 1-5 stars
    review_text: Optional[str] = None

def setup_sample_graph():
    """Create a sample graph with books, authors, and reviews"""
    db = duckdb.connect()
    graph = PQG(db, source="library")

    graph.registerType(Author)
    graph.registerType(Book)
    graph.registerType(Review)
    graph.initialize()

    # Add authors
    authors = [
        Author(pid="auth_1", name="Isaac Asimov", birth_year=1920, nationality="American"),
        Author(pid="auth_2", name="Ursula K. Le Guin", birth_year=1929, nationality="American"),
        Author(pid="auth_3", name="Philip K. Dick", birth_year=1928, nationality="American"),
    ]

    # Add books
    books = [
        Book(pid="book_1", title="Foundation", year=1951, genre="Science Fiction", pages=255),
        Book(pid="book_2", title="I, Robot", year=1950, genre="Science Fiction", pages=224),
        Book(pid="book_3", title="The Left Hand of Darkness", year=1969, genre="Science Fiction", pages=304),
        Book(pid="book_4", title="The Dispossessed", year=1974, genre="Science Fiction", pages=387),
        Book(pid="book_5", title="Do Androids Dream", year=1968, genre="Science Fiction", pages=210),
    ]

    # Add reviews
    reviews = [
        Review(pid="rev_1", reviewer_name="Alice", rating=5, review_text="Masterpiece!"),
        Review(pid="rev_2", reviewer_name="Bob", rating=4, review_text="Very good"),
        Review(pid="rev_3", reviewer_name="Carol", rating=5, review_text="Changed my life"),
        Review(pid="rev_4", reviewer_name="Dave", rating=3, review_text="It was okay"),
        Review(pid="rev_5", reviewer_name="Eve", rating=5, review_text="Brilliant"),
    ]

    # Add nodes
    for item in authors + books + reviews:
        graph.addNode(item)

    # Create relationships
    edges = [
        # Author relationships
        Edge(s="book_1", p="authored_by", o=["auth_1"]),
        Edge(s="book_2", p="authored_by", o=["auth_1"]),
        Edge(s="book_3", p="authored_by", o=["auth_2"]),
        Edge(s="book_4", p="authored_by", o=["auth_2"]),
        Edge(s="book_5", p="authored_by", o=["auth_3"]),

        # Review relationships
        Edge(s="rev_1", p="reviews", o=["book_1"]),
        Edge(s="rev_2", p="reviews", o=["book_1"]),
        Edge(s="rev_3", p="reviews", o=["book_3"]),
        Edge(s="rev_4", p="reviews", o=["book_5"]),
        Edge(s="rev_5", p="reviews", o=["book_4"]),

        # Series relationships
        Edge(s="book_1", p="series", o=["book_2"]),  # Foundation series
    ]

    for edge in edges:
        graph.addEdge(edge)

    db.commit()
    return db, graph
```

## Query Method 1: Get Node by ID

The simplest query - retrieve a single node:

```python
def query_by_id(graph):
    """Get a specific node by its PID"""

    print("="*60)
    print("QUERY BY ID")
    print("="*60)

    # Get a single node
    book = graph.getNode("book_1")

    print(f"\nBook PID: book_1")
    print(f"Title: {book['title']}")
    print(f"Year: {book['year']}")
    print(f"Genre: {book['genre']}")
    print(f"Pages: {book['pages']}")

    # Check if node exists
    node = graph.getNode("nonexistent_id")
    if node is None:
        print("\nNode 'nonexistent_id' not found")
```

## Query Method 2: Get All IDs by Type

Retrieve all nodes of a specific type:

```python
def query_by_type(graph):
    """Get all nodes of a specific type"""

    print("\n" + "="*60)
    print("QUERY BY TYPE")
    print("="*60)

    # Get all authors
    print("\nAll Authors:")
    author_ids = list(graph.getIds(otype="Author"))
    for author_id in author_ids:
        author = graph.getNode(author_id)
        print(f"  {author['name']} (b. {author['birth_year']})")

    # Get all books
    print("\nAll Books:")
    book_ids = list(graph.getIds(otype="Book"))
    for book_id in book_ids:
        book = graph.getNode(book_id)
        print(f"  {book['title']} ({book['year']})")

    # Limit results
    print("\nFirst 2 Books only:")
    limited_ids = list(graph.getIds(otype="Book", maxrows=2))
    print(f"  Got {len(limited_ids)} results")
```

## Query Method 3: Pattern Matching with getRelations()

The most powerful query method - find relationships:

```python
def query_relations(graph):
    """Demonstrate relationship pattern matching"""

    print("\n" + "="*60)
    print("RELATIONSHIP QUERIES")
    print("="*60)

    # Pattern 1: All relationships (s, p, o)
    print("\n1. ALL relationships:")
    all_relations = list(graph.getRelations())
    for s, p, o in all_relations[:5]:  # Show first 5
        print(f"  {s} --{p}--> {o}")
    print(f"  ... ({len(all_relations)} total)")

    # Pattern 2: Filter by subject (who/what is the source?)
    print("\n2. All relationships FROM book_1:")
    from_book1 = list(graph.getRelations(subject="book_1"))
    for s, p, o in from_book1:
        print(f"  {s} --{p}--> {o}")

    # Pattern 3: Filter by predicate (what type of relationship?)
    print("\n3. All 'authored_by' relationships:")
    authored = list(graph.getRelations(predicate="authored_by"))
    for s, p, o in authored:
        book = graph.getNode(s)
        author = graph.getNode(o)
        print(f"  '{book['title']}' by {author['name']}")

    # Pattern 4: Filter by object (who/what is the target?)
    print("\n4. All relationships TO auth_1 (Asimov):")
    to_asimov = list(graph.getRelations(obj="auth_1"))
    for s, p, o in to_asimov:
        book = graph.getNode(s)
        print(f"  {book['title']} --{p}--> Asimov")

    # Pattern 5: Combine filters
    print("\n5. Books authored by auth_2 (Le Guin):")
    leguin_books = list(graph.getRelations(predicate="authored_by", obj="auth_2"))
    for s, p, o in leguin_books:
        book = graph.getNode(s)
        print(f"  {book['title']} ({book['year']})")
```

## Query Method 4: Aggregation

Count and summarize your data:

```python
def query_aggregates(graph):
    """Count and aggregate data"""

    print("\n" + "="*60)
    print("AGGREGATION QUERIES")
    print("="*60)

    # Count by object type
    print("\nObjects by type:")
    for otype, count in graph.objectCounts():
        print(f"  {otype}: {count}")

    # Count by relationship type
    print("\nRelationships by predicate:")
    for predicate, count in graph.predicateCounts():
        print(f"  {predicate}: {count}")

    # Custom aggregations using Python
    print("\nCustom: Books per author:")

    # Get all authors
    author_ids = list(graph.getIds(otype="Author"))

    for author_id in author_ids:
        author = graph.getNode(author_id)

        # Count books by this author
        books = list(graph.getRelations(predicate="authored_by", obj=author_id))

        print(f"  {author['name']}: {len(books)} book(s)")

    # Another custom: Average rating per book
    print("\nCustom: Average rating per book:")

    book_ids = list(graph.getIds(otype="Book"))

    for book_id in book_ids:
        book = graph.getNode(book_id)

        # Get all reviews for this book
        review_edges = list(graph.getRelations(predicate="reviews", obj=book_id))

        if review_edges:
            ratings = []
            for review_id, _, _ in review_edges:
                review = graph.getNode(review_id)
                ratings.append(review['rating'])

            avg_rating = sum(ratings) / len(ratings)
            print(f"  {book['title']}: {avg_rating:.1f}/5 ({len(ratings)} reviews)")
        else:
            print(f"  {book['title']}: No reviews")
```

## Query Method 5: Graph Traversal

Navigate through connected nodes:

```python
def query_traversal(graph):
    """Traverse the graph in different ways"""

    print("\n" + "="*60)
    print("GRAPH TRAVERSAL")
    print("="*60)

    # Breadth-first traversal
    print("\n1. Breadth-first from book_1:")

    for s, p, o, depth in graph.breadthFirstTraversal("book_1"):
        indent = "  " * depth
        s_node = graph.getNode(s)
        o_node = graph.getNode(o)

        s_label = s_node.get('title') or s_node.get('name') or s
        o_label = o_node.get('title') or o_node.get('name') or o

        print(f"{indent}{s_label} --{p}--> {o_label}")

    # Manual depth-first traversal
    print("\n2. Manual traversal: Book -> Author -> All their books:")

    book_id = "book_1"
    book = graph.getNode(book_id)
    print(f"\nStarting from: {book['title']}")

    # Find the author
    author_edges = list(graph.getRelations(subject=book_id, predicate="authored_by"))

    if author_edges:
        _, _, author_id = author_edges[0]
        author = graph.getNode(author_id)
        print(f"  Author: {author['name']}")

        # Find all books by this author
        all_author_books = list(graph.getRelations(predicate="authored_by", obj=author_id))
        print(f"  All books by {author['name']}:")

        for other_book_id, _, _ in all_author_books:
            other_book = graph.getNode(other_book_id)
            print(f"    - {other_book['title']} ({other_book['year']})")
```

## Query Method 6: Finding Roots and Paths

Trace back to parent nodes:

```python
def query_roots(graph):
    """Find root nodes that reference a given node"""

    print("\n" + "="*60)
    print("FINDING ROOT NODES")
    print("="*60)

    # Find what references a specific author
    author_id = "auth_1"
    author = graph.getNode(author_id)

    print(f"\nFinding roots for: {author['name']}")

    roots = graph.getRootsForPid(author_id)

    print(f"Nodes that reference this author:")
    for root_id in roots:
        root = graph.getNode(root_id)
        root_type = root.get('otype', 'Unknown')

        if root_type == 'Book':
            print(f"  Book: {root.get('title')}")
        else:
            print(f"  {root_type}: {root_id}")

    # Find all nodes that contribute to a complex node
    print("\n\nFinding all contributing nodes for book_1:")

    node_ids = graph.getNodeIds("book_1")

    print(f"All PIDs that contribute to book_1: {node_ids}")
```

## Advanced Query Patterns

### Pattern 1: Multi-Hop Queries

```python
def multi_hop_query(graph):
    """Query across multiple hops"""

    print("\n" + "="*60)
    print("MULTI-HOP QUERIES")
    print("="*60)

    # Find: Review -> Book -> Author (2 hops)
    print("\nReviews and the authors they're reviewing:")

    review_ids = list(graph.getIds(otype="Review"))

    for review_id in review_ids:
        review = graph.getNode(review_id)

        # Hop 1: Review -> Book
        book_edges = list(graph.getRelations(subject=review_id, predicate="reviews"))

        if book_edges:
            _, _, book_id = book_edges[0]
            book = graph.getNode(book_id)

            # Hop 2: Book -> Author
            author_edges = list(graph.getRelations(subject=book_id, predicate="authored_by"))

            if author_edges:
                _, _, author_id = author_edges[0]
                author = graph.getNode(author_id)

                print(f"\n{review['reviewer_name']} rated {review['rating']}/5:")
                print(f"  Book: {book['title']}")
                print(f"  Author: {author['name']}")
```

### Pattern 2: Filtering with Python

```python
def filtered_queries(graph):
    """Combine PQG queries with Python filtering"""

    print("\n" + "="*60)
    print("FILTERED QUERIES")
    print("="*60)

    # Find books published in the 1950s
    print("\nBooks from the 1950s:")

    book_ids = list(graph.getIds(otype="Book"))

    fifties_books = []
    for book_id in book_ids:
        book = graph.getNode(book_id)
        if 1950 <= book['year'] < 1960:
            fifties_books.append(book)

    for book in sorted(fifties_books, key=lambda b: b['year']):
        print(f"  {book['year']}: {book['title']}")

    # Find highly-rated books (4+ stars average)
    print("\nHighly-rated books (4+ stars):")

    for book_id in book_ids:
        book = graph.getNode(book_id)

        # Get reviews
        review_edges = list(graph.getRelations(predicate="reviews", obj=book_id))

        if review_edges:
            ratings = [
                graph.getNode(rev_id)['rating']
                for rev_id, _, _ in review_edges
            ]

            avg_rating = sum(ratings) / len(ratings)

            if avg_rating >= 4.0:
                print(f"  {book['title']}: {avg_rating:.1f}/5")
```

### Pattern 3: Building Subgraphs

```python
def extract_subgraph(graph, root_pid, max_depth=2):
    """Extract a subgraph around a node"""

    print(f"\n" + "="*60)
    print(f"SUBGRAPH EXTRACTION")
    print("="*60)

    visited_nodes = set()
    visited_edges = []

    def traverse(pid, depth):
        if depth > max_depth or pid in visited_nodes:
            return

        visited_nodes.add(pid)

        # Get all edges from this node
        outgoing = list(graph.getRelations(subject=pid))

        for s, p, o in outgoing:
            visited_edges.append((s, p, o))
            traverse(o, depth + 1)

    # Start traversal
    traverse(root_pid, 0)

    print(f"\nSubgraph around {root_pid} (max depth {max_depth}):")
    print(f"Nodes: {len(visited_nodes)}")
    print(f"Edges: {len(visited_edges)}")

    print("\nNodes in subgraph:")
    for node_id in visited_nodes:
        node = graph.getNode(node_id)
        label = node.get('title') or node.get('name') or node_id
        print(f"  {label} ({node.get('otype', 'Unknown')})")

    return visited_nodes, visited_edges
```

## Performance Tips

### Tip 1: Use Specific Queries

```python
# Slower: Get all, then filter
all_relations = list(graph.getRelations())
filtered = [r for r in all_relations if r[1] == "authored_by"]

# Faster: Filter in the query
authored = list(graph.getRelations(predicate="authored_by"))
```

### Tip 2: Limit Results When Exploring

```python
# Get just enough to understand the data
sample_ids = list(graph.getIds(otype="Book", maxrows=10))
```

### Tip 3: Cache Frequently Used Nodes

```python
# Cache commonly accessed nodes
node_cache = {}

def get_cached_node(graph, pid):
    if pid not in node_cache:
        node_cache[pid] = graph.getNode(pid)
    return node_cache[pid]
```

## Complete Example

```python
def main():
    """Run all query examples"""

    print("Setting up sample graph...")
    db, graph = setup_sample_graph()

    # Run all query demonstrations
    query_by_id(graph)
    query_by_type(graph)
    query_relations(graph)
    query_aggregates(graph)
    query_traversal(graph)
    query_roots(graph)
    multi_hop_query(graph)
    filtered_queries(graph)
    extract_subgraph(graph, "book_1", max_depth=2)

    print("\n" + "="*60)
    print("TUTORIAL COMPLETE")
    print("="*60)

if __name__ == "__main__":
    main()
```

## Query Patterns Summary

| Task | Method | Example |
|------|--------|---------|
| Get one node | `getNode(pid)` | `graph.getNode("book_1")` |
| Get all of type | `getIds(otype)` | `graph.getIds(otype="Book")` |
| Find relationships | `getRelations()` | `graph.getRelations(predicate="authored_by")` |
| Count objects | `objectCounts()` | `graph.objectCounts()` |
| Count relationships | `predicateCounts()` | `graph.predicateCounts()` |
| Traverse graph | `breadthFirstTraversal(pid)` | `graph.breadthFirstTraversal("book_1")` |
| Find references | `getRootsForPid(pid)` | `graph.getRootsForPid("auth_1")` |

## Exercises

### Exercise 1: Find Co-Authors

Write a query that finds all pairs of authors who have books reviewed by the same person.

### Exercise 2: Calculate Statistics

Find:
- The most prolific author (most books)
- The most reviewed book
- The author with the highest average rating

### Exercise 3: Path Finding

Write a function that finds if there's a path from one node to another, and what the shortest path is.

### Exercise 4: Recommendation System

Given a book ID, recommend similar books based on:
- Same author
- Similar average rating
- Reviewed by same people

## What's Next?

Continue to [Tutorial 4: Visualization](04-visualization.md) to learn how to create visual representations of your graphs.

## Complete Code

The complete working code is available in `examples/tutorial_03_querying.py`.
