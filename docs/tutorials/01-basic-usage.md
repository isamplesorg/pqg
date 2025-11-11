# Tutorial 1: Basic Usage

In this tutorial, you'll learn the fundamentals of working with PQG by building a simple knowledge graph about books and authors.

## What You'll Learn

- Creating and initializing a graph
- Adding nodes with properties
- Creating relationships between nodes
- Querying your graph
- Saving and loading graphs

## Prerequisites

- PQG installed (see [Getting Started](../getting-started.md))
- Basic Python knowledge
- 15-20 minutes

## The Scenario

We'll build a small library catalog that tracks:
- Books with titles and publication years
- Authors with names
- The "authored_by" relationship between books and authors

## Step 1: Set Up Your Environment

Create a new Python file called `library_graph.py`:

```python
import duckdb
from dataclasses import dataclass
from typing import Optional, List
from pqg import PQG, Base

# We'll add more code here as we go
```

## Step 2: Define Custom Node Types

Instead of using the generic `Node` class, let's create specific types for our domain:

```python
@dataclass
class Book(Base):
    """Represents a book in our library"""
    title: Optional[str] = None
    year: Optional[int] = None
    isbn: Optional[str] = None

@dataclass
class Author(Base):
    """Represents an author"""
    name: Optional[str] = None
    birth_year: Optional[int] = None
    nationality: Optional[str] = None
```

These classes inherit from `Base`, which provides all the standard graph properties (`pid`, `label`, `description`, etc.).

## Step 3: Initialize the Graph

```python
def create_library_graph():
    """Create and initialize a library graph database"""

    # Create an in-memory DuckDB database
    # For persistent storage, use: duckdb.connect("library.duckdb")
    db = duckdb.connect()

    # Create PQG instance
    graph = PQG(db, source="library_catalog")

    # Register our custom types
    graph.registerType(Book)
    graph.registerType(Author)

    # Initialize the graph schema
    # This creates the necessary tables based on registered types
    graph.initialize()

    return db, graph
```

## Step 4: Add Authors

```python
def add_authors(graph, db):
    """Add some authors to our catalog"""

    authors = [
        Author(
            pid="author_001",
            name="Isaac Asimov",
            label="Isaac Asimov",
            birth_year=1920,
            nationality="American"
        ),
        Author(
            pid="author_002",
            name="Ursula K. Le Guin",
            label="Ursula K. Le Guin",
            birth_year=1929,
            nationality="American"
        ),
        Author(
            pid="author_003",
            name="Arthur C. Clarke",
            label="Arthur C. Clarke",
            birth_year=1917,
            nationality="British"
        ),
    ]

    # Add each author to the graph
    for author in authors:
        graph.addNode(author)
        print(f"Added author: {author.name}")

    # Commit the changes to the database
    db.commit()

    return authors
```

## Step 5: Add Books

```python
def add_books(graph, db):
    """Add some books to our catalog"""

    books = [
        Book(
            pid="book_001",
            title="Foundation",
            label="Foundation",
            year=1951,
            isbn="978-0553293357"
        ),
        Book(
            pid="book_002",
            title="The Left Hand of Darkness",
            label="The Left Hand of Darkness",
            year=1969,
            isbn="978-0441478125"
        ),
        Book(
            pid="book_003",
            title="2001: A Space Odyssey",
            label="2001: A Space Odyssey",
            year=1968,
            isbn="978-0451457998"
        ),
        Book(
            pid="book_004",
            title="I, Robot",
            label="I, Robot",
            year=1950,
            isbn="978-0553382563"
        ),
    ]

    for book in books:
        graph.addNode(book)
        print(f"Added book: {book.title} ({book.year})")

    db.commit()

    return books
```

## Step 6: Create Relationships

```python
from pqg import Edge

def link_books_to_authors(graph, db):
    """Create relationships between books and their authors"""

    # Define the relationships
    # Format: (book_pid, author_pid)
    relationships = [
        ("book_001", "author_001"),  # Foundation -> Isaac Asimov
        ("book_002", "author_002"),  # The Left Hand of Darkness -> Ursula K. Le Guin
        ("book_003", "author_003"),  # 2001 -> Arthur C. Clarke
        ("book_004", "author_001"),  # I, Robot -> Isaac Asimov
    ]

    for book_pid, author_pid in relationships:
        edge = Edge(
            s=book_pid,           # Subject: the book
            p="authored_by",      # Predicate: type of relationship
            o=[author_pid],       # Object: the author (note: list)
            label=f"Book {book_pid} authored by {author_pid}"
        )
        graph.addEdge(edge)
        print(f"Linked {book_pid} to {author_pid}")

    db.commit()
```

## Step 7: Query the Graph

Now let's explore what we've created:

```python
def explore_graph(graph):
    """Query and display information about our graph"""

    print("\n" + "="*60)
    print("GRAPH STATISTICS")
    print("="*60)

    # Count objects by type
    print("\nObjects in the graph:")
    for otype, count in graph.objectCounts():
        print(f"  {otype}: {count}")

    # Count relationship types
    print("\nRelationship types:")
    for predicate, count in graph.predicateCounts():
        print(f"  {predicate}: {count}")

    print("\n" + "="*60)
    print("BOOKS AND THEIR AUTHORS")
    print("="*60)

    # Get all books
    book_ids = list(graph.getIds(otype="Book"))

    for book_pid in book_ids:
        # Get the book data
        book_data = graph.getNode(book_pid)
        book_title = book_data.get('title', 'Unknown')
        book_year = book_data.get('year', 'Unknown')

        print(f"\n'{book_title}' ({book_year})")

        # Find who authored this book
        relations = list(graph.getRelations(subject=book_pid, predicate="authored_by"))

        for subj, pred, author_pid in relations:
            # Get author details
            author_data = graph.getNode(author_pid)
            author_name = author_data.get('name', 'Unknown')
            print(f"  Author: {author_name}")

    print("\n" + "="*60)
    print("AUTHORS AND THEIR BOOKS")
    print("="*60)

    # Get all authors
    author_ids = list(graph.getIds(otype="Author"))

    for author_pid in author_ids:
        author_data = graph.getNode(author_pid)
        author_name = author_data.get('name', 'Unknown')
        author_birth = author_data.get('birth_year', 'Unknown')

        print(f"\n{author_name} (b. {author_birth})")

        # Find books by this author
        # We need to search for edges where this author is the object
        all_relations = list(graph.getRelations(predicate="authored_by"))

        author_books = [
            book_pid for book_pid, pred, obj_pid in all_relations
            if obj_pid == author_pid
        ]

        if author_books:
            print("  Books:")
            for book_pid in author_books:
                book_data = graph.getNode(book_pid)
                book_title = book_data.get('title', 'Unknown')
                print(f"    - {book_title}")
        else:
            print("  No books in catalog")
```

## Step 8: Put It All Together

```python
def main():
    """Main function to run the tutorial"""

    print("Creating library graph...")
    db, graph = create_library_graph()

    print("\nAdding authors...")
    add_authors(graph, db)

    print("\nAdding books...")
    add_books(graph, db)

    print("\nCreating relationships...")
    link_books_to_authors(graph, db)

    print("\nExploring the graph...")
    explore_graph(graph)

    # Save to file
    print("\n" + "="*60)
    print("SAVING GRAPH")
    print("="*60)

    import pathlib
    output_path = pathlib.Path("library.parquet")
    graph.asParquet(output_path)
    print(f"\nGraph saved to: {output_path}")

    print("\nTutorial complete!")

if __name__ == "__main__":
    main()
```

## Running the Tutorial

Save your file and run it:

```bash
python library_graph.py
```

You should see output showing:
1. Authors and books being added
2. Relationships being created
3. Statistics about your graph
4. Books listed with their authors
5. Authors listed with their books

## Expected Output

```
Creating library graph...

Adding authors...
Added author: Isaac Asimov
Added author: Ursula K. Le Guin
Added author: Arthur C. Clarke

Adding books...
Added book: Foundation (1951)
Added book: The Left Hand of Darkness (1969)
Added book: 2001: A Space Odyssey (1968)
Added book: I, Robot (1950)

Creating relationships...
Linked book_001 to author_001
Linked book_002 to author_002
Linked book_003 to author_003
Linked book_004 to author_001

Exploring the graph...

============================================================
GRAPH STATISTICS
============================================================

Objects in the graph:
  Author: 3
  Book: 4
  _edge_: 4

Relationship types:
  authored_by: 4

============================================================
BOOKS AND THEIR AUTHORS
============================================================

'Foundation' (1951)
  Author: Isaac Asimov

'The Left Hand of Darkness' (1969)
  Author: Ursula K. Le Guin

'2001: A Space Odyssey' (1968)
  Author: Arthur C. Clarke

'I, Robot' (1950)
  Author: Isaac Asimov

============================================================
AUTHORS AND THEIR BOOKS
============================================================

Isaac Asimov (b. 1920)
  Books:
    - Foundation
    - I, Robot

Ursula K. Le Guin (b. 1929)
  Books:
    - The Left Hand of Darkness

Arthur C. Clarke (b. 1917)
  Books:
    - 2001: A Space Odyssey

============================================================
SAVING GRAPH
============================================================

Graph saved to: library.parquet

Tutorial complete!
```

## Using the CLI

Now that you've saved your graph, try exploring it with the CLI:

```bash
# List all entries
pqg entries library.parquet

# View a specific book
pqg node library.parquet book_001

# View a book with all its connections
pqg node library.parquet book_001 --expand

# See object types
pqg types library.parquet

# See relationship types
pqg predicates library.parquet
```

## Exercises

Try these challenges to deepen your understanding:

### Exercise 1: Add More Books

Add 2-3 more books to the catalog. Make sure to:
- Give them unique PIDs
- Add proper metadata (title, year, ISBN)
- Link them to existing or new authors

### Exercise 2: Add Publishers

Create a new `Publisher` dataclass and add:
- Publisher information (name, location, founded year)
- A "published_by" relationship between books and publishers

### Exercise 3: Co-Authors

Modify the code to handle books with multiple authors. Remember that the `o` field in an Edge is a list!

Example:
```python
edge = Edge(
    s="book_005",
    p="authored_by",
    o=["author_001", "author_002"]  # Multiple authors!
)
```

### Exercise 4: Query by Year

Write a function that finds all books published in a specific year or year range.

Hint: You'll need to get all books and filter them:
```python
def books_by_year(graph, year):
    for book_pid in graph.getIds(otype="Book"):
        book_data = graph.getNode(book_pid)
        if book_data.get('year') == year:
            print(f"Found: {book_data.get('title')}")
```

## Key Concepts Learned

- **Custom Node Types**: Creating domain-specific classes that inherit from `Base`
- **Type Registration**: Using `registerType()` to tell PQG about your classes
- **Adding Nodes**: Using `addNode()` to insert data
- **Creating Edges**: Using the `Edge` class to define relationships
- **Querying**: Using `getNode()`, `getIds()`, and `getRelations()` to retrieve data
- **Persistence**: Saving graphs to Parquet format

## What's Next?

Continue to [Tutorial 2: Complex Objects](02-complex-objects.md) to learn how PQG handles nested data structures and automatically creates edges.

## Complete Code

The complete working code for this tutorial is available in `examples/tutorial_01_library.py`.
