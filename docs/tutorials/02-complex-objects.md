# Tutorial 2: Working with Complex Objects

One of PQG's most powerful features is automatic decomposition of nested objects. In this tutorial, you'll learn how PQG handles complex, nested data structures.

## What You'll Learn

- How PQG decomposes nested objects
- Working with object references
- Understanding automatic edge creation
- Handling lists of objects
- Retrieving complex objects with `getNode()`

## Prerequisites

- Completed [Tutorial 1: Basic Usage](01-basic-usage.md)
- Understanding of Python dataclasses
- 20-25 minutes

## The Scenario

We'll build a research project graph that includes:
- Research projects
- Team members (researchers)
- Organizations (universities, labs)
- Publications resulting from projects

This involves nested structures where projects contain researchers, and researchers belong to organizations.

## Understanding Object Decomposition

When you add a complex object to PQG that contains references to other objects, PQG automatically:

1. **Separates** the nested objects into individual nodes
2. **Creates edges** to represent the relationships
3. **Uses property names** as predicates (relationship types)

This means you can work with natural Python objects while PQG handles the graph structure!

## Step 1: Define Your Data Model

```python
import duckdb
from dataclasses import dataclass
from typing import Optional, List
from pqg import PQG, Base, Edge

@dataclass
class Organization(Base):
    """A research institution"""
    name: Optional[str] = None
    org_type: Optional[str] = None  # "university", "lab", "company"
    country: Optional[str] = None
    established: Optional[int] = None

@dataclass
class Researcher(Base):
    """A person conducting research"""
    name: Optional[str] = None
    email: Optional[str] = None
    specialization: Optional[str] = None
    # Reference to organization - will become an edge!
    affiliation: Optional[str] = None  # PID of Organization

@dataclass
class Publication(Base):
    """A research publication"""
    title: Optional[str] = None
    year: Optional[int] = None
    doi: Optional[str] = None
    citation_count: Optional[int] = None

@dataclass
class ResearchProject(Base):
    """A research project with team members and outputs"""
    project_name: Optional[str] = None
    start_year: Optional[int] = None
    end_year: Optional[int] = None
    funding_amount: Optional[int] = None
    # These lists will create multiple edges
    team_members: Optional[List[str]] = None  # List of Researcher PIDs
    publications: Optional[List[str]] = None  # List of Publication PIDs
```

## Step 2: Initialize the Graph

```python
def create_research_graph():
    """Initialize the research project graph"""
    db = duckdb.connect()
    graph = PQG(db, source="research_projects")

    # Register all types
    graph.registerType(Organization)
    graph.registerType(Researcher)
    graph.registerType(Publication)
    graph.registerType(ResearchProject)

    graph.initialize()

    return db, graph
```

## Step 3: Add Organizations

```python
def add_organizations(graph, db):
    """Add research institutions"""

    orgs = [
        Organization(
            pid="org_001",
            label="MIT",
            name="Massachusetts Institute of Technology",
            org_type="university",
            country="USA",
            established=1861
        ),
        Organization(
            pid="org_002",
            label="CERN",
            name="European Organization for Nuclear Research",
            org_type="lab",
            country="Switzerland",
            established=1954
        ),
        Organization(
            pid="org_003",
            label="Stanford",
            name="Stanford University",
            org_type="university",
            country="USA",
            established=1885
        ),
    ]

    for org in orgs:
        graph.addNode(org)
        print(f"Added organization: {org.name}")

    db.commit()
    return orgs
```

## Step 4: Add Researchers with Affiliations

Notice how we reference organizations using their PIDs:

```python
def add_researchers(graph, db):
    """Add researchers with organizational affiliations"""

    researchers = [
        Researcher(
            pid="researcher_001",
            label="Dr. Jane Smith",
            name="Dr. Jane Smith",
            email="jsmith@mit.edu",
            specialization="Machine Learning",
            affiliation="org_001"  # References MIT
        ),
        Researcher(
            pid="researcher_002",
            label="Dr. John Doe",
            name="Dr. John Doe",
            email="jdoe@cern.ch",
            specialization="Particle Physics",
            affiliation="org_002"  # References CERN
        ),
        Researcher(
            pid="researcher_003",
            label="Dr. Alice Johnson",
            name="Dr. Alice Johnson",
            email="ajohnson@stanford.edu",
            specialization="Quantum Computing",
            affiliation="org_003"  # References Stanford
        ),
        Researcher(
            pid="researcher_004",
            label="Dr. Bob Wilson",
            name="Dr. Bob Wilson",
            email="bwilson@mit.edu",
            specialization="Robotics",
            affiliation="org_001"  # Also MIT
        ),
    ]

    for researcher in researchers:
        graph.addNode(researcher)
        print(f"Added researcher: {researcher.name}")

    db.commit()
    return researchers
```

## Step 5: Add Publications

```python
def add_publications(graph, db):
    """Add research publications"""

    publications = [
        Publication(
            pid="pub_001",
            label="Deep Learning Review 2023",
            title="A Comprehensive Review of Deep Learning Architectures",
            year=2023,
            doi="10.1234/dl.2023.001",
            citation_count=150
        ),
        Publication(
            pid="pub_002",
            label="Quantum Algorithms Paper",
            title="Novel Quantum Algorithms for Optimization",
            year=2023,
            doi="10.1234/qa.2023.042",
            citation_count=75
        ),
        Publication(
            pid="pub_003",
            label="Robotics Survey",
            title="Recent Advances in Autonomous Robotics",
            year=2022,
            doi="10.1234/rb.2022.015",
            citation_count=200
        ),
    ]

    for pub in publications:
        graph.addNode(pub)
        print(f"Added publication: {pub.title}")

    db.commit()
    return publications
```

## Step 6: Create Complex Projects

Here's where it gets interesting! Notice how we use lists of PIDs to reference multiple team members and publications:

```python
def add_projects(graph, db):
    """Add research projects with team members and publications"""

    projects = [
        ResearchProject(
            pid="project_001",
            label="AI Safety Project",
            project_name="Ensuring Safe Artificial Intelligence",
            start_year=2022,
            end_year=2024,
            funding_amount=500000,
            team_members=["researcher_001", "researcher_004"],  # Jane & Bob
            publications=["pub_001"]  # DL Review paper
        ),
        ResearchProject(
            pid="project_002",
            label="Quantum Computing Initiative",
            project_name="Practical Quantum Computing Applications",
            start_year=2021,
            end_year=2023,
            funding_amount=750000,
            team_members=["researcher_003"],  # Alice
            publications=["pub_002"]  # Quantum Algorithms paper
        ),
        ResearchProject(
            pid="project_003",
            label="Autonomous Systems",
            project_name="Next-Generation Autonomous Robotics",
            start_year=2020,
            end_year=2023,
            funding_amount=1000000,
            team_members=["researcher_001", "researcher_004"],  # Jane & Bob
            publications=["pub_003"]  # Robotics Survey
        ),
    ]

    # PQG will automatically create edges for team_members and publications!
    for project in projects:
        graph.addNode(project)
        print(f"Added project: {project.project_name}")

    db.commit()
    return projects
```

## Step 7: Understanding What PQG Created

Let's examine the graph structure:

```python
def analyze_graph_structure(graph):
    """See how PQG decomposed our complex objects"""

    print("\n" + "="*60)
    print("GRAPH STRUCTURE ANALYSIS")
    print("="*60)

    # Count nodes
    print("\nNode types:")
    for otype, count in graph.objectCounts():
        if otype != "_edge_":
            print(f"  {otype}: {count}")

    # Count edges
    print("\nRelationship types (edges):")
    for predicate, count in graph.predicateCounts():
        print(f"  {predicate}: {count} connections")

    # Show specific examples
    print("\n" + "="*60)
    print("EXAMPLE: How Project 001 was decomposed")
    print("="*60)

    # Get all edges for project_001
    edges_from_project = list(graph.getRelations(subject="project_001"))

    print(f"\nEdges created from project_001:")
    for subj, pred, obj in edges_from_project:
        print(f"  project_001 --{pred}--> {obj}")
```

## Step 8: Query Complex Structures

Now let's see how to retrieve complex objects:

```python
def query_complex_data(graph):
    """Demonstrate querying complex nested structures"""

    print("\n" + "="*60)
    print("PROJECTS WITH FULL DETAILS")
    print("="*60)

    # Get all projects
    project_ids = list(graph.getIds(otype="ResearchProject"))

    for proj_pid in project_ids:
        # Get basic project info
        project = graph.getNode(proj_pid)
        print(f"\n{project['project_name']}")
        print(f"  Duration: {project['start_year']} - {project['end_year']}")
        print(f"  Funding: ${project['funding_amount']:,}")

        # Get team members
        print("  Team Members:")
        team_edges = list(graph.getRelations(subject=proj_pid, predicate="team_members"))

        for _, _, researcher_pid in team_edges:
            researcher = graph.getNode(researcher_pid)
            print(f"    - {researcher['name']} ({researcher['specialization']})")

            # Get researcher's organization
            org_edges = list(graph.getRelations(subject=researcher_pid, predicate="affiliation"))
            if org_edges:
                _, _, org_pid = org_edges[0]
                org = graph.getNode(org_pid)
                print(f"      Affiliation: {org['name']}")

        # Get publications
        print("  Publications:")
        pub_edges = list(graph.getRelations(subject=proj_pid, predicate="publications"))

        for _, _, pub_pid in pub_edges:
            pub = graph.getNode(pub_pid)
            print(f"    - {pub['title']} ({pub['year']})")
            print(f"      Citations: {pub.get('citation_count', 0)}")
            print(f"      DOI: {pub.get('doi', 'N/A')}")
```

## Step 9: Using getNode with Expansion

PQG's `getNode()` method can automatically expand references:

```python
def demonstrate_expansion(graph):
    """Show how getNode can retrieve related objects"""

    print("\n" + "="*60)
    print("NODE EXPANSION EXAMPLE")
    print("="*60)

    # Get a project with all its connections
    expanded = graph.getNode("project_001", max_depth=2)

    print("\nProject_001 expanded data:")
    print(f"Project Name: {expanded.get('project_name')}")

    # The expanded data includes information about connected nodes
    print(f"\nKeys in expanded data: {list(expanded.keys())}")

    # Alternatively, traverse manually for full control
    print("\n" + "="*60)
    print("MANUAL TRAVERSAL")
    print("="*60)

    # Start from a researcher
    researcher_id = "researcher_001"
    researcher = graph.getNode(researcher_id)

    print(f"\nResearcher: {researcher['name']}")

    # Find all projects this researcher is part of
    all_projects = list(graph.getIds(otype="ResearchProject"))

    print("Projects:")
    for proj_id in all_projects:
        # Check if this researcher is a team member
        team_edges = list(graph.getRelations(subject=proj_id, predicate="team_members"))
        for _, _, member_id in team_edges:
            if member_id == researcher_id:
                proj = graph.getNode(proj_id)
                print(f"  - {proj['project_name']}")
                break
```

## Step 10: Complete Example

```python
def main():
    """Run the complete complex objects tutorial"""

    print("="*60)
    print("TUTORIAL 2: COMPLEX OBJECTS")
    print("="*60)

    # Initialize
    db, graph = create_research_graph()

    # Build the graph
    print("\nStep 1: Adding organizations...")
    add_organizations(graph, db)

    print("\nStep 2: Adding researchers...")
    add_researchers(graph, db)

    print("\nStep 3: Adding publications...")
    add_publications(graph, db)

    print("\nStep 4: Adding projects...")
    add_projects(graph, db)

    # Analyze
    analyze_graph_structure(graph)

    # Query
    query_complex_data(graph)

    # Demonstrate expansion
    demonstrate_expansion(graph)

    # Save
    print("\n" + "="*60)
    print("SAVING")
    print("="*60)

    import pathlib
    graph.asParquet(pathlib.Path("research_projects.parquet"))
    print("\nGraph saved to: research_projects.parquet")

if __name__ == "__main__":
    main()
```

## Key Takeaways

### Automatic Decomposition

When you add an object with fields containing PIDs or lists of PIDs, PQG:

```python
project = ResearchProject(
    pid="project_001",
    team_members=["researcher_001", "researcher_004"]
)
graph.addNode(project)
```

Creates:
- 1 node for the project
- 2 edges: `project_001 --team_members--> researcher_001`
- `project_001 --team_members--> researcher_004`

### Property Names Become Predicates

The field name becomes the edge type:
- `affiliation="org_001"` → edge with predicate `"affiliation"`
- `team_members=[...]` → edges with predicate `"team_members"`

### Lists Create Multiple Edges

A list field creates one edge per item, all with the same predicate.

### Retrieving Complex Objects

You have two approaches:

1. **Automatic expansion**: `graph.getNode(pid, max_depth=2)`
2. **Manual traversal**: Use `getRelations()` to follow edges yourself

## Exercises

### Exercise 1: Add Collaborations

Add a `collaborators` field to `Researcher` that references other researchers. Model a network of who works with whom.

### Exercise 2: Multi-Institutional Projects

Modify `ResearchProject` to track which organizations are involved (not just through researchers).

### Exercise 3: Publication Authors

Add an `authors` field to `Publication` to explicitly link papers to their authors. Then query to find:
- All papers by a specific researcher
- Researchers who co-authored papers

### Exercise 4: Deep Traversal

Write a function that, given a project PID:
1. Finds all team members
2. For each member, finds their organization
3. For each organization, finds ALL researchers affiliated with it
4. Lists projects those researchers are involved in

## Common Pitfalls

### Pitfall 1: Forgetting to Commit

```python
graph.addNode(obj)
db.commit()  # Don't forget this!
```

### Pitfall 2: Using Objects Instead of PIDs

```python
# Wrong - don't pass the object
researcher = Researcher(pid="r001", ...)
project = ResearchProject(team_members=[researcher])  # NO!

# Right - pass the PID
project = ResearchProject(team_members=["r001"])  # YES!
```

### Pitfall 3: Not Using Lists for Multiple References

```python
# If team_members is Optional[str], only one edge is created
# If team_members is Optional[List[str]], multiple edges are created correctly
```

## What's Next?

Continue to [Tutorial 3: Querying the Graph](03-querying.md) to learn advanced query techniques, graph traversal, and pattern matching.

## Complete Code

The complete working code is available in `examples/tutorial_02_research.py`.
