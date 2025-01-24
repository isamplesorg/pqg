# Property Graph in DuckDB

This is an implementation of a property graph using DuckDB. 

The intent is providing functionality that lies between a full property graph such as Apache GraphAR and a fully decomposed graph such as an RDF store.

The graph is composed of nodes (vertices) and edges (relations between vertices). Nodes may be extended to represent different classes of information, though all nodes inherit from a common base node.

Only a single table ise used to capture node and edge information.

The model of a node is:

```
pid         Globally unique identifier for the node.
tcreated    Time stamp for when the node instance is created.
tmodified   Time stamp for when the instance has been modified.
otype       The type of entity described by this node instance.
label       Optional textual label for human use.
description Optional human readable description of the node.
altids      A list of alternate identifiers for the node (aliases of pid)
...         Additional properties as needed for the class being stored at the node. 
```

Node classes store literal values or lists of literal values. Composite nodes are stored as separate nodes and associations between the separate nodes are stored as edges.

The model of an edge is:

```
pid         Globally unique identifier for the node.
tcreated    Time stamp for when the node instance is created.
tmodified   Time stamp for when the instance has been modified.
otype       "_edge_"
label       Optional textual label for human use.
description Optional human readable description of the node.
altids      A list of alternate identifiers for the node (aliases of pid)
s           Subject or source of the relation.
p           Predicate or type of the relation.
o           The object or target of the relation.
n           Name of the graph containing the relation.
```

Serializing entities to a `pqg` should follow the basic rule that nested structures should be decomposed to a single level, with each level stored as a separate instance and the relations between those instance captured as edges. Hence, a given entity may result in several node instances along with corresponding edge instances.

A simplified "Material Sample" representation is used in the examples.

A MaterialSample is composed of:

```
MaterialSample
    - identifier
    - label
    - description
    - alternate_identifiers[]
    - has_registrant Agent
    - produced_by SamplingEvent
    - curation_event MaterialSampleCuration
    
Agent
    - identifier
    - name
    - affiliation
    - role
    - contact_information
    
SamplingEvent
    - identifier
    - result_time
    - responsibility Agent
    - sampling_site SamplingSite
    
SamplingSite
    - identifier
    - location GeospatialCoordLocation
    
GeospatialCoordLocation
    - latitude
    - longitude
    - elevation
    
```