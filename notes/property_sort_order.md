# Order of properties

In some cases, the order of property values is important. An example is a list of authors.

Two ways of achieving this are:

1. Add a `sort_order` property to the edge.
2. Change the model for nodes to support a list of identifiers for subcomponents.

## Subcomponent list

This approach makes querying less generic
```
class Publication
    pid: str
    title: str
    authors: list[str]

class Author
    pid: str
    name: str
```

```
{
    otype: Publication,
    pid: pub_1,
    title: Pub One
    authors: [
        author_1,
        author_2
    ]
}

{
    otype: Author,
    pid: author_1,
    name: Mary
}

{
    otype: Author,
    pid: author_2,
    name: Frank
}
```

```
otype       pid         name    title   authors
Author      author_1    Mary
Author      author_2    Frank
Publication pub_1               Pub One [author_1, author_2]
```
