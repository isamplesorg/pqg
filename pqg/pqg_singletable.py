import contextlib
import dataclasses
import functools
import hashlib
import json
import logging
import pathlib
import typing

import duckdb
import sqlite3

import pqg.common

_DBMS_ = duckdb
#_DBMS_ = sqlite3


def getLogger():
    return logging.getLogger("pqg")

def is_dataclass_or_dataclasslist(o: typing.Any) -> bool:
    if isinstance(o, list):
        if len(o) == 0:
            return False
        return dataclasses.is_dataclass(o[0])
    return dataclasses.is_dataclass(o)


@dataclasses.dataclass(kw_only=True)
class Base:
    # Unique identifier for the entry. If not set, one is created.
    pid: pqg.common.OptionalStr
    # Human friendly label
    label: pqg.common.OptionalStr = None
    # Human description for the entry
    description: pqg.common.OptionalStr = None
    # Alternative unique identifiers for the entry.
    # Edges should always use PID, not an altid.
    altids: pqg.common.StringList = None

    ## Properties particular to edges
    # Subject of the relation
    s: pqg.common.OptionalStr = None
    # predicate, the type of the relation
    p: pqg.common.OptionalStr = None
    # object, i.e. target of relation
    o: pqg.common.OptionalStr = None
    # name of graph containing this relation
    n: pqg.common.OptionalStr = None


    def __post_init__(self, *_:typing.List[str], **kwargs: typing.Dict[str, typing.Any]):
        """Compute the PID if not already set.

        Generated identifiers are prefixed with "anon_" to convey that the
        identifier is an anonymous identifier.

        Could do a content bashed hash instead using a subset of the fields
        """
        L = getLogger()
        L.debug("post_init entry: pid = %s", self.pid)
        if self.pid is None:
            self.pid = f"anon_{pqg.common.getUUID()}"
        L.debug("exit: pid = %s", self.pid)


@dataclasses.dataclass(kw_only=True)
class Node(Base):
    # Node is a graph vertex, generally corresponding with a "thing"
    # otype is the name of the class this entry represents.
    pass


@dataclasses.dataclass(kw_only=True)
class Edge(Base):
    """An Edge instance defines a relationship between two nodes.

    Edges are used to form composite Things and also to define relationships
    between disjunct things.
    """
    def __post_init__(self, *_:typing.List[str], **kwargs: typing.Dict[str, typing.Any]):
        """Compute the PID if not already set.

        The edge PID is computed form the hash of s,p,o,n
        """
        L = getLogger()
        L.debug("post_init entry: pid = %s", self.pid)
        if self.pid is None:
            h = hashlib.sha256()
            h.update(self.s.encode("utf-8"))
            h.update(self.p.encode("utf-8"))
            h.update(self.o.encode("utf-8"))
            if self.n is not None:
                h.update(self.n.encode("utf-8"))
            self.pid = f"anon_{h.hexdigest()}"
        L.debug("exit: pid = %s", self.pid)
        super().__post_init__(**kwargs)


class PQG:
    """Implements a property graph database interface.

    The implementation is for DuckDB, though should be readily adaptable
    to other engines such as Postgres.

    Notes:
        List of params for IN, https://stackoverflow.com/questions/78364497/duckdb-prepared-statement-list
    """
    def __init__(self, connection_str: pqg.common.OptionalStr=None, primary_key_field:str=None) -> None:
        if connection_str is None:
            connection_str = ":memory:"
        self.connection_str = connection_str
        self._default_timestamp = "epoch(current_timestamp)::integer"
        if _DBMS_ == sqlite3:
            self._default_timestamp = "(unixepoch())"
        self._connection = None
        self._pidcache = {}
        self._node_pk: str = "pid"
        if primary_key_field is not None:
            self._node_pk: str = primary_key_field
        # a dict of {class_name: {field_name: sql_type, }, }
        self._types:typing.Dict[str, typing.Dict[str, str]] = {}
        #self._edgefields = ('pid', 'tcreated', 'tmodified', 'otype', 's', 'p', 'o', 'n', 'altids')
        self._edgefields = ('pid', 'otype', 's', 'p', 'o', 'n', 'altids')

    @contextlib.contextmanager
    def getCursor(self, as_dict:bool=False):
        if self._connection is None:
            self._connection = _DBMS_.connect(self.connection_str)
        yield self._connection.cursor()

    def close(self) -> None:
        if self._connection is not None:
            self._connection.close()
        self._connection = None

    def registerType(self, cls:pqg.common.IsDataclass)->typing.Dict[str, typing.Dict[str, str]]:
        """Registers a class with the graph.

        If a class references other classes, they too are registered, recursively.

        This must be done prior to calling initialize().

        Registering classes is necessary for generating the appropriate nodetable structure
        and for deserializing from the data store.

        Each row of the node table contains the union of simple fields from all data types
        stored in the graph.
        """
        fieldset = {}
        for field in dataclasses.fields(cls):
            if dataclasses.is_dataclass(field.type):
                self.registerType(field.type)
            else:
                if field.name not in self._edgefields:
                    fieldset[field.name] = pqg.common.fieldToSQLCreate(field, primary_key_field=self._node_pk)
        self._types[cls.__name__] = fieldset
        return self._types

    def fieldUnion(self)-> typing.Set[dataclasses.Field]:
        """Return the set fields that is the union across all registered types.
        """
        def _fieldUnion(cls:pqg.common.IsDataclass) -> typing.Set[dataclasses.Field]:
            fields = set()
            for field in dataclasses.fields(cls):
                _L.debug("fieldUnion: %s", field)
                if dataclasses.is_dataclass(field.type):
                    fields |= _fieldUnion(field.type)
                else:
                    fields.add(field)
            return fields

        _L = getLogger()
        fieldset = set()
        for otype, fields in self._types.items():
            for fieldname, fieldtype in fields.items():
                fieldset.add((fieldname, fieldtype))
        return fieldset

    def initialize(self, classes:typing.List[pqg.common.IsDataclass]):
        """
        DuckDB DDL for property graph.
        """
        for cls in classes:
            self.registerType(cls)
        node_fields = []
        for field in self.fieldUnion():
            node_fields.append(field[1])
        sql = []

        sql.append(f"""CREATE TABLE IF NOT EXISTS node (
            pid VARCHAR PRIMARY KEY,
            tcreated INTEGER DEFAULT {self._default_timestamp},
            tmodified INTEGER DEFAULT {self._default_timestamp},
            otype VARCHAR,
            s VARCHAR REFERENCES node (pid) DEFAULT NULL,
            p VARCHAR DEFAULT NULL,
            o VARCHAR REFERENCES node (pid) DEFAULT NULL,
            n VARCHAR DEFAULT NULL,
            altids VARCHAR[] DEFAULT NULL,
            {', '.join(node_fields)}
        );""")
        sql.append("CREATE INDEX IF NOT EXISTS node_otype ON node (otype);")
        sql.append("CREATE INDEX IF NOT EXISTS edge_s ON node (s);")
        sql.append("CREATE INDEX IF NOT EXISTS edge_p ON node (p);")
        sql.append("CREATE INDEX IF NOT EXISTS edge_o ON node (o);")
        sql.append("CREATE INDEX IF NOT EXISTS edge_n ON node (n);")

        _L = getLogger()
        with self.getCursor() as csr:
            for statement in sql:
                _L.debug(statement)
                csr.execute(statement)
                #csr.commit()
                self._connection.commit()

    def nodeExists(self, pid:str)->typing.Optional[typing.Tuple[str, str]]:
        if pid in self._pidcache:
            return (pid, self._pidcache[pid])
        with self.getCursor() as csr:
            res = csr.execute(f"SELECT {self._node_pk}, otype FROM node WHERE otype != '_edge_' AND {self._node_pk} = ?", (pid,)).fetchone()
            if res is not None:
                self._pidcache[pid] = res[1]
            return res

    def addNodeEntry(self, otype:str, data:typing.Dict[str, typing.Any]) -> str:
        """
        Add a new row to the node table or update a row if it already exists.
        """
        _L = getLogger()
        try:
            pid:str = data[self._node_pk]
        except KeyError:
            raise ValueError("pid cannot be None")
        ne = self.nodeExists(pid)
        if ne is not None:
            # update the existing entry
            #_L.warning("Update entry not implemented yet.")
            return pid
        else:
            # create a new entry
            try:
                _names = [self._node_pk, "otype", ]
                _values = [pid, otype, ]
                for k,v in data.items():
                    if k not in _names:
                        _names.append(k)
                        _values.append(v)
                sql = f"INSERT INTO node ({', '.join(_names)}) VALUES ({', '.join(['?',]*len(_values))})"
                _L.debug("addNodeEntry sql: %s", sql)
                with self.getCursor() as csr:
                    csr.execute(sql, _values)
                    #csr.commit()
                    #self._connection.commit()
            except Exception as e:
                pass
                _L.warning("addNodeEntry %s", e)
        return pid

    def getNodeEntry(self, pid:str)-> typing.Dict[str, typing.Any]:
        ne = self.nodeExists(pid)
        if ne is None:
            raise ValueError(f"Entry not found for pid = {pid}")
        _fields = [self._node_pk, ] + list(self._types[ne[1]].keys())
        sql = f"SELECT {', '.join(_fields)} FROM node WHERE {self._node_pk} = ?"
        with self.getCursor() as csr:
            values = csr.execute(sql, [pid]).fetchone()
            return dict(zip(_fields, values))

    def addEdge(self, edge: Edge) -> str:
        """Adds an edge.

        Note that edges may exist independently of nodes, e.g. to make an assertion
        between external entities.
        """
        _L = getLogger()
        _L.debug("addEdge: %s", edge.pid)
        existing = None
        if edge.pid is None:
            existing = self.getEdge(s=edge.s, p=edge.p, o=edge.o, n=edge.n)
        else:
            existing = self.getEdge(pid=edge.pid)
        if existing is not None:
            return existing.pid
        try:
            with self.getCursor() as csr:
                # ('pid', 'tcreated', 'tmodified', 's', 'p', 'o', 'n', 'label', 'description', 'altids')
                # epoch(current_timestamp)::integer, epoch(current_timestamp)::integer,
                csr.execute(
                    f"""INSERT INTO node ({', '.join(self._edgefields)}) VALUES (
                        ?, '_edge_', ?, ?, ?, ?, ?
                    ) RETURNING pid""",
                    (
                        edge.pid,
                        edge.s,
                        edge.p,
                        edge.o,
                        edge.n,
                        edge.altids,
                    ),
                )
                #csr.commit()
                rows = csr.fetchone()
                result = rows[0]
            self._connection.commit()
            return result
        except Exception as e:
            pass
            # Will expect unique constraint failures since edge.pid is a hash of s,p,o
            _L.debug("addEdge %s %s", edge.pid, e)
        return edge.pid

    def getEdge(
        self,
        pid: pqg.common.OptionalStr = None,
        s: pqg.common.OptionalStr = None,
        p: pqg.common.OptionalStr = None,
        o: pqg.common.OptionalStr = None,
        n: pqg.common.OptionalStr = None,
    ) -> typing.Optional[Edge]:
        _L = getLogger()
        return None
        if (id is None) and (s is None or p is None or o is None or n is None):
            raise ValueError("Must provide id or each of s, p, o, n")
        edgefields = ('pid', 's', 'p', 'o', 'n', 'altids')
        sql = f"SELECT {', '.join(edgefields)} FROM node WHERE otype='_edge_' AND"
        if pid is not None:
            sql += " pid = ?"
            qproperties = (pid,)
        else:
            sql += " s=? AND p=? AND o=? AND n=?"
            qproperties = (s, p, o, n)
        with self.getCursor() as csr:
            _L.debug("getEdge sql: %s", sql)
            csr.execute(sql, qproperties)
            values = csr.fetchone()
            if values is None:
                return None
            data = dict(zip(edgefields, values))
            return Edge(**data)

    def _addNode(self, o:pqg.common.IsDataclass)->str:
        _L = getLogger()
        deferred = []
        otype = o.__class__.__name__
        data = {}
        for field in dataclasses.fields(o):
            _v = getattr(o, field.name)
            if is_dataclass_or_dataclasslist(_v):
                deferred.append(field.name)
            else:
                data[field.name] = _v
        s_pid = self.addNodeEntry(otype, data)
        _L.debug("Added node pid= %s", s_pid)
        for field_name in deferred:
            _v = getattr(o, field_name)
            if isinstance(_v, list):
                for element in _v:
                    o_pid = self._addNode(element)
                    _edge = Edge(pid=None, s=s_pid, p=field_name, o=o_pid)
                    _L.debug("Created edge: %s", _edge)
                    self.addEdge(_edge)
            else:
                o_pid = self._addNode(_v)
                _edge = Edge(pid=None, s=s_pid, p=field_name, o=o_pid)
                _L.debug("Created edge: %s", _edge)
                self.addEdge(_edge)
        return s_pid

    def addNode(self, o:pqg.common.IsDataclass)->str:
        """Recursively adds a dataclass instance to the graph.

        Complex properties that are instances of dataclass will be
        added as separate nodes joined by an edge with predicate of the
        property name.
        """
        result = self._addNode(o)
        self._connection.commit()
        return result

    def getNode(self, pid:str)->typing.Dict[str, typing.Any]:
        # Retrieve graph of object referenced by pid
        # reconstruct object from the list of node entries
        # ? can we construct a JSON representation of the object using CTE
        data = self.getNodeEntry(pid)
        with self.getCursor() as csr:
            sql = f"SELECT p, o FROM node WHERE otype='_edge_' AND s = ?"
            edges = csr.execute(sql, [pid]).fetchall()
            for edge in edges:
                # Handle multiple values for related objects.
                # Convert entry to a list if another value is found
                if data[edge[0]] is not None:
                    if isinstance(data[edge[0]], list):
                        data[edge[0]].append(edge[1])
                    else:
                        _tmp = data[edge[0]]
                        data[edge[0]] = [_tmp, self.getNode(edge[1])]
                else:
                    data[edge[0]] = self.getNode(edge[1])
        return data

    def getNodeIds(self, pid:str)->typing.Set[str]:
        """Retrieve a list of PIDs for the objects contributing to the object identified by pid.
        Relations between entities is not returned, just the identifiers.
        """
        result = set()
        result.add(pid)
        with self.getCursor() as csr:
            sql = f"SELECT p, o FROM node WHERE otype='_edge_' AND s = ?"
            edges = csr.execute(sql, [pid]).fetchall()
            for edge in edges:
                result = result.union(self.getNodeIds(edge[1]))
        return result

    def toGraphviz(
            self,
            nlights:typing.Optional[list[str]]=None,
            elights:typing.Optional[list[str]]=None) -> list[str]:

        def qlabel(v):
            return f'"{v}"'

        if nlights is None:
            nlights = []
        if elights is None:
            elights = []
        dest = [
            "digraph {",
            "node [shape=record, fontname=\"JetBrains Mono\", fontsize=10];",
            "edge [fontname=\"JetBrains Mono\", fontsize=8]",
        ]
        with self.getCursor() as csr:
            sql = f"SELECT {self._node_pk}, otype, label FROM node WHERE otype != '_edge_';"
            csr.execute(sql)
            for n in csr.fetchall():
                color = ''
                if n[0] in nlights:
                    color = ',color=red'
                dest.append(f"{qlabel(n[0])} [label=\"" + "{" + f"{n[0]}|{n[1]}|{n[2]}" + "}\"" + color + "];")
            sql = f"SELECT {self._node_pk}, s, p, o FROM node WHERE otype='_edge_'"
            csr.execute(sql)
            for e in csr.fetchall():
                color = ''
                if e[0] in elights:
                    color = ',color=red'
                dest.append(f"{qlabel(e[1])} -> {qlabel(e[3])} [label=\"{e[2]}\"" + color + "];")
        dest.append("}")
        return dest

    def asParquet(self, dest_base_name: pathlib.Path):
        _L = getLogger()
        #self.close()
        #ddb = duckdb.connect(self.connection_str)
        node_dest = dest_base_name.parent/f"{dest_base_name.stem}.parquet"
        with self.getCursor() as csr:
            sql = f"COPY (SELECT * FROM node) TO '{node_dest}' (FORMAT PARQUET, KV_METADATA "
            sql += "{" + f"primary_key:'{self._node_pk}', node_types:'{json.dumps(self._types)}'" +"});"
            _L.debug(sql)
            csr.execute(sql)
        #ddb.close()
            #sql = f"COPY (SELECT * FROM edge) TO '{edge_dest}' (FORMAT PARQUET);"
            #_L.debug(sql)
            #csr.execute(sql)

    def getRootsForPid(
            self,
            pids: typing.List[str],
            target_type: pqg.common.OptionalStr=None,
            predicates:typing.Optional[typing.List[str]]=None
        )->typing.Iterator[typing.Tuple[str]]:
        """Follow relations starting with o=pid until top items are found.

        Yields:
            edge_pid        PID of edge
            subject         s value of edge
            predicate       p value of edge
            object          o value of edge
            n               n value of edge
            depth           distance in steps from starting pid
            subject_otype   otype of the edge subject

        by traversing the graph of relationships starting with edges for which o IN pids
        and optionally filtering by edge.p in predicates and/or subject_otype = target_type.
        """
        _L = getLogger()
        if predicates is None:
            predicates = []
        params = {
            "pids": pids,
        }
        t_where = ""
        if target_type is not None:
            t_where = " WHERE stype = $stype"
            params["stype"] = target_type
        p_where = ""
        if len(predicates) > 0:
            p_where = " AND e.p IN $predicates"
            params["predicates"] = predicates
        sql = f"""WITH RECURSIVE entity_for(pid, s, p, o, n, depth, stype) AS (
            SELECT e.pid, e.s, e.p, e.o, e.n, 1 as depth, node.otype as stype 
            FROM node AS e JOIN node ON node.pid = e.s WHERE e.o IN $pids {p_where}
        UNION ALL
            SELECT e.pid, e.s, e.p, e.o, e.n, eg.depth+1 AS depth, node.otype as stype,
            FROM node AS e, entity_for as eg JOIN node ON node.pid = e.s
            WHERE e.o = eg.s {p_where}
        ) SELECT pid, s, p, o, n, depth, stype FROM entity_for {t_where};
        """
        _L.debug("getRootsForPid: %s", sql)
        with self.getCursor() as csr:
            result = csr.execute(sql, params)
            row = result.fetchone()
            while row is not None:
                yield row
                row = result.fetchone()


