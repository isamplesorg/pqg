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
from pqg import __version__

_DBMS_ = duckdb
#_DBMS_ = sqlite3

DEFAULT_PARQUET_GROUP_SIZE = 122880

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
    # sort order of relation. Used when multiple relations
    # exist of the same predicate to retain the original order.
    # Consider using broad steps of e.g. 10s or 100s to facilitate later insertions
    #TODO: support order of insertion. Need to adjust SQL and object get/set
    #i: pqg.common.OptionalInt = 1


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
    def __init__(
            self, 
            dbinstance: duckdb.duckdb.DuckDBPyConnection, 
            source: pqg.common.OptionalStr=None, 
            primary_key_field:str=None
        ) -> None:
        _L = getLogger()
        self._isparquet = False
        self._default_timestamp = "epoch(current_timestamp)::integer"
        if _DBMS_ == sqlite3:
            self._default_timestamp = "(unixepoch())"
        self._connection = dbinstance
        # Alway load the spatial extension
        self._connection.execute("INSTALL spatial; LOAD spatial;")
        self.geometry_x_field = "longitude"
        self.geometry_y_field = "latitude"
        self._pidcache = {}
        self._source = None
        if source is not None:
            self._source = source.strip()
            if self._source.endswith(".parquet"):
                self._source = f"read_parquet('{self._source}')"
            if self._source.startswith("read_parquet("):
                self._isparquet = True

        # Name of the source table in the data store
        self._table = "node"
        if source is not None and not self._isparquet:
            self._table = source
        # Name of the primary key column for rows.
        # This is consistent across all nodes and edges in the graph.
        self._node_pk: str = "pid"
        if primary_key_field is not None:
            self._node_pk: str = primary_key_field
        # a dict of {class_name: {field_name: sql_type, }, }
        self._types:typing.Dict[str, typing.Dict[str, str]] = {}
        # Fields that are in edge records
        self._edgefields = ('pid', 'otype', 's', 'p', 'o', 'n', 'altids', 'geometry', )
        self._literal_field_names = []
        _L.debug("table = %s", self._table)
        _L.debug("source = %s", self._source)
        _L.debug("isParquet = %s", self._isparquet)


    @contextlib.contextmanager
    def getCursor(self, as_dict:bool=False):
        yield self._connection.cursor()

    def close(self) -> None:
        #if self._connection is not None:
        #    self._connection.close()
        self._connection = None

    def registerType(self, cls:pqg.common.IsDataclass)->typing.Dict[str, typing.Dict[str, str]]:
        """Registers a class with the graph.

        If a class references other classes, they too are registered, recursively. Note
        however, that because of how linkml relations are defined, it is usually necessary
        to add all individual linkml object types by calling registerType with each linkml type.

        This must be done prior to calling initialize().

        Registering classes is necessary for generating the appropriate nodetable structure
        that is used for deserializing from the data store.

        Each row of the node table contains the union of simple fields from all data types
        stored in the graph.
        """
        fieldset = {}
        try:
            for field in dataclasses.fields(cls):
                # if the field smells like a dataclass
                if pqg.common.dataclassish(field.type):
                    # recurse into the properties of that field
                    self.registerType(field.type)
                else:
                    # Otherwise add the field unless it coincides with a pre-defined field
                    if field.name not in self._edgefields:
                        fieldset[field.name] = pqg.common.fieldToSQLCreate(field, primary_key_field=self._node_pk)
            self._types[cls.__name__] = fieldset
        except TypeError as e:
            pass
        return self._types

    def fieldUnion(self)-> typing.Set[dataclasses.Field]:
        """Return the set of fields that is the union across all registered types. This
        lit of fields is used to define the columns in the store.
        """
        fieldset = set()
        for otype, fields in self._types.items():
            for fieldname, fieldtype in fields.items():
                fieldset.add((fieldname, fieldtype))
        return fieldset

    def loadMetadataSql(self):
        """
        If existing content, then loads the list of fields and classes.
        #TODO: handle upstream changes in registered class structure. 
        """
        _L = getLogger()
        with self.getCursor() as csr:
            # fetch the last row in the metadata table (should only be one row anyway)
            meta = csr.execute("""SELECT version, source, pkcolumn, classes, edgefields, literalfields 
                FROM metadata WHERE source=? ORDER BY rowid DESC LIMIT 1;""", (self._table, )).fetchone()
            if meta is None:
                _L.info("No metadata available.")
                return
            if meta[0] != __version__:
                _L.warning("Version mismatch. PQG version = %s. Metadata version = %s", __version__, meta[0])
            self._node_pk = meta[2]
            self._types = json.loads(meta[3])
            self._edgefields = meta[4]
            self._literal_field_names = meta[5]

    def loadMetadataParquet(self):
        if not self._isparquet:
            raise ValueError(f"Not a parquet source {self._source}")
        _L = getLogger()
        kv_source = self._source.replace("read_parquet(", "parquet_kv_metadata(", 1)
        version = None
        with self.getCursor() as csr:
            results = csr.execute(f"SELECT * FROM {kv_source} WHERE decode(key) LIKE 'pqg_%'").fetchall()
            for row in results:
                k = row[1].decode("utf-8")
                if k == 'pqg_version':
                    version = row[2].decode("utf-8")
                elif k == 'pqg_primary_key':
                    self._node_pk = row[2].decode("utf-8")
                elif k == 'pqg_node_types':
                    self._types = json.loads(row[2].decode("utf-8"))
                elif k == 'pqg_edge_fields':
                    self._edgefields = json.loads(row[2].decode("utf-8"))
                elif k == 'pqg_literal_fields':
                    self._literal_field_names = json.loads(row[2].decode("utf-8"))
            # Create a view to reference the parquet file
            sql = f"CREATE VIEW {self._table} AS SELECT * FROM {self._source};"
            csr.sql(sql)
        if version != __version__:
            _L.warning("Source version of %s different to current of %s", version, __version__)
        
    
    def loadMetadata(self):
        if self._isparquet:
            self.loadMetadataParquet()
        else:
            self.loadMetadataSql()

    def initialize(self, classes:typing.List[pqg.common.IsDataclass]):
        """
        DuckDB DDL for property graph.

        classes is a list
        """
        if self._isparquet:
            raise ValueError("Can not initialize a Parquet data source.")
        # Create the metadata tables if it doesn't exist.
        with self.getCursor() as csr:
            #csr.execute("INSTALL spatial; LOAD spatial;")
            csr.execute("""CREATE TABLE IF NOT EXISTS metadata (
                version VARCHAR,
                source VARCHAR,
                pkcolumn VARCHAR,
                classes JSON,
                edgefields VARCHAR[],
                literalfields VARCHAR[],
            );""")
        # Load existing graph metadata if any
        self.loadMetadataSql()
        # Figure out the column definitions
        for cls in classes:
            self.registerType(cls)
        all_fields = []
        for field in self.fieldUnion():
            # [1] is the SQL for defining the column
            all_fields.append(field[1])
            # [0] is the name of the field (column)
            self._literal_field_names.append(field[0])
        self._literal_field_names += list(self._edgefields)
        sql = []
        sql.append(f"""CREATE TABLE IF NOT EXISTS {self._table} (
            pid VARCHAR PRIMARY KEY,
            tcreated INTEGER DEFAULT {self._default_timestamp},
            tmodified INTEGER DEFAULT {self._default_timestamp},
            otype VARCHAR,
            s VARCHAR REFERENCES node (pid) DEFAULT NULL,
            p VARCHAR DEFAULT NULL,
            o VARCHAR REFERENCES node (pid) DEFAULT NULL,
            n VARCHAR DEFAULT NULL,
            altids VARCHAR[] DEFAULT NULL,
            geometry GEOMETRY DEFAULT NULL,
            {', '.join(all_fields)}
        );""")
        sql.append(f"CREATE INDEX IF NOT EXISTS node_otype ON {self._table} (otype);")
        sql.append(f"CREATE INDEX IF NOT EXISTS edge_s ON {self._table} (s);")
        sql.append(f"CREATE INDEX IF NOT EXISTS edge_p ON {self._table} (p);")
        sql.append(f"CREATE INDEX IF NOT EXISTS edge_o ON {self._table} (o);")
        sql.append(f"CREATE INDEX IF NOT EXISTS edge_n ON {self._table} (n);")
        _L = getLogger()
        with self.getCursor() as csr:
            # Create the database structure
            for statement in sql:
                _L.debug(statement)
                csr.execute(statement)
                #csr.commit()
                self._connection.commit()
            # Override the existing metadata record
            csr.execute("DELETE FROM metadata WHERE source=?;", (self._table, ))
            self._connection.commit()
            csr.execute("INSERT INTO metadata (version, source, pkcolumn, classes, edgefields, literalfields) VALUES (?, ?, ?, ?, ?, ?)",
                        (__version__, self._table, self._node_pk, self._types, self._edgefields, self._literal_field_names))
            self._connection.commit()
        # The above fully defines a new collection. If working with an existing collection, check if the
        # existing columns match all of the columns registered, and if not, add new columns as necessary.
        with self.getCursor() as cr:
            missing_columns = []
            existing_columns  = {}
            tabledef = csr.execute(f"DESCRIBE (SELECT * FROM {self._table} LIMIT 1);").fetchall()
            for row in tabledef:
                existing_columns[row[0]] = row[1]
            for field in self._literal_field_names:
                if field not in existing_columns.keys():
                    missing_columns.append(f"{field} {existing_columns[field]}")
            sql = []
            for col in missing_columns:
                _L.info("Adding column %s", col)
                csr.execute(f"ALTER TABLE {self._table} ADD COLUMN {col};")
                self._connection.commit()

    def nodeExists(self, pid:str)->typing.Optional[typing.Tuple[str, str]]:
        if pid in self._pidcache:
            return (pid, self._pidcache[pid])
        with self.getCursor() as csr:
            res = csr.execute(f"SELECT {self._node_pk}, otype FROM {self._table} WHERE otype != '_edge_' AND {self._node_pk} = ?", (pid,)).fetchone()
            if res is not None:
                self._pidcache[pid] = res[1]
            return res

    def addNodeEntry(self, otype:str, data:typing.Dict[str, typing.Any]) -> str:
        """
        Add a new row to the node table or update a row if it already exists.
        """
        if self._isparquet:
            raise ValueError("Parquet based instances are read only.")
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
                lat_lon = {"x":None, "y":None,}
                #TODO: Handling of geometry is pretty rough. This should really be something from the object level
                # rather that kluging tuff together here.
                for k,v in data.items():
                    if k not in _names:
                        _names.append(k)
                        _values.append(v)
                        if k == self.geometry_x_field:
                            lat_lon["x"] = v
                        elif k == self.geometry_y_field:
                            lat_lon['y'] = v
                if lat_lon['x'] is not None and lat_lon['y'] is not None:
                    _names.append("geometry")
                sql = f"INSERT INTO {self._table} ({', '.join(_names)}) VALUES ({', '.join(['?',]*len(_values))}"
                if lat_lon['x'] is not None and lat_lon['y'] is not None:
                    sql += ", ST_POINT(?,?)"
                    _values.append(lat_lon['x'])
                    _values.append(lat_lon['y'])
                sql += ")"
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
        _L = getLogger()
        ne = self.nodeExists(pid)
        if ne is None:
            raise ValueError(f"Entry not found for pid = {pid}")
        _fields = [self._node_pk, ] + list(self._types[ne[1]].keys())
        sql = f"SELECT {', '.join(_fields)} FROM {self._table} WHERE {self._node_pk} = ?"
        _L.debug(sql)
        with self.getCursor() as csr:
            values = csr.execute(sql, [pid]).fetchone()
            return dict(zip(_fields, values))

    def addEdge(self, edge: Edge) -> str:
        """Adds an edge.

        Note that edges may exist independently of nodes, e.g. to make an assertion
        between external entities.
        """
        if self._isparquet:
            raise ValueError("Parquet based instances are read only.")
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
                    f"""INSERT INTO {self._table} ({', '.join(self._edgefields)}) VALUES (
                        ?, '_edge_', ?, ?, ?, ?, ?, ?
                    ) RETURNING pid""",
                    (
                        edge.pid,
                        edge.s,
                        edge.p,
                        edge.o,
                        edge.n,
                        edge.altids,
                        None
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
        #return None
        if (id is None) and (s is None or p is None or o is None or n is None):
            raise ValueError("Must provide id or each of s, p, o, n")
        edgefields = ('pid', 's', 'p', 'o', 'n', 'altids')
        sql = f"SELECT {', '.join(edgefields)} FROM {self._table} WHERE otype='_edge_' AND"
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
            if is_dataclass_or_dataclasslist(_v) or str(type(_v)) in self._types:
                deferred.append(field.name)
            else:
                if field.name in self._literal_field_names:
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
                if _v is not None:
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

    def getNode(self, pid:str, max_depth:int=10, _depth:int=0)->typing.Dict[str, typing.Any]:
        # Retrieve graph of object referenced by pid
        # reconstruct object from the list of node entries
        # ? can we construct a JSON representation of the object using CTE
        _L = getLogger()
        _L.debug("getNode pid= %s", pid)
        data = self.getNodeEntry(pid)
        if _depth < max_depth:
            with self.getCursor() as csr:
                sql = f"SELECT p, o FROM {self._table} WHERE otype='_edge_' AND s = ?"
                _L.debug(sql)
                results = csr.execute(sql, [pid])
                while edge := results.fetchone():
                    # Handle multiple values for related objects.
                    # Convert entry to a list if another value is found
                    if data.get(edge[0]) is not None:
                        if isinstance(data[edge[0]], list):
                            data[edge[0]].append(edge[1])
                        else:
                            _tmp = data[edge[0]]
                            data[edge[0]] = [_tmp, self.getNode(edge[1],_depth=_depth+1)]
                    else:
                        data[edge[0]] = self.getNode(edge[1], _depth=_depth+1)
        return data

    def getNodeIds(self, pid:str)->typing.Set[str]:
        """Retrieve a list of PIDs for the objects contributing to the object identified by pid.
        Relations between entities is not returned, just the identifiers.
        """
        result = set()
        result.add(pid)
        with self.getCursor() as csr:
            sql = f"SELECT p, o FROM {self._table} WHERE otype='_edge_' AND s = ?"
            edges = csr.execute(sql, [pid]).fetchall()
            for edge in edges:
                result = result.union(self.getNodeIds(edge[1]))
        return result

    def getIds(self, otype:typing.Optional[str]=None, maxrows:int=0)->typing.Iterator[typing.Tuple[str, str]]:
        batch_size = 100
        sql = f"SELECT otype, pid FROM {self._table}"
        params = []
        if otype is not None:
            sql += " WHERE otype=?"
            params.append(otype)
        if maxrows > 0:
            sql += f" LIMIT {maxrows}"
        with self.getCursor() as csr:
            result = csr.execute(sql, params)
            while batch := result.fetchmany(size=batch_size):
                for row in batch:
                    yield row[0], row[1]

    def objectCounts(self)->typing.Iterator[typing.Tuple[str, int]]:
        with self.getCursor() as csr:
            result = csr.execute(f"SELECT otype, count(*) AS n FROM {self._table} GROUP BY otype")
            while row := result.fetchone():
                yield row[0], row[1]

    def predicateCounts(self)->typing.Iterator[typing.Tuple[str, int]]:
        with self.getCursor() as csr:
            result = csr.execute(f"SELECT p, count(*) AS n FROM {self._table} WHERE otype ='_edge_' GROUP BY p")
            while row := result.fetchone():
                yield row[0], row[1]


    def toGraphviz(
            self,
            nlights:typing.Optional[list[str]]=None,
            elights:typing.Optional[list[str]]=None,
            rankdir=None,
    ) -> list[str]:
        # todo: Move this to a utility or something

        def qlabel(v):
            return f'"{v}"'

        if rankdir is None:
            rankdir = 'LR'
        if nlights is None:
            nlights = []
        if elights is None:
            elights = []
        dest = [
            "digraph {",
            f'rankdir="{rankdir}"',
            "node [shape=record, fontname=\"JetBrains Mono\", fontsize=10];",
            "edge [fontname=\"JetBrains Mono\", fontsize=8]",
        ]
        with self.getCursor() as csr:
            sql = f"SELECT {self._node_pk}, otype, label FROM {self._table} WHERE otype != '_edge_';"
            csr.execute(sql)
            for n in csr.fetchall():
                color = ''
                if n[0] in nlights:
                    color = ',color=red'
                dest.append(f"{qlabel(n[0])} [label=\"" + "{" + f"{n[0]}|{n[1]}|{n[2]}" + "}\"" + color + "];")
            sql = f"SELECT {self._node_pk}, s, p, o FROM {self._table} WHERE otype='_edge_'"
            csr.execute(sql)
            for e in csr.fetchall():
                color = ''
                if e[0] in elights:
                    color = ',color=red'
                dest.append(f"{qlabel(e[1])} -> {qlabel(e[3])} [label=\"{e[2]}\"" + color + "];")
        dest.append("}")
        return dest

    def asParquet(self, dest_base_name: pathlib.Path, group_size:int=DEFAULT_PARQUET_GROUP_SIZE):
        _L = getLogger()
        if group_size < 2048:
            group_size = 2048
        node_dest = dest_base_name.parent/f"{dest_base_name.stem}.parquet"
        # COPY (SELECT * FROM ps ORDER BY 
        #   ST_Hilbert(geometry, ST_Extent(ST_MakeEnvelope(-180, -90, 180, 90))
        #   TO 'ps-sorted.parquet'  (FORMAT 'parquet', COMPRESSION 'zstd');
        # Sort by space filling curve values to improve retrieveal from smaller regions
        # This will have a performance hit on write, but should both decrease file size and
        # significantly improve performance of reads in a spatial context
        with self.getCursor() as csr:
            sql = f"COPY (SELECT * FROM {self._table} ORDER BY otype, pid, ST_Hilbert(geometry, ST_Extent(ST_MakeEnvelope(-180, -90, 180, 90))))"
            #sql = f"COPY (SELECT * FROM {self._table})"
            sql += f" TO '{node_dest}' (FORMAT PARQUET, KV_METADATA "
            sql += "{" 
            sql += f"pqg_version: '{__version__}', "
            sql += f"pqg_primary_key:'{self._node_pk}', "
            sql += f"pqg_node_types:'{json.dumps(self._types)}', "
            sql += f"pqg_edge_fields:'{json.dumps(self._edgefields)}', "
            sql += f"pqg_literal_fields:'{json.dumps(self._literal_field_names)}'" 
            sql += "}"
            sql += f", ROW_GROUP_SIZE {group_size});"
            print(sql)
            csr.execute(sql)

    def getRootsForPid(self, pid: str) -> typing.Iterator[typing.Tuple[str]]:
        """
        Yields s, p where o=pid, recursively to yield parent references.
        """
        sql = f"""WITH RECURSIVE s_of AS (
        SELECT s,p, 0 AS depth FROM {self._table} WHERE o=?
        UNION ALL SELECT n.s, n.p, depth+1 FROM {self._table} AS n, s_of WHERE n.o = s_of.s
        ) SELECT * FROM s_of"""
        _L = getLogger()
        _L.debug(sql)
        with self.getCursor() as csr:
            result = csr.execute(sql, [pid, ])
            while row := result.fetchone():
                yield row

    def getRootsXForPid(
            self,
            pids: typing.List[str],
            target_type: pqg.common.OptionalStr=None,
            predicates:typing.Optional[typing.List[str]]=None
        )->typing.Iterator[typing.Tuple[str]]:
        """Follow relations starting with o=pid until top items are found.

        For example, given an Agent, which samples reference that agent by any predicate at any depth.

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
            SELECT e.pid, e.s, e.p, e.o, e.n, 1 as depth, src.otype as stype 
            FROM {self._table} AS e JOIN {self._table} as src ON src.pid = e.s WHERE e.o IN $pids {p_where}
        UNION ALL
            SELECT e.pid, e.s, e.p, e.o, e.n, eg.depth+1 AS depth, src.otype as stype,
            FROM {self._table} AS e, entity_for as eg JOIN {self._table} as src ON src.pid = e.s
            WHERE e.o = eg.s {p_where}
        ) SELECT pid, s, p, o, n, depth, stype FROM entity_for {t_where};
        """
        _L.debug("getRootsForPid: %s", sql)
        with self.getCursor() as csr:
            result = csr.execute(sql, params)
            while row := result.fetchone():
                yield row

    def breadthFirstTraversal(self, pid:str) -> typing.Iterator[typing.Tuple[str, int]]:
        """Recursively yields (identifier, depth) of objects that are objects of pid or related.

        With statements subject S, predicate P, object O (i.e. all rows with otype=_edge_),
        and starting subject S(PID), find all objects O for any P, recursing with
        the next S being any O of S.

        #TODO: add restriction by predicate, graph name
        """
        params = {
            "pid": pid,
        }
        sql = f"""WITH RECURSIVE edge_for(s, p, o, depth) AS (
            SELECT e.s, e.p, e.o, 1 AS depth FROM {self._table} AS e WHERE e.s=$pid AND e.otype='_edge_'
        UNION ALL
            SELECT e.s, e.p, e.o, depth+1 AS depth FROM {self._table} AS e, edge_for WHERE e.s=edge_for.o AND e.otype='_edge_'
        ) SELECT s, p, o, depth FROM edge_for;
        """
        L = getLogger()
        L.debug(sql)
        with self.getCursor() as csr:
            result = csr.execute(sql, params)
            while row := result.fetchone():
                yield row


    def objectsAtPath(self, path:typing.List[str])->typing.Iterator[str]:
        """
        Yield identifiers of objects that match the specified list of predicates.

        For example, the geolocations from which material sampels were collected would be:
        [produced_by, sample_location, ]
        """
        raise NotImplementedError()

