__version__ = "0.2.0"

from pqg.common import (
    OptionalStr,
    OptionalInt,
    OptionalFloat,
    OptionalDateTime,
    OptionalDecimal,
    StringList,
    IntegerList,
    FloatList,
    DateTimeList,
    JSONDateTimeEncoder,
)
from pqg.pqg_singletable import (
    Base,
    PQG,
    Edge,
)
from pqg.edge_types import (
    ISamplesEdgeType,
    infer_edge_type,
    validate_edge_type,
    get_edge_types_by_subject,
    get_edge_types_by_object,
    EDGE_TYPE_CONSTRAINTS,
)
from pqg.typed_edges import (
    TypedEdgeQueries,
    TypedEdgeGenerator,
)


class Graph:
    """
    Simplified Graph API wrapper for iSamples PQG conversion.

    This is a lightweight graph builder that creates nodes and edges in DuckDB
    and exports to Parquet format. It doesn't use the full PQG class machinery
    but provides a compatible API for the ISamplesPQGConverter in export_client.

    Usage:
        graph = Graph(":memory:")
        graph.initialize()
        graph.addNode(pid="sample_1", otype="Sample", label="My Sample")
        graph.addEdge(s="sample_1", p="produced_by", o=["event_1"])
        graph.toParquet("output.parquet")
    """

    def __init__(self, db_path: str = ":memory:"):
        """Initialize the Graph wrapper.

        Args:
            db_path: Path to DuckDB database file, or ":memory:" for in-memory
        """
        import duckdb
        self._db_path = db_path
        self._connection = duckdb.connect(db_path)
        # Load spatial extension for geometry support
        self._connection.execute("INSTALL spatial; LOAD spatial;")
        self._initialized = False
        self._row_id_counter = 0
        self._pid_to_row_id = {}  # Map PIDs to row_ids for edge references

    @property
    def db(self):
        """Direct access to the underlying database connection."""
        return self._connection

    def initialize(self):
        """Initialize the graph database table.

        Creates a flexible node table that stores both entities and edges.
        """
        if not self._initialized:
            # Create a flexible node table with common fields
            self._connection.execute("""
                CREATE TABLE IF NOT EXISTS node (
                    row_id INTEGER PRIMARY KEY,
                    pid VARCHAR,
                    otype VARCHAR,
                    label VARCHAR,
                    description VARCHAR,
                    altids VARCHAR[],
                    -- Edge fields (NULL for entity nodes)
                    s INTEGER,  -- subject row_id
                    p VARCHAR,  -- predicate
                    o INTEGER[],  -- object row_ids
                    n VARCHAR,  -- named graph
                    -- Common entity fields (stored as JSON for flexibility)
                    properties JSON,
                    -- Geometry
                    geometry GEOMETRY
                )
            """)
            self._connection.execute("CREATE INDEX IF NOT EXISTS idx_node_pid ON node(pid)")
            self._connection.execute("CREATE INDEX IF NOT EXISTS idx_node_otype ON node(otype)")
            self._initialized = True

    def _get_next_row_id(self) -> int:
        """Get the next available row_id."""
        self._row_id_counter += 1
        return self._row_id_counter

    def _convert_to_serializable(self, obj):
        """Convert numpy arrays and other non-serializable types to JSON-safe values."""
        import numpy as np
        if obj is None:
            return None
        if hasattr(obj, 'tolist'):  # numpy arrays, pandas Series
            return obj.tolist()
        if isinstance(obj, (np.integer, np.floating)):
            return obj.item()
        if isinstance(obj, dict):
            return {k: self._convert_to_serializable(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple)):
            return [self._convert_to_serializable(i) for i in obj]
        return obj

    def addNode(self, pid: str, otype: str, **kwargs) -> str:
        """Add a node to the graph.

        Args:
            pid: Unique identifier for the node
            otype: Object type (e.g., 'Sample', 'SamplingEvent', 'Location')
            **kwargs: Additional node properties

        Returns:
            The PID of the created/existing node
        """
        import json

        # Check if node already exists
        if pid in self._pid_to_row_id:
            return pid

        row_id = self._get_next_row_id()
        self._pid_to_row_id[pid] = row_id

        label = kwargs.pop('label', None)
        description = kwargs.pop('description', None)
        altids = kwargs.pop('altids', None)
        n = kwargs.pop('n', None)

        # Handle geometry
        latitude = kwargs.pop('latitude', None)
        longitude = kwargs.pop('longitude', None)
        geometry_sql = "NULL"
        geometry_params = []
        if latitude is not None and longitude is not None:
            geometry_sql = "ST_POINT(?, ?)"
            geometry_params = [longitude, latitude]

        # Convert any numpy arrays or other non-serializable types before JSON encoding
        kwargs = self._convert_to_serializable(kwargs)
        altids = self._convert_to_serializable(altids)

        # Store remaining kwargs as JSON properties
        properties = json.dumps(kwargs) if kwargs else None

        sql = f"""
            INSERT INTO node (row_id, pid, otype, label, description, altids, n, properties, geometry)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, {geometry_sql})
        """
        params = [row_id, pid, otype, label, description, altids, n, properties] + geometry_params
        self._connection.execute(sql, params)

        return pid

    def addEdge(self, s: str, p: str, o: list, n: str = None) -> str:
        """Add an edge between nodes.

        Args:
            s: Subject PID (source node)
            p: Predicate (relationship type)
            o: Object PIDs (list of target nodes)
            n: Named graph (optional)

        Returns:
            The PID of the created edge
        """
        import hashlib

        # Convert PIDs to row_ids
        s_row_id = self._pid_to_row_id.get(s)
        if s_row_id is None:
            raise ValueError(f"Subject PID not found: {s}")

        o_row_ids = []
        for o_pid in o:
            o_row_id = self._pid_to_row_id.get(o_pid)
            if o_row_id is None:
                raise ValueError(f"Object PID not found: {o_pid}")
            o_row_ids.append(o_row_id)

        row_id = self._get_next_row_id()

        # Generate edge PID from hash of s, p, o
        edge_content = f"{s}|{p}|{','.join(o)}"
        edge_pid = f"edge_{hashlib.md5(edge_content.encode()).hexdigest()[:12]}"

        self._connection.execute("""
            INSERT INTO node (row_id, pid, otype, s, p, o, n)
            VALUES (?, ?, '_edge_', ?, ?, ?, ?)
        """, [row_id, edge_pid, s_row_id, p, o_row_ids, n])

        return edge_pid

    def toParquet(self, output_path: str, **kwargs):
        """Export the graph to a Parquet file.

        Args:
            output_path: Path to write the Parquet file
            **kwargs: Additional arguments (compression, etc.)
        """
        compression = kwargs.get('compression', 'ZSTD')
        self._connection.execute(f"""
            COPY node TO '{output_path}' (FORMAT PARQUET, COMPRESSION {compression})
        """)


__all__ = [
    OptionalStr,
    OptionalInt,
    OptionalFloat,
    OptionalDateTime,
    OptionalDecimal,
    StringList,
    IntegerList,
    FloatList,
    DateTimeList,
    Base,
    PQG,
    Edge,
    Graph,
    JSONDateTimeEncoder,
    ISamplesEdgeType,
    infer_edge_type,
    validate_edge_type,
    get_edge_types_by_subject,
    get_edge_types_by_object,
    EDGE_TYPE_CONSTRAINTS,
    TypedEdgeQueries,
    TypedEdgeGenerator,
]
