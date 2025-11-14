"""
Typed edge query and generation support for PQG.

This module provides specialized query and generation methods for the 14
theoretical edge types in iSamples, WITHOUT modifying the PQG schema.

Edge types are inferred dynamically from:
- Subject node's otype
- Predicate
- Object node's otype

This allows type-safe operations while maintaining backward compatibility.
"""

import logging
from typing import Optional, List, Tuple, Iterator
from pqg.edge_types import (
    ISamplesEdgeType,
    infer_edge_type,
    validate_edge_type,
    get_edge_types_by_subject,
    get_edge_types_by_object,
    EDGE_TYPE_CONSTRAINTS,
)
from pqg.pqg_singletable import PQG, Edge


def getLogger():
    return logging.getLogger("pqg.typed_edges")


class TypedEdgeQueries:
    """
    Specialized query methods for typed edges.

    This class wraps a PQG instance and provides methods to query edges
    by their inferred type, without modifying the underlying schema.

    Example:
        >>> graph = PQG()
        >>> typed = TypedEdgeQueries(graph)
        >>> # Get all "MaterialSampleRecord produced_by SamplingEvent" edges
        >>> for edge in typed.get_edges_by_type(ISamplesEdgeType.MSR_PRODUCED_BY):
        ...     print(edge)
    """

    def __init__(self, pqg_instance: PQG):
        """
        Initialize typed edge queries.

        Args:
            pqg_instance: The PQG instance to query
        """
        self.pqg = pqg_instance
        self._logger = getLogger()

    def infer_edge_type_from_pids(
        self,
        subject_pid: str,
        predicate: str,
        object_pid: str
    ) -> Optional[ISamplesEdgeType]:
        """
        Infer the edge type from PIDs by looking up node types.

        Args:
            subject_pid: PID of subject node
            predicate: Edge predicate
            object_pid: PID of object node

        Returns:
            The inferred ISamplesEdgeType, or None if not recognized
        """
        # Get subject otype
        with self.pqg.getCursor() as csr:
            result = csr.execute(
                f"SELECT otype FROM {self.pqg._table} WHERE {self.pqg._node_pk} = ? AND otype != '_edge_'",
                [subject_pid]
            ).fetchone()
            if not result:
                self._logger.warning(f"Subject node not found: {subject_pid}")
                return None
            subject_otype = result[0]

        # Get object otype
        with self.pqg.getCursor() as csr:
            result = csr.execute(
                f"SELECT otype FROM {self.pqg._table} WHERE {self.pqg._node_pk} = ? AND otype != '_edge_'",
                [object_pid]
            ).fetchone()
            if not result:
                self._logger.warning(f"Object node not found: {object_pid}")
                return None
            object_otype = result[0]

        # Infer type from SPO triple
        return infer_edge_type(subject_otype, predicate, object_otype)

    def get_edges_by_type(
        self,
        edge_type: ISamplesEdgeType,
        limit: Optional[int] = None
    ) -> Iterator[Tuple[str, str, List[str], Optional[str], ISamplesEdgeType]]:
        """
        Get all edges matching a specific type.

        Args:
            edge_type: The edge type to filter by
            limit: Optional limit on number of results

        Yields:
            Tuples of (subject_pid, predicate, object_pids, named_graph, edge_type)

        Example:
            >>> for s, p, o_list, n, et in typed.get_edges_by_type(ISamplesEdgeType.MSR_PRODUCED_BY):
            ...     print(f"{s} --{p}--> {o_list}")
        """
        subject_type, predicate, object_type = edge_type.as_triple

        # Query for edges with matching predicate
        # Then verify node types match
        sql = f"""
        SELECT e.pid, e.s, e.p, e.o, e.n,
               s_node.otype as s_otype,
               s_node.pid as s_pid
        FROM {self.pqg._table} AS e
        JOIN {self.pqg._table} AS s_node ON e.s = s_node.row_id
        WHERE e.otype = '_edge_'
          AND e.p = ?
          AND s_node.otype = ?
        """

        if limit:
            sql += f" LIMIT {limit}"

        with self.pqg.getCursor() as csr:
            results = csr.execute(sql, [predicate, subject_type]).fetchall()

        # Batch fetch all object nodes to avoid N+1 query problem
        # First, collect all unique object row_ids
        all_o_row_ids = set()
        for row in results:
            _, _, _, o_row_ids, _, _, _ = row
            all_o_row_ids.update(o_row_ids)

        # Batch query all object nodes at once
        o_node_lookup = {}  # row_id -> (pid, otype)
        if all_o_row_ids:
            placeholders = ','.join('?' * len(all_o_row_ids))
            with self.pqg.getCursor() as csr:
                o_nodes = csr.execute(
                    f"SELECT row_id, pid, otype FROM {self.pqg._table} WHERE row_id IN ({placeholders})",
                    list(all_o_row_ids)
                ).fetchall()
                o_node_lookup = {row[0]: (row[1], row[2]) for row in o_nodes}

        # Now process each edge using the lookup
        for row in results:
            edge_pid, s_row_id, p, o_row_ids, n, s_otype, s_pid = row

            # Verify all object nodes match the expected type
            o_pids = []
            all_match = True

            for o_row_id in o_row_ids:
                o_node = o_node_lookup.get(o_row_id)
                if not o_node:
                    all_match = False
                    break

                o_pid, o_otype = o_node

                if o_otype != object_type:
                    all_match = False
                    break

                o_pids.append(o_pid)

            if all_match and len(o_pids) > 0:
                yield (s_pid, p, o_pids, n, edge_type)

    def get_typed_relations(
        self,
        subject: Optional[str] = None,
        edge_type: Optional[ISamplesEdgeType] = None,
        object_node: Optional[str] = None,
        maxrows: int = 0,
    ) -> Iterator[Tuple[str, str, str, Optional[ISamplesEdgeType]]]:
        """
        Get relations with inferred edge types.

        Like PQG.getRelations() but also returns the inferred edge type.

        Args:
            subject: Optional subject PID filter
            edge_type: Optional edge type filter
            object_node: Optional object PID filter
            maxrows: Maximum rows to return (0 = unlimited)

        Yields:
            Tuples of (subject_pid, predicate, object_pid, edge_type)
        """
        count = 0

        # Use PQG's existing getRelations
        for s_pid, predicate, o_pid in self.pqg.getRelations(
            subject=subject,
            predicate=edge_type.predicate if edge_type else None,
            obj=object_node,
            maxrows=maxrows
        ):
            # Infer the edge type
            inferred_type = self.infer_edge_type_from_pids(s_pid, predicate, o_pid)

            # Filter by edge_type if specified
            if edge_type and inferred_type != edge_type:
                continue

            yield (s_pid, predicate, o_pid, inferred_type)

            count += 1
            if maxrows > 0 and count >= maxrows:
                break

    def get_edges_by_subject_type(
        self,
        subject_type: str,
        limit: Optional[int] = None
    ) -> Iterator[Tuple[str, str, str, ISamplesEdgeType]]:
        """
        Get all edges originating from nodes of a specific type.

        Args:
            subject_type: The otype of subject nodes
            limit: Optional limit on results

        Yields:
            Tuples of (subject_pid, predicate, object_pid, edge_type)
        """
        # Find all possible edge types for this subject type
        possible_types = get_edge_types_by_subject(subject_type)

        for et in possible_types:
            for s, p, o_list, n, edge_type in self.get_edges_by_type(et, limit=limit):
                for o in o_list:
                    yield (s, p, o, edge_type)

    def get_edges_by_object_type(
        self,
        object_type: str,
        limit: Optional[int] = None
    ) -> Iterator[Tuple[str, str, str, ISamplesEdgeType]]:
        """
        Get all edges pointing to nodes of a specific type.

        Args:
            object_type: The otype of object nodes
            limit: Optional limit on results

        Yields:
            Tuples of (subject_pid, predicate, object_pid, edge_type)
        """
        possible_types = get_edge_types_by_object(object_type)

        for et in possible_types:
            for s, p, o_list, n, edge_type in self.get_edges_by_type(et, limit=limit):
                for o in o_list:
                    yield (s, p, o, edge_type)

    def validate_edge(
        self,
        subject_pid: str,
        predicate: str,
        object_pid: str,
        expected_type: Optional[ISamplesEdgeType] = None
    ) -> Tuple[bool, Optional[str]]:
        """
        Validate that an edge matches iSamples schema constraints.

        Args:
            subject_pid: Subject node PID
            predicate: Edge predicate
            object_pid: Object node PID
            expected_type: Optional expected edge type

        Returns:
            Tuple of (is_valid, error_message)
        """
        # Get subject and object otypes for better error messages
        with self.pqg.getCursor() as csr:
            subject_result = csr.execute(
                f"SELECT otype FROM {self.pqg._table} WHERE {self.pqg._node_pk} = ? AND otype != '_edge_'",
                [subject_pid]
            ).fetchone()
            object_result = csr.execute(
                f"SELECT otype FROM {self.pqg._table} WHERE {self.pqg._node_pk} = ? AND otype != '_edge_'",
                [object_pid]
            ).fetchone()

        subject_otype = subject_result[0] if subject_result else "Unknown"
        object_otype = object_result[0] if object_result else "Unknown"

        # Infer the actual type
        inferred_type = self.infer_edge_type_from_pids(subject_pid, predicate, object_pid)

        if inferred_type is None:
            return False, (
                f"Edge pattern ({subject_otype}, {predicate}, {object_otype}) "
                f"does not match any known edge type"
            )

        # Check against expected type if provided
        if expected_type and inferred_type != expected_type:
            return False, f"Expected {expected_type}, but inferred {inferred_type}"

        return True, None

    def get_edge_type_statistics(self) -> List[Tuple[ISamplesEdgeType, int]]:
        """
        Get statistics on edge type usage in the graph.

        Returns:
            List of (edge_type, count) tuples sorted by count descending
        """
        stats = {}

        for et in ISamplesEdgeType:
            count = sum(1 for _ in self.get_edges_by_type(et))
            if count > 0:
                stats[et] = count

        return sorted(stats.items(), key=lambda x: x[1], reverse=True)


class TypedEdgeGenerator:
    """
    Helper class for generating typed edges with validation.

    This ensures that edges conform to the 14 iSamples edge types
    before being added to the graph.
    """

    def __init__(self, pqg_instance: PQG):
        self.pqg = pqg_instance
        self._logger = getLogger()

    def add_typed_edge(
        self,
        subject_pid: str,
        predicate: str,
        object_pids: List[str],
        expected_type: Optional[ISamplesEdgeType] = None,
        named_graph: Optional[str] = None,
        validate: bool = True
    ) -> str:
        """
        Add an edge with type validation.

        Args:
            subject_pid: Subject node PID
            predicate: Edge predicate
            object_pids: List of object node PIDs
            expected_type: Optional expected edge type for validation
            named_graph: Optional named graph
            validate: Whether to validate against iSamples schema (default True)

        Returns:
            The edge PID

        Raises:
            ValueError: If validation fails
        """
        # Validate if requested
        if validate:
            queries = TypedEdgeQueries(self.pqg)
            for object_pid in object_pids:
                is_valid, error = queries.validate_edge(
                    subject_pid, predicate, object_pid, expected_type
                )
                if not is_valid:
                    raise ValueError(f"Edge validation failed: {error}")

        # Create and add the edge
        edge = Edge(
            pid=None,  # Will be auto-generated
            s=subject_pid,
            p=predicate,
            o=object_pids,
            n=named_graph
        )

        return self.pqg.addEdge(edge)

    def add_msr_produced_by(self, msr_pid: str, event_pid: str) -> str:
        """MaterialSampleRecord produced_by SamplingEvent"""
        return self.add_typed_edge(
            msr_pid, "produced_by", [event_pid],
            expected_type=ISamplesEdgeType.MSR_PRODUCED_BY
        )

    def add_msr_registrant(self, msr_pid: str, agent_pid: str) -> str:
        """MaterialSampleRecord registrant Agent"""
        return self.add_typed_edge(
            msr_pid, "registrant", [agent_pid],
            expected_type=ISamplesEdgeType.MSR_REGISTRANT
        )

    def add_msr_curation(self, msr_pid: str, curation_pid: str) -> str:
        """MaterialSampleRecord curation MaterialSampleCuration"""
        return self.add_typed_edge(
            msr_pid, "curation", [curation_pid],
            expected_type=ISamplesEdgeType.MSR_CURATION
        )

    def add_msr_keywords(self, msr_pid: str, keyword_pids: List[str]) -> str:
        """MaterialSampleRecord keywords IdentifiedConcept (multivalued)"""
        return self.add_typed_edge(
            msr_pid, "keywords", keyword_pids,
            expected_type=ISamplesEdgeType.MSR_KEYWORDS
        )

    def add_msr_has_context_category(self, msr_pid: str, concept_pids: List[str]) -> str:
        """MaterialSampleRecord has_context_category IdentifiedConcept (multivalued)"""
        return self.add_typed_edge(
            msr_pid, "has_context_category", concept_pids,
            expected_type=ISamplesEdgeType.MSR_HAS_CONTEXT_CATEGORY
        )

    def add_msr_has_material_category(self, msr_pid: str, concept_pids: List[str]) -> str:
        """MaterialSampleRecord has_material_category IdentifiedConcept (multivalued)"""
        return self.add_typed_edge(
            msr_pid, "has_material_category", concept_pids,
            expected_type=ISamplesEdgeType.MSR_HAS_MATERIAL_CATEGORY
        )

    def add_msr_has_sample_object_type(self, msr_pid: str, concept_pids: List[str]) -> str:
        """MaterialSampleRecord has_sample_object_type IdentifiedConcept (multivalued)"""
        return self.add_typed_edge(
            msr_pid, "has_sample_object_type", concept_pids,
            expected_type=ISamplesEdgeType.MSR_HAS_SAMPLE_OBJECT_TYPE
        )

    def add_msr_related_resource(self, msr_pid: str, relation_pids: List[str]) -> str:
        """MaterialSampleRecord related_resource SampleRelation (multivalued)"""
        return self.add_typed_edge(
            msr_pid, "related_resource", relation_pids,
            expected_type=ISamplesEdgeType.MSR_RELATED_RESOURCE
        )

    def add_event_sampling_site(self, event_pid: str, site_pid: str) -> str:
        """SamplingEvent sampling_site SamplingSite"""
        return self.add_typed_edge(
            event_pid, "sampling_site", [site_pid],
            expected_type=ISamplesEdgeType.EVENT_SAMPLING_SITE
        )

    def add_event_responsibility(self, event_pid: str, agent_pids: List[str]) -> str:
        """SamplingEvent responsibility Agent (multivalued)"""
        return self.add_typed_edge(
            event_pid, "responsibility", agent_pids,
            expected_type=ISamplesEdgeType.EVENT_RESPONSIBILITY
        )

    def add_event_sample_location(self, event_pid: str, location_pid: str) -> str:
        """SamplingEvent sample_location GeospatialCoordLocation"""
        return self.add_typed_edge(
            event_pid, "sample_location", [location_pid],
            expected_type=ISamplesEdgeType.EVENT_SAMPLE_LOCATION
        )

    def add_event_has_context_category(self, event_pid: str, concept_pids: List[str]) -> str:
        """SamplingEvent has_context_category IdentifiedConcept (multivalued)"""
        return self.add_typed_edge(
            event_pid, "has_context_category", concept_pids,
            expected_type=ISamplesEdgeType.EVENT_HAS_CONTEXT_CATEGORY
        )

    def add_site_location(self, site_pid: str, location_pid: str) -> str:
        """SamplingSite site_location GeospatialCoordLocation"""
        return self.add_typed_edge(
            site_pid, "site_location", [location_pid],
            expected_type=ISamplesEdgeType.SITE_LOCATION
        )

    def add_curation_responsibility(self, curation_pid: str, agent_pids: List[str]) -> str:
        """MaterialSampleCuration responsibility Agent (multivalued)"""
        return self.add_typed_edge(
            curation_pid, "responsibility", agent_pids,
            expected_type=ISamplesEdgeType.CURATION_RESPONSIBILITY
        )
