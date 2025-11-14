"""
Edge type definitions and utilities for PQG (Provenance Query Graph).

This module provides support for 14 specialized edge types based on the
iSamples LinkML schema. Each edge type represents a specific (subject_type,
predicate, object_type) relationship pattern.

The 14 edge types are derived from entity-to-entity relationships in the
iSamples core schema, excluding primitive-valued properties.
"""

from enum import Enum
from typing import Dict, Optional, Tuple, List


class ISamplesEdgeType(Enum):
    """
    The 14 theoretical edge types in iSamples schema.

    Each edge type is named using the pattern: {SUBJECT}_{PREDICATE}
    The value encodes the full triple: subject__predicate__object

    These types enable:
    - Type-safe edge creation
    - Validation of edge constraints
    - Specialized query methods
    - Schema-aware graph operations
    """

    # MaterialSampleCuration edges (1 type)
    CURATION_RESPONSIBILITY = "MaterialSampleCuration__responsibility__Agent"

    # MaterialSampleRecord edges (8 types)
    MSR_CURATION = "MaterialSampleRecord__curation__MaterialSampleCuration"
    MSR_HAS_CONTEXT_CATEGORY = "MaterialSampleRecord__has_context_category__IdentifiedConcept"
    MSR_HAS_MATERIAL_CATEGORY = "MaterialSampleRecord__has_material_category__IdentifiedConcept"
    MSR_HAS_SAMPLE_OBJECT_TYPE = "MaterialSampleRecord__has_sample_object_type__IdentifiedConcept"
    MSR_KEYWORDS = "MaterialSampleRecord__keywords__IdentifiedConcept"
    MSR_PRODUCED_BY = "MaterialSampleRecord__produced_by__SamplingEvent"
    MSR_REGISTRANT = "MaterialSampleRecord__registrant__Agent"
    MSR_RELATED_RESOURCE = "MaterialSampleRecord__related_resource__SampleRelation"

    # SamplingEvent edges (4 types)
    EVENT_HAS_CONTEXT_CATEGORY = "SamplingEvent__has_context_category__IdentifiedConcept"
    EVENT_RESPONSIBILITY = "SamplingEvent__responsibility__Agent"
    EVENT_SAMPLE_LOCATION = "SamplingEvent__sample_location__GeospatialCoordLocation"
    EVENT_SAMPLING_SITE = "SamplingEvent__sampling_site__SamplingSite"

    # SamplingSite edges (1 type)
    SITE_LOCATION = "SamplingSite__site_location__GeospatialCoordLocation"

    @property
    def subject_type(self) -> str:
        """Get the expected subject (source) node type for this edge."""
        return self.value.split("__")[0]

    @property
    def predicate(self) -> str:
        """Get the predicate (relationship name) for this edge."""
        return self.value.split("__")[1]

    @property
    def object_type(self) -> str:
        """Get the expected object (target) node type for this edge."""
        return self.value.split("__")[2]

    @property
    def as_triple(self) -> Tuple[str, str, str]:
        """Get the edge type as a (subject_type, predicate, object_type) triple."""
        parts = self.value.split("__")
        return (parts[0], parts[1], parts[2])

    def __str__(self) -> str:
        """String representation showing the SPO pattern."""
        return f"{self.subject_type} --{self.predicate}--> {self.object_type}"

    @classmethod
    def from_spo(cls, subject_type: str, predicate: str, object_type: str) -> Optional["ISamplesEdgeType"]:
        """
        Find the edge type matching the given SPO triple.

        Args:
            subject_type: The otype of the subject node
            predicate: The relationship name
            object_type: The otype of the object node

        Returns:
            The matching ISamplesEdgeType, or None if no match

        Example:
            >>> ISamplesEdgeType.from_spo(
            ...     "MaterialSampleRecord", "produced_by", "SamplingEvent"
            ... )
            <ISamplesEdgeType.MSR_PRODUCED_BY: 'MaterialSampleRecord__produced_by__SamplingEvent'>
        """
        value = f"{subject_type}__{predicate}__{object_type}"
        for edge_type in cls:
            if edge_type.value == value:
                return edge_type
        return None

    @classmethod
    def from_predicate(cls, predicate: str) -> List["ISamplesEdgeType"]:
        """
        Find all edge types using the given predicate.

        This is useful when the same predicate appears in multiple contexts
        (e.g., 'responsibility' in both MaterialSampleCuration and SamplingEvent).

        Args:
            predicate: The relationship name

        Returns:
            List of matching ISamplesEdgeType instances

        Example:
            >>> ISamplesEdgeType.from_predicate("responsibility")
            [<ISamplesEdgeType.CURATION_RESPONSIBILITY: ...>,
             <ISamplesEdgeType.EVENT_RESPONSIBILITY: ...>]
        """
        return [et for et in cls if et.predicate == predicate]


# Edge type constraints for validation
EDGE_TYPE_CONSTRAINTS: Dict[str, Dict[str, str]] = {
    "MaterialSampleCuration__responsibility__Agent": {
        "subject_type": "MaterialSampleCuration",
        "predicate": "responsibility",
        "object_type": "Agent",
        "multivalued": "true",
        "description": "Agent responsible for sample curation",
    },
    "MaterialSampleRecord__curation__MaterialSampleCuration": {
        "subject_type": "MaterialSampleRecord",
        "predicate": "curation",
        "object_type": "MaterialSampleCuration",
        "multivalued": "false",
        "description": "Curation information for the sample",
    },
    "MaterialSampleRecord__has_context_category__IdentifiedConcept": {
        "subject_type": "MaterialSampleRecord",
        "predicate": "has_context_category",
        "object_type": "IdentifiedConcept",
        "multivalued": "true",
        "description": "Context category (sampled feature type)",
    },
    "MaterialSampleRecord__has_material_category__IdentifiedConcept": {
        "subject_type": "MaterialSampleRecord",
        "predicate": "has_material_category",
        "object_type": "IdentifiedConcept",
        "multivalued": "true",
        "description": "Material type classification",
    },
    "MaterialSampleRecord__has_sample_object_type__IdentifiedConcept": {
        "subject_type": "MaterialSampleRecord",
        "predicate": "has_sample_object_type",
        "object_type": "IdentifiedConcept",
        "multivalued": "true",
        "description": "Sample object type classification",
    },
    "MaterialSampleRecord__keywords__IdentifiedConcept": {
        "subject_type": "MaterialSampleRecord",
        "predicate": "keywords",
        "object_type": "IdentifiedConcept",
        "multivalued": "true",
        "description": "Keywords for sample discovery",
    },
    "MaterialSampleRecord__produced_by__SamplingEvent": {
        "subject_type": "MaterialSampleRecord",
        "predicate": "produced_by",
        "object_type": "SamplingEvent",
        "multivalued": "false",
        "description": "Sampling event that produced this sample",
    },
    "MaterialSampleRecord__registrant__Agent": {
        "subject_type": "MaterialSampleRecord",
        "predicate": "registrant",
        "object_type": "Agent",
        "multivalued": "false",
        "description": "Agent who registered the sample",
    },
    "MaterialSampleRecord__related_resource__SampleRelation": {
        "subject_type": "MaterialSampleRecord",
        "predicate": "related_resource",
        "object_type": "SampleRelation",
        "multivalued": "true",
        "description": "Related resources and samples",
    },
    "SamplingEvent__has_context_category__IdentifiedConcept": {
        "subject_type": "SamplingEvent",
        "predicate": "has_context_category",
        "object_type": "IdentifiedConcept",
        "multivalued": "true",
        "description": "Context category for sampling event",
    },
    "SamplingEvent__responsibility__Agent": {
        "subject_type": "SamplingEvent",
        "predicate": "responsibility",
        "object_type": "Agent",
        "multivalued": "true",
        "description": "Agent responsible for sampling event",
    },
    "SamplingEvent__sample_location__GeospatialCoordLocation": {
        "subject_type": "SamplingEvent",
        "predicate": "sample_location",
        "object_type": "GeospatialCoordLocation",
        "multivalued": "false",
        "description": "Geographic location where sample was collected",
    },
    "SamplingEvent__sampling_site__SamplingSite": {
        "subject_type": "SamplingEvent",
        "predicate": "sampling_site",
        "object_type": "SamplingSite",
        "multivalued": "false",
        "description": "Site where sampling occurred",
    },
    "SamplingSite__site_location__GeospatialCoordLocation": {
        "subject_type": "SamplingSite",
        "predicate": "site_location",
        "object_type": "GeospatialCoordLocation",
        "multivalued": "false",
        "description": "Geographic location of the sampling site",
    },
}


def validate_edge_type(
    edge_type: str,
    subject_otype: str,
    predicate: str,
    object_otype: str
) -> Tuple[bool, Optional[str]]:
    """
    Validate that an edge matches its declared type constraints.

    Args:
        edge_type: The edge type identifier (e.g., "MaterialSampleRecord__produced_by__SamplingEvent")
        subject_otype: The otype of the subject node
        predicate: The predicate of the edge
        object_otype: The otype of the object node

    Returns:
        Tuple of (is_valid, error_message)

    Example:
        >>> validate_edge_type(
        ...     "MaterialSampleRecord__produced_by__SamplingEvent",
        ...     "MaterialSampleRecord",
        ...     "produced_by",
        ...     "SamplingEvent"
        ... )
        (True, None)
    """
    constraints = EDGE_TYPE_CONSTRAINTS.get(edge_type)

    if not constraints:
        return False, f"Unknown edge type: {edge_type}"

    if constraints["subject_type"] != subject_otype:
        return False, (
            f"Subject type mismatch: expected {constraints['subject_type']}, "
            f"got {subject_otype}"
        )

    if constraints["predicate"] != predicate:
        return False, (
            f"Predicate mismatch: expected {constraints['predicate']}, "
            f"got {predicate}"
        )

    if constraints["object_type"] != object_otype:
        return False, (
            f"Object type mismatch: expected {constraints['object_type']}, "
            f"got {object_otype}"
        )

    return True, None


def infer_edge_type(
    subject_otype: str,
    predicate: str,
    object_otype: str
) -> Optional[ISamplesEdgeType]:
    """
    Infer the edge type from SPO components.

    This is useful for automatically typing edges when they are created
    from dataclass instances.

    Args:
        subject_otype: The otype of the subject node
        predicate: The predicate of the edge
        object_otype: The otype of the object node

    Returns:
        The inferred ISamplesEdgeType, or None if no match

    Example:
        >>> infer_edge_type("MaterialSampleRecord", "produced_by", "SamplingEvent")
        <ISamplesEdgeType.MSR_PRODUCED_BY: 'MaterialSampleRecord__produced_by__SamplingEvent'>
    """
    return ISamplesEdgeType.from_spo(subject_otype, predicate, object_otype)


def get_edge_types_by_subject(subject_type: str) -> List[ISamplesEdgeType]:
    """
    Get all edge types that can originate from a given subject type.

    Args:
        subject_type: The otype of the subject node

    Returns:
        List of edge types that can have this subject type

    Example:
        >>> get_edge_types_by_subject("MaterialSampleRecord")
        [<ISamplesEdgeType.MSR_CURATION: ...>, ...]
    """
    return [et for et in ISamplesEdgeType if et.subject_type == subject_type]


def get_edge_types_by_object(object_type: str) -> List[ISamplesEdgeType]:
    """
    Get all edge types that can target a given object type.

    Args:
        object_type: The otype of the object node

    Returns:
        List of edge types that can have this object type

    Example:
        >>> get_edge_types_by_object("Agent")
        [<ISamplesEdgeType.CURATION_RESPONSIBILITY: ...>,
         <ISamplesEdgeType.MSR_REGISTRANT: ...>,
         <ISamplesEdgeType.EVENT_RESPONSIBILITY: ...>]
    """
    return [et for et in ISamplesEdgeType if et.object_type == object_type]
