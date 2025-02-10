# Auto generated from isamples_core.yaml by pythongen.py version: 0.0.1
# Generation date: 2025-02-07T14:49:25
# Schema: materialSample
#
# id: https://w3id.org/isample/schema/1.0
# description: DV 2025-02-07. Modifications after discussion on iSamples tech. Add geo directly to SamplingEvent, add has_context_category to SamplingEvent.
#   SMR 2022-10-07. Schema for iSamples sample registry integration. Updated from 0.2 by synchronizing the vocabulary enumerations, change
#   'id' to '@id' and 'schema' to '$schema'.  Schema name is iSamplesSchemaCore1.0.json. Target JSON schema version is
#   https://json-schema.org/draft/2019-09/schema.  SMR 2023-03-17. Move authorized_by into SamplingEvent, change
#   keywords to list of Keyword objects with string, uri, scheme and scheme URI. Reorder elements in slot list. Update
#   scheme URI to 1.0 2023-06-13 SMR add project property on SamplingEvent, with text or URI value. 2024-01-17 SMR run
#   linkml-lint from current linkml version before rebuilding JSON schema.  2024-04-19. DV ran yaml linter on the file to clean
#   up formatting, put hard return in text bodies.  Update schema URI to dereference with w3id. SMR 2024-09-13 add last_modified_time
#   as property of MaterialSampleRecord.
#
# license: https://creativecommons.org/publicdomain/zero/1.0/

import dataclasses
import re
from dataclasses import dataclass
from datetime import (
    date,
    datetime,
    time
)
from typing import (
    Any,
    ClassVar,
    Dict,
    List,
    Optional,
    Union
)

from jsonasobj2 import (
    JsonObj,
    as_dict
)
from linkml_runtime.linkml_model.meta import (
    EnumDefinition,
    PermissibleValue,
    PvFormulaOptions
)
from linkml_runtime.utils.curienamespace import CurieNamespace
from linkml_runtime.utils.dataclass_extensions_376 import dataclasses_init_fn_with_kwargs
from linkml_runtime.utils.enumerations import EnumDefinitionImpl
from linkml_runtime.utils.formatutils import (
    camelcase,
    sfx,
    underscore
)
from linkml_runtime.utils.metamodelcore import (
    bnode,
    empty_dict,
    empty_list
)
from linkml_runtime.utils.slot import Slot
from linkml_runtime.utils.yamlutils import (
    YAMLRoot,
    extended_float,
    extended_int,
    extended_str
)
from rdflib import (
    Namespace,
    URIRef
)

from linkml_runtime.linkml_model.types import Boolean, Datetime, Decimal, String, Uriorcurie
from linkml_runtime.utils.metamodelcore import Bool, Decimal, URIorCURIE, XSDDateTime

metamodel_version = "1.7.0"
version = "20250207"

# Overwrite dataclasses _init_fn to add **kwargs in __init__
dataclasses._init_fn = dataclasses_init_fn_with_kwargs

# Namespaces
DATACITE = CurieNamespace('datacite', 'http://datacite.org/schema/kernel-4/')
DCT = CurieNamespace('dct', 'http://purl.org/dc/terms/')
ISAM = CurieNamespace('isam', 'https://w3id.org/isample/schema/1.0/')
LINKML = CurieNamespace('linkml', 'https://w3id.org/linkml/')
MAT = CurieNamespace('mat', 'https://w3id.org/isample/vocabulary/material/')
MSOT = CurieNamespace('msot', 'https://w3id.org/isample/vocabulary/materialsampleobjecttype/')
RDFS = CurieNamespace('rdfs', 'http://www.w3.org/2000/01/rdf-schema#')
SDO = CurieNamespace('sdo', 'http://schema.org/')
SF = CurieNamespace('sf', 'https://w3id.org/isample/vocabulary/sampledfeature/')
SKOS = CurieNamespace('skos', 'http://www.w3.org/2004/02/skos/core#')
SKOS_CONCEPT = CurieNamespace('skos_concept', 'http://www.w3.org/2004/02/skos/core#Concept')
W3CPOS = CurieNamespace('w3cpos', 'http://www.w3.org/2003/01/geo/wgs84_pos#')
XSD = CurieNamespace('xsd', 'http://www.w3.org/2001/XMLSchema#')
DEFAULT_ = ISAM


# Types

# Class references



@dataclass(repr=False)
class MaterialSampleRecord(YAMLRoot):
    """
    This is a data object that is a digital representation of a material sample, and thus shares the same identifier
    as the physical object. It provides descriptive properties for any iSamples material sample, URI for the metadata
    record is same as URI for material sample-- digital object is considered twin of physical object, a
    representation. IGSN is recommended. Must be a URI that can be dereferenced on the web.
    """
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = ISAM["MaterialSampleRecord"]
    class_class_curie: ClassVar[str] = "isam:MaterialSampleRecord"
    class_name: ClassVar[str] = "MaterialSampleRecord"
    class_model_uri: ClassVar[URIRef] = ISAM.MaterialSampleRecord

    pid: Union[str, URIorCURIE] = None
    label: str = None
    last_modified_time: Union[str, XSDDateTime] = None
    description: Optional[str] = None
    sample_identifier: Optional[str] = None
    alternate_identifiers: Optional[Union[str, List[str]]] = empty_list()
    produced_by: Optional[Union[dict, "SamplingEvent"]] = None
    sampling_purpose: Optional[str] = None
    has_context_category: Optional[Union[Union[dict, "IdentifiedConcept"], List[Union[dict, "IdentifiedConcept"]]]] = empty_list()
    has_material_category: Optional[Union[Union[dict, "IdentifiedConcept"], List[Union[dict, "IdentifiedConcept"]]]] = empty_list()
    has_sample_object_type: Optional[Union[Union[dict, "IdentifiedConcept"], List[Union[dict, "IdentifiedConcept"]]]] = empty_list()
    keywords: Optional[Union[Union[dict, "IdentifiedConcept"], List[Union[dict, "IdentifiedConcept"]]]] = empty_list()
    related_resource: Optional[Union[Union[dict, "SampleRelation"], List[Union[dict, "SampleRelation"]]]] = empty_list()
    complies_with: Optional[Union[str, List[str]]] = empty_list()
    dc_rights: Optional[str] = None
    curation: Optional[Union[dict, "MaterialSampleCuration"]] = None
    registrant: Optional[Union[dict, "Agent"]] = None

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if self._is_empty(self.pid):
            self.MissingRequiredField("pid")
        if not isinstance(self.pid, URIorCURIE):
            self.pid = URIorCURIE(self.pid)

        if self._is_empty(self.label):
            self.MissingRequiredField("label")
        if not isinstance(self.label, str):
            self.label = str(self.label)

        if self._is_empty(self.last_modified_time):
            self.MissingRequiredField("last_modified_time")
        if not isinstance(self.last_modified_time, XSDDateTime):
            self.last_modified_time = XSDDateTime(self.last_modified_time)

        if self.description is not None and not isinstance(self.description, str):
            self.description = str(self.description)

        if self.sample_identifier is not None and not isinstance(self.sample_identifier, str):
            self.sample_identifier = str(self.sample_identifier)

        if not isinstance(self.alternate_identifiers, list):
            self.alternate_identifiers = [self.alternate_identifiers] if self.alternate_identifiers is not None else []
        self.alternate_identifiers = [v if isinstance(v, str) else str(v) for v in self.alternate_identifiers]

        if self.produced_by is not None and not isinstance(self.produced_by, SamplingEvent):
            self.produced_by = SamplingEvent(**as_dict(self.produced_by))

        if self.sampling_purpose is not None and not isinstance(self.sampling_purpose, str):
            self.sampling_purpose = str(self.sampling_purpose)

        if not isinstance(self.has_context_category, list):
            self.has_context_category = [self.has_context_category] if self.has_context_category is not None else []
        self.has_context_category = [v if isinstance(v, IdentifiedConcept) else IdentifiedConcept(**as_dict(v)) for v in self.has_context_category]

        if not isinstance(self.has_material_category, list):
            self.has_material_category = [self.has_material_category] if self.has_material_category is not None else []
        self.has_material_category = [v if isinstance(v, IdentifiedConcept) else IdentifiedConcept(**as_dict(v)) for v in self.has_material_category]

        if not isinstance(self.has_sample_object_type, list):
            self.has_sample_object_type = [self.has_sample_object_type] if self.has_sample_object_type is not None else []
        self.has_sample_object_type = [v if isinstance(v, IdentifiedConcept) else IdentifiedConcept(**as_dict(v)) for v in self.has_sample_object_type]

        if not isinstance(self.keywords, list):
            self.keywords = [self.keywords] if self.keywords is not None else []
        self.keywords = [v if isinstance(v, IdentifiedConcept) else IdentifiedConcept(**as_dict(v)) for v in self.keywords]

        if not isinstance(self.related_resource, list):
            self.related_resource = [self.related_resource] if self.related_resource is not None else []
        self.related_resource = [v if isinstance(v, SampleRelation) else SampleRelation(**as_dict(v)) for v in self.related_resource]

        if not isinstance(self.complies_with, list):
            self.complies_with = [self.complies_with] if self.complies_with is not None else []
        self.complies_with = [v if isinstance(v, str) else str(v) for v in self.complies_with]

        if self.dc_rights is not None and not isinstance(self.dc_rights, str):
            self.dc_rights = str(self.dc_rights)

        if self.curation is not None and not isinstance(self.curation, MaterialSampleCuration):
            self.curation = MaterialSampleCuration(**as_dict(self.curation))

        if self.registrant is not None and not isinstance(self.registrant, Agent):
            self.registrant = Agent(**as_dict(self.registrant))

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class Agent(YAMLRoot):
    """
    Object to represent a person who plays a role relative to sample collection or curation.
    """
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = ISAM["Agent"]
    class_class_curie: ClassVar[str] = "isam:Agent"
    class_name: ClassVar[str] = "Agent"
    class_model_uri: ClassVar[URIRef] = ISAM.Agent

    name: Optional[str] = None
    affiliation: Optional[str] = None
    contact_information: Optional[str] = None
    pid: Optional[Union[str, URIorCURIE]] = None
    role: Optional[str] = None

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if self.name is not None and not isinstance(self.name, str):
            self.name = str(self.name)

        if self.affiliation is not None and not isinstance(self.affiliation, str):
            self.affiliation = str(self.affiliation)

        if self.contact_information is not None and not isinstance(self.contact_information, str):
            self.contact_information = str(self.contact_information)

        if self.pid is not None and not isinstance(self.pid, URIorCURIE):
            self.pid = URIorCURIE(self.pid)

        if self.role is not None and not isinstance(self.role, str):
            self.role = str(self.role)

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class SamplingSite(YAMLRoot):
    """
    Documentation of the site where the sample was collected, wtih place name(s) and a geospatial location.
    """
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = ISAM["SamplingSite"]
    class_class_curie: ClassVar[str] = "isam:SamplingSite"
    class_name: ClassVar[str] = "SamplingSite"
    class_model_uri: ClassVar[URIRef] = ISAM.SamplingSite

    pid: Optional[Union[str, URIorCURIE]] = None
    description: Optional[str] = None
    label: Optional[str] = None
    sample_location: Optional[Union[dict, "GeospatialCoordLocation"]] = None
    place_name: Optional[Union[str, List[str]]] = empty_list()
    is_part_of: Optional[Union[Union[str, URIorCURIE], List[Union[str, URIorCURIE]]]] = empty_list()

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if self.pid is not None and not isinstance(self.pid, URIorCURIE):
            self.pid = URIorCURIE(self.pid)

        if self.description is not None and not isinstance(self.description, str):
            self.description = str(self.description)

        if self.label is not None and not isinstance(self.label, str):
            self.label = str(self.label)

        if self.sample_location is not None and not isinstance(self.sample_location, GeospatialCoordLocation):
            self.sample_location = GeospatialCoordLocation(**as_dict(self.sample_location))

        if not isinstance(self.place_name, list):
            self.place_name = [self.place_name] if self.place_name is not None else []
        self.place_name = [v if isinstance(v, str) else str(v) for v in self.place_name]

        if not isinstance(self.is_part_of, list):
            self.is_part_of = [self.is_part_of] if self.is_part_of is not None else []
        self.is_part_of = [v if isinstance(v, URIorCURIE) else URIorCURIE(v) for v in self.is_part_of]

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class SamplingEvent(YAMLRoot):
    """
    Information about the event resulting in the creation of the material sample. Include information about permitting
    in the authorized_by property. The sampling procedure should be described in the description. If any special
    protocols were followed in the sampling procedure, they should be documented using the
    MaterialSampleRecord/complies_with property.
    """
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = ISAM["SamplingEvent"]
    class_class_curie: ClassVar[str] = "isam:SamplingEvent"
    class_name: ClassVar[str] = "SamplingEvent"
    class_model_uri: ClassVar[URIRef] = ISAM.SamplingEvent

    pid: Optional[Union[str, URIorCURIE]] = None
    label: Optional[str] = None
    description: Optional[str] = None
    has_feature_of_interest: Optional[str] = None
    has_context_category: Optional[Union[Union[dict, "IdentifiedConcept"], List[Union[dict, "IdentifiedConcept"]]]] = empty_list()
    project: Optional[str] = None
    responsibility: Optional[Union[Union[dict, Agent], List[Union[dict, Agent]]]] = empty_list()
    result_time: Optional[str] = None
    sampling_site: Optional[Union[dict, SamplingSite]] = None
    authorized_by: Optional[Union[str, List[str]]] = empty_list()
    sample_location: Optional[Union[dict, "GeospatialCoordLocation"]] = None

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if self.pid is not None and not isinstance(self.pid, URIorCURIE):
            self.pid = URIorCURIE(self.pid)

        if self.label is not None and not isinstance(self.label, str):
            self.label = str(self.label)

        if self.description is not None and not isinstance(self.description, str):
            self.description = str(self.description)

        if self.has_feature_of_interest is not None and not isinstance(self.has_feature_of_interest, str):
            self.has_feature_of_interest = str(self.has_feature_of_interest)

        if not isinstance(self.has_context_category, list):
            self.has_context_category = [self.has_context_category] if self.has_context_category is not None else []
        self.has_context_category = [v if isinstance(v, IdentifiedConcept) else IdentifiedConcept(**as_dict(v)) for v in self.has_context_category]

        if self.project is not None and not isinstance(self.project, str):
            self.project = str(self.project)

        if not isinstance(self.responsibility, list):
            self.responsibility = [self.responsibility] if self.responsibility is not None else []
        self.responsibility = [v if isinstance(v, Agent) else Agent(**as_dict(v)) for v in self.responsibility]

        if self.result_time is not None and not isinstance(self.result_time, str):
            self.result_time = str(self.result_time)

        if self.sampling_site is not None and not isinstance(self.sampling_site, SamplingSite):
            self.sampling_site = SamplingSite(**as_dict(self.sampling_site))

        if not isinstance(self.authorized_by, list):
            self.authorized_by = [self.authorized_by] if self.authorized_by is not None else []
        self.authorized_by = [v if isinstance(v, str) else str(v) for v in self.authorized_by]

        if self.sample_location is not None and not isinstance(self.sample_location, GeospatialCoordLocation):
            self.sample_location = GeospatialCoordLocation(**as_dict(self.sample_location))

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class GeospatialCoordLocation(YAMLRoot):
    """
    A physical location in decimal degrees using EPSG\:4326. Could be point location, or the centroid of a area.
    Elevation is specified as a string that should include the measure, units of measure, and the vertical reference
    system, e.g. 'above mean sea level', 'below ground surface', 'below sea floor'...
    """
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = ISAM["GeospatialCoordLocation"]
    class_class_curie: ClassVar[str] = "isam:GeospatialCoordLocation"
    class_name: ClassVar[str] = "GeospatialCoordLocation"
    class_model_uri: ClassVar[URIRef] = ISAM.GeospatialCoordLocation

    elevation: Optional[str] = None
    latitude: Optional[Decimal] = None
    longitude: Optional[Decimal] = None
    obfuscated: Optional[Union[bool, Bool]] = False

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if self.elevation is not None and not isinstance(self.elevation, str):
            self.elevation = str(self.elevation)

        if self.latitude is not None and not isinstance(self.latitude, Decimal):
            self.latitude = Decimal(self.latitude)

        if self.longitude is not None and not isinstance(self.longitude, Decimal):
            self.longitude = Decimal(self.longitude)

        if self.obfuscated is not None and not isinstance(self.obfuscated, Bool):
            self.obfuscated = Bool(self.obfuscated)

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class MaterialSampleCuration(YAMLRoot):
    """
    Information about the current storage of sample, access to sample, and events in curation history. Curation as
    used here starts when the sample is removed from its original context, and might include various processing steps
    for preservation. Processing related to analysis preparation such as crushing, dissolution, evaporation, filtering
    are considered part of the sampling method for the derived child sample.
    """
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = ISAM["MaterialSampleCuration"]
    class_class_curie: ClassVar[str] = "isam:MaterialSampleCuration"
    class_name: ClassVar[str] = "MaterialSampleCuration"
    class_model_uri: ClassVar[URIRef] = ISAM.MaterialSampleCuration

    pid: Optional[Union[str, URIorCURIE]] = None
    access_constraints: Optional[Union[str, List[str]]] = empty_list()
    curation_location: Optional[str] = None
    description: Optional[str] = None
    label: Optional[str] = None
    responsibility: Optional[Union[Union[dict, Agent], List[Union[dict, Agent]]]] = empty_list()

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if self.pid is not None and not isinstance(self.pid, URIorCURIE):
            self.pid = URIorCURIE(self.pid)

        if not isinstance(self.access_constraints, list):
            self.access_constraints = [self.access_constraints] if self.access_constraints is not None else []
        self.access_constraints = [v if isinstance(v, str) else str(v) for v in self.access_constraints]

        if self.curation_location is not None and not isinstance(self.curation_location, str):
            self.curation_location = str(self.curation_location)

        if self.description is not None and not isinstance(self.description, str):
            self.description = str(self.description)

        if self.label is not None and not isinstance(self.label, str):
            self.label = str(self.label)

        if not isinstance(self.responsibility, list):
            self.responsibility = [self.responsibility] if self.responsibility is not None else []
        self.responsibility = [v if isinstance(v, Agent) else Agent(**as_dict(v)) for v in self.responsibility]

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class SampleRelation(YAMLRoot):
    """
    Semantic link to other samples or related resources.
    """
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = ISAM["SampleRelation"]
    class_class_curie: ClassVar[str] = "isam:SampleRelation"
    class_name: ClassVar[str] = "SampleRelation"
    class_model_uri: ClassVar[URIRef] = ISAM.SampleRelation

    description: Optional[str] = None
    label: Optional[str] = None
    relationship: Optional[str] = None
    target: Optional[Union[str, URIorCURIE]] = None

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if self.description is not None and not isinstance(self.description, str):
            self.description = str(self.description)

        if self.label is not None and not isinstance(self.label, str):
            self.label = str(self.label)

        if self.relationship is not None and not isinstance(self.relationship, str):
            self.relationship = str(self.relationship)

        if self.target is not None and not isinstance(self.target, URIorCURIE):
            self.target = URIorCURIE(self.target)

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class IdentifiedConcept(YAMLRoot):
    """
    An identifier with a label, used for vocabulary terms.
    """
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = ISAM["IdentifiedConcept"]
    class_class_curie: ClassVar[str] = "isam:IdentifiedConcept"
    class_name: ClassVar[str] = "IdentifiedConcept"
    class_model_uri: ClassVar[URIRef] = ISAM.IdentifiedConcept

    pid: Optional[Union[str, URIorCURIE]] = None
    label: Optional[str] = None
    scheme_name: Optional[str] = None
    scheme_uri: Optional[str] = None

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if self.pid is not None and not isinstance(self.pid, URIorCURIE):
            self.pid = URIorCURIE(self.pid)

        if self.label is not None and not isinstance(self.label, str):
            self.label = str(self.label)

        if self.scheme_name is not None and not isinstance(self.scheme_name, str):
            self.scheme_name = str(self.scheme_name)

        if self.scheme_uri is not None and not isinstance(self.scheme_uri, str):
            self.scheme_uri = str(self.scheme_uri)

        super().__post_init__(**kwargs)


# Enumerations


# Slots

