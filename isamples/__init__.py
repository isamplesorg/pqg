"""
Wraps the generated classes so that they inherit properties from PQG Base class.

The generated/isamples_core.py is created by running:

gen-python --no-slots ${SOURCE}/src/schemas/isamples_core.yaml > ${DEST}/generated/isamples_core.py

gen-python is from linkml
SOURCE is the iSamples metadata model "20250207-adjustments" branch
DEST is this reop
"""

import dataclasses
import logging

import pqg
from isamples import generated


def getLogger():
    return logging.getLogger("isamples")


@dataclasses.dataclass(kw_only=True)
class Agent(pqg.Base, generated.Agent):
    pass


@dataclasses.dataclass(kw_only=True)
class SamplingEvent(pqg.Base, generated.SamplingEvent):
    pass


@dataclasses.dataclass(kw_only=True)
class SamplingSite(pqg.Base, generated.SamplingSite):
    pass


@dataclasses.dataclass(kw_only=True)
class GeospatialCoordLocation(pqg.Base, generated.GeospatialCoordLocation):
    pass


@dataclasses.dataclass(kw_only=True)
class IdentifiedConcept(pqg.Base, generated.IdentifiedConcept):
    pass


@dataclasses.dataclass(kw_only=True)
class SampleRelation(pqg.Base, generated.SampleRelation):
    pass


@dataclasses.dataclass(kw_only=True)
class MaterialSampleRecord(pqg.Base, generated.MaterialSampleRecord):
    pass


@dataclasses.dataclass(kw_only=True)
class MaterialSampleCuration(pqg.Base, generated.MaterialSampleCuration):
    pass


def createGraph(dbinstance):
    g = pqg.PQG(dbinstance)
    g.initialize(classes=[
        Agent,
        IdentifiedConcept,
        GeospatialCoordLocation,
        SamplingSite,
        SamplingEvent,
        MaterialSampleCuration,
        SampleRelation,
        MaterialSampleRecord,
    ])
    return g


def main():
    g = createGraph()
    a = Agent(pid="a01", name="Agent 01")
    g.addNode(a)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    main()
