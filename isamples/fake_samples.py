"""
Methods for creating fake entries for testing purposes.
"""

import json
import logging
import pathlib
import typing
import duckdb
import faker

import pqg
from isamples import *

# Used for generating random-ish values
generator = faker.Faker()

fake_project_names = [f"project-{generator.word()}" for _ in range(5)]
fake_place_names = [f"place-{generator.city()}" for _ in range(20)]

def fake_Agent(i: int) -> Agent:
    name = generator.name()
    return Agent(
        pid=f"agent_{i}",
        affiliation=str(generator.administrative_unit()),
        contact_information=str(generator.address()),
        name=name,
        role=generator.job(),
        label= name,
    )


def fake_Agents(count:int = 1, base:int=0) -> typing.Generator[Agent, None, None]:
    for i in range(count):
        yield fake_Agent(base+i)


def fake_GeospatialCoordLocation(i: int) -> GeospatialCoordLocation:
    return GeospatialCoordLocation(
        pid=f"geo_{i}",
        latitude=generator.latitude(),
        longitude=generator.longitude(),
        obfuscated=generator.boolean(0.1),
        elevation=f"{generator.random_int(-500, 10000)}m MSL"
    )

def fake_GeospatialCoordLocations(count:int = 1, base:int = 0) -> typing.Generator[GeospatialCoordLocation, None, None]:
    for i in range(count):
        yield fake_GeospatialCoordLocation(base+i)

def fake_IdentifiedConcept(domain: str) -> IdentifiedConcept:
    uri = f"https://fake.uri/{domain}/"
    label = generator.word()
    return IdentifiedConcept(
        pid=f"{uri}{label}",
        label=generator.word(),
        scheme_name=domain,
        scheme_uri=uri
    )


def fake_IdentifiedConcepts(domain:str, count:int = 1) -> typing.Generator[IdentifiedConcept, None, None]:
    for i in range(count):
        yield fake_IdentifiedConcept(domain)


def fake_SampleRelation(i: int, target: str) -> SampleRelation:
    return SampleRelation(
        pid=f"rel_{i}",
        target=target,
        description=generator.text(),
        label=generator.word(),
        relationship="subsample"
    )


def fake_SampleRelations(target:str, count:int = 1, base:int=0) -> typing.Generator[SampleRelation, None, None]:
    for i in range(count):
        yield fake_SampleRelation(base+i, target)


def fake_SamplingSite(
        i: int,
        sample_location: typing.Optional[GeospatialCoordLocation] = None,
        is_part_of: typing.Optional[str] = None,
) -> SamplingSite:
    return SamplingSite(
        pid=f"site_{i}",
        description=generator.text(),
        label=generator.word(part_of_speech="noun"),  # noun
        place_name=list(generator.random_choices(fake_place_names, 2)),
        site_location=sample_location,
        is_part_of=[is_part_of,],
    )

def fake_SamplingSites(
        count:int = 1,
        base:int = 0,
        sample_location: typing.Optional[GeospatialCoordLocation] = None,
) -> typing.Generator[SamplingSite, None, None]:
    _fakes = [None, ]
    for i in range(count):
        # randomly select a site that the newly generated one is part of
        is_part_of = _fakes[generator.random_int(0, len(_fakes)-1)]
        entry = fake_SamplingSite(
            base+i,
            sample_location=sample_location,
            is_part_of=is_part_of,
        )
        _fakes.append(entry.pid)
        yield entry


def fake_MaterialSampleCuration(
        i: int,
        responsibility:typing.Optional[typing.List[Agent]] = None
) -> MaterialSampleCuration:
    return MaterialSampleCuration(
        pid=f"cur_{i}",
        responsibility=responsibility,
        label=generator.word(),
        description=generator.text(),
        access_constraints=[generator.word(), ]
    )

def fake_MaterialSampleCurations(
        count:int = 1,
        base:int = 0,
        responsibility:typing.Optional[typing.List[Agent]] = None
)-> typing.Generator[MaterialSampleCuration, None, None]:
    for i in range(count):
        yield fake_MaterialSampleCuration(base+i, responsibility=responsibility)

def fake_SamplingEvent(
        i: int,
        responsibility: typing.Optional[typing.List[Agent]] = None,
        sampling_site: typing.Optional[SamplingSite] = None,
        sample_location: typing.Optional[GeospatialCoordLocation] = None,
        has_context_category: typing.Optional[IdentifiedConcept] = None,
        authorized_by: typing.Optional[typing.List[Agent]] = None,
) -> SamplingEvent:
    hcc = None
    if has_context_category is not None:
        hcc = [has_context_category,]
    resp = None
    if responsibility is not None:
        resp = responsibility
    return SamplingEvent(
        pid=f"event_{i}",
        description=generator.text(),
        authorized_by=authorized_by,
        has_context_category=hcc,
        has_feature_of_interest=generator.word(part_of_speech="noun"),
        label=generator.word(),
        project=generator.word(ext_word_list=fake_project_names),
        responsibility=resp,
        result_time=generator.date_time().isoformat(timespec="seconds"),
        sample_location=sample_location,
        sampling_site=sampling_site,
    )

def fake_SamplingEvents(
        count:int = 1,
        base:int = 0,
        responsibility:typing.Optional[typing.List[Agent]] = None,
        sampling_site:typing.Optional[SamplingSite] = None,
        sample_location: typing.Optional[GeospatialCoordLocation] = None,
        authorized_by: typing.Optional[typing.List[Agent]] = None,
)-> typing.Generator[SamplingEvent, None, None]:
    for i in range(count):
        yield fake_SamplingEvent(
            base+i,
            responsibility=responsibility,
            sampling_site=sampling_site,
            sample_location=sample_location,
            authorized_by=authorized_by,
        )

def fake_MaterialSampleRecord(
        i: int,
        curation: typing.Optional[MaterialSampleCuration] = None,
        has_context_category: typing.List[IdentifiedConcept] = None,
        has_material_category: typing.List[IdentifiedConcept] = None,
        has_sample_object_type: typing.List[IdentifiedConcept] = None,
        keywords: typing.Optional[typing.List[IdentifiedConcept]] = None,
        produced_by: typing.Optional[SamplingEvent] = None,
        registrant: typing.Optional[Agent] = None,
        related_resource: typing.Optional[SampleRelation] = None,
) -> MaterialSampleRecord:
    pid = f"msr_{i}"
    return MaterialSampleRecord(
        pid=pid,
        alternate_identifiers=None,
        complies_with=None,
        curation=curation,
        dc_rights=None,
        description=generator.text(),
        has_context_category=has_context_category,
        has_material_category=has_material_category,
        has_sample_object_type=has_sample_object_type,
        keywords=None if keywords is None else keywords,
        label=generator.word(),
        last_modified_time=generator.date_time(),
        produced_by=produced_by,
        registrant=registrant,
        related_resource=None if related_resource is None else [related_resource,],
        sample_identifier=pid,
        sampling_purpose=generator.sentence(),
    )


def make_fakes(g:pqg.PQG, count:int=5):
    keywords = list(fake_IdentifiedConcepts("keyword", count=count*5))

    context_categories = list(fake_IdentifiedConcepts("context_category", count=count*5))
    material_categories = list(fake_IdentifiedConcepts("material_category", count=count*5))
    sample_object_types = list(fake_IdentifiedConcepts("sample_object_type", count=count*5))

    agents = list(fake_Agents(count=count*5))

    locations = list(fake_GeospatialCoordLocations(count=count))

    sites = list(
        fake_SamplingSites(
            count=count,
            base=0,
            sample_location=locations[generator.random_int(0, len(locations)-1)]
        )
    )

    curations = list(
        fake_MaterialSampleCurations(
            count=count,
            base=0,
            responsibility=list(generator.random_choices(agents, 2)),
        )
    )

    events = list(fake_SamplingEvents(
        count=count,
        base=0,
        responsibility=list(generator.random_choices(agents, 2)),
        sampling_site=sites[generator.random_int(0, len(sites)-1)],
        sample_location=locations[generator.random_int(0, len(locations)-1)],
    ))

    for i in range(count):
        kw = list(generator.random_choices(keywords, 3))
        ms = fake_MaterialSampleRecord(
            i,
            produced_by=events[generator.random_int(min=0, max=len(events) - 1)],
            registrant=agents[generator.random_int(min=0, max=len(agents) - 1)],
            curation=curations[generator.random_int(min=0, max=len(curations) - 1)],
            keywords=kw,
            has_context_category=list(generator.random_choices(context_categories, 2)),
            has_material_category=list(generator.random_choices(material_categories,2)),
            has_sample_object_type=list(generator.random_choices(sample_object_types, 2)),
        )
        g.addNode(ms)


def get_record(g, pid):
    record = g.getNode(pid)
    #print(record)
    print(json.dumps(record, indent=2, cls=pqg.JSONDateTimeEncoder))


def main(dest:str=None):
    dbinstance = duckdb.connect(dest)
    g = createGraph(dbinstance)
    make_fakes(g, count=10)
    get_record(g, "msr_0")
    if dest is not None:
        g.asParquet(pathlib.Path(dest))
    dbinstance.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main("data/test_10.ddb")
    #main()
