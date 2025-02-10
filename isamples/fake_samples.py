"""
Methods for creating fake entries for testing purposes.
"""

import json
import logging
import pathlib
import typing
import pqg
from isamples import *
import faker

# Used for generating random-ish values
generator = faker.Faker()

fake_project_names = [f"project-{generator.word()}" for _ in range(5)]


def fake_Agent(i: int) -> Agent:
    return Agent(
        pid=f"agent_{i}",
        label=generator.name(),
        affiliation=generator.administrative_unit(),
        contact_information=generator.address(),
        role=generator.job()
    )


def fake_GeospatialCoordLocation(i: int) -> GeospatialCoordLocation:
    return GeospatialCoordLocation(
        pid=f"geo_{i}",
        latitude=generator.latitude(),
        longitude=generator.longitude(),
        obfuscated=generator.boolean(0.1),
        elevation=f"{generator.random_int(-500, 10000)}m MSL"
    )


def fake_IdentifiedConcept(domain: str) -> IdentifiedConcept:
    uri = f"https://fake.uri/{domain}/"
    label = generator.word()
    return IdentifiedConcept(
        pid=f"{uri}{label}",
        label=generator.word(),
        scheme_name=domain,
        scheme_uri=uri
    )


def fake_SampleRelation(i: int, target: str) -> SampleRelation:
    return SampleRelation(
        pid=f"rel_{i}",
        target=target,
        description=generator.text(),
        label=generator.word(),
        relationship="subsample"
    )


def fake_SamplingSite(i: int, sample_location: typing.Optional[GeospatialCoordLocation] = None) -> SamplingSite:
    return SamplingSite(
        pid=f"site_{i}",
        description=generator.text(),
        label=generator.word(part_of_speech="noun"),  # noun
        place_name=[generator.city(),],
        sample_location=sample_location
    )


def fake_MaterialSampleCuration(i: int, responsibility: typing.Optional[Agent] = None) -> MaterialSampleCuration:
    return MaterialSampleCuration(
        pid=f"cur_{i}",
        responsibility=None if responsibility is None else [responsibility,],
        label=generator.word(),
        description=generator.text(),
        access_constraints=[generator.word(), ]
    )


def fake_SamplingEvent(
        i: int,
        responsibility: typing.Optional[typing.List[Agent]] = None,
        sampling_site: typing.Optional[SamplingSite] = None,
        sample_location: typing.Optional[GeospatialCoordLocation] = None,
        has_context_category: typing.Optional[IdentifiedConcept] = None,
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
        has_context_category=hcc,
        has_feature_of_interest=generator.word(part_of_speech="noun"),
        label=generator.word(),
        project=generator.word(ext_word_list=fake_project_names),
        responsibility=resp,
        result_time=generator.date_time().isoformat(timespec="seconds"),
        sample_location=sample_location,
        sampling_site=sampling_site,
    )


def fake_MaterialSampleRecord(
        i: int,
        curation: typing.Optional[MaterialSampleCuration] = None,
        has_context_category: typing.Optional[IdentifiedConcept] = None,
        has_material_category: typing.Optional[IdentifiedConcept] = None,
        has_sample_object_type: typing.Optional[IdentifiedConcept] = None,
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
        has_context_category=None if has_context_category is None else [has_context_category,],
        has_material_category=None if has_material_category is None else [has_material_category,],
        has_sample_object_type=None if has_sample_object_type is None else [has_sample_object_type,],
        keywords=None if keywords is None else [keywords,],
        label=generator.word(),
        last_modified_time=generator.date_time(),
        produced_by=produced_by,
        registrant=registrant,
        related_resource=None if related_resource is None else [related_resource,],
        sample_identifier=pid,
        sampling_purpose=generator.sentence(),
    )


def make_fakes(g:pqg.PQG, count:int=5):
    agents = []
    for i in range(5):
        agents.append(fake_Agent(i))

    sites = []
    for i in range(10):
        loc = fake_GeospatialCoordLocation(i)
        sites.append(fake_SamplingSite(i, sample_location=loc))

    curations = []
    for i in range(5):
        curations.append(fake_MaterialSampleCuration(
            i,
            responsibility=agents[generator.random_int(min=0, max=len(agents) - 1)])
        )

    for i in range(count):
        revent = fake_SamplingEvent(
            i,
            responsibility=[
                agents[0],
                agents[generator.random_int(min=1, max=len(agents) - 1)],
                ],
            sampling_site=sites[generator.random_int(min=0, max=len(sites) - 1)],
        )
        ms = fake_MaterialSampleRecord(
            i,
            produced_by=revent,
            registrant=agents[generator.random_int(min=0, max=len(agents) - 1)],
            curation=curations[generator.random_int(min=0, max=len(curations) - 1)],
        )
        g.addNode(ms)


def get_record(g, pid):
    record = g.getNode(pid)
    print(json.dumps(record, indent=2, cls=pqg.JSONDateTimeEncoder))


def main():
    g = createGraph("../data/test_1.ddb")
    make_fakes(g, count=1)
    get_record(g, "msr_0")
    g.asParquet(pathlib.Path("../data/test_1.parquet"))


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    main()