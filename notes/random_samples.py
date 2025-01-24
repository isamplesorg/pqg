'''Generate random samples using faker.
'''

import dataclasses
import datetime
import json
import logging
import pathlib
import time

import faker

import pqg


@dataclasses.dataclass(kw_only=True)
class Agent(pqg.Base):
    affiliation: pqg.OptionalStr = None
    contact: pqg.OptionalStr = None


@dataclasses.dataclass(kw_only=True)
class GeoLocation(pqg.Base):
    longitude: float
    latitude: float
    zoffset: pqg.OptionalFloat = None
    zreference: pqg.OptionalStr = None


@dataclasses.dataclass(kw_only=True)
class SamplingSite(pqg.Base):
    location: GeoLocation = None


@dataclasses.dataclass(kw_only=True)
class SamplingEvent(pqg.Base):
    tstamp: pqg.OptionalDateTime = None
    responsibility: Agent = None
    sampling_site: SamplingSite = None


@dataclasses.dataclass(kw_only=True)
class MaterialSample(pqg.Base):
    registrant: Agent = None
    produced_by: SamplingEvent = None


def main():
    logging.basicConfig(level=logging.INFO)
    L = logging.getLogger()
    graph = pqg.PQG("data/test100k.ddb")
    graph.initialize(classes=[MaterialSample, ])
    gen = faker.Faker()
    agents = []
    locations = []
    sites = []

    def get_agent(n):
        if n >= len(agents):
            agents.append(Agent(
                pid=f"agent_{i}",
                label=gen.name(),
                description=gen.text(),
                affiliation=gen.administrative_unit(),
                contact=gen.address(),
            ))
            return agents[-1]
        return agents[n]

    def get_location(n):
        if n >= len(locations):
            locations.append(GeoLocation(
                pid=f"geo_{i}",
                longitude=float(gen.longitude()),
                latitude=float(gen.latitude()),
                zoffset=gen.random_number(digits=3),
                zreference='MSL'
            ))
            return locations[-1]
        return locations[n]

    def get_site(n):
        if n >= len(sites):
            sites.append(SamplingSite(
                pid=f"site_{i}",
                location=get_location(gen.random_int(min=0, max=len(locations)*10)),
                label=gen.word(),
                description=gen.text()
            ))
            return sites[-1]
        return sites[n]

    n_samples = 100000
    t0 = time.time()
    for i in range(n_samples):
        if i % 100 == 1:
            L.info("agents: %s locations: %s sites: %s", len(agents), len(locations), len(sites))
            L.info("generate sample n=%s", i)
            t1 = time.time()
            L.info("rate = %.2f samples / sec",i / (t1-t0))
        revent = SamplingEvent(
            pid=f"event_{i}",
            tstamp=gen.date_time(tzinfo=datetime.timezone.utc),
            responsibility=get_agent(gen.random_int(min=0, max=len(agents))),
            sampling_site=get_site(gen.random_int(min=0, max=len(sites)))
        )
        ms = MaterialSample(
            pid=f"sample_{i}",
            label=f"Fake Sample {i}",
            description=gen.text(),
            registrant = get_agent(gen.random_int(min=0, max=len(agents))),
            produced_by = revent
        )
        graph.addNode(ms)
    t1 = time.time()
    L.info("Elapsed = %s", t1 - t0)
    L.info("Rate = %s", n_samples / (t1-t0))
    L.info("Storing to parquet...")
    base_name = pathlib.Path("./notes/data/test100k")
    graph.toParquet(base_name)

    #n = graph.getNode('sample_100')
    #print(json.dumps(n, indent=2, cls=pqg.JSONDateTimeEncoder))

    L.info("Done.")

if __name__ == "__main__":
    main()