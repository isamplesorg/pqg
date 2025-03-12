#from pqg import __version__
import datetime

from __init__ import *
from pqg.common import *
from pqg.pqg_singletable import *
#from line_profiler import profile

LOGGER = logging.getLogger('insertlist')

def get_testrecord()->MaterialSampleRecord:
    testrecord = MaterialSampleRecord(pid='sam.6', label='SAV-1 2F15-06',
                last_modified_time=datetime.datetime(2013, 9, 23, 14, 43, 1,
                                    tzinfo=datetime.timezone(datetime.timedelta(days=-1, seconds=61200))),
                description='Rock; . Material: Rock; Igneous>Volcanic>Mafic; . Age: blank; bounds: 5.03 - 5.11 million years (Ma). Age units million years (Ma)',
                sample_identifier='10.58052/KOP000006', alternate_identifiers=['Alia 115-03'],
                produced_by=SamplingEvent(pid='urn:local:evt.6',
                    label='Collection of sample SAV-1 2F15-06',
                    description='method:Chain bag dredge',
                    has_feature_of_interest=None,
                    has_context_category='Seamount',
                    project=None,
                    responsibility=[Agent(name='Hart, Stanley',
                          affiliation=None,
                          contact_information='shart@whoi.edu',
                          pid='urn:local:ind.35',
                          role='', label=None,
                          description=None, altids=['ind.35'], s=None, p=None, o=None, n=None)],
                    result_time=None,
                    sampling_site=SamplingSite(pid='urn:local:sst.6',
                            description="Savai''i Island. seamount. Deep submarine flanks of Savai''i Island, Samoan Seamount Trail, Pacific Plate, Pacific Ocean. , Samoa.  Sampling elevation is -45.0, datum not reported.", label="Savai''i Island",
                            site_location=None, place_name=['Samoa'],
                            is_part_of=None, altids=None, s=None, p=None, o=None, n=None),
                    authorized_by=None,
                    sample_location=GeospatialCoordLocation(elevation='-45.0000',
                            latitude=-14.092000,
                            longitude=172.941000,
                            obfuscated=False,
                            pid='urn:local:geo.6',
                            label=None,
                            description=None,
                            altids=None, s=None, p=None, o=None, n=None), altids=None, s=None, p=None, o=None, n=None),
                sampling_purpose=None,
                has_context_category=IdentifiedConcept(pid='sessf:seamount', label='Seamount',
                            scheme_name='SESAR Sampled Feature Type',
                            scheme_uri='sessf:sfvocabulary',
                            description=None, altids=['sft.97'], s=None, p=None, o=None, n=None),
                has_material_category=IdentifiedConcept(pid='https://w3id.org/isample/vocabulary/material/rockorsediment',
                            label='Rock or non-consolidated Earth material',
                            scheme_name='SESAR Material Type',
                            scheme_uri='https://w3id.org/isample/vocabulary/material/materialsvocabulary',
                            description=None, altids=['mat.1'], s=None, p=None, o=None, n=None),
                has_sample_object_type=IdentifiedConcept(pid='https://w3id.org/isample/vocabulary/materialsampleobjecttype/materialsample', label='Material Sample', scheme_name='SESAR Sample Type', scheme_uri='https://w3id.org/sesar/objecttype/objecttypevocabulary', description=None, altids=['sat.152'], s=None, p=None, o=None, n=None), keywords=None, related_resource=None, complies_with=None, dc_rights=None, curation=MaterialSampleCuration(pid='urn:local:cur.6', access_constraints=[], curation_location=None, description='current owner', label='Koppers, Anthony',
                responsibility=Agent(name='Koppers, Anthony',
                            affiliation=None,
                            contact_information='akoppers@coas.oregonstate.edu',
                            pid='urn:local:ind.1', role='',
                            label=None, description=None, altids=['ind.1'], s=None, p=None, o=None, n=None),
                            altids=None, s=None, p=None, o=None, n=None),
                registrant=Agent(name='Koppers, Anthony', affiliation=None,
                            contact_information='akoppers@coas.oregonstate.edu',
                            pid='urn:local:ind.1', role='',
                            label=None, description=None, altids=['ind.1'], s=None, p=None, o=None, n=None),
                altids=None, s=None, p=None, o=None, n=None)
    return testrecord

#@profile
def addNodeToList(g, o:pqg.common.IsDataclass,insertDict:dict) -> str:
    '''
    insert dict structure:
    {
    tableName:
        tableFields:({', '.join(_names)})
        valuesList:[
            ({', '.join(['?',]*len(_values))} (with lat long if applicable) ),
            (...),
            (...)
        ]
    }
    Process-- for each add node, first see if tableName is in dict
        if not-- create new dict entry and add tableFields and empty valuesList
                then append new values tuple to valuesList
        if exists
            then append new values tuple to valuesList

    for deferred, which are links to other objects for field values, have to create edge,
        and possibly add node if target is curation or samplingEvent.
    '''
    _L = logging.getLogger("insertDict")
    deferred = []
    otype = o.__class__.__name__
    data = {}
    for field in dataclasses.fields(o):
        _v = getattr(o, field.name)
        if is_dataclass_or_dataclasslist(_v) or str(type(_v)) in g._types:
            deferred.append(field.name)
        else:
            if field.name in g._literal_field_names:
                data[field.name] = _v
    s_pid = addNodeEntryList(g, otype, data,insertDict)
    _L.debug("Added node pid= %s", s_pid)
    for field_name in deferred:
        _v = getattr(o, field_name)
        if isinstance(_v, list):
            for element in _v:
                #o_pid = self._addNode(element)
                o_pid = addNodeToList(g,element,insertDict)
                _edge = Edge(pid=None, s=s_pid, p=field_name, o=o_pid)
                _L.debug("Created edge: %s", _edge)
                addEdgeToList(g,_edge,insertDict)
        else:
            if _v is not None:
                o_pid = addNodeToList(g,_v,insertDict)
                _edge = Edge(pid=None, s=s_pid, p=field_name, o=o_pid)
                _L.debug("Created edge: %s", _edge)
                addEdgeToList(g,_edge,insertDict)

    return s_pid

#@profile
def addNodeEntryList(g, otype:str, data:typing.Dict[str, typing.Any], insertDict:dict) -> str:
        """
        Add a new entry in values list for  the node table or update a row if it already exists.
        """
        _L = getLogger()
        try:
            pid:str = data[g._node_pk]
        except KeyError:
            raise ValueError("pid cannot be None")

        #ne = g.nodeExists(pid)      #check if pid is already in database
        ne = None       # skip the exists check, use INSERT or REPLACE in the sql to handl existing.
        #check if pid is already in insert dict


        if ne is not None:
            # update the existing entry
            return pid
        else:
            try:
                pidindex = insertDict[otype]['fieldList'].split(", ").index('pid')
                pidlist = list(set(sublist[pidindex] for sublist in insertDict[otype]['valuesList']))
                if pid in pidlist:
                    return pid
            except:
                pass    # no object type entry in insertDict yet, process the node

            # create a new entry
            try:
                _names = ["otype", ]
                _values = [otype, ]
                lat_lon = {"x":None, "y":None,}
                #TODO: Handling of geometry is pretty rough. This should really be something from the object level
                # rather that kluging stuff together here.
                _L.debug(f"addNodeEntry called with {repr(data)}")

                for k,v in data.items():
                    if k not in _names:
                        _names.append(k)
                    if isinstance(v, datetime.datetime):
                        # format date as string
                        v =  f'{v:%Y-%m-%d %H:%M:%S %z}'
                    # elif isinstance(v,list):
                    #     v = v  # have to pass list of None
                    # elif v is None:
                    #     v = 'None'
                    _values.append(v)
                    if k == g.geometry_x_field:
                        lat_lon["x"] = v
                    elif k == g.geometry_y_field:
                        lat_lon['y'] = v

                if lat_lon['x'] is not None and lat_lon['y'] is not None:
                    _names.append("geometry")
                #     #thegeom = f"ST_POINT({str(lat_lon['x'])}, {str(lat_lon['y'])})"
                #     #_values.append(thegeom)
                    _values.append(lat_lon['x'])
                    _values.append(lat_lon['y'])

                valuestuple = tuple(_values)

                try:
                    insertDict[otype]['valuesList'].append(valuestuple)
                except:
                    insertDict[otype] = {}
                    insertDict[otype]['fieldList'] = ', '.join(_names)
                    insertDict[otype]['valuesList'] = []
                    insertDict[otype]['valuesList'].append(valuestuple)
                #except Exception as e:
                #    _L.debug(f"insertDict fail, exception {repr(e)}")


                _L.debug("add InsertDict Entry %s", insertDict[otype])

            except Exception as e:
                _L.info("addNodeEntry fail %s", e)
                pass
        return pid


#@profile
def addEdgeToList(g, edge: Edge, insertDict) -> str:
        """Adds an edge.

        Note that edges may exist independently of nodes, e.g. to make an assertion
        between external entities.
        """
        _L = getLogger()
        _L.debug("addEdge: %s", edge.pid)
        # existing = None  # smr 2025-03-12 the sql queries here slow it all down
                            # so use INSERT or UPDATE in SQL to handle ones that are already there
        # if edge.pid is None:
        #     existing = getEdge(g=g, s=edge.s, p=edge.p, o=edge.o, n=edge.n)
        # else:
        #     existing = getEdge(g=g, pid=edge.pid)
        # if existing is not None:
        #     return existing.pid

        try:
            pidindex = insertDict['_edge_']['fieldList'].split(", ").index('pid')
            pidlist = list(set(sublist[pidindex] for sublist in insertDict['_edge_']['valuesList']))
            if edge.pid in pidlist:  # don't add to insert list if we already have this one one the list
                return edge.pid
        except:
            pass

        vlist = [edge.pid, '_edge_', edge.s,  edge.p,   edge.o,  edge.n,   edge.altids, None]
        valuestuple = tuple(vlist )
        otype = '_edge_'

        try:
            insertDict[otype]['valuesList'].append(valuestuple)
        except:
            insertDict[otype] = {}
            fieldList = ', '.join(g._edgefields)  #'pid, otype, s, p, o, n, altids, geometry'
            insertDict[otype]['fieldList'] = fieldList
            insertDict[otype]['valuesList'] = []
            insertDict[otype]['valuesList'].append(valuestuple)
        # except Exception as e:

        return edge.pid


def getEdge( g,
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
    #edgefields = ('pid', 's', 'p', 'o', 'n', 'altids')
    sql = f"SELECT {', '.join(g._edgefields)} FROM {g._table} WHERE otype='_edge_' AND"
    if pid is not None:
        sql += " pid = ?"
        qproperties = (pid,)
    else:
        sql += " s=? AND p=? AND o=? AND n=?"
        qproperties = (s, p, o, n)
    with g.getCursor() as csr:
        _L.debug("getEdge sql: %s", sql)
        csr.execute(sql, qproperties)
        values = csr.fetchone()
        if values is None:
            return None
        data = dict(zip(g._edgefields, values))
        return Edge(**data)


#@profile
def writeduckdb(g, insertDict:dict):
    for key in sorted(insertDict.keys()):   # do edges last
        thefields = insertDict[key]['fieldList']
        thevalues = insertDict[key]['valuesList']

        with g.getCursor() as csr:
            try:
                thesql = f"INSERT OR IGNORE INTO {g._table} ({thefields}) VALUES ({', '.join(['?',]*len(thevalues[0]))})"
                if key != '_edge_' and 'geometry' in thefields:
                        thesql = thesql[:-7] + ", ST_POINT(?,?) )"
                csr.executemany(thesql,thevalues)
            except Exception as e:
                LOGGER.info(f"writeduckdb fail, e: {repr(e)}")
    g._connection.commit()

def main(dest: str = None):

    tstart_time = time.time()
    theddb = 'sesarduck2.ddb'
    dbinstance = duckdb.connect(theddb)
    g = createGraph(dbinstance)

    # g.addNode(ms)
    ms = get_testrecord()
    insertDict = {}
    addNodeToList(g, ms, insertDict)

    #dest = 'sesarTest2'
    dest = None
    if dest is not None:
        g.asParquet(pathlib.Path(dest))

    tend_time = time.time()
    execution_time = tend_time - tstart_time
    LOGGER.info(f'total run time: {execution_time / 3600} hours')
    dbinstance.close()

if __name__ == "__main__":
    #    logging.basicConfig(level=logging.DEBUG)
    logging.basicConfig(filename='pqg.log', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')

    # main("data/test_0.ddb")
    main()