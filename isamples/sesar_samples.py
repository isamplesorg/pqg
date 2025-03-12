"""
load data from SESAR 2025 database into duckDb database
save as Parquet file.
based on Code by Dave Vieglais
Modified for SESAR by SM Richard 2025-02-28
"""

# import json
# import pathlib
# import typing
# import pqg
# from isamples import *
# import time
import psycopg2
# import logging
# import duckdb
import sys
sys.path.append('C:/Users/smrTu/OneDrive/Documents/GithubC/iSamples/pqg/')
from load_insert_lists import *
#from line_profiler import profile
import pickle

LOGGER = logging.getLogger('sesarParquet')
SESAR_USER_LKUP = {}
INIT_LKUP = {}
COLLECTOR_LKUP = {}
ARCHIVE_LKUP = {}
LOCALITY_LKUP = {}


# connect to database
def get_2025Connection() -> psycopg2.extensions.connection | None:
    try:
        return psycopg2.connect(
            database="SESAR2025",
            user="postgres",
            password="smrpostgis",
            host="127.0.0.1",
            port=5432,
        )
    except Exception as e:
        LOGGER.info(f'2025db connection problem {repr(e)}')
        return None


newDb = get_2025Connection()
TABLES = ['affiliation',
          'affiliation_type',
          'agent_role_type',
          'collection_member',
          'collection_type',
          'country',
          'geologic_time_scale',
          'geospatial_location',
          'group_member',
          'individual',
          'initiative',
          'initiative_type',
          'institution',
          'institution_type',
          'launch_type',
          'locality',
          'location_method',
          'material_role_type',
          'material_type',
          'other_property',
          'parent_institution',
          'permission',
          'platform',
          'platform_type',
          'property_type',
          'public.group',
          'related_local_doc',
          'related_resource',
          'related_sample_agent',
          'relation_type',
          'resource_type',
          'sample',
          'sample_additional_name',
          'sample_collection',
          'sample_doc',
          'sample_material',
          'sample_publication_url',
          'sample_type',
          'sampled_feature_type',  # populate has_context_category
          'sampling_method',
          'sesar_spatial_ref_sys',
          'sesar_user'
          ]


def executeQuery(conn, querystring):
    # CREATE A CURSOR USING THE CONNECTION OBJECT
    curr = conn.cursor()
    # EXECUTE THE SQL QUERY
    curr.execute(querystring)
    # FETCH ALL THE ROWS FROM THE CURSOR
    data = curr.fetchall()
    curr.close()
    return data


def getFields(conn, tableName):
    #  get field names for table
    fieldsquery = "SELECT column_name,ordinal_position " \
                  "FROM information_schema.columns " + \
                  "WHERE table_schema = 'public' AND table_name = '" + \
                  tableName + "' ORDER BY ordinal_position"
    LOGGER.debug(f'getFields query: {fieldsquery}')
    fields = executeQuery(conn, fieldsquery)

    fieldlist = []
    for row in fields:
        fieldlist.append(row[0])
    return fieldlist


def load_concept_lkup(args: list, concept_lkup):
    start_time = time.time()  # time the function execution
    # args list order:
    # tableName,abbrev,urifield,labelfield,schemename,schemeurifield,idfield
    tableName = args[0]
    abbrev = args[1]
    selectRecordQuery = 'SELECT * from ' + tableName
    LOGGER.debug(f"get {tableName} record query: {repr(selectRecordQuery)}")
    try:
        thedata = executeQuery(newDb, selectRecordQuery)
    except Exception as e:
        LOGGER.info(f'{tableName} data query failed. {e}')
    thefields = getFields(newDb, tableName)
    for row in thedata:
        theobj = {}
        for nc in range(len(row)):
            if row[nc]:
                theobj[thefields[nc]] = row[nc]
            else:
                theobj[thefields[nc]] = None

        if args[2] and theobj[args[2]]:
            thepid = theobj[args[2]]
        else:
            thepid = 'urn:local:' + f"{abbrev}.{str(theobj[args[6]])}"

        if args[5]:
            theschemeuri = theobj[args[5]]
        else:
            theschemeuri = ''

        theid = f"{abbrev}.{str(theobj[args[6]])}"

        theConcept = IdentifiedConcept(
            pid=thepid,
            label=theobj[args[3]],
            scheme_name=args[4],
            scheme_uri=theschemeuri,
            altids=[theid]
        )

        concept_lkup[theid] = theConcept

    end_time = time.time()
    execution_time = end_time - start_time
    LOGGER.info(f"load vocabs {tableName} execution time: {execution_time} seconds")
    return 1


def load_material_type(concept_lkup):
    start_time = time.time()  # time the function execution
    tableName = 'material_type'
    abbrev = 'mat'
    fieldlist = ['material_type.material_type_id', 'material_type.label',
                 'scheme_uri', 'material_type_uri']
    qfields = ','.join(fieldlist)
    selectRecordQuery = 'SELECT ' + qfields + \
                        ' FROM material_type join sample on material_type_id = general_material_type_id ' + \
                        ' union SELECT ' + qfields + \
                        ' FROM material_type join sample_material on material_type.material_type_id = ' + \
                        ' sample_material.material_type_id;'

    LOGGER.debug(f"get_sample_data record query: {repr(selectRecordQuery)}")
    try:
        mat_data = executeQuery(newDb, selectRecordQuery)
    except Exception as e:
        LOGGER.info(f'get {tableName} data query failed, error: {repr(e)}')
        return None
    #   thefields = getFields(newDb, tableName)
    for row in mat_data:
        theobj = {}
        for nc in range(len(row)):
            if row[nc] is None:
                theobj[fieldlist[nc]] = 'blank'
                # replace null values with 'blank'
            else:
                theobj[fieldlist[nc]] = row[nc]
        theConcept = IdentifiedConcept(
            pid=theobj['material_type_uri'],
            #        pid=f"{tableName[4:]}.{str(id)}",
            label=theobj['material_type.label'],
            scheme_name="SESAR Material Type",
            scheme_uri=theobj['scheme_uri'],
            altids=[f"{abbrev}.{str(theobj['material_type.material_type_id'])}"]
        )
#        g.addNode(theConcept)
        concept_lkup[abbrev + '.' + str(theobj['material_type.material_type_id'])] = theConcept
    end_time = time.time()
    execution_time = end_time - start_time
    LOGGER.info(f"load material_types execution time: {execution_time} seconds")
    return 1


def load_individuals(agent_lkup):
    start_time = time.time()  # time the function execution
    # individuals are loaded into Agent nodes
    tableName = 'individual'
    abbrev = 'ind'
    selectRecordQuery = 'SELECT * FROM public.' + tableName
    LOGGER.debug(f"individuals record query: {repr(selectRecordQuery)}")
    try:
        indiv_data = executeQuery(newDb, selectRecordQuery)
    except Exception as e:
        LOGGER.info(f'load {tableName} data query failed, with error: {repr(e)}')
        return None
    thefields = getFields(newDb, tableName)
    for row in indiv_data:
        theobj = {}
        for nc in range(len(row)):
            if row[nc]:
                theobj[thefields[nc]] = row[nc]
            else:
                theobj[thefields[nc]] = None

        if theobj['label'] is not None:
            thelabel = theobj['label']
        elif theobj['individual_uri'] is not None:
            thelabel = theobj['individual_uri']
        elif theobj['description'] is not None:
            thelabel = theobj['description']
        elif theobj['email'] is not None:
            thelabel = theobj['email']
        else:
            continue  # no label, skip

        if theobj['individual_uri'] is not None:
            thepid = theobj['individual_uri']
        else:
            thepid = 'urn:local:' + f"{abbrev}.{str(theobj['individual_id'])}"

        contactinfo = ''
        if theobj['address'] is not None:
            contactinfo = theobj['address']
        if theobj['email'] is not None:
            if len(contactinfo) > 0:
                contactinfo += ", "
            contactinfo += theobj['email']
        if theobj['phone'] is not None:
            if len(contactinfo) > 0:
                contactinfo += ", Phone:"
            contactinfo += theobj['phone']

        theagent = Agent(
            pid=thepid,
            affiliation=None,
            contact_information=contactinfo,
            name=thelabel,
            role='',
            altids=[f"{abbrev}.{str(theobj['individual_id'])}"]
        )
#        g.addNode(theagent)
        agent_lkup[abbrev + '.' + str(theobj['individual_id'])] = theagent
    end_time = time.time()
    execution_time = end_time - start_time
    LOGGER.info(f"load individualas in agent_lkup execution time: {execution_time} seconds")
    return 1


def load_related_resource_lkup():
    start_time = time.time()  # time the function execution
    # related resource are loaded into Sample_Relation nodes
    tableName = 'related_resource'
    abbrev = 'rel'
    selectRecordQuery = 'SELECT * FROM public.' + tableName
    LOGGER.debug(f"{tableName} record query: {repr(selectRecordQuery)}")
    try:
        idata = executeQuery(newDb, selectRecordQuery)
    except Exception as e:
        LOGGER.info('get_sample_data data query failed with error: %s', e)
        return None
    thefields = getFields(newDb, tableName)
    rel_lookup = {}  # dictionary in which key is sample_id, value is list of
    #  sample relation objects with links for whic the sample
    #  is the subject
    for row in idata:
        theobj = {}
        for nc in range(len(row)):
            if row[nc]:
                theobj[thefields[nc]] = row[nc]
            else:
                theobj[thefields[nc]] = None
        if theobj['sample_id'] is not None:  # skip if no relationships from the sample
            try:
                thesamplerel = SampleRelation(
                    pid=f"rel.{str(theobj['relation_id'])}",
                    target=theobj['related_resource_uri'],
                    description='',
                    label="child of " + str(theobj['related_resource_uri']),
                    relationship='has parent material sample'
                )
                try:
                    rel_lookup[str(theobj['sample_id'])].append(thesamplerel)
                except:
                    rel_lookup[str(theobj['sample_id'])] = []
                    rel_lookup[str(theobj['sample_id'])].append(thesamplerel)
            except Exception as e:
                LOGGER.debug(f'get SampleRelation error: {e}')
    end_time = time.time()
    execution_time = end_time - start_time
    LOGGER.info(f"load related resources lkup execution time: {execution_time} seconds")
    return rel_lookup


def load_additional_name_lkup():
    start_time = time.time()  # time the function execution
    # related resource are loaded into Sample_Relation nodes
    tableName = 'sample_additional_name'
    abbrev = 'san'
    selectRecordQuery = 'SELECT * FROM public.' + tableName
    LOGGER.debug(f"{tableName} record query: {repr(selectRecordQuery)}")
    try:
        idata = executeQuery(newDb, selectRecordQuery)
    except:
        LOGGER.info(f'load {tableName} data query failed')
        return None
    thefields = getFields(newDb, tableName)
    add_name_lookup = {}  # dictionary in which key is sample_id, value is list of
    #  additional name strings
    for row in idata:
        theobj = {}
        for nc in range(len(row)):
            if row[nc]:
                theobj[thefields[nc]] = row[nc]
            else:
                theobj[thefields[nc]] = None
        if theobj['sample_id'] is not None:
            try:
                add_name_lookup[str(theobj['sample_id'])].append(theobj['name'])
            except:
                add_name_lookup[str(theobj['sample_id'])] = []
                add_name_lookup[str(theobj['sample_id'])].append(theobj['name'])
    end_time = time.time()
    execution_time = end_time - start_time
    LOGGER.info(f"load additional name execution time: {execution_time} seconds")
    return add_name_lookup


def load_locality_lkup():
    start_time = time.time()  # time the function execution
    tableName = 'locality'
    abbrev = 'loc'
    selectRecordQuery = 'SELECT * FROM public.' + tableName
    LOGGER.debug(f"{tableName} record query: {repr(selectRecordQuery)}")
    try:
        idata = executeQuery(newDb, selectRecordQuery)
    except:
        LOGGER.info(f'{tableName} data query failed')
        return None
    thefields = getFields(newDb, tableName)
    locality_lookup = {}
    for row in idata:
        theobj = {}
        for nc in range(len(row)):
            if row[nc]:
                theobj[thefields[nc]] = row[nc]
            else:
                theobj[thefields[nc]] = None
        locality_lookup[abbrev + '.' + str(theobj['locality_id'])] = theobj
    end_time = time.time()
    execution_time = end_time - start_time
    LOGGER.info(f"load locality_lkup execution time: {execution_time} seconds")
    return locality_lookup


def load_institution(agent_lkup):
    start_time = time.time()  # time the function execution
    # individuals are loaded into Agent nodes
    tableName = 'institution'
    abbrev = 'ins'
    selectRecordQuery = 'SELECT * FROM public.' + tableName
    LOGGER.debug(f"{tableName} record query: {repr(selectRecordQuery)}")
    try:
        idata = executeQuery(newDb, selectRecordQuery)
    except:
        LOGGER.info(f'{tableName} data query failed')
        return None
    thefields = getFields(newDb, tableName)
    for row in idata:
        theobj = {}
        for nc in range(len(row)):
            if row[nc]:
                theobj[thefields[nc]] = row[nc]
            else:
                theobj[thefields[nc]] = None

        if theobj['label'] is not None:
            thelabel = theobj['label']
        elif theobj['description'] is not None:
            thelabel = theobj['description']
        elif theobj['email'] is not None:
            thelabel = theobj['email']
        else:
            continue  # no label, skip

        thepid = 'urn:local:' + f"{abbrev}.{str(theobj['institution_id'])}"

        contactinfo = ''
        if theobj['address'] is not None:
            contactinfo = theobj['address']
        if theobj['email'] is not None:
            if len(contactinfo) > 0:
                contactinfo += ", "
            contactinfo += theobj['email']

        theagent = Agent(
            pid=thepid,
            affiliation=None,
            contact_information=contactinfo,
            name=thelabel,
            role='',
            altids=[f"{abbrev}.{str(theobj['institution_id'])}"]
        )
#        g.addNode(theagent)
        agent_lkup[abbrev + '.' + str(theobj['institution_id'])] = theagent
    end_time = time.time()
    execution_time = end_time - start_time
    LOGGER.info(f"load institution to Agent execution time: {execution_time} seconds")
    return 1


def load_collector_lkup() -> dict | None:
    start_time = time.time()  # time the function execution
    # individuals are loaded into Agent nodes
    tableName = 'related_sample_agent'
    selectRecordQuery = "SELECT * FROM related_sample_agent where " + \
                        " relation_type_id = 1 ORDER BY sample_id"
    LOGGER.debug(f"related_sample_agent collector record query: {repr(selectRecordQuery)}")
    try:
        idata = executeQuery(newDb, selectRecordQuery)
    except:
        LOGGER.info(f'related_sample_agent collector data query failed')
        return None
    collectorlkup = {}
    thefields = getFields(newDb, tableName)
    for row in idata:
        theobj = {}
        for nc in range(len(row)):
            if row[nc]:
                theobj[thefields[nc]] = row[nc]
            else:
                theobj[thefields[nc]] = None
        thekey = "sam." + str(theobj['sample_id'])
        theval = ''
        if theobj['agent_type'] == 'Individual':
            theval = 'ind.' + str(theobj['individual_id'])
        if theobj['agent_type'] == 'Institution':
            theval = 'ins.' + str(theobj['institution_id'])
        try:
            collectorlkup[thekey].append(theval)
        except:
            collectorlkup[thekey] = []
            collectorlkup[thekey].append(theval)
    end_time = time.time()
    execution_time = end_time - start_time
    LOGGER.info(f"load collector_lkup execution time: {execution_time} seconds")
    return collectorlkup


def load_archive_lkup() -> dict | None:
    start_time = time.time()  # time the function execution
    tableName = 'related_sample_agent'
    selectRecordQuery = "SELECT * FROM related_sample_agent where " + \
                        " relation_type_id = 3 ORDER BY sample_id"
    LOGGER.debug(f"related_sample_agent archive record query: {repr(selectRecordQuery)}")
    try:
        idata = executeQuery(newDb, selectRecordQuery)
    except:
        LOGGER.info(f'related_sample_agent archive data query failed')
        return None
    archivelkup = {}
    thefields = getFields(newDb, tableName)
    for row in idata:
        theobj = {}
        for nc in range(len(row)):
            if row[nc]:
                theobj[thefields[nc]] = row[nc]
            else:
                theobj[thefields[nc]] = None
        thekey = 'sam.' + str(theobj['sample_id'])
        theind = None
        theins = None
        if theobj['agent_type'] == 'Individual':
            theind = 'ind.' + str(theobj['individual_id'])
        if theobj['agent_type'] == 'Institution':
            theins = 'ins.' + str(theobj['institution_id'])
        archivelkup[thekey] = [theind, theins]
    end_time = time.time()
    execution_time = end_time - start_time
    LOGGER.info(f"load archive_lkup execution time: {execution_time} seconds")
    return archivelkup


def load_initiative_lkup():
    start_time = time.time()  # time the function execution
    tableName = 'initiative'
    abbrev = 'ini'
    selectRecordQuery = 'SELECT * FROM public.' + tableName
    LOGGER.debug(f"{tableName} record query: {repr(selectRecordQuery)}")
    try:
        idata = executeQuery(newDb, selectRecordQuery)
    except:
        LOGGER.info(f'{tableName} data query failed')
        return None
    initiativelkup = {}
    thefields = getFields(newDb, tableName)
    for row in idata:
        theobj = {}
        for nc in range(len(row)):
            if row[nc]:
                theobj[thefields[nc]] = row[nc]
            else:
                theobj[thefields[nc]] = None
        thelabel = ''
        if theobj['label'] is not None:
            thelabel = theobj['label']
        if len(thelabel) > 0:
            thelabel += '; '
        if theobj['initiative_uri'] is not None:
            thelabel += 'URI--' + theobj['initiative_uri'] + '. '
        if theobj['description'] is not None:
            thelabel += theobj['description'] + '. '
        if theobj['funding'] is not None:
            thelabel += 'Funding--' + theobj['initiative_uri']

        thepid = 'urn:local:' + f"{abbrev}.{str(theobj['initiative_id'])}"
        initiativelkup[thepid] = thelabel
    end_time = time.time()
    execution_time = end_time - start_time
    LOGGER.info(f"load initiative lkup execution time: {execution_time} seconds")
    return initiativelkup


def load_sesar_user_lkup() -> dict | None:
    tableName = 'sesar_user'
    selectRecordQuery = 'SELECT * FROM public.' + tableName
    LOGGER.debug(f"{tableName} record query: {repr(selectRecordQuery)}")
    try:
        sdata = executeQuery(newDb, selectRecordQuery)
    except Exception as e:
        LOGGER.info(f'{tableName} data query failed, error: {repr(e)}')
        return None
    thefields = getFields(newDb, tableName)
    sesaruserlkup = {}
    for row in sdata:
        theobj = {}
        for nc in range(len(row)):
            if row[nc]:
                theobj[thefields[nc]] = row[nc]
            else:
                theobj[thefields[nc]] = None
        if theobj['individual_id'] is not None:
            sesaruserlkup[str(theobj['sesar_user'])] = 'ind.' + str(theobj['individual_id'])
        elif theobj['institution_id'] is not None:
            sesaruserlkup[str(theobj['sesar_user'])] = 'ind.' + str(theobj['institution_id'])
    return sesaruserlkup


def get_GeospatialCoordLocation(theobj, concept_lkup) -> GeospatialCoordLocation | None:
    lat = theobj['latitude']
    long = theobj['longitude']
    elev = theobj['elevation']
    depth_min = theobj['depth_min']
    depth_max = theobj['depth_max']

    verticalpos = ''
    if (lat is None and long is None and elev is None and
            depth_min is None and depth_max is None):
        return None
    else:
        if ((elev is not None) or (depth_min is not None)
                or (depth_max is not None)):
            verticalpos = ""
            if elev:
                verticalpos = verticalpos + str(elev)
            if theobj['elevation_uom'] is not None:
                verticalpos = verticalpos + " UOM: " + theobj['elevation_uom']

            if depth_min is not None or depth_max is not None:
                if depth_min == depth_max:
                    verticalpos = " Depth: " + str(depth_min)
                elif depth_min and depth_max:
                    verticalpos = verticalpos + "Depth range: " + str(depth_min) + " to " + str(depth_max)
                elif depth_min:
                    verticalpos = verticalpos + "Depth minimum: " + str(depth_min)
                elif depth_max:
                    verticalpos = verticalpos + "Depth maximum: " + str(depth_min)
            if theobj['depth_uom'] is not None:
                verticalpos = verticalpos + " UOM: " + theobj['depth_uom']
            if theobj['depth_spatial_ref_id'] is not None:
                #                theconcept = get_altid_identifiedconcept(g, 'ssr.' + str(theobj['depth_spatial_ref_id']))
                try:
                    theconcept = concept_lkup['ssr.' + str(theobj['depth_spatial_ref_id'])]
                    verticalpos = verticalpos + " Datum: " + theconcept['label']
                except Exception as e:
                    verticalpos = verticalpos + " Datum: " + str(theobj['depth_spatial_ref_id'])

        return GeospatialCoordLocation(
            pid=f"urn:local:geo.{str(theobj['sample_id'])}",
            latitude=lat,
            longitude=long,
            # obfuscated=generator.boolean(0.1),
            elevation=verticalpos
        )


def get_altid_identifiedconcept(g, idin: str) -> IdentifiedConcept | None:
    try:
        with g.getCursor() as crsr:
            result = crsr.execute("select pid from node where '" + idin + "' in altids")
            apid = result.fetchone()
            theconcept = g.getNodeEntry(pid=apid[0])
    except Exception as e:
        LOGGER.debug(f'get identified concept error: {e}')
        return None
    return theconcept


def get_pid_by_altid(g, idin: str) -> str | None:
    try:
        with g.getCursor() as crsr:
            result = crsr.execute("select pid from node where '" + idin + "' in altids")
            apid = result.fetchone()
    except Exception as e:
        LOGGER.debug(f'get identified concept error: {e}')
        return None
    return str(apid[0])


def get_Agent(g, theid: str) -> Agent | None:
    try:
        thepid = get_pid_by_altid(g, theid)
        theagent = g.getNodeEntry(pid=thepid)
    except Exception as e:
        LOGGER.debug(f'get agent error: {e}')
        return None
    return theagent


def get_SamplingEvent(g, theobj, COLLECTOR_LKUP, LOCALITY_LKUP, INIT_LKUP, concept_lkup, agent_lkup) -> SamplingEvent:
    # description
    start_time = time.time()  # time the function execution
    collectionDesc = ''
    if theobj['collection_method_id'] is not None:
        #        theconcept = get_altid_identifiedconcept(g, 'sam.' + str(theobj['collection_method_id']))
        theconcept = concept_lkup['sam.' + str(theobj['collection_method_id'])]
        #        test = concept_lkup['sam.' + str(theobj['collection_method_id'])]
        collectionDesc = collectionDesc + 'method:' + theconcept['label']
    if theobj['collection_method_detail'] is not None:
        if len(collectionDesc) > 0:
            collectionDesc += ', '
        collectionDesc += theobj['collection_method_detail'] + '. '
    if theobj['platform_id'] is not None:
        #        theconcept = get_altid_identifiedconcept(g, 'pla.' + str(theobj['platform_id']))
        theconcept = concept_lkup['pla.' + str(theobj['platform_id'])]
        #        test = concept_lkup['pla.' + str(theobj['platform_id'])]
        if theconcept:
            collectionDesc = collectionDesc + ' Platform: ' + theconcept['label']
    if theobj['launch_platform_id'] is not None:
        #        theconcept = get_altid_identifiedconcept(g, 'pla.' + str(theobj['launch_platform_id']))
        theconcept = concept_lkup['pla.' + str(theobj['launch_platform_id'])]
        if theconcept:
            collectionDesc = collectionDesc + ' Launch Platform: ' + theconcept['label']
    if theobj['launch_label'] is not None:
        collectionDesc = collectionDesc + ' Launch: ' + theobj['launch_label']

    hcc = None
    if theobj['sampled_feature_type_id'] is not None:
        #        theconcept = get_altid_identifiedconcept(g, 'sft.' + str(theobj['sampled_feature_type_id']))
        theconcept = concept_lkup['sft.' + str(theobj['sampled_feature_type_id'])]
        hcc = theconcept['label']
    #    resp = None
    thecollectors = []
    try:
        for collector in COLLECTOR_LKUP['sam.' + str(theobj['sample_id'])]:
#            thecollectors.append(get_Agent(g, collector))
            thecollectors.append(agent_lkup[collector])
#            test = agent_lkup[collector]
    except Exception as e:
        LOGGER.debug(f"no collector {theobj['sample_id']}.  {repr(e)}")

    thelocality = None
    try:
        thelocality = LOCALITY_LKUP['locality_id']['label']
    except:
        pass

    thetime = None
    if (theobj['collection_start_date'] is not None) and (theobj['collection_end_date'] is not None):
        thetime = f"{theobj['collection_start_date']}/{theobj['collection_end_date']}"
    elif theobj['collection_start_date'] is not None:
        thetime = f"{theobj['collection_start_date']}"
    elif theobj['collection_end_date'] is not None:
        thetime = f"{theobj['collection_end_date']}"

    theprj = None
    if theobj['cruise_field_prgrm_id'] is not None:
        theprj = INIT_LKUP['urn:local:ini.' + str(theobj['cruise_field_prgrm_id'])]

    thelabel = None
    if theprj:
        if theobj['launch_label'] is not None:
            thelabel = f"{theprj}, {theobj['launch_label']}"
        else:
            thelabel = theprj
    else:
        if theobj['name'] is not None:
            thelabel = f"Collection of sample {theobj['name']}"
        else:
            thelabel = f"Collection of sample on "
    if thetime:
        if thelabel:
            thelabel += "; " + thetime
    end_time = time.time()
    execution_time = end_time - start_time
    # LOGGER.info(f"load_vocab execution time: {execution_time} seconds")
    return SamplingEvent(
        pid=f"urn:local:evt.{theobj['sample_id']}",
        description=collectionDesc,
        authorized_by=None,
        has_context_category=hcc,
        has_feature_of_interest=thelocality,
        label=thelabel,
        project=theprj,
        responsibility=thecollectors,
        result_time=thetime,
        sample_location=get_GeospatialCoordLocation(theobj, concept_lkup),
        sampling_site=get_SamplingSite(theobj, LOCALITY_LKUP, concept_lkup),
    )


def get_SamplingSite(theobj, LOCALITY_LKUP, concept_lkup) -> SamplingSite:
    start_time = time.time()  # time the function execution
    thedesc = None
    if theobj['locality_detail'] is not None:
        thedesc = theobj['locality_detail']
    if theobj['latitude_end'] is not None:
        if len(thedesc) > 0:
            thedesc += ". "
        thedesc += f"Sampling from lat, long {theobj['latitude']}, {theobj['longitude']} to {theobj['latitude_end']}, {theobj['longitude_end']}"
    if theobj['location_qualifier'] is not None:
        if len(thedesc) > 0:
            thedesc += ". "
        thedesc += theobj['location_qualifier']
    theplaces = []
    locname = None
    try:
        thelocality = LOCALITY_LKUP['loc.' + str(theobj['locality_id'])]
        placelist = ['province', 'county', 'city']
        for item in placelist:
            if thelocality[item]:
                theplaces.append(thelocality[item])
        if thelocality['country_id'] is not None:
            #            theconcept = get_altid_identifiedconcept(g, 'cty.'+ str(thelocality['country_id']))
            theconcept = concept_lkup['cty.' + str(thelocality['country_id'])]
            #            test = concept_lkup['cty.'+ str(thelocality['country_id'])]
            theplaces.append(theconcept['label'])
        if thelocality['name'] is not None:
            locname = thelocality['name']
    except:
        pass
    end_time = time.time()
    execution_time = end_time - start_time
    # LOGGER.info(f"get sampling_site execution time: {execution_time} seconds")
    return SamplingSite(
        pid=f"urn:local:sst.{theobj['sample_id']}",
        description=thedesc,
        label=locname,
        place_name=theplaces,
        site_location=None,
        is_part_of=None,
    )


def get_MaterialSampleCuration(g, theobj, SESAR_USER_LKUP, agent_lkup) -> MaterialSampleCuration | None:
    start_time = time.time()  # time the function execution
    theowner = None
    try:
        thepid = SESAR_USER_LKUP[str(theobj['cur_owner_id'])]
#        theowner = get_Agent(g, thepid)
        theowner = agent_lkup[thepid]
#        test = agent_lkup[thepid]
        matsamcur = MaterialSampleCuration(
            pid='urn:local:cur.' + str(theobj['sample_id']),
            responsibility=theowner,
            label=theowner.name,
            description='current owner',
            access_constraints=[]
        )
        end_time = time.time()
        execution_time = end_time - start_time
        LOGGER.debug(f"get MaterialSampleCuration execution time: {execution_time} seconds")
        return matsamcur
    except Exception as e:
        LOGGER.info(f"Sample Curation; No owner; Exception: {repr(e)}")
        return None

def load_lkup(lkup_name:str, lkup_function:callable) -> dict | None:
    lkup_dict = {}
    try:
        with open(f'{lkup_name}.pkl', 'rb') as file:
            lkup_dict = pickle.load(file)
            return lkup_dict
    except Exception as e:
        lkup_dict = lkup_function()
        if lkup_dict:
            with open(f'{lkup_name}.pkl', 'wb') as file:
                pickle.dump(lkup_dict, file)
            print(f'{lkup_name} lkup loaded')
            return lkup_dict
        else:
            print(f'{lkup_name} fail !!!!!!')
            return None

#@profile
def load_samples(g, concept_lkup, agent_lkup):
    start_time = time.time()  # time the function execution
    tableName = 'sample'
    batchsize = 100000

    SESAR_USER_LKUP = {}
    SESAR_USER_LKUP = load_lkup('SESAR_USER_LKUP',load_sesar_user_lkup)

    INIT_LKUP = {}
    INIT_LKUP = load_lkup('INIT_LKUP',load_initiative_lkup)

    COLLECTOR_LKUP = {}
    COLLECTOR_LKUP = load_lkup('COLLECTOR_LKUP',load_collector_lkup)

    ARCHIVE_LKUP = {}
    ARCHIVE_LKUP = load_lkup('ARCHIVE_LKUP', load_archive_lkup)

    LOCALITY_LKUP = {}
    LOCALITY_LKUP = load_lkup('LOCALITY_LKUP',  load_locality_lkup)

    addName_lkup = {}
    addName_lkup = load_lkup('addName_lkup',load_additional_name_lkup)

    relres_lkup = {}
    relres_lkup = load_lkup('relres_lkup',load_related_resource_lkup)

    end_time = time.time()
    execution_time = (end_time - start_time)
    LOGGER.info(f"load lookups for sample loop execution time: {execution_time} seconds")
    thefields = getFields(newDb, tableName)

    maxIDQuery = "SELECT max(sample_id) FROM public.sample"
    try:
        result = executeQuery(newDb, maxIDQuery)
        sample_max_id = result[0][0]
    except:
         sample_max_id = 0

    #max_id = 0  # starting value
    max_id = 4234568  # starting value
    insertDict = {}
    while True:
        selectRecordQuery = 'SELECT * FROM public.' + tableName + ' where sample_id > ' + str(max_id) + \
                            '  order by sample_id ' + \
                            '  LIMIT ' + str(batchsize) + ';'
        LOGGER.info(f"get_sample_data record query: {repr(selectRecordQuery)}")
        try:
            data = executeQuery(newDb, selectRecordQuery)
        except:
            LOGGER.info('get_sample_data data query failed')
            break

        selectMaxQuery = "SELECT max(subset.sample_id) FROM (select * from public." + tableName + \
                         " where sample_id > " + str(max_id) + \
                         "  order by sample_id " + \
                         "  LIMIT " + str(batchsize) + ") as subset;"
        LOGGER.debug("get_sample_data max sample_id query: ", repr(selectMaxQuery))
        try:
            result = executeQuery(newDb, selectMaxQuery)
            max_id = result[0][0]
        except:
            LOGGER.info('get_max sample ID failed')
            break
        LOGGER.info(f'got sample data, start at max_id {max_id}')

        therow = 0
        rept_time = end_time
        for row in data:
            loopstart_time = time.time()
            theobj = {}
            for nc in range(len(row)):
                # if row[nc] is None:
                #     theobj[thefields[nc]] = None
                #     # replace null values with 'blank'
                # else:
                theobj[thefields[nc]] = row[nc]


            theregistrant = None
            if theobj['cur_registrant_id'] is not None:
                try:
                    thepid = SESAR_USER_LKUP[str(theobj['cur_registrant_id'])]
                    theregistrant = agent_lkup[thepid]
                except Exception as e:
                    LOGGER.info(f'registration lookup fail, exception {repr(e)}')

            samdesc = ''
            if theobj['sample_description'] is not None:
                samdesc = theobj['sample_description']
            if theobj['material_name_verbatim'] is not None:
                samdesc += ". Material: " + theobj['material_name_verbatim']
            if theobj['size'] is not None:
                samdesc += ". Sample size: " + theobj['size']
            # if have start and endpoints for lat and long
            if theobj['latitude'] is not None and theobj['latitude_end'] is not None:
                samdesc += ". Latitude from " + str(theobj['latitude']) + " to " + str(theobj['latitude_end'])
            if theobj['longitude'] is not None and theobj['longitude_end'] is not None:
                samdesc += ", Longitude from " + str(theobj['longitude']) + " to " + str(theobj['longitude_end'])
            if theobj['geologic_age_verbatim'] is not None:
                samdesc += ". Age: " + theobj['geologic_age_verbatim']
            # add numeric ages
            if theobj['numeric_age_min'] is not None and theobj['numeric_age_max'] is not None:
                samdesc += ". Age between " + str(theobj['numeric_age_min']) + " and " + str(theobj['numeric_age_max'])
            elif theobj['numeric_age_min'] is not None:
                samdesc += ". Minimum age " + str(theobj['numeric_age_min'])
            elif theobj['numeric_age_max'] is not None:
                samdesc += ". Maximum age " + str(theobj['numeric_age_max'])

            if theobj['numeric_age_unit'] is not None:
                samdesc += " Age units " + theobj['numeric_age_unit']
            if theobj['age_qualifier'] is not None:
                samdesc += ", Age qualitifer: " + theobj['age_qualifier']


            thekeywords = []
            if theobj['geologic_unit'] is not None:
                thek = IdentifiedConcept(
                    pid='geo_unit.' + str(theobj['sample_id']),
                    label=theobj['geologic_unit'],
                    scheme_name="Geologic Unit",
                    scheme_uri=None)
                thekeywords.append(thek)
            thealtid = []
            if theobj['geologic_age_older_id'] is not None:
                #            theconcept = get_altid_identifiedconcept(g, 'gts.' + str(theobj['geologic_age_older_id']))
                theconcept = concept_lkup['gts.' + str(theobj['geologic_age_older_id'])]
                #            test = concept_lkup['gts.' + str(theobj['geologic_age_older_id'])]
                theconcept['scheme_name'] = 'Geologic Age Older'
                thealtid.append('gts.' + str(theobj['geologic_age_older_id']))
                theconcept['altids'] = thealtid
                thekeywords.append(theconcept)
            if theobj['geologic_age_younger_id'] is not None:
                #            theconcept = get_altid_identifiedconcept(g, 'gts.' + str(theobj['geologic_age_younger_id']))
                theconcept = concept_lkup['gts.' + str(theobj['geologic_age_younger_id'])]
                theconcept['scheme_name'] = 'Geologic Age Younger'
                thealtid.append('gts.' + str(theobj['geologic_age_younger_id']))
                theconcept['altids'] = thealtid
                thekeywords.append(theconcept)
            if len(thekeywords) == 0:
                thekeywords = None

            try:
                addnames = addName_lkup[str(theobj['sample_id'])]
            except:
                addnames = None

            try:
                therels = relres_lkup[str(theobj['sample_id'])]
            except:
                therels = None

            try:
                has_context_cat = concept_lkup['sft.' + str(theobj['sampled_feature_type_id'])]
            except:
                has_context_cat = None

            try:
                has_material_cat = concept_lkup['mat.' + str(theobj['general_material_type_id'])]
            except:
                has_material_cat = None

            try:
                has_sample_obj_type = concept_lkup['sat.' + str(theobj['sample_type_id'])]
            except:
                has_sample_obj_type = None

            ms = MaterialSampleRecord(
                pid=f"sam.{str(theobj['sample_id'])}",
                alternate_identifiers=addnames,
                complies_with=None,
                curation=get_MaterialSampleCuration(g, theobj, SESAR_USER_LKUP, agent_lkup),
                dc_rights=None,
                description=samdesc,

                has_context_category=has_context_cat,
                has_material_category=has_material_cat,
                has_sample_object_type=has_sample_obj_type,

                keywords=thekeywords,
                label=theobj['name'],
                last_modified_time=theobj['last_update_date'],
                produced_by=get_SamplingEvent(g, theobj, COLLECTOR_LKUP, LOCALITY_LKUP, INIT_LKUP, concept_lkup, agent_lkup),
                registrant=theregistrant,
                related_resource=therels,
                sample_identifier=theobj['igsn'],
                sampling_purpose=theobj['purpose']
            )
            addNodeToList(g,  ms, insertDict)

            end_tim4 = time.time()
            execution_time = (end_tim4 - loopstart_time)*1000
            LOGGER.debug(f"total for{str(theobj['sample_id'])}: {execution_time} milliseconds")

            therow += 1
            writebatchsize = 10000
            if therow % writebatchsize == 0:
                writeduckdb(g,insertDict)
                insertDict = {}
                LOGGER.info(f'load sample therow: {therow}')
                end_time = time.time()
                execution_time = end_time - rept_time
                LOGGER.info(f"get {writebatchsize} samples execution time: {execution_time} seconds")
                print(f"load sample {therow}")
                rept_time = time.time()

        LOGGER.info(f'load iteration done. max_id: {max_id}')
        if max_id == sample_max_id:
        #if therow >= 1000:
            print(f'load sample loop done. break')
            break
    return 1


def get_record(g, pid):
    record = g.getNode(pid)
    # print(record)
    print(json.dumps(record, indent=2, cls=pqg.JSONDateTimeEncoder))


def main(dest: str = None):
    loadvocabs = False
    load_agent_lkup = False
    loadsamples = True
    tstart_time = time.time()

    sesarDb = get_2025Connection()
    if sesarDb:
        print("Connection to SESAR2025 PostgresSQL database established successfully.")
    else:
        print("Connection to SESAR2025 PostgresSQL encountered an error.")
        exit()

    theddb = 'sesarduck2.ddb'
    dbinstance = duckdb.connect(theddb)
    g = createGraph(dbinstance)

    concept_lkup = {}
    if loadvocabs:
        vocablist = []
        vocablist.append(['affiliation_type', 'aft', '', 'label',
                          'SESAR Affiliation Type', '', 'affiliation_type_id'])
        vocablist.append(['agent_role_type', 'art', '', 'label',
                          'SESAR agent roles', '', 'agent_role_id'])
        # vocablist.append(['collection_type', '', 'feature_type_uri', 'label',
        #         'SESAR Sampled Feature Type', 'scheme_uri', 'feature_type_id'])
        vocablist.append(['country', 'cty', 'iso3166code', 'label',
                          'ISO3166 country', '', 'country_id'])
        vocablist.append(['geologic_time_scale', 'gts', 'geologic_time_interval_uri', 'label',
                          'ICS 2020 Chronostratigraphic', 'scheme_uri', 'geologic_time_id'])
        vocablist.append(['initiative_type', 'ini', '', 'label',
                          'SESAR Initiative Type', '', 'initiative_type_id'])
        vocablist.append(['institution_type', 'ins', '', 'label',
                          'SESAR Institution Type', '', 'institution_type_id'])
        vocablist.append(['launch_type', 'lat', '', 'label',
                          'SESAR Launch Type', '', 'launch_type_id'])
        # vocablist.append(['locality', 'loc', 'locality_uri', 'name',
        #         'SESAR locality', '', 'locality_id'])
        vocablist.append(['location_method', 'lom', '', 'label',
                          'SESAR Location Method', '', 'location_method_id'])
        vocablist.append(['material_role_type', 'mar', '', 'label',
                          'SESAR Material Role', '', 'material_role_id'])
        vocablist.append(['platform', 'pla', '', 'label',
                          'SESAR Platform', '', 'platform_id'])
        vocablist.append(['platform_type', 'plt', '', 'label',
                          'SESAR Platform Type', '', 'platform_type_id'])
        # vocablist.append(['property_type', '', 'sample_type_uri', 'label',
        #         'SESAR Sample Type', 'scheme_uri', 'sample_type_id'])
        vocablist.append(['relation_type', 'rel', '', 'label',
                          'SESAR relation Type', 'scheme_uri', 'relation_type_id'])
        vocablist.append(['resource_type', 'ret', 'resource_type_uri', 'label',
                          'SESAR Resource Type', 'scheme_uri', 'resource_type_id'])
        vocablist.append(['sample_type', 'sat', 'sample_type_uri', 'label',
                          'SESAR Sample Type', 'scheme_uri', 'sample_type_id'])
        vocablist.append(['sampled_feature_type', 'sft', 'feature_type_uri', 'label',
                          'SESAR Sampled Feature Type', 'scheme_uri', 'feature_type_id'])
        vocablist.append(['sampling_method', 'sam', 'method_uri', 'label',
                          'SESAR Sampling Method', 'scheme_uri', 'collection_method_id'])
        vocablist.append(['sesar_spatial_ref_sys', 'ssr', 'identifier', 'name',
                          'SESAR Spatial Reference Systems', '', 'spatial_ref_id'])

        for vocab in vocablist:
            result = load_concept_lkup(vocab, concept_lkup)
            print(f'vocabulary {vocab[0]} loaded')

        result = load_material_type(concept_lkup)
        print(f'material type loaded {result}')

        with open('concept_lkup.pkl', 'wb') as file:
            pickle.dump(concept_lkup, file)
    else:   #load the cached lookup file
        try:
            with open('concept_lkup.pkl', 'rb') as file:
                concept_lkup = pickle.load(file)
        except Exception as e:
            print(f'cached concept_lkup file needs to be created, make loadvocabs TRUE')
            exit()

    agent_lkup = {}
    if load_agent_lkup:
        result = load_individuals(agent_lkup)
        print(f'individuals in agent_lkup loaded {result}')
        result = load_institution(agent_lkup)
        print(f'institution in agent_lkup loaded {result}')
        with open('agent_lkup.pkl', 'wb') as file:
            pickle.dump(agent_lkup, file)
    else:   #load the cached lookup file
        try:
            with open('agent_lkup.pkl', 'rb') as file:
                agent_lkup = pickle.load(file)
        except Exception as e:
            print(f'cached agent_lkup file needs to be created, make load_agent_lkup TRUE')
            exit()

    if loadsamples:
        load_samples(g, concept_lkup, agent_lkup)

    dest = 'sesarTest2'
    if dest is not None:
        g.asParquet(pathlib.Path(dest))

    tend_time = time.time()
    execution_time = tend_time - tstart_time
    LOGGER.info(f'total run time: {execution_time / 3600} hours')
    newDb.close()
    dbinstance.close()

if __name__ == "__main__":
    #    logging.basicConfig(level=logging.DEBUG)
    logging.basicConfig(filename='pqg.log', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')

    # main("data/test_0.ddb")
    main()
