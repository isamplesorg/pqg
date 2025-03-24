"""
load data from SESAR 2025 database into duckDb database
save as Parquet file.
based on Code by Dave Vieglais
Modified for SESAR by SM Richard 2025-02-28
"""
import io
import json
import psycopg2
import hashlib
from shapely import wkt
import copy
# import logging
# import duckdb
import sys

sys.path.append('C:/Users/smrTu/OneDrive/Documents/GithubC/iSamples/pqg/')
from load_insert_lists import *
# from line_profiler import profile
import pickle
import numbers

LOGGER = logging.getLogger('sesarParquet')

INSERTTEMPLATE = {}
INSERT_VALS = []
AGENT_PID_LKUP = {}


# test_vals = []


# SESAR_USER_LKUP = {}
# INIT_LKUP = {}
# COLLECTOR_LKUP = {}
# ARCHIVE_LKUP = {}
# LOCALITY_LKUP = {}


# connect to database
def get_2025Connection() -> psycopg2.extensions.connection | None:
    try:
        return psycopg2.connect(
            database="SESAR2025bku",
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


def write_json_lines(data, filename):
    """Writes a list of dictionaries to a JSON Lines file."""
    with open(filename, 'w') as f:
        for entry in data:
            if entry['pid'] is not None:
                json.dump(entry, f)
                f.write('\n')


def write_json_lines_fast(data, filename):
    with open(filename, 'w', buffering=io.DEFAULT_BUFFER_SIZE) as f:
        f.writelines(json.dumps(entry) + '\n' for entry in data if entry.get('pid') is not None)


# def get_blank_insert():
#     theblank = copy.deepcopy(INSERTTEMPLATE)
#     return theblank

def get_blank_insert():
    return dict.fromkeys(INSERTTEMPLATE, None)


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
    #  get field names for table, use to access newDb
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
    # global insert_vals
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

        # # have problem with concepts that have multiple parents; these show up in concept list more than ones
        # try:
        #     pidlist = list({d["pid"] for d in INSERT_VALS})
        #     if thepid in pidlist:
        #         pass
        #     else:
        #         insert_val = get_blank_insert()
        #         insert_val['otype'] = 'IdentifiedConcept'
        #         insert_val['pid'] = thepid
        #         insert_val['label'] = theobj[args[3]]
        #         insert_val['scheme_name'] = args[4]
        #         insert_val['scheme_uri'] = theschemeuri
        #         insert_val['altids'] = [theid]
        #         INSERT_VALS.append(insert_val)
        # except:
        #     pass

        concept_lkup[theid] = {'flag': False, 'obj': theConcept}

    end_time = time.time()
    execution_time = end_time - start_time
    LOGGER.info(f"load vocabs {tableName} execution time: {execution_time} seconds")
    return 1


def add_concept(theconcept):
    try:
        insert_val = get_blank_insert()
        insert_val['otype'] = 'IdentifiedConcept'
        insert_val['pid'] = theconcept['pid']
        insert_val['label'] = theconcept['label']
        insert_val['scheme_name'] = theconcept['scheme_name']
        insert_val['scheme_uri'] = theconcept['scheme_uri']
        insert_val['altids'] = theconcept['altids']
        INSERT_VALS.append(insert_val)
        return 1
    except:
        return None


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

        if theobj['material_type_uri'] != 'blank':
            thepid = theobj['material_type_uri']
        else:
            thepid = 'urn:local:' + f"{abbrev}.{str(theobj['material_type.material_type_id'])}"
        theConcept = IdentifiedConcept(
            pid=thepid,
            label=theobj['material_type.label'],
            scheme_name="SESAR Material Type",
            scheme_uri=theobj['scheme_uri'],
            altids=[f"{abbrev}.{str(theobj['material_type.material_type_id'])}"]
        )
        concept_lkup[abbrev + '.' + str(theobj['material_type.material_type_id'])] = {'flag': False, 'obj': theConcept}

        # insert_val = get_blank_insert()
        # insert_val['otype'] = 'IdentifiedConcept',
        # insert_val['pid'] = thepid,
        # insert_val['label'] = theobj['material_type.label'],
        # insert_val['scheme_name'] = "SESAR Material Type",
        # insert_val['scheme_uri'] = theobj['scheme_uri'],
        # insert_val['altids'] = [f"{abbrev}.{str(theobj['material_type.material_type_id'])}"]
        # INSERT_VALS.append(insert_val)

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
        agent_lkup[abbrev + '.' + str(theobj['individual_id'])] = {'flag': False, 'obj': theagent}

    end_time = time.time()
    execution_time = end_time - start_time
    LOGGER.info(f"load individuals in agent_lkup execution time: {execution_time} seconds")
    return 1


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
        agent_lkup[abbrev + '.' + str(theobj['institution_id'])] = {'flag': False, 'obj': theagent}

    end_time = time.time()
    execution_time = end_time - start_time
    LOGGER.info(f"load institution to Agent execution time: {execution_time} seconds")
    return 1


def add_agent(theagent):
    try:
        insert_val = get_blank_insert()
        insert_val['otype'] = 'Agent'
        insert_val['pid'] = theagent['pid']
        insert_val['affiliation'] = None
        insert_val['contact_information'] = theagent['contact_information']
        insert_val['name'] = theagent['name']
        insert_val['role'] = ''
        insert_val['altids'] = theagent['altids']
        INSERT_VALS.append(insert_val)
        return 1
    except Exception as e:
        LOGGER.info(f"add agent fail, {theagent['pid']}, e: {repr(e)}")
        return None


# def load_agent_altid_pid_lkup(agent_lkup) -> dict:
#     agent_altid_pid_lkup = {}
#     for key in agent_lkup:
#         thepid = agent_lkup[key]['obj']['pid']
#         thealtid = agent_lkup[key]['obj']['altids'][0]
#         agent_altid_pid_lkup[thealtid] = thepid
#     return agent_altid_pid_lkup


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
    #  sample relation objects with links for which the sample
    #  is the subject
    for row in idata:
        theobj = {}
        for nc in range(len(row)):
            theobj[thefields[nc]] = row[nc]

        if theobj['sample_id'] is not None:  # skip if no relationships from the sample
            try:
                thesamplerel = SampleRelation(
                    pid=f"{abbrev}.{str(theobj['relation_id'])}",
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
    # initiatives (projects, cruises, field programs, etc.) are represented in iSamples
    #   with text descriptions. SESAR does not provide identifiers for initiatives....
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
    # lookup a legacy sesar user ID, return individual PID if there is one or institution PID if
    #   not an individual
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
        else:
            return None
    return sesaruserlkup


def get_GeospatialCoordLocation(theobj, concept_lkup) -> GeospatialCoordLocation | None:
    lat = theobj['latitude']
    long = theobj['longitude']
    elev = theobj['elevation']
    depth_min = theobj['depth_min']
    depth_max = theobj['depth_max']

    if lat is not None:
        lat = str(lat)
    if long is not None:
        long = str(long)

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
                try:
                    # only use the label, don't add the concept to the graph
                    theconcept = concept_lkup['ssr.' + str(theobj['depth_spatial_ref_id'])]
                    verticalpos = verticalpos + " Datum: " + theconcept['label']
                except Exception as e:
                    verticalpos = verticalpos + " Datum: " + str(theobj['depth_spatial_ref_id'])

            if len(verticalpos) == 0:
                verticalpos = None

        gcpid = f"urn:local:geo.{str(theobj['sample_id'])}"
        #        gcvalues.append((gcpid, lat, long, False, verticalpos))

        try:
            if (isinstance(lat, numbers.Number)) and (isinstance(long, numbers.Number)):
                point_wkt = f"POINT({lat} {long})"
                thegeom = str(wkt.loads(point_wkt))
            else:
                thegeom = None
        except Exception as e:
            LOGGER.info(f"get point geometry fail sample {theobj['sample_id']}")
            thegeom = None

        insert_val = get_blank_insert()
        insert_val['otype'] = 'GeospatialCoordLocation'
        insert_val['pid'] = f"urn:local:geo.{str(theobj['sample_id'])}"
        insert_val['latitude'] = lat
        insert_val['longitude'] = long
        insert_val['obfuscated'] = False
        insert_val['elevation'] = verticalpos
        insert_val['geometry'] = thegeom
        INSERT_VALS.append(insert_val)

        return GeospatialCoordLocation(
            pid=f"urn:local:geo.{str(theobj['sample_id'])}",
            latitude=lat,
            longitude=long,
            obfuscated=False,
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


# def get_pid_by_altid(g, idin: str) -> str | None:
#     try:
#         with g.getCursor() as crsr:
#             result = crsr.execute("select pid from node where '" + idin + "' in altids")
#             apid = result.fetchone()
#     except Exception as e:
#         LOGGER.debug(f'get identified concept error: {e}')
#         return None
#     return str(apid[0])


# def get_Agent(g, thealtid: str) -> Agent | None:
#     try:
#         thepid = get_pid_by_altid(g, thealtid)
#         thepid = AGENT_PID_LKUP[thealtid]
#         theagent = g.getNodeEntry(pid=thepid)
#     except Exception as e:
#         LOGGER.debug(f'get agent error: {e}')
#         return None
#     return theagent


def get_SamplingEvent(g, theobj, COLLECTOR_LKUP, LOCALITY_LKUP, INIT_LKUP, concept_lkup, agent_lkup
                      ) -> SamplingEvent:
    # description
    start_time = time.time()  # time the function execution
    eventpid = f"urn:local:evt.{theobj['sample_id']}"

    collectionDesc = ''
    if theobj['collection_method_id'] is not None:
        try:
            theitem = concept_lkup['sam.' + str(theobj['collection_method_id'])]
            theconcept = theitem['obj']
            if not theitem['flag']:
                result = add_concept(theconcept)
                if result is not None:
                    concept_lkup['sam.' + str(theobj['collection_method_id'])]['flag'] = True
            collectionDesc = collectionDesc + 'method:' + theconcept['label'] + '; pid:' + theconcept['pid']
        except Exception as e:
            LOGGER.info(f"collection description, collection method fail. sample {theobj['sample_id']}, e: {repr(e)}")
    if theobj['collection_method_detail'] is not None:
        if len(collectionDesc) > 0:
            collectionDesc += ', '
        collectionDesc += theobj['collection_method_detail'] + '. '
    try:
        if theobj['platform_id'] is not None:
            theitem = concept_lkup['pla.' + str(theobj['platform_id'])]
            theconcept = theitem['obj']
            if not theitem['flag']:
                result = add_concept(theconcept)
                if result is not None:
                    concept_lkup['pla.' + str(theobj['platform_id'])]['flag'] = True
            collectionDesc = collectionDesc + ' Platform: ' + theconcept['label'] + '; pid:' + theconcept['pid']
    except Exception as e:
        LOGGER.info(f"collection description, platform fail. sample {theobj['sample_id']}, e: {repr(e)}")
    try:
        if theobj['launch_platform_id'] is not None:
            theitem = concept_lkup['pla.' + str(theobj['launch_platform_id'])]
            theconcept = theitem['obj']
            if not theitem['flag']:
                result = add_concept(theconcept)
                if result is not None:
                    concept_lkup['pla.' + str(theobj['platform_id'])]['flag'] = True
            collectionDesc = collectionDesc + ' Launch Platform: ' + theconcept['label'] + '; pid:' + theconcept['pid']
    except Exception as e:
        LOGGER.info(f"collection description, launch platform fail. sample {theobj['sample_id']}, e: {repr(e)}")
    if theobj['launch_label'] is not None:
        collectionDesc = collectionDesc + ' Launch: ' + theobj['launch_label']

    hcc = None
    if theobj['sampled_feature_type_id'] is not None:
        theitem = concept_lkup['sft.' + str(theobj['sampled_feature_type_id'])]
        theconcept = theitem['obj']
        if not theitem['flag']:
            result = add_concept(theconcept)
            if result is not None:
                concept_lkup['sft.' + str(theobj['sampled_feature_type_id'])]['flag'] = True
        hcc = theconcept

    thecollectors = []
    try:
        for collector in COLLECTOR_LKUP['sam.' + str(theobj['sample_id'])]:
            agent_lkup[collector]['obj']['role'] = 'collector'
            thecollectors.append(agent_lkup[collector]['obj'])
            if not collector['flag']:
                result = add_agent(collector['obj'])
                if result is not None:
                    agent_lkup[collector]['flag'] = True
            # add edges to link agent to event after the event insert
    except Exception as e:
        LOGGER.debug(f"no collector {theobj['sample_id']}.  {repr(e)}")

    thelocality = None
    # returns text label for the locality
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
    # lookup returns string description of initiative
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
    LOGGER.debug(f"Sampling Event execution time: {execution_time} seconds")
    if len(collectionDesc) == 0:
        collectionDesc = None

    insert_val = get_blank_insert()
    insert_val['otype'] = 'SamplingEvent'
    insert_val['pid'] = eventpid
    insert_val['description'] = collectionDesc
    insert_val['authorized_by'] = None
    insert_val['has_feature_of_interest'] = thelocality
    insert_val['label'] = thelabel
    insert_val['project'] = theprj
    insert_val['result_time'] = thetime
    INSERT_VALS.append(insert_val)  # Sampling Event

    sampleloc = get_GeospatialCoordLocation(theobj, concept_lkup)
    if sampleloc is not None:
        # add edge to link location to sampleingevent
        insert_val = get_edge_insert_val('_edge_', eventpid, 'sample_location', sampleloc['pid'])
        if insert_val is not None:
            INSERT_VALS.append(insert_val)

    samplingsite = get_SamplingSite(theobj, LOCALITY_LKUP, concept_lkup)
    if samplingsite is not None:
        # add edge to link site to samplingEvent
        insert_val = get_edge_insert_val('_edge_', eventpid, 'sampling_site', samplingsite['pid'])
        if insert_val is not None:
            INSERT_VALS.append(insert_val)

    try:
        for collector in COLLECTOR_LKUP['sam.' + str(theobj['sample_id'])]:
            # add edge to link agent to event
            insert_val = get_edge_insert_val('_edge_', eventpid, 'responsibility', agent_lkup[collector]['obj']['pid'])
            if insert_val is not None:
                INSERT_VALS.append(insert_val)
    except Exception as e:
        LOGGER.debug(f'no collector edges for sample {theobj["sample_id"]}, e: {repr(e)}')

    # insert    'has_context_category'
    if hcc is not None:
        # add edge to link site to samplingEvent
        insert_val = get_edge_insert_val('_edge_', eventpid, 'has_context_category', hcc['pid'])
        if insert_val is not None:
            INSERT_VALS.append(insert_val)

    return SamplingEvent(
        pid=eventpid,
        description=collectionDesc,
        authorized_by=None,
        has_context_category=hcc,
        has_feature_of_interest=thelocality,
        label=thelabel,
        project=theprj,
        responsibility=thecollectors,
        result_time=thetime,
        sample_location=sampleloc,
        sampling_site=samplingsite,
    )


def get_SamplingSite(theobj, LOCALITY_LKUP, concept_lkup) -> SamplingSite:
    start_time = time.time()  # time the function execution

    ssitepid = f"urn:local:sst.{theobj['sample_id']}"

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

    insert_val = get_blank_insert()
    insert_val['otype'] = 'SamplingSite'
    insert_val['pid'] = ssitepid
    insert_val['description'] = thedesc
    insert_val['label'] = locname
    insert_val['place_name'] = theplaces
    # insert_val['site_location'] = None
    insert_val['is_part_of'] = None
    INSERT_VALS.append(insert_val)  # sampling site

    # site_location is an edge, but for this mapping, populate location on samplingEvent
    #  site_location would be for more precise location with context of samplingEvent/location

    end_time = time.time()
    execution_time = end_time - start_time
    LOGGER.debug(f"get sampling_site execution time: {execution_time} seconds")

    return SamplingSite(
        pid=ssitepid,
        description=thedesc,
        label=locname,
        place_name=theplaces,
        site_location=None,
        is_part_of=None,
    )


def get_MaterialSampleCuration(g, theobj, SESAR_USER_LKUP, agent_lkup) -> MaterialSampleCuration | None:
    start_time = time.time()  # time the function execution
    curpid = 'urn:local:cur.' + str(theobj['sample_id'])
    theowner = None
    try:
        thepid = SESAR_USER_LKUP[str(theobj['cur_owner_id'])]
        theitem = agent_lkup[thepid]
        theowner = theitem['obj']
        theowner['role'] = 'owner'
        if not theitem['flag']:
            result = add_agent(theowner)
            if result is not None:
                agent_lkup[thepid]['flag'] = True

        matsamcur = MaterialSampleCuration(
            pid=curpid,
            responsibility=theowner,
            label=theowner.name,
            description='current owner',
            access_constraints=[]
        )
        insert_val = get_blank_insert()
        insert_val['otype'] = 'MaterialSampleCuration',
        insert_val['pid'] = curpid,
        insert_val['label'] = theowner.name,
        insert_val['description'] = 'current owner',
        insert_val['access_constraints'] = []
        INSERT_VALS.append(insert_val)

        # add edge to link owner to curation/responsibility
        insert_val = get_edge_insert_val('_edge_', curpid, 'responsibility', theowner['pid'])
        if insert_val is not None:
            INSERT_VALS.append(insert_val)

        end_time = time.time()
        execution_time = end_time - start_time
        LOGGER.debug(f"get MaterialSampleCuration execution time: {execution_time} seconds")
        return matsamcur
    except Exception as e:
        LOGGER.info(f"Sample Curation; No owner; Exception: {repr(e)}")
        return None


def load_lkup(lkup_name: str, lkup_function: callable) -> dict | None:
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


# @profile
def load_samples(g, concept_lkup, agent_lkup):
    print('start load samples')
    start_time = time.time()  # time the function execution
    tableName = 'sample'
    batchsize = 100000

    SESAR_USER_LKUP = load_lkup('SESAR_USER_LKUP', load_sesar_user_lkup)
    INIT_LKUP = load_lkup('INIT_LKUP', load_initiative_lkup)
    COLLECTOR_LKUP = load_lkup('COLLECTOR_LKUP', load_collector_lkup)
    # ARCHIVE_LKUP = load_lkup('ARCHIVE_LKUP', load_archive_lkup)
    LOCALITY_LKUP = load_lkup('LOCALITY_LKUP', load_locality_lkup)
    addName_lkup = load_lkup('addName_lkup', load_additional_name_lkup)

    relres_lkup = load_lkup('relres_lkup', load_related_resource_lkup)

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

    max_id = 0  # starting value
    # max_id = 4234568  # starting value
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
                theobj[thefields[nc]] = row[nc]

            sampid = f"sam.{str(theobj['sample_id'])}"

            theregistrant = None
            if theobj['cur_registrant_id'] is not None:
                try:
                    thepid = SESAR_USER_LKUP[str(theobj['cur_registrant_id'])]
                    theitem = agent_lkup[thepid]
                    theregistrant = theitem['obj']
                    theregistrant['role'] = 'registrant'  # this will only catch first role if agent
                    # plays multiple roles
                    if theitem['flag'] == False:
                        result = add_agent(theregistrant)
                        if result is not None:
                            agent_lkup[thepid]['flag'] = True

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
                samdesc += ", Age qualifier: " + theobj['age_qualifier']

            thekeywords = []
            if theobj['geologic_unit'] is not None:
                thek = IdentifiedConcept(
                    pid='geo_unit.' + str(theobj['sample_id']),
                    label=theobj['geologic_unit'],
                    scheme_name="Geologic Unit",
                    scheme_uri=None)
                thekeywords.append(thek)
                result = add_concept(thek)
            thealtid = []
            if theobj['geologic_age_older_id'] is not None:
                theitem = concept_lkup['gts.' + str(theobj['geologic_age_older_id'])]
                theconcept = theitem['obj']
                if theitem['flag'] == False:
                    result = add_concept(theconcept)
                    if result is not None:
                        concept_lkup['gts.' + str(theobj['geologic_age_older_id'])]['flag'] = True
                theconcept['scheme_name'] = 'Geologic Age Older'
                thealtid.append('gts.' + str(theobj['geologic_age_older_id']))
                theconcept['altids'] = thealtid
                thekeywords.append(theconcept)
            if theobj['geologic_age_younger_id'] is not None:
                theitem = concept_lkup['gts.' + str(theobj['geologic_age_younger_id'])]
                theconcept = theitem['obj']
                if theitem['flag'] == False:
                    result = add_concept(theconcept)
                    if result is not None:
                        concept_lkup['gts.' + str(theobj['geologic_age_younger_id'])]['flag'] = True
                theconcept['scheme_name'] = 'Geologic Age Younger'
                thealtid.append('gts.' + str(theobj['geologic_age_younger_id']))
                theconcept['altids'] = thealtid
                thekeywords.append(theconcept)
            if len(thekeywords) == 0:
                thekeywords = None

            try:
                addnames = addName_lkup[str(theobj['sample_id'])]
                # returns a list of names/identifier
            except:
                addnames = None

            try:
                therels = relres_lkup[str(theobj['sample_id'])]
                for rel in therels:
                    insert_val = get_blank_insert()
                    insert_val['otype'] = 'SampleRelation'
                    insert_val['pid'] = rel['pid']
                    insert_val['target'] = rel['target']
                    insert_val['description'] = ''
                    insert_val['label'] = rel['label']
                    insert_val['relationship'] = 'has parent material sample'
                    INSERT_VALS.append(insert_val)
            except:
                therels = None

            try:
                theitem = concept_lkup['sft.' + str(theobj['sampled_feature_type_id'])]
                has_context_cat = theitem['obj']
                if theitem['flag'] == False:
                    result = add_concept(theitem['obj'])
                    if result is not None:
                        concept_lkup['sft.' + str(theobj['sampled_feature_type_id'])]['flag'] = True
            except:
                has_context_cat = None

            try:
                theitem = concept_lkup['mat.' + str(theobj['general_material_type_id'])]
                has_material_cat = theitem['obj']
                if theitem['flag'] == False:
                    result = add_concept(theitem['obj'])
                    if result is not None:
                        concept_lkup['mat.' + str(theobj['general_material_type_id'])]['flag'] = True
            except:
                has_material_cat = None

            try:
                theitem = concept_lkup['sat.' + str(theobj['sample_type_id'])]
                has_sample_obj_type = item['obj']
                if theitem['flag'] == False:
                    result = add_concept(theitem['obj'])
                    if result is not None:
                        concept_lkup['sat.' + str(theobj['sample_type_id'])]['flag'] = True
            except:
                has_sample_obj_type = None

            theCuration = get_MaterialSampleCuration(g, theobj, SESAR_USER_LKUP, agent_lkup)
            theSamplingEvent = get_SamplingEvent(g, theobj, COLLECTOR_LKUP, LOCALITY_LKUP, INIT_LKUP, concept_lkup,
                                                 agent_lkup),
            if len(samdesc) == 0:
                samdesc = None

            insert_val = get_blank_insert()
            insert_val['otype'] = 'MaterialSampleRecord'
            insert_val['pid'] = sampid
            insert_val['alternate_identifiers'] = addnames
            insert_val['complies_with'] = None
            insert_val['dc_rights'] = None
            insert_val['description'] = samdesc
            insert_val['label'] = theobj['name']
            insert_val['last_modified_time'] = str(theobj['last_update_date'])
            insert_val['sample_identifier'] = theobj['igsn']
            insert_val['sampling_purpose'] = theobj['purpose']
            try:
                INSERT_VALS.append(insert_val)  # Material sample record
            except Exception as e:
                LOGGER.info(f'insert sam fail, values:{insert_val}. Exception {repr(e)}')

            # add edges
            if theCuration is not None:
                insert_val = get_edge_insert_val('_edge_', sampid, 'curation', theCuration['pid'])
                if insert_val is not None:
                    INSERT_VALS.append(insert_val)

            if has_context_cat is not None:
                insert_val = get_edge_insert_val('_edge_', sampid, 'has_context_category', has_context_cat['pid'])
                if insert_val is not None:
                    INSERT_VALS.append(insert_val)

            if has_material_cat is not None:
                insert_val = get_edge_insert_val('_edge_', sampid, 'has_material_category', has_material_cat['pid'])
                if insert_val is not None:
                    INSERT_VALS.append(insert_val)

            if has_sample_obj_type is not None:
                insert_val = get_edge_insert_val('_edge_', sampid, 'has_sample_object_type', has_sample_obj_type['pid'])
                if insert_val is not None:
                    INSERT_VALS.append(insert_val)

            if thekeywords is not None:
                for item in thekeywords:
                    insert_val = get_edge_insert_val('_edge_', sampid, 'keywords', item['pid'])
                    if insert_val is not None:
                        INSERT_VALS.append(insert_val)

            # produced_by
            if theSamplingEvent is not None:
                insert_val = get_edge_insert_val('_edge_', sampid, 'produced_by', theSamplingEvent[0]['pid'])
                if insert_val is not None:
                    INSERT_VALS.append(insert_val)

            if theregistrant is not None:
                insert_val = get_edge_insert_val('_edge_', sampid, 'registrant', theregistrant['pid'])
                if insert_val is not None:
                    INSERT_VALS.append(insert_val)

            # related_resource
            if therels is not None:
                for item in therels:
                    insert_val = get_edge_insert_val('_edge_', sampid, 'related_resource', item['pid'])
                    if insert_val is not None:
                        INSERT_VALS.append(insert_val)

            end_tim4 = time.time()
            execution_time = (end_tim4 - loopstart_time) * 1000
            LOGGER.debug(f"total for{str(theobj['sample_id'])}: {execution_time} milliseconds")

            therow += 1
            writebatchsize = 50000
            if therow % writebatchsize == 0:
                try:
                    write_json_lines_fast(INSERT_VALS, 'samples.json')
                    thesql = "INSERT OR IGNORE INTO node SELECT * FROM read_json('samples.json');"
                    with g.getCursor() as csr:
                        csr.execute(thesql)
                    g._connection.commit()
                    INSERT_VALS.clear()
                    LOGGER.info(f'load sample therow: {therow}')
                except Exception as e:
                    LOGGER.info(f'Error inserting samples; exception {repr(e)}')

                end_time = time.time()
                execution_time = end_time - rept_time
                LOGGER.info(f"get {writebatchsize} samples execution time: {execution_time} seconds")
                print(f"load sample {therow}")
                rept_time = time.time()

        LOGGER.info(f'load iteration done. max_id: {max_id}')
        if max_id == sample_max_id:
            # if therow >= 100001:
            print(f'load sample loop done. break')
            break

    if len(INSERT_VALS) > 0:
        write_json_lines_fast(data=INSERT_VALS, filename='samples.json')
        thesql = "INSERT OR IGNORE INTO node SELECT * FROM read_json('samples.json');"
        with g.getCursor() as csr:
            csr.execute(thesql)
        g._connection.commit()
        INSERT_VALS.clear()
        LOGGER.info(f'load sample therow: {therow}')
    return 1


def get_edge_pid(s: str, p: str, o: str,
                 n: pqg.common.OptionalStr = None, ) -> str:
    try:
        h = hashlib.md5()  # smrChange to md5, for shorter random pids
        h.update(s.encode("utf-8"))
        h.update(p.encode("utf-8"))
        h.update(o.encode("utf-8"))
        if n is not None:
            h.update(n.encode("utf-8"))
        thepid = f"anon_{h.hexdigest()}"
    except Exception as e:
        thepid = None
        LOGGER.info(f"get edge pid fail s:{s},p:{p},o:{o}, exception {repr(e)}")
    return thepid


def get_edge_insert_val(otype: str, s: str, p: str, o: str,
                        n: pqg.common.OptionalStr = None):
    edgepid = get_edge_pid(s, p, o, n)
    if edgepid is not None:
        insert_val = get_blank_insert()
        insert_val['otype'] = str(otype),
        insert_val['pid'] = str(edgepid),
        insert_val['s'] = s
        insert_val['p'] = p
        insert_val['o'] = o
        insert_val['n'] = n
        return insert_val
    else:
        return None


def main(dest: str = None):
    loadvocabs = False
    load_agent_lkup = False
    loadsamples = False
    tstart_time = time.time()
    #    insert_vals = []
    #    test_vals = []
    sesarDb = get_2025Connection()
    if sesarDb:
        print("Connection to SESAR2025 PostgresSQL database established successfully.")
    else:
        print("Connection to SESAR2025 PostgresSQL encountered an error.")
        exit()

    theddb = 'sesarduck6.ddb'
    dbinstance = duckdb.connect(theddb)
    g = createGraph(dbinstance)

    thesql = "describe node;"
    with g.getCursor() as csr:
        csr.execute(thesql)
        data = csr.fetchall()
    for item in data:
        INSERTTEMPLATE[item[0]] = None

    concept_lkup = {}
    if loadvocabs:
        vocablist = []
        vocablist.append(['affiliation_type', 'aft', '', 'label',
                          'SESAR Affiliation Type', '', 'affiliation_type_id'])
        vocablist.append(['agent_role_type', 'art', '', 'label',
                          'SESAR agent roles', '', 'agent_role_id'])
        # vocablist.append(['collection_type', '', 'feature_type_uri', 'label',
        #         'SESAR Sampled Feature Type', 'scheme_uri', 'feature_type_id'])
        vocablist.append(['country', 'cty', '', 'label',
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
    else:  # load the cached lookup file; assume that the concept nodes are already in the graph
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
    else:  # load the cached lookup file
        try:
            with open('agent_lkup.pkl', 'rb') as file:
                agent_lkup = pickle.load(file)
        except Exception as e:
            print(f'cached agent_lkup file needs to be created, make load_agent_lkup TRUE')
            exit()

    #    AGENT_PID_LKUP = load_agent_altid_pid_lkup(agent_lkup)

    if loadsamples:
        load_samples(g, concept_lkup, agent_lkup)

    dest = 'sesarTest6'
    try:
        if dest is not None:
            g.asParquet(pathlib.Path(dest))
    except Exception as e:
        LOGGER.info(f'could not generate parquet file {dest}')

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
