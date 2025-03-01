"""
load data from SESAR 2025 database into duckDb database
save as Parquet file.
based on Code by Dave Vieglais
Modified for SESAR by SM Richard 2025-02-28
"""

import json
import pathlib
import typing
import pqg
from isamples import *
import faker
import psycopg2
import logging
import duckdb

LOGGER = logging.getLogger('sesarParquet')
SESAR_USER_LKUP = {}
INIT_LKUP = {}
COLLECTOR_LKUP = {}
ARCHIVE_LKUP = {}
LOCALITY_LKUP = {}

# connect to database
def get_2025Connection() -> psycopg2.extensions.connection:
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
          'sampled_feature_type',       # populate has_context_category
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




def load_vocab_table(newDb, g, args:list):
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
        return 0
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

        # theConcept = IdentifiedConcept(
        #     pid=thepid,
        #     label=theobj[args[3]],
        #     scheme_name=args[4],
        #     scheme_uri=theschemeuri,
        #     altids=[f"{abbrev}.{str(theobj[args[6]])}"]
        # )
       # g.addNode(theConcept)
        sdata = {}
        sdata['pid'] = thepid
        sdata['label'] = theobj[args[3]],
        sdata['scheme_name'] = args[4],
        sdata['scheme_uri'] = theschemeuri,
        sdata['altids'] = [f"{abbrev}.{str(theobj[args[6]])}"]
        otype = 'IdentifiedConcept'
        s_pid = g.addNodeEntry(otype, sdata)
    return 1


def load_material_type(newDb, g):
    tableName = 'material_type'
    abbrev='mat'
    fieldlist = ['material_type.material_type_id', 'material_type.label',
                'scheme_uri', 'material_type_uri']
    qfields = ','.join(fieldlist)
    selectRecordQuery = 'SELECT ' + qfields + \
        ' FROM material_type join sample on material_type_id = general_material_type_id ' + \
        ' union SELECT ' + qfields + \
        ' FROM material_type join sample_material on material_type.material_type_id = sample_material.material_type_id;'

    LOGGER.debug(f"get_sample_data record query: {repr(selectRecordQuery)}")
    try:
        mat_data = executeQuery(newDb, selectRecordQuery)
    except:
        LOGGER.info('get_sample_data data query failed')
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
        g.addNode(theConcept)

    return 1


def load_individuals(newDb, g):
    # individuals are loaded into Agent nodes
    tableName = 'individual'
    abbrev = 'ind'
    selectRecordQuery = 'SELECT * FROM public.' + tableName
    LOGGER.debug(f"individuals record query: {repr(selectRecordQuery)}")
    try:
        indiv_data = executeQuery(newDb, selectRecordQuery)
    except:
        LOGGER.info('get_sample_data data query failed')
        return None
    thefields = getFields(newDb, tableName)
    for row in indiv_data:
        theobj = {}
        for nc in range(len(row)):
            if row[nc]:
                theobj[thefields[nc]] = row[nc]
            else:
                theobj[thefields[nc]] = None

        if theobj['label']:
            thelabel = theobj['label']
        elif theobj['individual_uri']:
            thelabel = theobj['individual_uri']
        elif theobj['description'] is not None:
            thelabel = theobj['description']
        elif theobj['email'] is not None:
            thelabel = theobj['email']
        else:
            continue  # no label, skip

        if theobj['individual_uri']:
            thepid = theobj['individual_uri']
        else:
            thepid = 'urn:local:' + f"{abbrev}.{str(theobj['individual_id'])}"

        contactinfo = ''
        if theobj['address']:
            contactinfo = theobj['address']
        if theobj['email']:
            if len(contactinfo) > 0:
                contactinfo += ", "
            contactinfo += theobj['email']
        if theobj['phone']:
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
        g.addNode(theagent)
    return 1


def load_locality_lkup(newDb):
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

    return locality_lookup



def load_institution(newDb, g):
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

        if theobj['label']:
            thelabel = theobj['label']
        elif theobj['description'] is not None:
            thelabel = theobj['description']
        elif theobj['email'] is not None:
            thelabel = theobj['email']
        else:
            continue  #no label, skip

        thepid = 'urn:local:' + f"{abbrev}.{str(theobj['institution_id'])}"

        contactinfo = ''
        if theobj['address']:
            contactinfo = theobj['address']
        if theobj['email']:
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
        g.addNode(theagent)
    return 1


def load_collector_lkup(newDb) -> dict:
    # individuals are loaded into Agent nodes
    tableName = 'related_sample_agent'
    selectRecordQuery = "SELECT * FROM related_sample_agent where " + \
                        " relation_type_id = 1 ORDER BY sample_id ASC"
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
        thekey = 'sam.' + str(theobj['sample_id'])
        if theobj['agent_type'] == 'Individual':
            theval = 'ind.'+ str(theobj['individual_id'])
        if theobj['agent_type'] == 'Institution':
            theval = 'ins.' + str(theobj['institution_id'])
        try:
            collectorlkup[thekey].append(theval)
        except:
            collectorlkup[thekey] = [theval]
    return collectorlkup


def load_archive_lkup(newDb) -> dict:
    # individuals are loaded into Agent nodes
    tableName = 'related_sample_agent'
    selectRecordQuery = "SELECT * FROM related_sample_agent where " + \
                        " relation_type_id = 3 ORDER BY sample_id ASC"
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
            theind = 'ind.'+ str(theobj['individual_id'])
        if theobj['agent_type'] == 'Institution':
            theins = 'ins.' + str(theobj['institution_id'])
        archivelkup[thekey] = [theind, theins]
    return archivelkup



def load_initiative_lkup(newDb):
    # individuals are loaded into Agent nodes
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
        if theobj['label']:
            thelabel = theobj['label']
        if len(thelabel) > 0:
            thelabel += '; '
        if theobj['initiative_uri'] is not None:
                thelabel += 'URI--'+theobj['initiative_uri'] + '. '
        if theobj['description']:
            thelabel += theobj['description'] + '. '
        if theobj['funding'] is not None:
            label += 'Funding--' + theobj['initiative_uri']

        thepid = 'urn:local:' + f"{abbrev}.{str(theobj['initiative_id'])}"
        initiativelkup[thepid] = thelabel

    return initiativelkup


def load_sesar_user_lkup(newDb) -> dict:
    tableName = 'sesar_user'
    selectRecordQuery = 'SELECT * FROM public.' + tableName
    LOGGER.debug(f"{tableName} record query: {repr(selectRecordQuery)}")
    try:
        sdata = executeQuery(newDb, selectRecordQuery)
    except:
        LOGGER.info(f'{tableName} data query failed')
        return None
    thefields = getFields(newDb, tableName)
    sesaruserlkup={}
    for row in sdata:
        theobj = {}
        for nc in range(len(row)):
            if row[nc]:
                theobj[thefields[nc]] = row[nc]
            else:
                theobj[thefields[nc]] = None
        if theobj['individual_id']:
            sesaruserlkup[str(theobj['sesar_user'])] = 'ind.' + str(theobj['individual_id'])
        elif theobj['institution_id']:
            sesaruserlkup[str(theobj['sesar_user'])] = 'ind.' + str(theobj['institution_id'])
    return sesaruserlkup

def get_GeospatialCoordLocation(theobj) -> GeospatialCoordLocation:
    lat = theobj['latitude']
    long = theobj['longitude']
    elev = theobj['elevation']

    if lat == None and long == None and elev == None:
        return None
    else:
        return GeospatialCoordLocation(
            pid=f"urn:local:geo.{str(theobj['sample_id'])}",
            latitude=lat,
            longitude=long,
            #obfuscated=generator.boolean(0.1),
            elevation=elev
        )


def GetIdentifiedConcept(g, idin: str) -> IdentifiedConcept:
    try:
        with g.getCursor() as crsr:
            result = crsr.execute("select pid from node where '" + idin + "' in altids")
            apid = result.fetchone()
        theconcept = g.getNodeEntry(pid=apid[0])
    except Exception as e:
        LOGGER.debug(f'get identified concept error: {e}')
        return None
    return theconcept

def get_pid_by_altid(g, idin: str) -> IdentifiedConcept:
    try:
        with g.getCursor() as crsr:
            result = crsr.execute("select pid from node where '" + idin + "' in altids")
            apid = result.fetchone()
    except Exception as e:
        LOGGER.debug(f'get identified concept error: {e}')
        return None
    return apid[0]


def get_Agent(g, theid: str) -> Agent:
    try:
        thepid = get_pid_by_altid(g, theid)
        theagent = g.getNodeEntry(pid=thepid)
    except Exception as e:
        LOGGER.debug(f'get agent error: {e}')
        return None
    return theagent


def get_SamplingEvent(g, theobj,COLLECTOR_LKUP,LOCALITY_LKUP,INIT_LKUP) -> SamplingEvent:
    # description
    collectionDesc = ''
    if theobj['collection_method_id']:
        theconcept = GetIdentifiedConcept(g, 'sam.'+str(theobj['collection_method_id'])),
        collectionDesc = collectionDesc + 'method:' + theconcept[0]['label']
    if theobj['collection_method_detail']:
        if len(collectionDesc) > 0:
            collectionDesc += ', '
        collectionDesc += theobj['collection_method_detail'] + '. '
    if theobj['platform_id']:
        theconcept = GetIdentifiedConcept(g, 'pla.' + str(theobj['platform_id']))
        if theconcept:
            collectionDesc = collectionDesc + ' Platform: ' + theconcept['label']
    if theobj['launch_platform_id']:
        theconcept = GetIdentifiedConcept(g, 'pla.'+str(theobj['launch_platform_id']))
        if theconcept:
            collectionDesc = collectionDesc + ' Launch Platform: ' + theconcept[0]['label']
    if theobj['launch_label']:
        collectionDesc = collectionDesc + ' Launch: ' + theobj['launch_label']

    hcc = None
    if theobj['sampled_feature_type_id'] is not None:
        theconcept = GetIdentifiedConcept(g, 'sft.'+str(theobj['sampled_feature_type_id']))
        hcc = theconcept['label']
    resp = None
    thecollectors = []
    try:
        for collector in COLLECTOR_LKUP['sam.' + str(theobj['sample_id'])]:
            thecollectors.append(get_Agent(g, collector))
    except Exception as e:
        print(f"no collector {theobj['sample_id']}.  {repr(e)}")



    thelocality = None
    try:
        thelocality = LOCALITY_LKUP['locality_id']['label']
    except:
        pass

    thetime = None
    if (theobj['collection_start_date'] is not None) and (theobj['collection_end_date'] is not None):
        thetime = f"{theobj['collection_start_date']}/{theobj['collection_end_date']}"
    elif theobj['collection_start_date']:
        thetime = f"{theobj['collection_start_date']}"
    elif theobj['collection_end_date']:
        thetime = f"{theobj['collection_end_date']}"

    theprj = None
    if theobj['cruise_field_prgrm_id'] is not None:
        theprj = INIT_LKUP['urn:local:ini.'+str(theobj['cruise_field_prgrm_id'])]

    thelabel = None
    if theprj:
        if theobj['launch_label']:
            thelabel = f"{theprj}, {theobj['launch_label']}"
        else:
            thelabel = theprj
    else:
        if theobj['name']:
            thelabel = f"Collection of sample {theobj['name']}"
        else:
            thelabel = f"Collection of sample on "
    if thetime:
        if thelabel:
            thelabel += "; " + thetime

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
        sample_location=get_GeospatialCoordLocation(theobj),
        sampling_site=get_SamplingSite(g,theobj),
    )


def get_SamplingSite(g,theobj) -> SamplingSite:
    thedesc = None
    if theobj['locality_detail'] is not None:
        thedesc = theobj['locality_detail']
    if theobj['latitude_end']:
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
        thelocality = LOCALITY_LKUP(str(theobj['locality_id']))
        placelist = ['province','county','city']
        for item in placelist:
            if thelocality[item]:
                theplaces.append(thelocality[item])
        if thelocality['country_id']:
            theconcept = GetIdentifiedConcept(g,thelocality['country_id'])
            theplaces.append(theconcept['label'])
        if thelocality['name']:
            locname = thelocality['name']
    except:
        pass

    return SamplingSite(
        pid=f"urn:local:sst.{theobj['sample_id']}",
        description=thedesc,
        label=locname,
        place_name=theplaces,
        site_location=None,
        is_part_of=None,
    )


def load_samples(newDb, g):
    tableName = 'sample'
    selectRecordQuery = 'SELECT * FROM public.' + tableName + ' limit 100000'
    LOGGER.debug(f"get_sample_data record query: {repr(selectRecordQuery)}")
    try:
        data = executeQuery(newDb, selectRecordQuery)
    except:
        LOGGER.info('get_sample_data data query failed')
        return
    SESAR_USER_LKUP = load_sesar_user_lkup(newDb)
    if SESAR_USER_LKUP:
        print(f'SESAR_USER_LKUP loaded')
    else:
        print(f'SESAR_USER_LKUP fail !!!!!!')
    INIT_LKUP = load_initiative_lkup(newDb)
    if INIT_LKUP:
        print(f'INIT_LKUP loaded ')
    else:
        print(f'INIT_LKUP fail !!!!!!')
    COLLECTOR_LKUP = load_collector_lkup(newDb)
    if COLLECTOR_LKUP:
        print(f'COLLECTOR_LKUP loaded ')
    else:
        print(f'COLLECTOR_LKUP fail !!!!!!')
    ARCHIVE_LKUP = load_archive_lkup(newDb)
    if ARCHIVE_LKUP:
        print(f'ARCHIVE_LKUP loaded ')
    else:
        print(f'ARCHIVE_LKUP fail !!!!!!')
    LOCALITY_LKUP = load_locality_lkup(newDb)
    if LOCALITY_LKUP:
        print(f'LOCALITY_LKUP loaded ')
    else:
        print(f'LOCALITY_LKUP fail !!!!!!')

    thefields = getFields(newDb, tableName)

    for row in data:
        theobj = {}
        for nc in range(len(row)):
            if row[nc] is None:
                theobj[thefields[nc]] = None
                # replace null values with 'blank'
            else:
                theobj[thefields[nc]] = row[nc]

        theregistrant = None
        if theobj['cur_registrant_id']:
            thepid = SESAR_USER_LKUP[str(theobj['cur_registrant_id'])]
            theregistrant = get_Agent(g, thepid)

        thekeywords = []
        if theobj['geologic_unit']:
            thek = IdentifiedConcept(
             pid='geo_unit.' + str(theobj['sample_id']),
             label=theobj['geologic_unit'],
             scheme_name="Geologic Unit",
             scheme_uri=None )
            thekeywords.append(thek)
        if theobj['geologic_age_older_id']:
            theconcept = GetIdentifiedConcept(g,'gts.'+str(theobj['geologic_age_older_id']))
            theconcept[0]['scheme_name'] = 'Geologic Age Older'
            thekeywords.append(theconcept)
        if theobj['geologic_age_younger_id']:
            theconcept = GetIdentifiedConcept(g,'gts.'+str(theobj['geologic_age_younger_id']))
            theconcept[0]['scheme_name'] = 'Geologic Age Younger'
            thekeywords.append(theconcept)
        if len(thekeywords) == 0:
            thekeywords = None


        ms = MaterialSampleRecord(
            pid=f"sam.{str(theobj['sample_id'])}",
            alternate_identifiers=None,
            complies_with=None,
            curation=None,
            dc_rights=None,
            description=theobj['sample_description'],
            has_context_category=GetIdentifiedConcept(g, 'sft.'+str(theobj['sampled_feature_type_id'])),
                # material category is populated from general_material_type_id, material_name_verbatim
                # and  sample_material.material_type_id
            has_material_category=GetIdentifiedConcept(g, 'mat.'+str(theobj['general_material_type_id'])),

            has_sample_object_type=GetIdentifiedConcept(g, 'sat.'+str(theobj['sample_type_id'])),

            keywords=thekeywords,
            label=theobj['name'],
            last_modified_time=theobj['last_update_date'],
            produced_by=get_SamplingEvent(g,theobj,COLLECTOR_LKUP,LOCALITY_LKUP,INIT_LKUP),
            registrant=theregistrant,
            related_resource=None,
            sample_identifier=theobj['igsn'],
            sampling_purpose=theobj['purpose']
        )
        g.addNode(ms)
        print(f"sam.{str(theobj['sample_id'])}")
    return 1


def get_record(g, pid):
    record = g.getNode(pid)
    # print(record)
    print(json.dumps(record, indent=2, cls=pqg.JSONDateTimeEncoder))


def main(dest: str = None):

    loadvocabs = False
    loadtables = False

    sesarDb = get_2025Connection()
    if sesarDb:
        print("Connection to SESAR2025 PostgreSQL database established successfully.")
    else:
        print("Connection to SESAR2025 PostgreSQL encountered an error.")
        exit()

    theddb ='sesarduck.ddb'
    dbinstance = duckdb.connect(theddb)
    g = createGraph(dbinstance)

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

        for vocab in vocablist:
            result = load_vocab_table(newDb, g, vocab)
            print(f'{vocab[0]} loaded')

    if loadtables:
        result = load_material_type(newDb, g)
        print(f'material type loaded {result}')
        result = load_individuals(newDb,g)
        print(f'individuals loaded {result}')
        result = load_institution(newDb,g)
        print(f'institution loaded {result}')


    result = load_samples(newDb, g)

    dest = 'sesarTest'
    if dest is not None:
        g.asParquet(pathlib.Path(dest))


if __name__ == "__main__":
    #    logging.basicConfig(level=logging.DEBUG)
    logging.basicConfig(filename='pqg.log', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')

    # main("data/test_0.ddb")
    main()
