from elasticsearch.exceptions import (
    ConflictError,
    ConnectionError,
    NotFoundError,
    TransportError,
)
from pyramid.view import view_config
from pyramid.settings import asbool
from sqlalchemy.exc import StatementError
from snovault import (
    COLLECTIONS,
    DBSESSION,
    STORAGE
)
from snovault.storage import (
    TransactionRecord,
)
from urllib3.exceptions import ReadTimeoutError
from .interfaces import (
    ELASTIC_SEARCH,
    INDEXER,
    RESOURCES_INDEX,
)
from .indexer_state import (
    IndexerState,
    all_uuids,
    all_types,
    SEARCH_MAX
)
from .simple_queue import SimpleUuidServer

import datetime
import logging
import pytz
import time
import copy
import json
import requests

es_logger = logging.getLogger("elasticsearch")
es_logger.setLevel(logging.ERROR)
log = logging.getLogger('snovault.elasticsearch.es_index_listener')
MAX_CLAUSES_FOR_ES = 8192
DEFAULT_QUEUE = 'Simple'

pg93_uuids=['860c4750-8d3c-40f5-8f2c-90c5e5d19e88','4d07ff68-9372-407c-8479-ad3709a7e917','81a6cc12-2847-4e2e-8f2c-f566699eb29e','0abbd494-b852-433c-b360-93996f679dae','bc5b62f7-ce28-4a1e-b6b3-81c9c5a86d7a','9f599825-97c1-4dfe-86a0-d3c8a2b69759','431a4968-0562-41d2-8ea1-6888e11e0674','9f161054-aa2c-40b7-8239-04eb7f08b15d','0d48e8c4-5954-4bf5-8c75-1e386eb27e31','5e99a8f5-2808-43c6-b1ba-13d5d110a6c1','25fb3bfe-62c1-4c52-a170-cee6b00b5b9a','26e33f32-5dd7-4219-bff1-850c95607127','aa57ecb5-3d0e-4b1f-9608-665785270ad7','ea5e2bd6-be7a-4b18-a2e8-dee86005422f','ad6ffd98-4bd5-4955-ba7c-c3d53da7fc06','ea06d722-1ab1-4488-97c5-0f411359ee07','4fa80b58-322a-4c8e-9b33-d2a00c8405bd','c163343f-7769-45a0-b9a1-e959a8d83935','a174314e-4e7a-4b51-bdab-f5d0a83bfeee','082a3ac8-2c72-4692-95b4-63bb35b78ab9','de7f5444-ee87-4e4e-b621-9c136ea1d4b1','39cd0a5f-9796-4905-8c6d-d01ea4589ae4','4c23ec32-c7c8-4ac0-affb-04befcc881d4','ce2bde01-07ec-4b8a-b179-554ef95b71dd','c57cef43-7038-4f6d-95cc-e1e9425e73ed','df9f3c8e-b819-4885-8f16-08f6ef0001e8','45582c66-7ac6-43cb-a85b-fc2a452e1a95','a8d9311a-be24-473c-8a7e-a1a964d8e4d3','3787a0ac-f13a-40fc-a524-69628b04cd59','a53cb5c0-62fd-4da8-a503-48211db16326','29f3829e-a942-4175-9637-1b691ff268e5','6aa65394-5a5a-4ea9-835e-45da22e09c7c','0bf1abc4-835d-4e9e-a84d-a36be6c6d441','aedcf5ea-3187-4ea2-b384-0c519cc320aa','4ddf3240-907d-40c1-b4fe-4c1547a89b0e','e74547f3-1676-43de-bcfc-9b946f0024c9','95f5b53d-d206-4ec3-97ba-7c2abd84aab5','57d57433-0c1d-4bd0-a230-63b73d0ab6e4','0dbd48ff-057b-413b-bcfa-0c4859716fdf','8be3a346-1440-4bd6-85e8-3236ae469c0c','1965996b-bb36-4d3a-a7ac-93c3fb988a4a','57e13eba-0f09-454e-9035-3198c6bac0e1','9a144e02-c84f-4cc8-b474-49e5c8afa7c7','4640ff09-f1b4-4cc4-9167-6a36f2b8d9bc','0e3dde9b-aaf9-42dd-87f7-975a85072ed2','0576f4d3-de19-4013-8a86-e9805c84033a','f3582cd0-995c-4549-82b7-5adede87638c','09d05b87-4d30-4dfb-b243-3327005095f2','e6784ced-4a3e-4f4d-a1b8-a3034a73a5e4','62ae40d7-b5d1-498d-8106-f102fb1e14cf','f56a4449-1a87-4824-8a34-9c6917544b1b','6b61df51-5359-4c23-ec36-e9e18870f53c','077c758e-f7e9-47f0-bf8d-c862bc45bde8','6183c69b-e862-41ab-99be-69214fc9f27e','6800d05f-7213-48b1-9ad8-254c73c5b83f','ec729133-c5f3-4c53-be5b-a5ea73a367dd','f5b7857d-208e-4acc-ac4d-4c2520814fe1','8f9f5064-3e2f-4182-8925-c66ab6b24e15','bf270458-cfaf-4366-b951-dfc14b03dc3f','746eef27-d857-4b38-a469-cac93fb02164','17a8d3bf-b027-4d5f-aa49-0c8c14ada1a8','b0a10b06-9ad0-4e59-aac9-a7797781d29e','864c3e01-dce4-4f41-925a-383ccc82ee5d','4f6e1132-f893-4011-8197-848187303a10','1ec85168-059e-407e-9cb1-f96200e72499','6b769b78-e629-45a4-a860-37b25b0d364b','7b198f29-c02f-4776-927b-f852002709bf','f74511d2-d30a-44d9-8e54-79104353e7af','4ad5a69d-3b59-4ff1-a633-aca619efb97d','27e105ca-c741-4459-bf17-90e003508639','9b52a07a-e46f-4b74-bbe3-e5fd45b768e0','f1843c60-e027-4b18-8582-64d3f3eae45b','78f58cc3-fb9a-49b0-b636-66fd9b980ceb','68810334-0023-46f8-85a3-df29c39ef127','5d76a10b-1f18-4544-a00e-d0d83f1021a1','2eb068c5-b7a6-48ec-aca2-c439e4dabb08','0a61ce48-e33d-48bf-8b6b-b085b722b467','a62cfec5-57a0-45ab-b943-8ca0e0057bb6','9303af54-3ae9-4ba1-9765-5ede86490daf','046eea49-9ab6-4e4d-ba42-be8f8fc92de3','1281f6d9-6acc-4045-a22f-da80f13c61e8','e9248e0d-e840-4415-b509-9c1e5df57dc2','4b135999-ec2d-41b5-b962-fc6a5f8934ac','178014f7-0e8f-4a01-a3d9-1088d2b86e7f','073079fd-d589-4663-9fd4-3d488208d075','fc2db6c5-3c30-4aae-dd5d-96e2607211d0','5aa7758b-02fe-4f40-a255-bdc0fd0eabfb','261fcf1a-04d9-4879-a56a-320915587586','d48be354-153c-4ca8-acaa-bf067c1fd836','cc0edf23-b9fd-48e2-8609-54ad07bdd5f7','ac9a9291-0f98-44a6-9ecc-b2127a351e27','50a19cb0-542f-473e-9f59-2b8fac97b253','494d1118-529d-47ac-bd42-f2b2bbc8d5bd','6282e6de-8696-4fa1-91e4-415e729558e9','ff7b77e7-bb55-4307-b665-814c9f1e65fb','599e4901-78bf-422f-b65d-f3443c6a6450','0598c868-0b4a-4c5b-9112-8f85c5de5374','94c85be4-e034-4647-b6b3-15055701a656','edf008cd-c076-4a86-838e-c6d189ac5395','d9143089-89df-481c-b322-6e1fe7bb6e9f']

pg11_uuids=['2b139ca2-462a-4bf7-a6b9-503fb32462cf','bb3cff23-ee12-4a1e-81f7-d33995d10979','a8bb9d00-1b96-4d92-b24c-bd9bc2f6d8f8','6cffce7a-cc80-4ea4-a4af-1c05739a0d37','7a7a82a3-ec1c-48f7-b467-03d6bf5ed9cb','898d48fc-277e-4491-acd0-bb38bfae08a8','63841779-247a-466a-96e4-2adfe2f276dc','759f35af-7a00-4af7-8383-b3937ba1855c','9c77c326-e656-4202-b0af-5f42631f7750','78e2c43d-7341-4279-ad33-725020274b4f','de13e10d-3551-47fe-b806-d6a44b5be5e4','15285ba7-034a-4b40-8a0d-3bb74d05f507','d3b64f6b-aee5-49e6-a34f-0b712fd99d17','cd85a7cb-eaa5-4593-ad77-fe5fa01cdcc9','eadced27-935c-463c-9f45-224a27460cff','99c45d10-3509-4346-a0be-95b45527fe90','4bcb541d-b93c-4bf5-9a93-b8b8f6322d37','0daf8429-8ec2-4da8-bb48-4bf0d5581ff6','72160565-ed43-43a0-9702-dde2d3f3be9a','78eff075-7a73-4b30-8987-e5b81fdef072','bf09c1e0-52a5-4efb-acbd-88671257964d','d48bba76-bf96-4be3-ab56-3043658a8003','74aeb0b7-6910-4b11-ac8d-742bbd85a4c7','9f30b89b-e071-497b-8987-a50195f4b81f','f47c5519-35d6-4b8c-88ca-616b2c95f24a','f00cc59e-fd7b-4092-a553-912548345c55','8dff523e-0edf-41bd-892e-65870e71a1c7','30f931c7-fb48-4c8b-8563-28a624664763','6450a19f-298d-4e81-9abb-a762af581ee9','f9aae557-174d-4147-b14c-ae2e393447f1','71b93bcf-83ec-4102-aef7-8050073d685d','0c958f52-d9b6-49b4-97de-ad7c87224c17','4a3f2064-9b19-4cb0-8aeb-9336a59e8ffd','b183237a-4ba4-4fd2-9db2-7b0431a336c1','31275a66-1c3a-4fe4-a7ea-c160b5fb57ed','9a3648ee-a8ab-43cb-8fa9-d299866b002a','892a3357-34ff-4897-852a-d103c66b6112','1f95d746-58d8-4970-b884-b91775836962','747633e0-0d5d-4bb1-b849-d51b1ce849f9','11408f10-0424-4e46-b53c-c8af75976e2d','22c2c4cd-d82a-4ce9-bfac-a784f1f2a3dc','9eb5be63-de93-438d-b3c0-0eb9764dfbe3','54ec20ad-7ca2-48b0-a726-f46ce53c54f1','67b5f75a-cc1b-4282-9a65-ae927466bc00','d9182a27-573e-4d4c-ba21-6dde9a7d3fab','ee08b91a-48a5-46b9-af4f-a7f09830bcea','529ef5bd-1548-446f-9543-bf4aa7be8114','12385e98-bfc5-4278-b01f-b51e6dd2a795','3387d16c-27b7-4352-b9f4-67004bb53427','5a886ef7-84c5-4dfc-bdc5-285f5ae0b92b','7ea2827b-8f3b-446b-b450-a556b8283309','6bae687f-b77a-46b9-af0e-a02c135cf42e','6d01284b-a57a-40eb-8404-1f479e35f57a','21283635-a78d-4ef1-9937-c3fdd105ddda','3815298c-8cd7-49e7-bc53-7e8d27619c25','426fd42b-1f6a-483f-a6b1-31fc752b9e80','61bb5696-732d-4412-aa44-2dc136d5ad65','ca6e2de3-a6bd-4fce-8b6d-52ef41e7c0c5','7e95dcd6-9c35-4082-9c53-09d14c5752be','00057821-3393-4bee-b4d3-4f162e2bae01','4e4a43d4-3b62-46ba-b101-88e1041422ee','afa1b51e-5535-4981-ba0a-5d95bda72a25','fd86980d-e1db-4fe3-8908-4feb80987011','2cc22839-ba45-4e27-92e9-85e569c22c1a','0662b05e-97ab-4356-ab0c-f339973ec38c','955b8eeb-6d60-4794-b304-60826f7aa880','546039fd-0a31-4692-9ec3-d17e5c207446','411fbf1c-7f45-472b-99d8-c514abd1b193','47d4577c-edd4-4ec1-a672-20ca499b4339','38dc09c2-4c05-49d2-b73d-4b75261fcbf1','6532863f-0b3a-4d95-93a6-6b7695018cdb','a31bf6b7-e610-4f5e-9a92-115deca3a73a','4a98b6bf-c4fc-48f8-b3d8-f09b68e77e9c','82ff6145-221b-4b1a-90c5-efbff6e6ef03','acddeb3e-cdba-4159-9a47-875cf06045a9','454435fe-702d-4657-a955-641e5631982c','f39eb4b2-5ccb-4f75-9b92-19e9b1e2abc5','c440dede-3cc8-4e8a-b037-e963fd49b854','82c0d9b2-478c-45f5-b547-cc2e6ce0d5ce','0db98457-a91e-4cde-b058-a0c972c008e3','7b7c1e6a-8f6e-4b0d-a7a7-9232b95e0590','9fc24d9f-fb94-479c-84f8-79c8814cda8e','efd5feb1-ed9b-46dd-ba98-0114b0ad91cc','9bb19b7d-842d-4a77-ba9a-79a2e7473970','f6a752e6-930e-4111-a577-fdb0437cbe58','780c7991-e27d-4ca0-b37f-9d9f540441b4','1efd7e96-08f6-4619-927e-0d8e9ae94977','4136f132-304e-4ddd-b87a-db04605f47b7','363e67f3-7fb2-4122-9feb-54703e819ce6','8f8f5231-705e-4f67-b977-c2e243af39c9','56786789-9842-47be-b27d-261b46a0d64c','0684ce25-9511-438e-9ccd-d72662dbd0eb','cfda8eae-fdf9-4652-a81a-5dfe23ea36ab','f93e33f3-5089-436d-b03c-16b755437b54','10206575-5737-467d-b101-e12b6895f19a','abe70cb0-72ee-4c06-afa1-6a829256cd74','3e023c0a-9467-4d5f-a3db-d1b57df2f89f','9799c729-0270-4515-b283-e8e0a74e6fb5','af0e7005-5148-4cac-a140-3ab324fd5d11','ee35bea3-b6e4-4959-8ef9-d5fdfd89554d']


def _update_for_uuid_queues(registry):
    """
    Update registry with uuid queue module if it exists
    """
    extra_queues = []
    try:
        import snovault.elasticsearch.uuid_queue as queue_adapter
    except ImportError as ecp:
        log.info('No uuid_queue package in elasticsearch module: %s', repr(ecp))
    else:
        registry['UuidQueue'] = queue_adapter.QueueAdapter
        extra_queues = queue_adapter.QueueTypes.get_all()
        log.info('Extra Indexer Queues Available: %s', ','.join(extra_queues))
    registry['available_queues'].extend(extra_queues)


def includeme(config):
    """Add index listener endpoint and setup Indexer"""
    config.add_route('index', '/index')
    config.scan(__name__)
    registry = config.registry
    processes = registry.settings.get('indexer.processes')
    is_indexer = registry.settings.get('indexer')
    if is_indexer:
        available_queues = [DEFAULT_QUEUE]
        registry['available_queues'] = available_queues
        _update_for_uuid_queues(registry)
        if not processes:
            registry[INDEXER] = Indexer(registry)

def get_related_uuids(request, es, updated, renamed):
    '''Returns (set of uuids, False) or (list of all uuids, True) if full reindex triggered'''

    updated_count = len(updated)
    renamed_count = len(renamed)
    if (updated_count + renamed_count) > MAX_CLAUSES_FOR_ES:
        return (list(all_uuids(request.registry)), True)  # guaranteed unique
    elif (updated_count + renamed_count) == 0:
        return (set(), False)

    es.indices.refresh(RESOURCES_INDEX)

    # TODO: batching may allow us to drive a partial reindexing much greater than 99999
    #BATCH_COUNT = 100  # NOTE: 100 random uuids returned > 99999 results!
    #beg = 0
    #end = BATCH_COUNT
    #related_set = set()
    #updated_list = list(updated)  # Must be lists
    #renamed_list = list(renamed)
    #while updated_count > beg or renamed_count > beg:
    #    if updated_count > end or beg > 0:
    #        log.error('Indexer looking for related uuids by BATCH[%d,%d]' % (beg, end))
    #
    #    updated = []
    #    if updated_count > beg:
    #        updated = updated_list[beg:end]
    #    renamed = []
    #    if renamed_count > beg:
    #        renamed = renamed_list[beg:end]
    #
    #     search ...
    #     accumulate...
    #
    #    beg += BATCH_COUNT
    #    end += BATCH_COUNT

    query = {
        'query': {
            'bool': {
                'should': [
                    {
                        'terms': {
                            'embedded_uuids': updated,
                            '_cache': False,
                        },
                    },
                    {
                        'terms': {
                            'linked_uuids': renamed,
                            '_cache': False,
                        },
                    },
                ],
            },
        },
        '_source': False,
    }
    res = es.search(index=RESOURCES_INDEX, size=SEARCH_MAX, request_timeout=60, body=query)

    if res['hits']['total'] > SEARCH_MAX:
        return (list(all_uuids(request.registry)), True)  # guaranteed unique

    related_set = {hit['_id'] for hit in res['hits']['hits']}

    return (related_set, False)



@view_config(route_name='index', request_method='POST', permission="index")
def index(request):
    INDEX = request.registry.settings['snovault.elasticsearch.index']
    # Setting request.datastore here only works because routed views are not traversed.
    request.datastore = 'database'
    record = request.json.get('record', False)
    dry_run = request.json.get('dry_run', False)
    recovery = request.json.get('recovery', False)
    es = request.registry[ELASTIC_SEARCH]
    indexer = request.registry[INDEXER]
    session = request.registry[DBSESSION]()
    connection = session.connection()
    first_txn = None
    snapshot_id = None
    restart=False
    invalidated = []
    xmin = -1

    # Currently 2 possible followup indexers (base.ini [set stage_for_followup = vis_indexer, region_indexer])
    stage_for_followup = list(request.registry.settings.get("stage_for_followup", '').replace(' ','').split(','))

    # May have undone uuids from prior cycle
    state = IndexerState(es, INDEX, followups=stage_for_followup)

    (xmin, invalidated, restart) = state.priority_cycle(request)
    state.log_reindex_init_state()
    # OPTIONAL: restart support
    if restart:  # Currently not bothering with restart!!!
        xmin = -1
        invalidated = []
    # OPTIONAL: restart support

    result = state.get_initial_state()  # get after checking priority!

    if xmin == -1 or len(invalidated) == 0:
        xmin = get_current_xmin(request)

        last_xmin = None
        if 'last_xmin' in request.json:
            last_xmin = request.json['last_xmin']
        else:
            status = es.get(index=INDEX, doc_type='meta', id='indexing', ignore=[400, 404])
            if status['found'] and 'xmin' in status['_source']:
                last_xmin = status['_source']['xmin']
        if last_xmin is None:  # still!
            if 'last_xmin' in result:
                last_xmin = result['last_xmin']
            elif 'xmin' in result and result['xmin'] < xmin:
                last_xmin = result['state']

        result.update(
            xmin=xmin,
            last_xmin=last_xmin,
        )

    if len(invalidated) > SEARCH_MAX:  # Priority cycle already set up
        flush = True
    else:

        flush = False
        if last_xmin is None:
            result['types'] = types = request.json.get('types', None)
            invalidated = list(all_uuids(request.registry, types))
            flush = True
        else:
            txns = session.query(TransactionRecord).filter(
                TransactionRecord.xid >= last_xmin,
            )

            invalidated = set(invalidated)  # not empty if API index request occurred
            updated = set()
            renamed = set()
            max_xid = 0
            txn_count = 0
            for txn in txns.all():
                txn_count += 1
                max_xid = max(max_xid, txn.xid)
                if first_txn is None:
                    first_txn = txn.timestamp
                else:
                    first_txn = min(first_txn, txn.timestamp)
                renamed.update(txn.data.get('renamed', ()))
                updated.update(txn.data.get('updated', ()))

            if invalidated:        # reindex requested, treat like updated
                updated |= invalidated

            result['txn_count'] = txn_count
            if txn_count == 0 and len(invalidated) == 0:
                state.send_notices()
                return result

            (related_set, full_reindex) = get_related_uuids(request, es, updated, renamed)
            if full_reindex:
                invalidated = related_set
                flush = True
            else:
                invalidated = related_set | updated
                result.update(
                    max_xid=max_xid,
                    renamed=renamed,
                    updated=updated,
                    referencing=len(related_set),
                    invalidated=len(invalidated),
                    txn_count=txn_count
                )
                if first_txn is not None:
                    result['first_txn_timestamp'] = first_txn.isoformat()

            if invalidated and not dry_run:
                # Exporting a snapshot mints a new xid, so only do so when required.
                # Not yet possible to export a snapshot on a standby server:
                # http://www.postgresql.org/message-id/CAHGQGwEtJCeHUB6KzaiJ6ndvx6EFsidTGnuLwJ1itwVH0EJTOA@mail.gmail.com
                if snapshot_id is None and not recovery:
                    snapshot_id = connection.execute('SELECT pg_export_snapshot();').scalar()

    if invalidated and not dry_run:
        invalid = []
        for uuid in invalidated:
            if len(invalid) < 100:
                invalid.append(uuid)
        invalidated = invalid
        invalid = copy.copy(pg93_uuids)
        if len(stage_for_followup) > 0:
            # Note: undones should be added before, because those uuids will (hopefully) be indexed in this cycle
            state.prep_for_followup(xmin, invalidated)

        result = state.start_cycle(invalidated, result)

        # Do the work...

        errors, err_msg = indexer.serve_objects(
            request,
            invalidated,
            xmin,
            snapshot_id=snapshot_id,
            restart=restart,
        )
        if err_msg:
            log.warning('Could not start indexing: %s', err_msg)
        result = state.finish_cycle(result,errors)
        if errors:
            result['errors'] = errors
        if record:
            try:
                es.index(index=INDEX, doc_type='meta', body=result, id='indexing')
            except:
                error_messages = copy.deepcopy(result['errors'])
                del result['errors']
                es.index(index=INDEX, doc_type='meta', body=result, id='indexing')
                for item in error_messages:
                    if 'error_message' in item:
                        log.error('Indexing error for {}, error message: {}'.format(item['uuid'], item['error_message']))
                        item['error_message'] = "Error occured during indexing, check the logs"
                result['errors'] = error_messages
        es.indices.refresh(RESOURCES_INDEX)
        if False and flush:
            try:
                es.indices.flush_synced(index=RESOURCES_INDEX)  # Faster recovery on ES restart
            except ConflictError:
                pass

    if first_txn is not None:
        result['txn_lag'] = str(datetime.datetime.now(pytz.utc) - first_txn)
    state.send_notices()
    return result


def get_current_xmin(request):
    session = request.registry[DBSESSION]()
    connection = session.connection()
    recovery = request.json.get('recovery', False)

    # http://www.postgresql.org/docs/9.3/static/functions-info.html#FUNCTIONS-TXID-SNAPSHOT
    if recovery:
        query = connection.execute(
            "SET TRANSACTION ISOLATION LEVEL READ COMMITTED, READ ONLY;"
            "SELECT txid_snapshot_xmin(txid_current_snapshot());"
        )
    else:
        query = connection.execute(
            "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE, READ ONLY, DEFERRABLE;"
            "SELECT txid_snapshot_xmin(txid_current_snapshot());"
        )
    # DEFERRABLE prevents query cancelling due to conflicts but requires SERIALIZABLE mode
    # which is not available in recovery.
    xmin = query.scalar()  # lowest xid that is still in progress
    return xmin


class Indexer(object):
    def __init__(self, registry):
        self.es = registry[ELASTIC_SEARCH]
        self.esstorage = registry[STORAGE]
        self.index = registry.settings['snovault.elasticsearch.index']
        self.queue_server = None
        self.queue_server_backup = None
        self.queue_worker = None
        self.chunk_size = None
        self.batch_size = None
        self.worker_runs = []
        if registry.settings.get('indexer'):
            self._setup_queues(registry)

    def _setup_queues(self, registry):
        '''Init helper - Setup server and worker queues'''
        queue_type = registry.settings.get('queue_type', None)
        is_queue_server = asbool(registry.settings.get('queue_server'))
        is_queue_worker = asbool(registry.settings.get('queue_worker'))
        queue_options = self._get_queue_options(registry)
        self.chunk_size = queue_options['chunk_size']
        self.batch_size = queue_options['batch_size']
        if is_queue_server:
            cp_q_ops = queue_options.copy()
            cp_q_ops['batch_size'] = cp_q_ops['get_size']
            self.queue_server_backup = SimpleUuidServer(cp_q_ops)
            if (
                    not queue_type or
                    queue_type == DEFAULT_QUEUE or
                    queue_type not in registry['available_queues']
                ):
                self.queue_type = DEFAULT_QUEUE
                self.queue_server = self.queue_server_backup
                self.queue_server_backup = None
            elif 'UuidQueue' in registry:
                try:
                    queue_options['uuid_len'] = 36
                    self.queue_server = registry['UuidQueue'](
                        queue_options['queue_name'],
                        queue_type,
                        queue_options,
                    )
                except Exception as exp:  # pylint: disable=broad-except
                    log.warning(repr(exp))
                    log.warning(
                        'Failed to initialize UuidQueue. Switching to backup.'
                    )
                    self._serve_object_switch_queue()
                else:
                    self.queue_type = queue_type
            else:
                log.error('No queue available for Indexer')
            if self.queue_server and is_queue_worker:
                self.queue_worker = self.queue_server.get_worker()
            log.warning('Primary indexer queue type: %s', self.queue_type)

    @staticmethod
    def _get_queue_options(registry):
        '''Init helper - Extract queue options from registry settings'''
        queue_name = registry.settings.get('queue_name', 'indxQ')
        queue_worker_processes = int(
            registry.settings.get('queue_worker_processes', 1)
        )
        queue_worker_chunk_size = int(
            registry.settings.get('queue_worker_chunk_size', 1024)
        )
        queue_worker_batch_size = int(
            registry.settings.get('queue_worker_batch_size', 5000)
        )
        queue_worker_get_size = int(
            registry.settings.get('queue_worker_get_size', 2000000)
        )
        # Only Used for Redis Queues
        queue_host = registry.settings.get('queue_host', 'localhost')
        queue_port = registry.settings.get('queue_port', 6379)
        queue_db = registry.settings.get('queue_db', 2)
        return {
            'queue_name': queue_name,
            'processes': queue_worker_processes,
            'chunk_size': queue_worker_chunk_size,
            'batch_size': queue_worker_batch_size,
            'get_size': queue_worker_get_size,
            'host': queue_host,
            'port': queue_port,
            'db': queue_db,
        }

    def _serve_object_switch_queue(self, set_worker=False):
        # If a non simple queue server fails we end up back here
        # on the next pass of the index listenter with the original
        # list of uuids(given the functionality of the indexer_state)
        # For any failure we switch to simple backup server permanently.
        self.queue_server = self.queue_server_backup
        self.queue_server_backup = None
        self.queue_type = DEFAULT_QUEUE
        if set_worker:
            self.queue_worker = self.queue_server.get_worker()


    def _serve_objects_init(self, uuids):
        err_msg = 'Cannot initialize indexing process: '
        try:
            is_indexing = self.queue_server.is_indexing()
            if is_indexing:
                return err_msg + 'Already Indexing'
            elif not uuids:
                return err_msg + 'No uuids given to Indexer.serve_objects'
        except Exception as exp:  # pylint: disable=broad-except
            log.warning(repr(exp))
            if self.queue_server_backup:
                log.warning('uuid init issue:  Switching to simple server.')
                self._serve_object_switch_queue(set_worker=True)
            else:
                return err_msg + 'Cannot failover to simple queue'
        return None

    def _serve_objects_load_uuids(self, uuids):
        err_msg = None
        try:
            uuids_loaded_len = self.queue_server.load_uuids(uuids)
            if not uuids_loaded_len:
                err_msg = 'Uuids given to Indexer.serve_objects failed to load'
            elif uuids_loaded_len != len(uuids):
                err_msg = (
                    'Uuids given to Indexer.serve_objects '
                    'failed to all load. {} of {} only'.format(
                        uuids_loaded_len,
                        len(uuids),
                    )
                )
        except Exception as exp:  # pylint: disable=broad-except
            log.warning(repr(exp))
            err_msg = 'Indexer load uuids failed.'
            if self.queue_server_backup:
                log.warning('uuid load issue:  Switching to simple server')
                self._serve_object_switch_queue(set_worker=True)
        return err_msg

    def serve_objects(
            self,
            request,
            uuids,
            xmin,
            snapshot_id=None,
            restart=False,
            timeout=None,
        ):
        '''Run indexing process with queue server and optional worker'''
        # pylint: disable=too-many-arguments
        errors = []
        err_msg = self._serve_objects_init(uuids)
        if err_msg:
            return errors, err_msg
        err_msg = self._serve_objects_load_uuids(uuids)
        if err_msg:
            return errors, err_msg
        # Run Process Loop
        start_time = time.time()
        self.worker_runs = []
        while self.queue_server.is_indexing(errs_cnt=len(errors)):
            if self.queue_worker and not self.queue_worker.is_running:
                # Server Worker
                uuids_ran = self.run_worker(
                    request, xmin, snapshot_id, restart
                )
                if not uuids_ran:
                    break
                self.worker_runs.append({
                    'worker_id':self.queue_worker.worker_id,
                    'uuids': uuids_ran,
                })
            # Handling Errors must happen or queue will not stop
            batch_errors = self.queue_server.pop_errors()
            for error in batch_errors:
                errors.append(error)
            if timeout and time.time() - start_time > timeout:
                err_msg = 'Indexer sleep timeout'
                break
        self.queue_server.close_indexing()
        return errors, err_msg

    def run_worker(self, request, xmin, snapshot_id, restart):
        '''Run the uuid queue worker'''
        batch_uuids = self.queue_worker.get_uuids(get_all=False)
        log.warning(
            'running %s with %d',
            self.queue_worker.worker_id,
            len(batch_uuids),
        )
        if batch_uuids:
            self.queue_worker.is_running = True
            batch_errors = self.update_objects(
                request,
                batch_uuids,
                xmin,
                snapshot_id=snapshot_id,
                restart=restart,
            )
            batch_results = {
                'errors': batch_errors,
                'successes': len(batch_uuids) - len(batch_errors),
            }
            err_msg = self.queue_worker.update_finished(batch_results)
            if err_msg:
                log.warning('Issue closing worker: %s', err_msg)
            self.queue_worker.is_running = False
            return len(batch_uuids)
        else:
            log.warning('No uudis to run %d', self.queue_worker.get_cnt)
        return None

    def update_objects(
            self,
            request,
            uuids,
            xmin,
            snapshot_id=None,
            restart=False,
        ):
        # pylint: disable=too-many-arguments, unused-argument
        '''Run indexing process on uuids'''
        errors = []
        for i, uuid in enumerate(uuids):
            update_info = self.update_object(self.es, request, uuid, xmin)
            error = update_info.get('error')
            if error is not None:
                errors.append(error)
            if (i + 1) % 1000 == 0:
                log.info('Indexing %d', i + 1)
                print('****')
                print("%.6f" % (update_info['start_time']))
                print("%.6f" % (update_info['end_time']))
                print("%.6f" % (update_info['run_time']))
        return errors

    @staticmethod
    def update_object(encoded_es, request, uuid, xmin, restart=False):
        update_info = {
            'uuid': uuid,
            'xmin': xmin,
            'start_time': time.time(),
            'end_time': None,
            'run_time': None,
            'error': None,
            'return_time': None,
        }
        req_info = {
            'start_time': None,
            'end_time': None,
            'run_time': None,
            'errors': [],
            'url': None
        }
        es_info = {
            'start_time': None,
            'end_time': None,
            'run_time': None,
            'backoffs': {},
            'item_type': None,
        }
        request.datastore = 'database'
        last_exc = None
        req_info['start_time'] = time.time()
        backoff = 0
        try:
            req_info['url'] ='/%s/@@index-data/' % uuid
            doc = request.embed(req_info['url'], as_user='INDEXER')
        except StatementError:
            # Can't reconnect until invalid transaction is rolled back
            raise
        except Exception as e:
            msg = 'Error rendering /%s/@@index-data' % uuid
            log.error(msg, exc_info=True)
            last_exc = repr(e)
            req_info['errors'].append(
                {
                    'backoff': backoff,
                    'msg': msg,
                    'last_exc': last_exc,
                }
            )
        req_info['end_time'] = time.time()
        req_info['run_time'] = req_info['end_time'] - req_info['start_time']
        if last_exc is None:
            es_info['start_time'] = time.time()
            es_info['item_type'] = doc['item_type']
            do_break = False
            for backoff in [0, 10, 20, 40, 80]:
                time.sleep(backoff)
                backoff_info = {
                    'start_time': time.time(),
                    'end_time': None,
                    'run_time': None,
                    'error': [],
                }
                try:
                    encoded_es.index(
                        index=doc['item_type'], doc_type=doc['item_type'], body=doc,
                        id=str(uuid), version=xmin, version_type='external_gte',
                        request_timeout=30,
                    )
                except StatementError:
                    # Can't reconnect until invalid transaction is rolled back
                    raise
                except ConflictError:
                    msg = 'Conflict indexing %s at version %d' % (uuid, xmin)
                    log.warning(msg)
                    backoff_info['error'].append(
                        {
                            'msg': msg,
                            'last_exc': None,
                        }
                    )
                    do_break = True
                except (ConnectionError, ReadTimeoutError, TransportError) as e:
                    msg = 'Retryable error indexing %s: %r' % (uuid, e)
                    log.warning(msg)
                    last_exc = repr(e)
                    backoff_info['error'].append(
                        {
                            'msg': msg,
                            'last_exc': last_exc,
                        }
                    )
                except Exception as e:
                    msg = 'Error indexing %s' % (uuid)
                    log.error(msg, exc_info=True)
                    last_exc = repr(e)
                    backoff_info['error'].append(
                        {
                            'msg': msg,
                            'last_exc': None,
                        }
                    )
                    do_break = True
                else:
                    # Get here on success and outside of try
                    do_break = True
                end_time = time.time()
                backoff_info['end_time'] = end_time
                backoff_info['run_time'] = end_time - backoff_info['start_time']
                es_info['backoffs'][str(backoff)] = backoff_info
                if do_break:
                    break
            es_info['end_time'] = time.time()
            es_info['run_time'] = es_info['end_time'] - es_info['start_time']
        update_info['req_info'] = req_info
        update_info['es_info'] = es_info
        if last_exc:
            update_info['error'] = {
                'error_message': last_exc,
                'timestamp': datetime.datetime.now().isoformat(),
                'uuid': str(uuid)
            }
        end_time = time.time()
        update_info['end_time'] = end_time
        update_info['run_time'] = end_time - update_info['start_time']
        return update_info

    def shutdown(self):
        pass
