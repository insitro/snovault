from os import getpid as os_getpid
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
    INDEXER
)
from .indexer_state import (
    IndexerState,
    all_uuids,
    all_types,
    SEARCH_MAX
)
from .log_index_data import LogIndexData
import datetime
import logging
import pytz
import time
import copy
import json
import requests

es_logger = logging.getLogger("elasticsearch")
es_logger.setLevel(logging.ERROR)
log = logging.getLogger(__name__)
MAX_CLAUSES_FOR_ES = 8192

def includeme(config):
    config.add_route('index', '/index')
    config.scan(__name__)
    registry = config.registry
    registry[INDEXER] = Indexer(registry)

def get_related_uuids(request, es, updated, renamed):
    '''Returns (set of uuids, False) or (list of all uuids, True) if full reindex triggered'''

    updated_count = len(updated)
    renamed_count = len(renamed)
    if (updated_count + renamed_count) > MAX_CLAUSES_FOR_ES:
        return (list(all_uuids(request.registry)), True)  # guaranteed unique
    elif (updated_count + renamed_count) == 0:
        return (set(), False)

    es.indices.refresh('_all')

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


    res = es.search(index='_all', size=SEARCH_MAX, request_timeout=60, body={
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
    })

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
        tmp = []
        for uuid in invalidated:
            tmp.append(uuid)
            if len(tmp) > 100:
                break
        invalidated = tmp
        if len(stage_for_followup) > 0:
            # Note: undones should be added before, because those uuids will (hopefully) be indexed in this cycle
            state.prep_for_followup(xmin, invalidated)

        result = state.start_cycle(invalidated, result)

        # Do the work...

        errors = indexer.update_objects(request, invalidated, xmin, snapshot_id, restart)

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


        es.indices.refresh('_all')
        if flush:
            try:
                es.indices.flush_synced(index='_all')  # Faster recovery on ES restart
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
    is_mp_indexer = False

    def __init__(self, registry):
        self.es = registry[ELASTIC_SEARCH]
        self.esstorage = registry[STORAGE]
        self.index = registry.settings['snovault.elasticsearch.index']
        self.indexer_name = ''
        if self.is_mp_indexer:
            self.index_name = 'mp-'
        if registry.settings.get('indexer'):
            self.index_name += 'primaryindexer'
        elif registry.settings.get('visindexer'):
            self.index_name += 'visindexer'
        elif registry.settings.get('regionindexer'):
            self.index_name += 'regionindexer'
        index_info = {
            'index_name': self.indexer_name
        }
        index_info_keys = [
            'elasticsearch.server',
            'embed_cache.capacity',
            'indexer.chunk_size',
            'indexer.processes',
            'snovault.app_version',
            'snovault.elasticsearch.index',
            'snp_search.server',
            'sqlalchemy.url',
        ]
        for key in index_info_keys:
            index_info[key] = registry.settings.get(key)
        data_log = asbool(registry.settings.get('indexer.data_log'))
        self._index_data = LogIndexData(index_info, data_log=data_log)

    @staticmethod
    def _get_embed_dict(uuid):
        return {
            'doc_embedded': None,
            'doc_linked': None,
            'doc_path': None,
            'doc_type': None,
            'doc_size': None,
            'end_time': None,
            'exception': None,
            'exception_type': None,
            'failed': False,
            'start_time': time.time(),
            'url': "/%s/@@index-data/" % uuid,
        }

    @staticmethod
    def _get_es_dict(backoff):
        return {
            'backoff': backoff,
            'exception': None,
            'exception_type': None,
            'failed': False,
            'start_time': time.time(),
            'end_time': None,
        }

    def _get_run_info(self, uuid_count, xmin, snapshot_id, restart):
        return {
            'end_time': None,
            'indexer_name': self.indexer_name,
            'pid': os_getpid(),
            'restart': restart,
            'snapshot_id': snapshot_id,
            'start_time': time.time(),
            'uuid': 'run_info',
            'uuid_count': uuid_count,
            'xmin': xmin,
        }

    def _post_index_process(self, outputs, run_info):
        '''
        Handles any post processing needed for finished indexing processes
        '''
        print('Indexer', '_post_index_process')
        self._index_data.handle_outputs(outputs, run_info)

    def update_objects(self, request, uuids, xmin, snapshot_id=None, restart=False):
        uuid_count = len(uuids)
        errors = []
        outputs = []
        run_info = self._get_run_info(uuid_count, xmin, snapshot_id, restart)
        for i, uuid in enumerate(uuids):
            output = self.update_object(request, uuid, xmin)
            if output:
                outputs.append(output)
                error = output.get('error')
                if error is not None:
                    errors.append(error)
            if (i + 1) % 50 == 0:
                log.info('Indexing %d', i + 1)
        self._post_index_process(outputs, run_info)
        return errors

    def update_object(self, request, uuid, xmin, restart=False):
        request.datastore = 'database'  # required by 2-step indexer

        # OPTIONAL: restart support
        # If a restart occurred in the middle of indexing, this uuid might have already been indexd, so skip redoing it.
        # if restart:
        #     try:
        #         #if self.es.exists(index=self.index, id=str(uuid), version=xmin, version_type='external_gte'):  # couldn't get exists to work.
        #         result = self.es.get(index=self.index, id=str(uuid), _source_include='uuid', version=xmin, version_type='external_gte')
        #         if result.get('_source') is not None:
        #             return
        #     except:
        #         pass
        # OPTIONAL: restart support
        output = {
            'end_time': None,
            'embed_dict': None,
            'es_dicts': [],
            'pid': os_getpid(),
            'restart': restart,
            'start_time': time.time(),
            'uuid': uuid,
            'xmin': xmin,
        }
        embed_dict = self._get_embed_dict(uuid)
        last_exc = None
        try:
            doc = request.embed(embed_dict['url'], as_user='INDEXER')
        except StatementError:
            # Can't reconnect until invalid transaction is rolled back
            raise
        except Exception as e:
            log.error('Error rendering %s', embed_dict['url'], exc_info=True)
            last_exc = repr(e)
            embed_dict['exception_type'] = 'Embed Exception'
            embed_dict['exception'] = last_exc
        doc_paths = doc.get('paths')
        if doc_paths:
            embed_dict['doc_path'] = doc_paths[0]
        embed_dict['doc_type'] = doc.get('item_type')
        embed_dict['doc_embedded'] = len(doc.get('embedded_uuids', []))
        embed_dict['doc_linked'] = len(doc.get('linked_uuids', []))
        embed_dict['end_time'] = time.time()
        output['embed_dict'] = embed_dict
        if last_exc is None:
            for backoff in [0, 10, 20, 40, 80]:
                es_dict = self._get_es_dict(backoff)
                time.sleep(backoff)
                try:
                    self.es.index(
                        index=doc['item_type'], doc_type=doc['item_type'], body=doc,
                        id=str(uuid), version=xmin, version_type='external_gte',
                        request_timeout=30,
                    )
                except StatementError as ecp:
                    # Can't reconnect until invalid transaction is rolled back
                    es_dict['exception_type'] = 'StatementError Exception'
                    es_dict['exception'] = repr(ecp)
                    es_dict['end_time'] = time.time()
                    output['end_time'] = time.time()
                    output['es_dicts'].append(es_dict)
                    raise
                except ConflictError as ecp:
                    log.warning('Conflict indexing %s at version %d', uuid, xmin)
                    es_dict['exception_type'] = 'ConflictError Exception'
                    es_dict['exception'] = repr(ecp)
                    es_dict['end_time'] = time.time()
                    es_dict['end_time'] = time.time()
                    output['end_time'] = time.time()
                    output['es_dicts'].append(es_dict)
                    return output
                except (ConnectionError, ReadTimeoutError, TransportError) as e:
                    log.warning('Retryable error indexing %s: %r', uuid, e)
                    last_exc = repr(e)
                    es_dict['exception_type'] = 'ConnectionError, ReadTimeoutError, TransportError Exception'
                    es_dict['exception'] = last_exc
                    output['end_time'] = time.time()
                    output['es_dicts'].append(es_dict)
                except Exception as e:
                    log.error('Error indexing %s', uuid, exc_info=True)
                    last_exc = repr(e)
                    es_dict['exception_type'] = 'Catch All Exception'
                    es_dict['exception'] = last_exc
                    es_dict['end_time'] = time.time()
                    output['end_time'] = time.time()
                    output['es_dicts'].append(es_dict)
                    break
                else:
                    # Get here on success and outside of try
                    es_dict['end_time'] = time.time()
                    output['end_time'] = time.time()
                    output['es_dicts'].append(es_dict)
                    return output

        timestamp = datetime.datetime.now().isoformat()
        output['error'] = {
            'error_message': last_exc,
            'timestamp': timestamp,
            'uuid': str(uuid)
        }
        return output

    def shutdown(self):
        pass
