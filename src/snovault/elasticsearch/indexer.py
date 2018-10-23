'''Indexer Listener Callback'''
import datetime
import logging
import copy
import itertools
import sys
import time

import pytz

from elasticsearch.exceptions import ConflictError as ESConflictError
from pyramid.view import view_config
from pyramid.settings import asbool

from snovault import DBSESSION
from snovault.storage import TransactionRecord
from snovault.elasticsearch.primary_indexer import PrimaryIndexer
from snovault.elasticsearch.mpindexer import MPIndexer

from .indexer_state import (
    IndexerState,
    all_uuids,
    SEARCH_MAX
)
from .interfaces import (
    ELASTIC_SEARCH,
    INDEXER
)
from .uuid_queue import (
    UuidQueue,
    UuidQueueTypes,
    UuidQueueWorker,
)


log = logging.getLogger('snovault.elasticsearch.es_index_listener')  # pylint: disable=invalid-name
MAX_CLAUSES_FOR_ES = 8192
SHORT_INDEXING = None  # Falsey value will turn off short
PY2 = sys.version_info.major == 2
QUEUE_NAME = 'indexQ'
QUEUE_TYPE = UuidQueueTypes.REDIS_LIST_PIPE
BATCH_GET_SIZE = 100


def includeme(config):
    '''Initialize ES Indexers'''
    config.registry['DEBUG_RESET_QUEUE'] = True
    config.add_route('index', '/index')
    config.add_route('index_worker', '/index_worker')
    config.scan(__name__)
    is_indexer = asbool(
        config.registry.settings.get(INDEXER, False)
    )
    is_index_worker = asbool(
        config.registry.settings.get('index_worker', False)
    )
    processes = get_processes(config.registry)
    if (is_indexer or is_index_worker) and not config.registry.get(INDEXER):
        if processes == 1 or PY2:
            log.info('Initialized Single %s', INDEXER)
            config.registry[INDEXER] = PrimaryIndexer(config.registry)
        else:
            log.info('Initialized Multi %s', INDEXER)
            config.registry[INDEXER] = MPIndexer(
                config.registry,
                processes=processes
            )


def get_processes(registry):
    '''Get indexer processes as integer'''
    processes = registry.settings.get('indexer.processes')
    try:
        processes = int(processes)
    except (TypeError, ValueError):
        processes = None
    return processes


def _do_record(index_listener, result):
    '''
    Helper for index view_config function
    Runs after _run_index if record parameter was in request
    '''
    try:
        index_listener.registry_es.index(
            index=index_listener.index_registry_key,
            doc_type='meta',
            body=result,
            id='indexing'
        )
    except Exception as ecp:  # pylint: disable=broad-except
        log.warning('Index listener: %r', ecp)
        error_messages = copy.deepcopy(result['errors'])
        del result['errors']
        index_listener.registry_es.index(
            index=index_listener.index_registry_key,
            doc_type='meta',
            body=result,
            id='indexing'
        )
        for item in error_messages:
            if 'error_message' in item:
                log.error(
                    'Indexing error for %s, error message: %s',
                    item['uuid'],
                    item['error_message']
                )
                item['error_message'] = "Error occured during indexing, check the logs"
        result['errors'] = error_messages


def get_current_xmin(request):
    '''Determine Postgres minimum transaction'''
    session = request.registry[DBSESSION]()
    connection = session.connection()
    recovery = request.json.get('recovery', False)
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
    xmin = query.scalar()  # lowest xid that is still in progress
    return xmin


def get_related_uuids(request, registry_es, updated, renamed):
    '''
    Returns (set of uuids, False) or
    (list of all uuids, True) if full reindex triggered
    '''
    updated_count = len(updated)
    renamed_count = len(renamed)
    if (updated_count + renamed_count) > MAX_CLAUSES_FOR_ES:
        return (list(all_uuids(request.registry)), True)
    elif (updated_count + renamed_count) == 0:
        return (set(), False)
    registry_es.indices.refresh('_all')
    res = registry_es.search(
        index='_all',
        size=SEARCH_MAX,
        request_timeout=60,
        body={
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
    )
    if res['hits']['total'] > SEARCH_MAX:
        return (list(all_uuids(request.registry)), True)
    related_set = {hit['_id'] for hit in res['hits']['hits']}
    return (related_set, False)


@view_config(route_name='index_worker', request_method='POST', permission="index")
def index_worker(request):
    '''Run Worker, server must be started'''
    skip_consume = 0
    uuid_queue_worker = _get_uuid_queue_worker(request)
    _run_uuid_queue_worker(uuid_queue_worker, request, skip_consume)
    return {}


def _get_uuid_queue_worker(request):
    client_options = {
        'host': request.registry.settings['redis_ip'],
        'port': request.registry.settings['redis_port'],
    }
    uuid_queue_worker = UuidQueueWorker(
        QUEUE_NAME,
        QUEUE_TYPE,
        client_options,
    )
    return uuid_queue_worker


def _run_uuid_queue_worker(
        uuid_queue_worker,
        request,
        skip_consume,
        uuid_queue_server=None,
        max_age_secs=7200,
        listener_restarted=False,
    ):
    '''index_worker helper that can be used from index listener too'''
    indexer = request.registry[INDEXER]
    log_tag = 'wrk'
    if uuid_queue_server:
        log_tag = 'srv'
    if uuid_queue_worker.server_ready():
        if uuid_queue_worker.queue_running():
            print('index worker uuid_queue.queue_running looping')
            processed = 0
            while uuid_queue_worker.queue_running():
                batch_id, uuids, _ = uuid_queue_worker.get_uuids(
                    get_count=BATCH_GET_SIZE
                )
                if batch_id and uuids:
                    if skip_consume > 0:
                        skip_consume -= 1
                    else:
                        indexer.log_store = []
                        errors = indexer.update_objects(
                            request,
                            uuids,
                            uuid_queue_worker.xmin,
                            is_reindex=False,
                            log_tag=log_tag,
                        )
                        successes = len(uuids) - len(errors)
                        processed += successes
                        uuid_queue_worker.add_finished(
                            batch_id,
                            successes,
                            errors,
                            batch_logs=indexer.log_store,
                        )
                        indexer.log_store = []
                time.sleep(0.05)
                if uuid_queue_server:
                    return uuid_queue_server.is_finished(
                        max_age_secs=max_age_secs,
                        listener_restarted=listener_restarted,
                    )
            print('run_worker done', processed)
    # return values for uuid_queue_server
    # not used in real worker
    return [], False

class UuidStore(object):
    '''
    Consumable holder for uuids
    - Previously called 'invalidated'
    '''
    def __init__(self):
        self.uuids = set()

    def is_empty(self):
        '''Returns true if store has uuids'''
        if self.uuids:
            return False
        return True

    def over_threshold(self, threshold):
        '''Returns true if store is greater than threshold'''
        if len(self.uuids) > threshold:
            return True
        return False


class IndexListener(object):
    '''Encapsulated index view config functionality'''
    def __init__(self, request):
        self.session = request.registry[DBSESSION]()
        self.dry_run = request.json.get('dry_run', False)
        self.index_registry_key = request.registry.settings['snovault.elasticsearch.index']
        self.uuid_store = UuidStore()
        self.registry_es = request.registry[ELASTIC_SEARCH]
        self.request = request
        self.xmin = -1

    def _get_transactions(self, last_xmin, first_txn=None):
        'Check postgres transaction with last xmin'''
        txns = self.session.query(TransactionRecord).filter(
            TransactionRecord.xid >= last_xmin,
        )
        updated = set()
        renamed = set()
        max_xid = 0
        txn_count = 0
        for txn in txns.all():
            txn_count += 1
            max_xid = max(max_xid, txn.xid)
            if not first_txn:
                first_txn = txn.timestamp
            else:
                first_txn = min(first_txn, txn.timestamp)
            renamed.update(txn.data.get('renamed', ()))
            updated.update(txn.data.get('updated', ()))
        return renamed, updated, txn_count, max_xid, first_txn

    def get_current_last_xmin(self, result):
        '''Handle xmin and last_xmin'''
        xmin = get_current_xmin(self.request)
        last_xmin = None
        if 'last_xmin' in self.request.json:
            last_xmin = self.request.json['last_xmin']
        else:
            status = self.registry_es.get(
                index=self.index_registry_key,
                doc_type='meta',
                id='indexing',
                ignore=[400, 404]
            )
            if status['found'] and 'xmin' in status['_source']:
                last_xmin = status['_source']['xmin']
        if last_xmin is None:
            if 'last_xmin' in result:
                last_xmin = result['last_xmin']
            elif 'xmin' in result and result['xmin'] < xmin:
                last_xmin = result['state']
        return xmin, last_xmin

    def get_txns_and_update(self, last_xmin, result):
        '''Get PG Transaction and check against uuid_store'''
        (renamed, updated, txn_count,
         max_xid, first_txn) = self._get_transactions(last_xmin)
        result['txn_count'] = txn_count
        if txn_count == 0 and self.uuid_store.is_empty():
            return None, txn_count
        flush = None
        if not self.uuid_store.is_empty():
            updated |= self.uuid_store.uuids
        related_set, full_reindex = get_related_uuids(
            self.request,
            self.registry_es,
            updated,
            renamed
        )
        if full_reindex:
            self.uuid_store.uuids = related_set
            flush = True
        else:
            self.uuid_store.uuids = related_set | updated
            result.update(
                max_xid=max_xid,
                renamed=renamed,
                updated=updated,
                referencing=len(related_set),
                invalidated=len(self.uuid_store.uuids),
            )
            if first_txn is not None:
                result['first_txn_timestamp'] = first_txn.isoformat()
        return flush, txn_count

    def set_priority_cycle(self, indexer_state):
        '''Call priority cycle and update self'''
        (xmin, uuids_set, restart) = indexer_state.priority_cycle(self.request)
        indexer_state.log_reindex_init_state()
        # Currently not bothering with restart!!!
        if restart:
            xmin = -1
            uuids_set = set()
        self.uuid_store.uuids = uuids_set
        self.xmin = xmin
        return restart

    def short_uuids(self, short_to=100):
        '''
        Limit uuids to index for debugging
        '''
        if short_to <= 0:
            short_to = 100
        self.uuid_store.uuids = set(itertools.islice(
            self.uuid_store.uuids, short_to
        ))

    def try_set_snapshot_id(self, recovery, snapshot_id):
        '''Check for snapshot_id in postgres under certain conditions'''
        if not self.uuid_store.is_empty() and not self.dry_run:
            if snapshot_id is None and not recovery:
                connection = self.session.connection()
                snapshot_id = connection.execute(
                    'SELECT pg_export_snapshot();'
                ).scalar()
        return snapshot_id


@view_config(route_name='index', request_method='POST', permission="index")
def index(request):
    '''Index listener for main indexer'''
    # pylint: disable=too-many-branches, too-many-locals
    request.datastore = 'database'
    followups = list(
        request.registry.settings.get(
            "stage_for_followup",
            ''
        ).replace(' ', '').split(',')
    )
    index_listener = IndexListener(request)
    indexer_state = IndexerState(
        index_listener.registry_es,
        index_listener.index_registry_key,
        followups=followups
    )
    restart = index_listener.set_priority_cycle(indexer_state)
    result = indexer_state.get_initial_state()
    snapshot_id = None
    first_txn = None
    last_xmin = None
    if index_listener.xmin == -1 or index_listener.uuid_store.is_empty():
        tmp_xmin, last_xmin = index_listener.get_current_last_xmin(result)
        result.update(
            xmin=tmp_xmin,
            last_xmin=last_xmin,
        )
        index_listener.xmin = tmp_xmin
    client_options = {
        'host': request.registry.settings['redis_ip'],
        'port': request.registry.settings['redis_port'],
    }
    uuid_queue = UuidQueue(
        QUEUE_NAME,
        QUEUE_TYPE,
        client_options,
    )
    if asbool(request.registry['DEBUG_RESET_QUEUE']):
        print('purging')
        uuid_queue.purge()
        request.registry['DEBUG_RESET_QUEUE'] = False
        return result
    flush = False
    if index_listener.uuid_store.over_threshold(SEARCH_MAX):
        flush = True
    elif last_xmin is None:
        result['types'] = types = request.json.get('types', None)
        index_listener.uuid_store.uuids = set(
            all_uuids(request.registry, types)
        )
        flush = True
    else:
        tmp_flush, txn_count = index_listener.get_txns_and_update(last_xmin, result)
        if txn_count == 0 and index_listener.uuid_store.is_empty():
            indexer_state.send_notices()
            uuid_queue.purge()
            return result
        if tmp_flush:
            flush = tmp_flush
        snapshot_id = index_listener.try_set_snapshot_id(
            request.json.get('recovery', False),
            snapshot_id
        )
    if index_listener.uuid_store.is_empty():
        uuid_queue.purge()
    elif not index_listener.dry_run:
        has_server_worker = False
        _run_index(
            index_listener,
            indexer_state,
            uuid_queue,
            result,
            restart,
            snapshot_id,
            request,
            has_server_worker=has_server_worker,
        )

        if request.json.get('record', False):
            _do_record(index_listener, result)
        index_listener.registry_es.indices.refresh('_all')
        if flush:
            try:
                index_listener.registry_es.indices.flush_synced(index='_all')
            except ESConflictError as ecp:
                log.warning('Index listener ESConflictError: %r', ecp)
    if first_txn is not None:
        result['txn_lag'] = str(datetime.datetime.now(pytz.utc) - first_txn)
    indexer_state.send_notices()
    return result


def _run_index(
        index_listener,
        indexer_state,
        uuid_queue,
        result,
        restart,
        snapshot_id,
        request,
        has_server_worker=False
    ):
    '''
    Helper for index view_config function
    Runs the indexing processes for index listener
    '''
    # pylint: disable=too-many-arguments
    if indexer_state.followups:
        indexer_state.prep_for_followup(
            index_listener.xmin,
            index_listener.uuid_store.uuids
        )
    uuid_queue_run_args = {
        'batch_by': BATCH_GET_SIZE,
        'uuid_len': 36,
        'xmin': index_listener.xmin,
        'snapshot_id': snapshot_id,
        'restart': restart,
    }
    listener_restarted = False
    did_fail = True
    indexer = index_listener.request.registry[INDEXER]
    uuids = index_listener.uuid_store.uuids
    if uuid_queue.queue_running():
        print('indexer uuid_queue.queue_running')
        listener_restarted = True
        did_fail = False
    else:
        if SHORT_INDEXING:
            # If value is truthly then uuids will be limited.
            log.warning(
                'Shorting UUIDS from %d to %d',
                len(index_listener.uuid_store.uuids),
                SHORT_INDEXING,
            )
            index_listener.short_uuids(SHORT_INDEXING)
        result, did_fail = init_cycle(
            uuid_queue,
            indexer,
            uuids,
            indexer_state,
            result,
            uuid_queue_run_args,
        )
    if did_fail:
        log.warning(
            'Index initalization failed for %d uuids.',
            len(uuids)
        )
    else:
        errors = server_loop(
            uuid_queue,
            uuid_queue_run_args,
            request,
            listener_restarted=listener_restarted,
            has_server_worker=has_server_worker,
        )
        result = indexer_state.finish_cycle(result, errors)
        indexer.clear_state()
        if errors:
            result['errors'] = errors


def init_cycle(uuid_queue, indexer, uuids, indexer_state, result, run_args):
    # pylint: disable=too-many-arguments
    '''Starts an index cycle'''
    did_pass = uuid_queue.initialize(run_args)
    did_fail = True
    if did_pass:
        did_fail = False
        failed, success_cnt, call_cnt = uuid_queue.load_uuids(uuids)
        print('indexer init_cycle load_uuids', failed, success_cnt, call_cnt)
        if not success_cnt:
            did_fail = True
        else:
            indexer.set_state(
                indexer_state.is_initial_indexing,
                indexer_state.is_reindexing,
            )
            result = indexer_state.start_cycle(uuids, result)
    return result, did_fail


def server_loop(
        uuid_queue,
        run_args,
        request,
        listener_restarted=False,
        has_server_worker=False,
    ):
    '''wait for workers to finish loop'''
    skip_consume = 0
    max_age_secs = 7200
    queue_done = False
    errors = None
    uuid_queue_worker = None
    if has_server_worker:
        uuid_queue_worker = _get_uuid_queue_worker(request)
    print('server looping')
    while not queue_done:
        if uuid_queue_worker:
            readd_uuids, queue_done = _run_uuid_queue_worker(
                uuid_queue_worker,
                request,
                skip_consume,
                uuid_queue_server=uuid_queue,
                max_age_secs=max_age_secs,
                listener_restarted=listener_restarted,
            )
        else:
            readd_uuids, queue_done = uuid_queue.is_finished(
                max_age_secs=max_age_secs, listener_restarted=listener_restarted,
            )
        if readd_uuids:
            if listener_restarted:
                if not uuid_queue.initialize(run_args):
                    print('restart issue, not solved')
            uuid_queue.load_uuids(
                readd_uuids,
                readded=True,
            )
        if queue_done:
            errors, _ = uuid_queue.get_errors()
        time.sleep(1.00)
    print('done, try readding errors?', len(errors))
    uuid_queue.stop()
    return errors
