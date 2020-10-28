from zope.interface import Interface

from snovault import COLLECTIONS


# Registry tool id
APP_FACTORY = 'app_factory'
ELASTIC_SEARCH = 'elasticsearch'
SNP_SEARCH_ES = 'snp_search'
INDEXER = 'indexer'
RESOURCES_INDEX = 'snovault-resources'

# Shared Vars
SEARCH_MAX = 99999  # OutOfMemoryError if too high


class ICachedItem(Interface):
    """ Marker for cached Item
    """


def all_types(registry):
    collections = registry[COLLECTIONS]
    return sorted(collections.by_item_type)


def all_uuids(registry, types=None):
    # First index user and access_key so people can log in
    collections = registry[COLLECTIONS]
    initial = ['user', 'access_key']
    for collection_name in initial:
        collection = collections.by_item_type.get(collection_name, [])
        # for snovault test application, there are no users or keys
        if types is not None and collection_name not in types:
            continue
        for uuid in collection:
            yield str(uuid)
    uuid_generator_map = {}
    for collection_name in sorted(collections.by_item_type):
        if collection_name in initial:
            continue
        if types is not None and collection_name not in types:
            continue
        collection = collections.by_item_type[collection_name]
        uuid_generator_map[collection_name] = (
            uuid
            for uuid in collection
        )
    for uuid in heterogeneous_stream(uuid_generator_map):
        yield str(uuid)


def heterogeneous_stream(generator_map):
    '''
    Will zip together generators and yield until all are exhausted, e.g.:

    >>> generator_map = {
        'experiments': (e for e in experiment_uuids),
        'files': (f for f in file_uuids)
    }
    >>> assert list(heterogeneous_stream(generator_map)) == [e1, f1, e2, f2, ..., eN, fM]

    where:
         N is number of experiment uuids
         M is number of file uuids
         N doesn't have to equal M

    This allows for structured mixing of collections, though the stream will become
    more homogeneous as shorter collections are exhausted.
    '''
    for x in chain(*zip_longest(*generator_map.values())):
        if x is None:
            continue
        yield x
