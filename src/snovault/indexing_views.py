import time

from pyramid.security import (
    Authenticated,
    Everyone,
    principals_allowed_by_permission,
)
from pyramid.traversal import resource_path
from pyramid.view import view_config
from .resources import Item


def includeme(config):
    config.scan(__name__)


@view_config(context=Item, name='index-data', permission='index', request_method='GET')
def item_index_data(context, request):
    print('indexing views', 'start')
    start_time = time.time()
    uuid = str(context.uuid)
    properties = context.upgrade_properties()
    links = context.links(properties)
    unique_keys = context.unique_keys(properties)

    principals_allowed = {}
    print('indexing views', 'setup_time %.6f' % (time.time() - start_time))

    start_time = time.time()
    for permission in ('view', 'edit', 'audit'):
        principals = principals_allowed_by_permission(context, permission)
        if principals is Everyone:
            principals = [Everyone]
        elif Everyone in principals:
            principals = [Everyone]
        elif Authenticated in principals:
            principals = [Authenticated]
        # Filter our roles
        principals_allowed[permission] = [
            p for p in sorted(principals) if not p.startswith('role.')
        ]
    print('indexing views', 'permission_time %.6f' % (time.time() - start_time))

    start_time = time.time()
    path = resource_path(context)
    paths = {path}
    collection = context.collection
    print('indexing views', 'setup_time 2 %.6f' % (time.time() - start_time))

    start_time = time.time()
    if collection.unique_key in unique_keys:
        paths.update(
            resource_path(collection, key)
            for key in unique_keys[collection.unique_key])
    print('indexing views', 'col_time 1 %.6f' % (time.time() - start_time))

    start_time = time.time()
    for base in (collection, request.root):
        for key_name in ('accession', 'alias'):
            if key_name not in unique_keys:
                continue
            paths.add(resource_path(base, uuid))
            paths.update(
                resource_path(base, key)
                for key in unique_keys[key_name])
    print('indexing views', 'col_time 2 %.6f' % (time.time() - start_time))

    start_time = time.time()
    path = path + '/'
    embedded = request.embed(path, '@@embedded')
    print('indexing views', 'req_embedded %.6f' % (time.time() - start_time))

    start_time = time.time()
    object = request.embed(path, '@@object')
    print('indexing views', 'req_object %.6f' % (time.time() - start_time))

    start_time = time.time()
    audit = request.embed(path, '@@audit')['audit']
    print('indexing views', 'req_audit %.6f' % (time.time() - start_time))

    start_time = time.time()
    document = {
        'audit': audit,
        'embedded': embedded,
        'embedded_uuids': sorted(request._embedded_uuids),
        'item_type': context.type_info.item_type,
        'linked_uuids': sorted(request._linked_uuids),
        'links': links,
        'object': object,
        'paths': sorted(paths),
        'principals_allowed': principals_allowed,
        'properties': properties,
        'propsheets': {
            name: context.propsheets[name]
            for name in context.propsheets.keys() if name != ''
        },
        'tid': context.tid,
        'unique_keys': unique_keys,
        'uuid': uuid,
    }
    print('indexing views', 'doc_time %.6f' % (time.time() - start_time))

    print('indexing views', 'end')
    return document
