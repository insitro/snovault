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
    print('')
    print('indexing_views.py:item_index_data', '@@index-data start', request.url)
    start_time = time.time()
    uuid = str(context.uuid)
    properties = context.upgrade_properties()
    links = context.links(properties)
    unique_keys = context.unique_keys(properties)

    principals_allowed = {}

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

    start_time = time.time()
    path = resource_path(context)
    paths = {path}
    collection = context.collection

    start_time = time.time()
    if collection.unique_key in unique_keys:
        paths.update(
            resource_path(collection, key)
            for key in unique_keys[collection.unique_key])

    start_time = time.time()
    for base in (collection, request.root):
        for key_name in ('accession', 'alias'):
            if key_name not in unique_keys:
                continue
            paths.add(resource_path(base, uuid))
            paths.update(
                resource_path(base, key)
                for key in unique_keys[key_name])

    start_time = time.time()
    path = path + '/'
    # embedded endpoint is in resource_views.py:item_view_embedded
    print('indexing_views.py:item_index_data', '@@index-data start', path)
    embedded = request.embed(path, '@@embedded')
    print('indexing_views.py:item_index_data', '@@embedded %s %.6f' % (path, time.time() - start_time))
    asdf
    start_time = time.time()
    object = request.embed(path, '@@object')

    start_time = time.time()
    audit = request.embed(path, '@@audit')['audit']

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

    print('indexing_views.py:item_index_data', '@@index-data end', request.url)
    return document
