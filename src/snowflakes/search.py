import re
from urllib.parse import urlencode
from pyramid.view import view_config
from pyramid.httpexceptions import HTTPBadRequest
from pyramid.security import effective_principals
from elasticsearch.helpers import scan
from snovault import (
    AbstractCollection,
    TYPES,
)
from snovault.viewconfigs.searchview import SearchView
from snovault.elasticsearch import ELASTIC_SEARCH
from snovault.elasticsearch.interfaces import RESOURCES_INDEX
from snovault.resource_views import collection_view_listing_db
from snovault.helpers.helper import (
    sort_query,
    get_pagination,
    get_filtered_query,
    prepare_search_term,
    set_sort_order,
    get_search_fields,
    list_visible_columns_for_schemas,
    list_result_fields,
    set_filters,
    set_facets,
    format_results,
    normalize_query,
    iter_long_json
)


CHAR_COUNT = 32



@view_config(route_name='search', request_method='GET', permission='search')
def search(context, request, search_type=None, return_generator=False, default_doc_types=None, views=None, search_result_actions=None):
    search = SearchView(context, request, search_type, return_generator, default_doc_types)
    return search.preprocess_view(views=views, search_result_actions=search_result_actions)

@view_config(route_name='report', request_method='GET', permission='search')
def report(context, request):
    doc_types = request.params.getall('type')
    if len(doc_types) != 1:
        msg = 'Report view requires specifying a single type.'
        raise HTTPBadRequest(explanation=msg)

    # schemas for all types
    types = request.registry[TYPES]

    # Get the subtypes of the requested type
    try:
        sub_types = types[doc_types[0]].subtypes
    except KeyError:
        # Raise an error for an invalid type
        msg = "Invalid type: " + doc_types[0]
        raise HTTPBadRequest(explanation=msg)

    # Raise an error if the requested type has subtypes.
    if len(sub_types) > 1:
        msg = 'Report view requires a type with no child types.'
        raise HTTPBadRequest(explanation=msg)

    # Ignore large limits, which make `search` return a Response
    # -- UNLESS we're being embedded by the download_report view
    from_, size = get_pagination(request)
    if ('limit' in request.GET and request.__parent__ is None
            and (size is None or size > 1000)):
        del request.GET['limit']
    # Reuse search view
    res = search(context, request)

    # change @id, @type, and views
    res['views'][0] = {
        'href': res['@id'],
        'title': 'View results as list',
        'icon': 'list-alt',
    }
    search_base = normalize_query(request)
    res['@id'] = '/report/' + search_base
    # TODO add this back one day
    # res['download_tsv'] = request.route_path('report_download') + search_base
    res['title'] = 'Report'
    res['@type'] = ['Report']
    return res