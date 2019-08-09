from pyramid.view import view_config

from snovault.elasticsearch.searches.interfaces import REPORT_TITLE
from snovault.elasticsearch.searches.interfaces import SEARCH_TITLE
from snovault.elasticsearch.searches.fields import AllResponseField
from snovault.elasticsearch.searches.fields import BasicSearchResponseField
from snovault.elasticsearch.searches.fields import BasicSearchWithFacetsResponseField
from snovault.elasticsearch.searches.fields import BasicReportWithFacetsResponseField
from snovault.elasticsearch.searches.fields import ClearFiltersResponseField
from snovault.elasticsearch.searches.fields import ColumnsResponseField
from snovault.elasticsearch.searches.fields import ContextResponseField
from snovault.elasticsearch.searches.fields import DebugQueryResponseField
from snovault.elasticsearch.searches.fields import FiltersResponseField
from snovault.elasticsearch.searches.fields import IDResponseField
from snovault.elasticsearch.searches.fields import NotificationResponseField
from snovault.elasticsearch.searches.fields import NonSortableResponseField
from snovault.elasticsearch.searches.fields import RawSearchWithAggsResponseField
from snovault.elasticsearch.searches.fields import TitleResponseField
from snovault.elasticsearch.searches.fields import TypeResponseField
from snovault.elasticsearch.searches.parsers import ParamsParser
from snovault.elasticsearch.searches.responses import FieldedResponse


def includeme(config):
    config.add_route('searchv2', '/searchv2{slash:/?}')
    config.add_route('searchv2_raw', '/searchv2_raw{slash:/?}')
    config.add_route('searchv2_quick', '/searchv2_quick{slash:/?}')
    config.add_route('reportv2', '/reportv2{slash:/?}')
    config.scan(__name__)


DEFAULT_ITEM_TYPES = [
    'Lab',
    'Snowset',
    'Snowball',
    'Snowfort',
    'Snowflake',
]


@view_config(route_name='searchv2', request_method='GET', permission='search')
def searchv2(context, request):
    # Note the order of rendering matters for some fields, e.g. AllResponseField and
    # NotificationResponseField depend on results from BasicSearchWithFacetsResponseField.
    fr = FieldedResponse(
        _meta={
            'params_parser': ParamsParser(request)
        },
        response_fields=[
            TitleResponseField(
                title=SEARCH_TITLE
            ),
            TypeResponseField(
                at_type=[SEARCH_TITLE]
            ),
            IDResponseField(),
            ContextResponseField(),
            BasicSearchWithFacetsResponseField(
                default_item_types=DEFAULT_ITEM_TYPES
            ),
            AllResponseField(),
            NotificationResponseField(),
            FiltersResponseField(),
            ClearFiltersResponseField(),
            ColumnsResponseField(),
            DebugQueryResponseField()
        ]
    )
    return fr.render()


@view_config(route_name='searchv2_raw', request_method='GET', permission='search')
def searchv2_raw(context, request):
    fr = FieldedResponse(
        _meta={
            'params_parser': ParamsParser(request)
        },
        response_fields=[
            RawSearchWithAggsResponseField(
                default_item_types=DEFAULT_ITEM_TYPES
            )
        ]
    )
    return fr.render()


@view_config(route_name='searchv2_quick', request_method='GET', permission='search')
def searchv2_quick(context, request):
    fr = FieldedResponse(
        _meta={
            'params_parser': ParamsParser(request)
        },
        response_fields=[
            BasicSearchResponseField(
                default_item_types=DEFAULT_ITEM_TYPES
            )
        ]
    )
    return fr.render()


@view_config(route_name='reportv2', request_method='GET', permission='search')
def reportv2(context, request):
    fr = FieldedResponse(
        _meta={
            'params_parser': ParamsParser(request)
        },
        response_fields=[
            TitleResponseField(
                title=REPORT_TITLE
            ),
            TypeResponseField(
                at_type=[REPORT_TITLE]
            ),
            IDResponseField(),
            ContextResponseField(),
            BasicReportWithFacetsResponseField(),
            AllResponseField(),
            NotificationResponseField(),
            FiltersResponseField(),
            ClearFiltersResponseField(),
            ColumnsResponseField(),
            NonSortableResponseField(),
            DebugQueryResponseField()
        ]
    )
    return fr.render()
