import pytest


@pytest.fixture()
def dummy_parent(dummy_request):
    from pyramid.testing import DummyResource
    from pyramid.security import Allow
    from snovault.elasticsearch.searches.parsers import ParamsParser
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    from snovault.elasticsearch.interfaces import ELASTIC_SEARCH
    from elasticsearch import Elasticsearch
    dummy_request.registry[ELASTIC_SEARCH] = Elasticsearch()
    dummy_request.context = DummyResource()
    dummy_request.context.__acl__ = lambda: [(Allow, 'group.submitter', 'search_audit')]
    class DummyParent():
        def __init__(self):
            self._meta = {}
            self.response = {}
    dp = DummyParent()
    pp = ParamsParser(dummy_request)
    dp._meta = {
        'params_parser': pp,
        'query_builder': AbstractQueryFactory(pp)
    }
    return dp


def test_searches_fields_response_field_init():
    from snovault.elasticsearch.searches.fields import ResponseField
    rf = ResponseField()
    assert isinstance(rf, ResponseField)


def test_searches_fields_response_field_get_params_parser(dummy_parent):
    from snovault.elasticsearch.searches.fields import ResponseField
    from snovault.elasticsearch.searches.parsers import ParamsParser
    rf = ResponseField(parent=dummy_parent)
    assert isinstance(rf.get_params_parser(), ParamsParser)


def test_searches_fields_response_field_get_request(dummy_parent):
    from snovault.elasticsearch.searches.fields import ResponseField
    from pyramid.request import Request
    rf = ResponseField(parent=dummy_parent)
    assert isinstance(rf.get_request(), Request)


def test_searches_fields_basic_search_response_field_init():
    from snovault.elasticsearch.searches.fields import BasicSearchWithFacetsResponseField
    brf = BasicSearchWithFacetsResponseField()
    assert isinstance(brf, BasicSearchWithFacetsResponseField)


def test_searches_fields_basic_search_response_build_query(dummy_parent):
    from snovault.elasticsearch.searches.fields import BasicSearchWithFacetsResponseField
    from elasticsearch_dsl import Search
    brf = BasicSearchWithFacetsResponseField()
    brf.parent = dummy_parent
    brf._build_query()
    assert isinstance(brf.query, Search)


def test_searches_fields_basic_search_response_register_query(dummy_parent):
    from snovault.elasticsearch.searches.fields import BasicSearchWithFacetsResponseField
    from snovault.elasticsearch.searches.queries import BasicSearchQueryFactoryWithFacets
    brf = BasicSearchWithFacetsResponseField()
    brf.parent = dummy_parent
    brf._build_query()
    assert isinstance(brf.query_builder, BasicSearchQueryFactoryWithFacets)
    brf._register_query()
    assert isinstance(brf.get_query_builder(), BasicSearchQueryFactoryWithFacets)


def test_searches_fields_basic_search_response_execute_query(dummy_parent, mocker):
    from elasticsearch_dsl import Search
    mocker.patch.object(Search, 'execute')
    Search.execute.return_value = []
    from snovault.elasticsearch.searches.fields import BasicSearchWithFacetsResponseField
    brf = BasicSearchWithFacetsResponseField()
    brf.parent = dummy_parent
    brf._build_query()
    brf._execute_query()
    assert Search.execute.call_count == 1


def test_searches_fields_raw_search_with_aggs_response_field_init():
    from snovault.elasticsearch.searches.fields import RawSearchWithAggsResponseField
    rs = RawSearchWithAggsResponseField()
    assert isinstance(rs, RawSearchWithAggsResponseField)


def test_searches_fields_raw_search_with_aggs_response_field_maybe_scan_over_results(dummy_parent, mocker):
    from snovault.elasticsearch.searches.fields import BasicSearchQueryFactoryWithFacets
    from snovault.elasticsearch.searches.fields import RawSearchWithAggsResponseField
    from snovault.elasticsearch.searches.mixins import RawHitsToGraphMixin
    from snovault.elasticsearch.searches.responses import RawQueryResponseWithAggs
    rs = RawSearchWithAggsResponseField()
    rs.parent = dummy_parent
    rs._build_query()
    rs.results = RawQueryResponseWithAggs(
        results={},
        query_builder={}
    )
    rs.response = {'hits': {'hits': []}}
    mocker.patch.object(RawHitsToGraphMixin, 'to_graph')
    mocker.patch.object(BasicSearchQueryFactoryWithFacets, '_should_scan_over_results')
    BasicSearchQueryFactoryWithFacets._should_scan_over_results.return_value = False
    rs._maybe_scan_over_results()
    assert RawHitsToGraphMixin.to_graph.call_count == 0
    BasicSearchQueryFactoryWithFacets._should_scan_over_results.return_value = True
    rs._maybe_scan_over_results()
    assert RawHitsToGraphMixin.to_graph.call_count == 1


def test_searches_fields_title_response_field_init():
    from snovault.elasticsearch.searches.fields import TitleResponseField
    tf = TitleResponseField()
    assert isinstance(tf, TitleResponseField)


def test_searches_fields_title_field_title_value():
    from snovault.elasticsearch.searches.fields import TitleResponseField
    tf = TitleResponseField(title='Search')
    rtf = tf.render()
    assert rtf == {'title': 'Search'}


def test_searches_fields_type_response_field():
    from snovault.elasticsearch.searches.fields import TypeResponseField
    tr = TypeResponseField(at_type=['Snowflake'])
    assert isinstance(tr, TypeResponseField)
    assert tr.render() == {'@type': ['Snowflake']}


def test_searches_fields_context_response_field(dummy_parent):
    from snovault.elasticsearch.searches.fields import ContextResponseField
    cr = ContextResponseField()
    assert isinstance(cr, ContextResponseField)
    assert cr.render(parent=dummy_parent) == {'@context': '/terms/'}


def test_searches_fields_id_response_field(dummy_parent):
    from snovault.elasticsearch.searches.fields import IDResponseField
    dummy_parent._meta['params_parser']._request.environ['QUERY_STRING'] = (
        'type=Experiment&assay_title=Histone+ChIP-seq&award.project=Roadmap'
    )
    ir = IDResponseField()
    assert isinstance(ir, IDResponseField)
    assert ir.render(parent=dummy_parent) == {
        '@id': '/dummy?type=Experiment&assay_title=Histone+ChIP-seq&award.project=Roadmap'
    }


def test_searches_fields_all_response_field_init(dummy_parent):
    from snovault.elasticsearch.searches.fields import AllResponseField
    ar = AllResponseField()
    assert isinstance(ar, AllResponseField)


def test_searches_fields_all_response_field_get_limit(dummy_parent):
    from snovault.elasticsearch.searches.fields import AllResponseField
    ar = AllResponseField()
    ar.parent = dummy_parent
    assert ar._get_limit() == [('limit', 25)]
    ar.parent._meta['params_parser']._request.environ['QUERY_STRING'] = (
        'type=Experiment&assay_title=Histone+ChIP-seq&award.project=Roadmap&limit=99'
    )
    assert ar._get_limit() == [('limit', '99')]


def test_searches_fields_all_response_field_get_qs_with_limit_all(dummy_parent):
    from snovault.elasticsearch.searches.fields import AllResponseField
    ar = AllResponseField()
    ar.parent = dummy_parent
    ar.parent._meta['params_parser']._request.environ['QUERY_STRING'] = (
        'type=Experiment&assay_title=Histone+ChIP-seq&award.project=Roadmap&limit=99'
    )
    assert ar._get_qs_with_limit_all() == (
        'type=Experiment&assay_title=Histone+ChIP-seq'
        '&award.project=Roadmap&limit=all'
    )


def test_searches_fields_all_response_field_maybe_add_all(dummy_parent):
    from snovault.elasticsearch.searches.fields import AllResponseField
    ar = AllResponseField()
    ar.parent = dummy_parent
    ar._maybe_add_all()
    assert 'all' not in ar.response
    ar = AllResponseField()
    ar.parent = dummy_parent
    ar.parent.response.update({'total': 150})
    ar._maybe_add_all()
    assert 'all' in ar.response
    assert ar.response['all'] == '/dummy?limit=all'
    ar = AllResponseField()
    ar.parent = dummy_parent
    ar.parent.response.update({'total': 150})
    ar.parent._meta['params_parser']._request.environ['QUERY_STRING'] = (
        'type=Experiment&assay_title=Histone+ChIP-seq&award.project=Roadmap&limit=99'
    )
    ar._maybe_add_all()
    assert ar.response['all'] == (
        '/dummy?type=Experiment&assay_title=Histone+ChIP-seq'
        '&award.project=Roadmap&limit=all'
    )
    ar = AllResponseField()
    ar.parent = dummy_parent
    ar.parent.response.update({'total': 150})
    ar.parent._meta['params_parser']._request.environ['QUERY_STRING'] = (
        'type=Experiment&assay_title=Histone+ChIP-seq&award.project=Roadmap&limit=all'
    )
    ar._maybe_add_all()
    assert 'all' not in ar.response
    ar = AllResponseField()
    ar.parent = dummy_parent
    ar.parent.response.update({'total': 150})
    ar.parent._meta['params_parser']._request.environ['QUERY_STRING'] = (
        'type=Experiment&assay_title=Histone+ChIP-seq&award.project=Roadmap&limit=200'
    )
    ar._maybe_add_all()
    assert 'all' not in ar.response


def test_searches_fields_notification_response_field_init(dummy_parent):
    from snovault.elasticsearch.searches.fields import NotificationResponseField
    nr = NotificationResponseField()
    assert isinstance(nr, NotificationResponseField)


def test_searches_fields_notification_response_field_results_found(dummy_parent):
    from snovault.elasticsearch.searches.fields import NotificationResponseField
    nr = NotificationResponseField()
    nr.parent = dummy_parent
    assert not nr._results_found()
    nr.parent.response.update({'total': 0})
    assert not nr._results_found()
    nr.parent.response.update({'total': 150})
    assert nr._results_found()


def test_searches_fields_notification_response_field_set_notification(dummy_parent):
    from snovault.elasticsearch.searches.fields import NotificationResponseField
    nr = NotificationResponseField()
    nr.parent = dummy_parent
    assert 'notification' not in nr.response
    nr._set_notification('lots of results')
    assert nr.response['notification'] == 'lots of results'


def test_searches_fields_notification_response_field_set_status_code(dummy_parent):
    from snovault.elasticsearch.searches.fields import NotificationResponseField
    nr = NotificationResponseField()
    nr.parent = dummy_parent
    assert nr.parent._meta['params_parser']._request.response.status_code == 200
    nr._set_status_code(404)
    assert nr.parent._meta['params_parser']._request.response.status_code == 404


def test_searches_fields_filters_response_field_init(dummy_parent):
    from snovault.elasticsearch.searches.fields import FiltersResponseField
    frf = FiltersResponseField()
    assert isinstance(frf, FiltersResponseField)


def test_searches_fields_filters_response_field_get_filters_and_search_terms_from_query_string(dummy_parent):
    dummy_parent._meta['params_parser']._request.environ['QUERY_STRING'] = (
        'type=Experiment&assay_title=Histone+ChIP-seq&award.project=Roadmap'
        '&limit=all&frame=embedded&restricted!=*&searchTerm=ctcf'
    )
    from snovault.elasticsearch.searches.fields import FiltersResponseField
    frf = FiltersResponseField()
    frf.parent = dummy_parent
    expected = [
        ('assay_title', 'Histone ChIP-seq'),
        ('award.project', 'Roadmap'),
        ('restricted!', '*'),
        ('type', 'Experiment'),
        ('searchTerm', 'ctcf')
    ]
    actual = frf._get_filters_and_search_terms_from_query_string()
    assert len(actual) == len(expected)
    assert all([e in actual for e in expected])


def test_searches_fields_filters_response_field_get_path_qs_without_filter(dummy_parent):
    dummy_parent._meta['params_parser']._request.environ['QUERY_STRING'] = (
        'type=Experiment&assay_title=Histone+ChIP-seq&award.project=Roadmap'
        '&limit=all&frame=embedded&restricted!=*&searchTerm=ctcf'
    )
    from snovault.elasticsearch.searches.fields import FiltersResponseField
    frf = FiltersResponseField()
    frf.parent = dummy_parent
    assert frf._get_path_qs_without_filter('type', 'Experiment') == (
        '/dummy?assay_title=Histone+ChIP-seq&award.project=Roadmap&limit=all'
        '&frame=embedded&restricted%21=%2A&searchTerm=ctcf'
    )
    assert frf._get_path_qs_without_filter('searchTerm', 'ctcf') == (
        '/dummy?type=Experiment&assay_title=Histone+ChIP-seq&award.project=Roadmap&limit=all'
        '&frame=embedded&restricted%21=%2A'
    )
    assert frf._get_path_qs_without_filter('searchTerm', 'ctcaf') == (
        '/dummy?type=Experiment&assay_title=Histone+ChIP-seq&award.project=Roadmap&limit=all'
        '&frame=embedded&restricted%21=%2A&searchTerm=ctcf'
    )
    assert frf._get_path_qs_without_filter('restricted!', '*') == (
        '/dummy?type=Experiment&assay_title=Histone+ChIP-seq&award.project=Roadmap&limit=all'
        '&frame=embedded&searchTerm=ctcf'
    )


def test_searches_fields_filters_response_field_make_filter(dummy_parent):
    dummy_parent._meta['params_parser']._request.environ['QUERY_STRING'] = (
        'type=Experiment&assay_title=Histone+ChIP-seq&award.project=Roadmap'
        '&limit=all&frame=embedded&restricted!=*&searchTerm=ctcf'
    )
    from snovault.elasticsearch.searches.fields import FiltersResponseField
    frf = FiltersResponseField()
    frf.parent = dummy_parent
    frf._make_filter('type', 'Experiment')
    assert frf.filters[0] == {
        'field': 'type',
        'remove': '/dummy?assay_title=Histone+ChIP-seq&award.project=Roadmap&limit=all&frame=embedded&restricted%21=%2A&searchTerm=ctcf',
        'term': 'Experiment'
    }


def test_searches_fields_filters_response_field_make_filters(dummy_parent):
    dummy_parent._meta['params_parser']._request.environ['QUERY_STRING'] = (
        'type=Experiment&assay_title=Histone+ChIP-seq&award.project=Roadmap'
        '&limit=all&frame=embedded&restricted!=*&searchTerm=ctcf'
    )
    from snovault.elasticsearch.searches.fields import FiltersResponseField
    frf = FiltersResponseField()
    frf.parent = dummy_parent
    frf._make_filters()
    expected = [
        {
            'remove': '/dummy?type=Experiment&award.project=Roadmap&limit=all&frame=embedded&restricted%21=%2A&searchTerm=ctcf',
            'field': 'assay_title',
            'term': 'Histone ChIP-seq'
        },
        {
            'remove': '/dummy?type=Experiment&assay_title=Histone+ChIP-seq&limit=all&frame=embedded&restricted%21=%2A&searchTerm=ctcf',
            'field': 'award.project',
            'term': 'Roadmap'
        },
        {
            'remove': '/dummy?type=Experiment&assay_title=Histone+ChIP-seq&award.project=Roadmap&limit=all&frame=embedded&searchTerm=ctcf',
            'field': 'restricted!',
            'term': '*'
        },
        {
            'remove': '/dummy?assay_title=Histone+ChIP-seq&award.project=Roadmap&limit=all&frame=embedded&restricted%21=%2A&searchTerm=ctcf',
            'field': 'type',
            'term': 'Experiment'
        },
        {
            'remove': '/dummy?type=Experiment&assay_title=Histone+ChIP-seq&award.project=Roadmap&limit=all&frame=embedded&restricted%21=%2A',
            'field': 'searchTerm',
            'term': 'ctcf'
        }
    ]
    actual = frf.filters
    assert len(actual) == len(expected)
    assert all([e in actual for e in expected])


def test_searches_fields_clear_filter_response_field_get_search_term_or_types_from_query_string(dummy_parent):
    dummy_parent._meta['params_parser']._request.environ['QUERY_STRING'] = (
        'type=Experiment&assay_title=Histone+ChIP-seq&award.project=Roadmap'
        '&limit=all&frame=embedded&restricted!=*&searchTerm=ctcf'
    )
    from snovault.elasticsearch.searches.fields import ClearFiltersResponseField
    cfr = ClearFiltersResponseField()
    cfr.parent = dummy_parent
    search_term_or_types = cfr._get_search_term_or_types_from_query_string()
    assert search_term_or_types == [('searchTerm', 'ctcf')]
    cfr.parent._meta['params_parser']._request.environ['QUERY_STRING'] = (
        'type=Experiment&assay_title=Histone+ChIP-seq&award.project=Roadmap'
        '&limit=all&frame=embedded&restricted!=*'
    )
    search_term_or_types = cfr._get_search_term_or_types_from_query_string()
    assert search_term_or_types == [('type', 'Experiment')]


def test_searches_fields_clear_filter_response_field_get_path_qs_with_no_filters(dummy_parent):
    dummy_parent._meta['params_parser']._request.environ['QUERY_STRING'] = (
        'type=Experiment&assay_title=Histone+ChIP-seq&award.project=Roadmap'
        '&limit=all&frame=embedded&restricted!=*&searchTerm=ctcf'
    )
    from snovault.elasticsearch.searches.fields import ClearFiltersResponseField
    cfr = ClearFiltersResponseField()
    cfr.parent = dummy_parent
    path = cfr._get_path_qs_with_no_filters()
    assert path == '/dummy?searchTerm=ctcf'


def test_searches_fields_clear_filter_response_field_add_clear_filters(dummy_parent):
    dummy_parent._meta['params_parser']._request.environ['QUERY_STRING'] = (
        'type=Experiment&assay_title=Histone+ChIP-seq&award.project=Roadmap'
        '&limit=all&frame=embedded&restricted!=*'
    )
    from snovault.elasticsearch.searches.fields import ClearFiltersResponseField
    cfr = ClearFiltersResponseField()
    cfr.parent = dummy_parent
    cfr._add_clear_filters()
    assert cfr.response['clear_filters'] == '/dummy?type=Experiment'


def test_searches_fields_debug_query_response_field(dummy_parent):
    dummy_parent._meta['params_parser']._request.environ['QUERY_STRING'] = (
        'type=Experiment&assay_title=Histone+ChIP-seq&award.project=Roadmap'
        '&limit=all&frame=embedded&restricted!=*&debug=true'
    )
    dummy_parent._meta['query_builder'].add_post_filters()
    from snovault.elasticsearch.searches.fields import DebugQueryResponseField
    dbr = DebugQueryResponseField()
    r = dbr.render(parent=dummy_parent)
    assert 'query' in r['debug']['raw_query']
    assert 'post_filter' in r['debug']['raw_query']


def test_searches_fields_clear_filter_response_field_add_clear_filters(dummy_parent):
    dummy_parent._meta['params_parser']._request.environ['QUERY_STRING'] = (
        'type=TestingSearchSchema'
    )
    from snovault.elasticsearch.searches.fields import ColumnsResponseField
    crf = ColumnsResponseField()
    r = crf.render(parent=dummy_parent)
    assert r['columns'] == {'accession': {'title': 'Accession'}, 'status': {'title': 'Status'}}
