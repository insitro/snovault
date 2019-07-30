import pytest


@pytest.fixture()
def dummy_parent(dummy_request):
    from snovault.elasticsearch.searches.parsers import ParamsParser
    class DummyParent():
        def __init__(self):
            self._meta = {}
    dp = DummyParent()
    dp._meta = {
        'params_parser': ParamsParser(dummy_request)
    }
    return dp


def test_searches_fields_response_field_init():
    from snovault.elasticsearch.searches.fields import ResponseField
    rf = ResponseField()
    assert isinstance(rf, ResponseField)


def test_searches_fields_response_field_get_params_parser():
    from snovault.elasticsearch.searches.fields import ResponseField
    rf = ResponseField()
    assert False


def test_searches_fields_response_field_get_request():
    from snovault.elasticsearch.searches.fields import ResponseField
    rf = ResponseField()
    assert False


def test_searches_fields_basic_search_response_field_init():
    from snovault.elasticsearch.searches.fields import BasicSearchWithFacetsResponseField
    brf = BasicSearchWithFacetsResponseField()
    assert isinstance(brf, BasicSearchWithFacetsResponseField)


def test_searches_fields_basic_search_response_build_query():
    from snovault.elasticsearch.searches.fields import BasicSearchWithFacetsResponseField
    brf = BasicSearchWithFacetsResponseField()
    assert False


def test_searches_fields_basic_search_response_execute_query():
    from snovault.elasticsearch.searches.fields import BasicSearchWithFacetsResponseField
    brf = BasicSearchWithFacetsResponseField()
    assert False


def test_searches_fields_basic_search_response_format_results_query():
    from snovault.elasticsearch.searches.fields import BasicSearchWithFacetsResponseField
    brf = BasicSearchWithFacetsResponseField()
    assert False


def test_searches_fields_basic_search_response_render():
    from snovault.elasticsearch.searches.fields import BasicSearchWithFacetsResponseField
    brf = BasicSearchWithFacetsResponseField()
    assert False


def test_searches_fields_raw_search_with_aggs_response_field_init():
    from snovault.elasticsearch.searches.fields import RawSearchWithAggsResponseField
    rs = RawSearchWithAggsResponseField()
    assert isinstance(rs, RawSearchWithAggsResponseField)


def test_searches_fields_raw_search_with_aggs_response_field_execute_query():
    from snovault.elasticsearch.searches.fields import RawSearchWithAggsResponseField
    rs = RawSearchWithAggsResponseField()
    assert False


def test_searches_fields_raw_search_with_aggs_response_field_format_results():
    from snovault.elasticsearch.searches.fields import RawSearchWithAggsResponseField
    rs = RawSearchWithAggsResponseField()
    assert False


def test_searches_fields_raw_search_with_aggs_response_field_render():
    from snovault.elasticsearch.searches.fields import RawSearchWithAggsResponseField
    rs = RawSearchWithAggsResponseField()
    assert False


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


def test_searches_fields_all_response_field(dummy_parent):
    from snovault.elasticsearch.searches.fields import AllResponseField
    ar = AllResponseField()
    assert isinstance(ar, AllResponseField)
    assert False

