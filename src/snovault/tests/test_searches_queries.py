import pytest


@pytest.fixture
def params_parser(dummy_request):
    from snovault.elasticsearch.searches.parsers import ParamsParser
    from snovault.elasticsearch import ELASTIC_SEARCH
    from elasticsearch import Elasticsearch
    dummy_request.environ['QUERY_STRING'] = (
        'type=Experiment&assay_title=Histone+ChIP-seq&award.project=Roadmap'
        '&assembly=GRCh38&biosample_ontology.classification=primary+cell'
        '&target.label=H3K27me3&biosample_ontology.classification%21=cell+line'
        '&biosample_ontology.term_name%21=naive+thymus-derived+CD4-positive%2C+alpha-beta+T+cell'
        '&limit=10&status=released&searchTerm=chip-seq&sort=date_created&sort=-files.file_size'
        '&field=@id&field=accession'
    )
    dummy_request.registry[ELASTIC_SEARCH] = Elasticsearch()
    return ParamsParser(dummy_request)


@pytest.fixture
def params_parser_snovault_types(dummy_request):
    from snovault.elasticsearch.searches.parsers import ParamsParser
    from snovault.elasticsearch import ELASTIC_SEARCH
    from elasticsearch import Elasticsearch
    dummy_request.environ['QUERY_STRING'] = (
        'type=TestingSearchSchema&status=released'
        '&limit=10&field=@id&field=accession'
    )
    dummy_request.registry[ELASTIC_SEARCH] = Elasticsearch()
    return ParamsParser(dummy_request)


def test_searches_queries_abstract_query_factory_init():
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory({})
    assert isinstance(aq, AbstractQueryFactory)


def test_searches_queries_abstract_query_factory_get_or_create_search(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    from elasticsearch_dsl import Search
    aq = AbstractQueryFactory(params_parser)
    assert aq.search is None
    s = aq._get_or_create_search()
    assert s is aq.search
    assert isinstance(aq.search, Search)


def test_searches_queries_abstract_query_factory_get_client(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    from elasticsearch import Elasticsearch
    aq = AbstractQueryFactory(params_parser, client={'a': 'client'})
    c = aq._get_client()
    assert c == {'a': 'client'}
    aq = AbstractQueryFactory(params_parser)
    c = aq._get_client()
    assert isinstance(c, Elasticsearch)


def test_searches_queries_abstract_query_factory_get_index(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    from snovault.elasticsearch.interfaces import RESOURCES_INDEX
    aq = AbstractQueryFactory(params_parser)
    assert aq._get_index() == RESOURCES_INDEX


def test_searches_queries_abstract_query_factory_get_item_types(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser)
    item_types = aq._get_item_types()
    assert item_types == [
        ('type', 'Experiment')
    ]


def test_searches_queries_abstract_query_factory_get_principals(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser)
    principals = aq._get_principals()
    assert principals == ['system.Everyone']


def test_searches_queries_abstract_query_factory_get_default_item_types(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(
        params_parser,
        default_item_types=[
            'Snowflake',
            'Pancake'
        ]
    )
    default_item_types = aq._get_default_item_types()
    assert default_item_types == [
        'Snowflake',
        'Pancake'
    ]


def test_searches_queries_abstract_query_factory_get_query(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser)
    search_terms = aq._get_query()
    assert search_terms == '(chip-seq)'


def test_searches_queries_abstract_query_factory_get_filters(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser)
    filters = aq._get_filters()
    assert filters == [
        ('assay_title', 'Histone ChIP-seq'),
        ('award.project', 'Roadmap'),
        ('assembly', 'GRCh38'),
        ('biosample_ontology.classification', 'primary cell'),
        ('target.label', 'H3K27me3'),
        ('biosample_ontology.classification!', 'cell line'),
        ('biosample_ontology.term_name!', 'naive thymus-derived CD4-positive, alpha-beta T cell'),
        ('status', 'released')
    ]


def test_searches_queries_abstract_query_factory_get_post_filters(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser)
    assert aq._get_post_filters() == [
        ('assay_title', 'Histone ChIP-seq'),
        ('award.project', 'Roadmap'),
        ('assembly', 'GRCh38'),
        ('biosample_ontology.classification', 'primary cell'),
        ('target.label', 'H3K27me3'),
        ('biosample_ontology.classification!', 'cell line'),
        ('biosample_ontology.term_name!', 'naive thymus-derived CD4-positive, alpha-beta T cell'),
        ('status', 'released')
    ]


def test_searches_queries_abstract_query_factory_get_sort(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser)
    sort_by = aq._get_sort()
    assert sort_by == [
        ('sort', 'date_created'),
        ('sort', '-files.file_size')
    ]


def test_searches_queries_abstract_query_factory_get_limit(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser)
    limit = aq.params_parser.param_values_to_list(aq._get_limit())
    assert limit == [
        '10'
    ]


def test_searches_queries_abstract_query_factory_get_search_fields(params_parser_snovault_types):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser_snovault_types)
    search_fields = aq._get_search_fields()
    assert all(
        f in search_fields
        for f in [
                'embedded.status',
                '*.md5sum',
                '_all',
                'unique_keys.*',
                'embedded.accession',
                '*.uuid',
                '*.submitted_file_name'
        ]
    )


def test_searches_queries_abstract_query_factory_get_return_fields(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser)
    return_fields = aq.params_parser.param_values_to_list(aq._get_return_fields())
    assert return_fields == [
        '@id',
        'accession'
    ]


def test_searches_queries_abstract_query_factory_combine_search_term_queries(dummy_request):
    from snovault.elasticsearch.searches.parsers import ParamsParser
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    dummy_request.environ['QUERY_STRING'] = (
        'searchTerm=chip-seq&searchTerm=rna&searchTerm!=ENCODE+2'
    )
    params_parser = ParamsParser(dummy_request)
    aq = AbstractQueryFactory(params_parser)
    combined_search_terms = aq._combine_search_term_queries(
        must_match_filters=aq.params_parser.get_must_match_search_term_filters(),
        must_not_match_filters=aq.params_parser.get_must_not_match_search_term_filters()
    )
    assert combined_search_terms == '(chip-seq) AND (rna) AND NOT (ENCODE 2)'
    dummy_request.environ['QUERY_STRING'] = (
        'searchTerm=chip-seq'
    )
    params_parser = ParamsParser(dummy_request)
    aq = AbstractQueryFactory(params_parser)
    combined_search_terms = aq._combine_search_term_queries(
        must_match_filters=aq.params_parser.get_must_match_search_term_filters(),
        must_not_match_filters=aq.params_parser.get_must_not_match_search_term_filters()
    )
    assert combined_search_terms == '(chip-seq)'
    dummy_request.environ['QUERY_STRING'] = (
        'searchTerm!=rna&searchTerm!=ENCODE+2'
    )
    params_parser = ParamsParser(dummy_request)
    aq = AbstractQueryFactory(params_parser)
    combined_search_terms = aq._combine_search_term_queries(
        must_match_filters=aq.params_parser.get_must_match_search_term_filters(),
        must_not_match_filters=aq.params_parser.get_must_not_match_search_term_filters()
    )
    assert combined_search_terms == 'NOT (rna) AND NOT (ENCODE 2)'
    dummy_request.environ['QUERY_STRING'] = (
        'type=Experiment'
    )
    params_parser = ParamsParser(dummy_request)
    aq = AbstractQueryFactory(params_parser)
    combined_search_terms = aq._combine_search_term_queries(
        must_match_filters=aq.params_parser.get_must_match_search_term_filters(),
        must_not_match_filters=aq.params_parser.get_must_not_match_search_term_filters()
    )
    assert combined_search_terms is None


def test_searches_queries_abstract_query_factory_get_facets(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser)
    assert aq._get_facets() == []


def test_searches_queries_abstract_query_factory_get_facet_size(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser)
    assert aq._get_facet_size() is None


def test_searches_queries_abstract_query_factory_get_boost_values_from_item_type(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser)
    assert aq._get_boost_values_from_item_type(
        'TestingSearchSchema'
    ) == {'accession': 1.0, 'status': 1.0}


def test_searches_queries_abstract_query_factory_prefix_values(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser)
    assert aq._prefix_values(
        'embedded.',
        ['uuid', 'status', '@type']
    ) == ['embedded.uuid', 'embedded.status', 'embedded.@type']


def test_searches_queries_abstract_query_factory_make_bool_filter_query(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser)
    bf = aq._make_bool_query(
        filter=[
            aq._make_must_equal_terms_query(
                field='embedded.status',
                terms=['revoked', 'archived']
            )
        ]
    )
    assert bf.to_dict() == {
        'bool': {
            'filter': [
                {
                    'terms': {
                        'embedded.status': [
                            'revoked',
                            'archived'
                        ]
                    }
                }
            ]
        }
    }


def test_searches_queries_abstract_query_factory_make_bool_filter_query_must_not(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser)
    bf = aq._make_bool_query(
        filter=[
            ~aq._make_must_equal_terms_query(
                field='embedded.status',
                terms=['revoked', 'archived']
            )
        ]
    )
    assert bf.to_dict() == {
        'bool': {
            'filter': [
                {
                    'bool': {
                        'must_not': [
                            {
                                'terms': {
                                    'embedded.status': [
                                        'revoked',
                                        'archived'
                                    ]
                                }
                            }
                        ]
                    }
                }
            ]
        }
    }


def test_searches_queries_abstract_query_factory_make_bool_filter_query_must_and_must_not(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser)
    bf = aq._make_bool_query(
        filter=[
            ~aq._make_must_equal_terms_query(
                field='embedded.status',
                terms=['revoked', 'archived']
            ),
            ~aq._make_field_must_exist_query(
                field='embedded.file_size'
            ),
            aq._make_must_equal_terms_query(
                field='embedded.@type',
                terms=['Item']
            ),
            aq._make_field_must_exist_query(
                field='embedded.@type'
            )
        ]
    )
    assert bf.to_dict() == {
        'bool': {
            'filter': [
                {'bool': {'must_not': [{'terms': {'embedded.status': ['revoked', 'archived']}}]}},
                {'bool': {'must_not': [{'exists': {'field': 'embedded.file_size'}}]}},
                {'terms': {'embedded.@type': ['Item']}},
                {'exists': {'field': 'embedded.@type'}}
            ]
        }
    }


def test_searches_queries_abstract_query_factory_make_bool_filter_and_query_context(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser)
    bf = aq._make_bool_query(
        filter=[
            ~aq._make_must_equal_terms_query(
                field='embedded.status',
                terms=['revoked', 'archived']
            ),
            ~aq._make_field_must_exist_query(
                field='embedded.file_size'
            ),
            aq._make_must_equal_terms_query(
                field='embedded.@type',
                terms=['Item']
            ),
            aq._make_field_must_exist_query(
                field='embedded.@type'
            )
        ]
    )
    aq.search = aq._get_or_create_search().query(bf)
    aq.search = aq.search.query(
        aq._make_query_string_query('test query', fields=['name, title'])
    )
    assert aq.search.to_dict() == {
        'query': {
            'bool': {
                'filter': [
                    {'bool': {'must_not': [{'terms': {'embedded.status': ['revoked', 'archived']}}]}},
                    {'bool': {'must_not': [{'exists': {'field': 'embedded.file_size'}}]}},
                    {'terms': {'embedded.@type': ['Item']}},
                    {'exists': {'field': 'embedded.@type'}}
                ],
                'must': [
                    {
                        'query_string': {
                            'query': 'test query',
                            'default_operator': 'AND',
                            'fields': ['name, title']
                        }
                    }
                ]
            }
        }
    }



    fa = aq._make_filter_aggregation(
        filter_context=aq._make_must_equal_terms_query(
            field='embedded.@type',
            terms=['File']
        )
    )
    assert fa.to_dict() == {
        'filter': {
            'terms': {
                'embedded.@type': ['File']
            }
        }
    }


def test_searches_queries_abstract_query_factory_make_filter_aggregation_bool_context(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser)
    fa = aq._make_filter_aggregation(
        filter_context=aq._make_bool_query(
            filter=[
                aq._make_field_must_exist_query(
                    field='embedded.status'
                ),
                ~aq._make_field_must_exist_query(
                    field='embedded.audit'
                )
            ]
        )
    )
    assert fa.to_dict() == {
        'filter': {
            'bool': {
                'filter': [
                    {'exists': {'field': 'embedded.status'}},
                    {'bool': {'must_not': [{'exists': {'field': 'embedded.audit'}}]}}]
            }
        }
    }


def test_searches_queries_abstract_query_factory_make_filter_and_sub_aggregation(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser)
    fasa = aq._make_filter_and_sub_aggregation(
        title='Lab name terms on Experiments that have files with file_size',
        filter_context=(
            aq._make_must_equal_terms_query(
                field='@type',
                terms=['Experiment']
            )
            & aq._make_bool_query(
                filter=[
                    aq._make_field_must_exist_query(
                        field='embeddded.files.file_size'
                    )
                ]
            )
        ),
        sub_aggregation=aq._make_terms_aggregation(
            field='embedded.lab.name'
        )
    )
    assert fasa.to_dict() == {
        'aggs': {
            'Lab name terms on Experiments that have files with file_size': {
                'terms': {
                    'size': 200,
                    'field': 'embedded.lab.name',
                    'exclude': []
                }
            }
        },
        'filter': {
            'bool': {
                'filter': [
                    {'exists': {'field': 'embeddded.files.file_size'}}
                ],
                'must': [
                    {'terms': {'@type': ['Experiment']}}
                ]
            }
        }
    }


def test_searches_queries_abstract_query_factory_make_filter_and_sub_aggregation_bool(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser)
    fasa = aq._make_filter_and_sub_aggregation(
        title='Small processed versus raw files on first day of ENCODE4',
        filter_context=aq._make_bool_query(
            must=[
                aq._make_field_must_exist_query(
                    field='embedded.file_type'
                )
            ],
            should=[
                aq._make_must_equal_terms_query(
                    field='embedded.file_size',
                    terms=['1', '2', '3']
                )
            ],
            must_not=[
                aq._make_must_equal_terms_query(
                    field='embedded.@type',
                    terms=['Publication']
                ),
                aq._make_must_equal_terms_query(
                    field='embedded.award.rfa',
                    terms=['ENCODE2']
                )
            ],
            filter=[
                aq._make_must_equal_terms_query(
                    field='embedded.date_created',
                    terms=['05/01/2017']
                )
            ]
        ),
        sub_aggregation=aq._make_exists_aggregation(
            field='embedded.derived_from'
        )
    )
    assert fasa.to_dict() == {
        'aggs': {
            'Small processed versus raw files on first day of ENCODE4': {
                'filters': {
                    'filters': {
                        'no': {
                            'bool': {
                                'must_not': [{'exists': {'field': 'embedded.derived_from'}}]}
                        },
                        'yes': {
                            'exists': {'field': 'embedded.derived_from'}}}
                }
            }
        },
        'filter': {
            'bool': {
                'must_not': [
                    {'terms': {'embedded.@type': ['Publication']}},
                    {'terms': {'embedded.award.rfa': ['ENCODE2']}}
                ],
                'filter': [
                    {'terms': {'embedded.date_created': ['05/01/2017']}}
                ],
                'must': [
                    {'exists': {'field': 'embedded.file_type'}}
                ],
                'should': [
                    {'terms': {'embedded.file_size': ['1', '2', '3']}}]
            }
        }
    }


def test_searches_queries_abstract_query_factory_make_query_string_query(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser)
    qsq = aq._make_query_string_query(
        'my TEST',
        fields=['embedded.description'],
        default_operator='OR'
    )
    assert qsq.to_dict() == {
        'query_string': {
            'default_operator': 'OR',
            'fields': ['embedded.description'],
            'query': 'my TEST'
        }
    }


def test_searches_queries_abstract_query_factory_make_must_equal_terms_query(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser)
    mtq = aq._make_must_equal_terms_query(
        field='embedded.status',
        terms=['released']
    )
    assert mtq.to_dict() == {
        'terms': {
            'embedded.status': ['released']
        }
    }


def test_searches_queries_abstract_query_factory_make_must_equal_terms_queries_from_params(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser)
    mtqs = aq._make_must_equal_terms_queries_from_params(
        params=sorted(aq._get_filters())
    )
    actual = [m.to_dict() for m in mtqs]
    expected = [
        {'terms': {'assay_title': ['Histone ChIP-seq']}},
        {'terms': {'status': ['released']}},
        {'terms': {'biosample_ontology.term_name': ['naive thymus-derived CD4-positive, alpha-beta T cell']}},
        {'terms': {'biosample_ontology.classification': ['primary cell', 'cell line']}},
        {'terms': {'target.label': ['H3K27me3']}},
        {'terms': {'assembly': ['GRCh38']}},
        {'terms': {'award.project': ['Roadmap']}}
    ]
    assert all(e in actual for e in expected)


def test_searches_queries_abstract_query_factory_make_field_must_exist_query(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser)
    meq = aq._make_field_must_exist_query(
        field='embedded.file_size'
    )
    assert meq.to_dict() == {
        'exists': {
            'field': 'embedded.file_size'
        }
    }

def test_searches_queries_abstract_query_factory_make_field_must_exist_queries_from_params(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser)
    meqs = aq._make_field_must_exist_query_from_params(
        params=aq._get_filters()
    )
    actual = [m.to_dict() for m in meqs]
    expected = [
        {'exists': {'field': 'assay_title'}},
        {'exists': {'field': 'assembly'}},
        {'exists': {'field': 'biosample_ontology.classification'}},
        {'exists': {'field': 'status'}},
        {'exists': {'field': 'biosample_ontology.term_name'}},
        {'exists': {'field': 'target.label'}},
        {'exists': {'field': 'award.project'}}
    ]
    assert all(e in actual for e in expected)


def test_searches_queries_abstract_query_factory_make_default_filters(params_parser_snovault_types):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser_snovault_types)
    df = aq._make_default_filters()
    assert df[0].to_dict() == {
        'terms': {
            'principals_allowed.view': ['system.Everyone']
        }
    }
    assert df[1].to_dict() == {
        'terms': {
            'embedded.@type': ['TestingSearchSchema']
        }
    }


def test_searches_queries_abstract_query_factory_make_default_filters_default_types(dummy_request):
    from snovault.elasticsearch.searches.parsers import ParamsParser
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    dummy_request.environ['QUERY_STRING'] = (
        'assay_title=Histone+ChIP-seq&award.project=Roadmap'
        '&assembly=GRCh38&biosample_ontology.classification=primary+cell'
        '&target.label=H3K27me3&biosample_ontology.classification%21=cell+line'
    )
    p = ParamsParser(dummy_request)
    aq = AbstractQueryFactory(p, default_item_types=['Snowflake', 'Pancake'])
    df = aq._make_default_filters()
    assert df[0].to_dict() == {
        'terms': {
            'principals_allowed.view': ['system.Everyone']
        }
    }
    assert df[1].to_dict() == {
        'terms': {
            'embedded.@type': [
                'Snowflake', 'Pancake'
            ]
        }
    }


def test_searches_queries_abstract_query_factory_make_terms_aggregation(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser)
    ta = aq._make_terms_aggregation(
        field='embedded.lab.name',
        exclude=['other lab'],
        size=14
    )
    assert ta.to_dict() == {
        'terms': {
            'exclude': ['other lab'],
            'field': 'embedded.lab.name',
            'size': 14
        }
    }


def test_searches_queries_abstract_query_factory_make_exists_aggregation(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser)
    eq = aq._make_exists_aggregation(
        field='embedded.file_available'
    )
    assert eq.to_dict() == {
        'filters': {
            'filters': {
                'no': {
                    'bool': {
                        'must_not': [
                            {'exists': {'field': 'embedded.file_available'}}
                        ]
                    }
                },
                'yes': {
                    'exists': {'field': 'embedded.file_available'}
                }
            }
        }
    }


def test_searches_queries_abstract_query_factory_add_query_string_query(dummy_request):
    from snovault.elasticsearch.searches.parsers import ParamsParser
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    dummy_request.environ['QUERY_STRING'] = (
        'searchTerm=chip-seq'
    )
    params_parser = ParamsParser(dummy_request)
    aq = AbstractQueryFactory(params_parser)
    aq.add_query_string_query()
    constructed_query = aq.search.to_dict()
    expected_query = {
        'query': {
            'query_string': {
                'default_operator': 'AND',
                'fields': [
                    '_all',
                    '*.uuid',
                    '*.md5sum',
                    '*.submitted_file_name',
                    'unique_keys.*'
                ],
                'query': '(chip-seq)'
            }
        }
    }
    assert (
        constructed_query['query']['query_string']['query']
        == expected_query['query']['query_string']['query']
    )
    assert (
        constructed_query['query']['query_string']['default_operator']
        == expected_query['query']['query_string']['default_operator']
    )
    assert (
        set(constructed_query['query']['query_string']['fields'])
        == set(expected_query['query']['query_string']['fields'])
    )


def test_searches_queries_abstract_query_factory_add_query_string_query_with_type(dummy_request):
    from snovault.elasticsearch.searches.parsers import ParamsParser
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    dummy_request.environ['QUERY_STRING'] = (
        'searchTerm=chip-seq&type=TestingSearchSchema&status=released'
    )
    params_parser = ParamsParser(dummy_request)
    aq = AbstractQueryFactory(params_parser)
    aq.add_query_string_query()
    constructed_query = aq.search.to_dict()
    expected_query = {
        'query': {
            'query_string': {
                'default_operator': 'AND',
                'fields': [
                    '_all',
                    '*.uuid',
                    '*.md5sum',
                    '*.submitted_file_name',
                    'unique_keys.*',
                    'embedded.accession',
                    'embedded.status'
                ],
                'query': '(chip-seq)'
            }
        }
    }
    assert (
        constructed_query['query']['query_string']['query']
        == expected_query['query']['query_string']['query']
    )
    assert (
        constructed_query['query']['query_string']['default_operator']
        == expected_query['query']['query_string']['default_operator']
    )
    assert (
        set(constructed_query['query']['query_string']['fields'])
        == set(expected_query['query']['query_string']['fields'])
    )


def test_searches_queries_abstract_query_factory_add_query_string_query_with_default_type(dummy_request):
    from snovault.elasticsearch.searches.parsers import ParamsParser
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    dummy_request.environ['QUERY_STRING'] = (
        'searchTerm=chip-seq'
    )
    params_parser = ParamsParser(dummy_request)
    aq = AbstractQueryFactory(
        params_parser,
        default_item_types=[
            'TestingSearchSchema'
        ]
    )
    aq.add_query_string_query()
    constructed_query = aq.search.to_dict()
    expected_query = {
        'query': {
            'query_string': {
                'default_operator': 'AND',
                'fields': [
                    '_all',
                    '*.uuid',
                    '*.md5sum',
                    '*.submitted_file_name',
                    'unique_keys.*',
                    'embedded.accession',
                    'embedded.status'
                ],
                'query': '(chip-seq)'
            }
        }
    }
    assert (
        constructed_query['query']['query_string']['query']
        == expected_query['query']['query_string']['query']
    )
    assert (
        constructed_query['query']['query_string']['default_operator']
        == expected_query['query']['query_string']['default_operator']
    )
    assert (
        set(constructed_query['query']['query_string']['fields'])
        == set(expected_query['query']['query_string']['fields'])
    )


def test_searches_queries_abstract_query_factory_add_query_string_query_no_search_term(dummy_request):
    from snovault.elasticsearch.searches.parsers import ParamsParser
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    dummy_request.environ['QUERY_STRING'] = (
        'type=TestingSearchSchema&status=released'
    )
    params_parser = ParamsParser(dummy_request)
    aq = AbstractQueryFactory(params_parser)
    aq.add_query_string_query()
    assert aq.search is None


def test_searches_queries_abstract_query_factory_add_must_equal_terms_filter(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser)
    aq._add_must_equal_terms_filter(
        field='status',
        terms=['released', 'archived']
    )
    assert aq.search.to_dict() == {
        'query': {
            'bool': {
                'filter': [
                    {
                        'terms': {
                            'status': [
                                'released',
                                'archived'
                            ]
                        }
                    }
                ]
            }
        }
    }


def test_searches_queries_abstract_query_factory_add_must_equal_terms_post_filter(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser)
    aq._add_must_equal_terms_post_filter(
        field='status',
        terms=['released', 'archived']
    )
    assert aq.search.to_dict() == {
        'query': {
            'match_all': {}
        },
        'post_filter': {
            'terms': {'status': ['released', 'archived']}}
    }


def test_searches_queries_abstract_query_factory_add_must_not_equal_terms_post_filter(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser)
    aq._add_must_not_equal_terms_post_filter(
        field='status',
        terms=['released', 'archived']
    )
    assert aq.search.to_dict() == {
        'query': {
            'match_all': {}
        },
        'post_filter': {
            'bool': {
                'filter': [
                    {'bool': {'must_not': [{'terms': {'status': ['released', 'archived']}}]}}]
            }
        }
    }


def test_searches_queries_abstract_query_factory_add_must_not_equal_terms_filter(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser)
    aq._add_must_not_equal_terms_filter(
        field='status',
        terms=['released', 'archived']
    )
    assert aq.search.to_dict() == {
        'query': {
            'bool': {
                'filter': [
                    {
                        'bool': {
                            'must_not': [
                                {
                                    'terms': {
                                        'status': [
                                            'released',
                                            'archived'
                                        ]
                                    }
                                }
                            ]
                        }
                    }
                ]
            }
        }
    }


def test_searches_queries_abstract_query_factory_add_field_must_exist_filter(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser)
    aq._add_field_must_exist_filter(
        'embedded.status'
    )
    assert aq.search.to_dict() == {
        'query': {
            'bool': {
                'filter': [{'exists': {'field': 'embedded.status'}}]
                }
        }
    }


def test_searches_queries_abstract_query_factory_add_field_must_exist_post_filter(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser)
    aq._add_field_must_exist_post_filter(
        'embedded.status'
    )
    assert aq.search.to_dict() == {
        'post_filter': {
            'bool': {
                'filter': [
                    {'exists': {'field': 'embedded.status'}}]
            }
        },
        'query': {
            'match_all': {}
        }
    }


def test_searches_queries_abstract_query_factory_add_field_must_exist_filter_multiple(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser)
    aq._add_field_must_exist_filter(
        'embedded.status'
    )
    aq._add_field_must_exist_filter(
        'embedded.lab'
    )
    assert aq.search.to_dict() == {
        'query': {
            'bool': {
                'filter': [
                    {'exists': {'field': 'embedded.status'}},
                    {'exists': {'field': 'embedded.lab'}}
                ]
            }
        }
    }


def test_searches_queries_abstract_query_factory_add_field_must_not_exist_filter(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser)
    aq._add_field_must_not_exist_filter(
        'embedded.file_size'
    )
    assert aq.search.to_dict() == {
       'query': {
            'bool': {
                'filter': [{'bool': {'must_not': [{'exists': {'field': 'embedded.file_size'}}]}}]
            }
        }
    }


def test_searches_queries_abstract_query_factory_add_field_must_not_exist_post_filter(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser)
    aq._add_field_must_not_exist_post_filter(
        'embedded.file_size'
    )
    assert aq.search.to_dict() == {
        'query': {
            'match_all': {}
        },
        'post_filter': {
            'bool': {
                'filter': [
                    {'bool': {'must_not': [{'exists': {'field': 'embedded.file_size'}}]}}]
            }
        }
    }


def test_searches_queries_abstract_query_factory_add_field_must_and_must_not_exist_filter(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser)
    aq._add_field_must_exist_filter(
        'embedded.status'
    )
    aq._add_field_must_not_exist_filter(
        'embedded.file_size'
    )
    assert aq.search.to_dict() == {
        'query': {
            'bool': {
                'filter': [
                    {'exists': {'field': 'embedded.status'}},
                    {'bool': {'must_not': [{'exists': {'field': 'embedded.file_size'}}]}}
                ]
            }
        }
    }


def test_searches_queries_abstract_query_factory_add_terms_aggregation(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser)
    aq._add_terms_aggregation('Statuses', 'embedded.status', size=10)
    assert aq.search.to_dict() == {
        'aggs': {
            'Statuses': {
                'terms': {
                    'exclude': [],
                    'field': 'embedded.status',
                    'size': 10,
                }
            }
        },
        'query': {'match_all': {}}
    }


def test_searches_queries_abstract_query_factory_make_split_filter_queries(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser)
    must, must_not, exists, not_exists = aq._make_split_filter_queries()
    expected_must = [
        {'terms': {'award.project': ['Roadmap']}},
        {'terms': {'target.label': ['H3K27me3']}},
        {'terms': {'assembly': ['GRCh38']}},
        {'terms': {'status': ['released']}},
        {'terms': {'biosample_ontology.classification': ['primary cell']}},
        {'terms': {'assay_title': ['Histone ChIP-seq']}}
    ]
    actual_must = [m.to_dict() for m in must]
    assert all(e in actual_must for e in expected_must)
    expected_must_not = [
        {'terms': {'biosample_ontology.classification': ['cell line']}},
        {
            'terms': {
                'biosample_ontology.term_name': [
                    'naive thymus-derived CD4-positive, alpha-beta T cell'
                ]
            }
        }
    ]
    actual_must_not = [m.to_dict() for m in must_not]
    assert all(e in actual_must_not for e in expected_must_not)
    assert [e.to_dict() for e in exists] == []
    assert [e.to_dict() for e in not_exists] == []


def test_searches_queries_abstract_query_factory_make_split_filter_queries_wildcards(dummy_request):
    from snovault.elasticsearch.searches.parsers import ParamsParser
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    dummy_request.environ['QUERY_STRING'] = (
        'type=TestingSearchSchema&status=*&restricted!=*&no_file_available!=*'
        '&limit=10&field=@id&field=accession&lab.name=*&file_type=bam'
    )
    p = ParamsParser(dummy_request)
    aq = AbstractQueryFactory(p)
    must, must_not, exists, not_exists = aq._make_split_filter_queries()
    assert [m.to_dict() for m in must] == [{'terms': {'file_type': ['bam']}}]
    assert [m.to_dict() for m in must_not] == []
    expected_exists = [{'exists': {'field': 'lab.name'}}, {'exists': {'field': 'status'}}]
    actual_exists = [e.to_dict() for e in exists]
    assert all(e in actual_exists for e in expected_exists)
    expected_not_exists = [{'exists': {'field': 'restricted'}}, {'exists': {'field': 'no_file_available'}}]
    actual_not_exists = [e.to_dict() for e in not_exists]
    assert all(e in actual_not_exists for e in expected_not_exists)


def test_searches_queries_abstract_query_factory_make_filter_aggregation(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser)
    fa = aq._make_filter_aggregation(
        filter_context=aq._make_must_equal_terms_query(
            field='embedded.@type',
            terms=['File']
        )
    )
    assert fa.to_dict() == {
        'filter': {
            'terms': {
                'embedded.@type': ['File']
            }
        }
    }


def test_searches_queries_abstract_query_factory_make_filter_aggregation_bool_context(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser)
    fa = aq._make_filter_aggregation(
        filter_context=aq._make_bool_query(
            filter=[
                aq._make_field_must_exist_query(
                    field='embedded.status'
                ),
                ~aq._make_field_must_exist_query(
                    field='embedded.audit'
                )
            ]
        )
    )
    assert fa.to_dict() == {
        'filter': {
            'bool': {
                'filter': [
                    {'exists': {'field': 'embedded.status'}},
                    {'bool': {'must_not': [{'exists': {'field': 'embedded.audit'}}]}}]
            }
        }
    }


def test_searches_queries_abstract_query_factory_make_filter_and_sub_aggregation(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser)
    fasa = aq._make_filter_and_sub_aggregation(
        title='Lab name terms on Experiments that have files with file_size',
        filter_context=(
            aq._make_must_equal_terms_query(
                field='@type',
                terms=['Experiment']
            )
            & aq._make_bool_query(
                filter=[
                    aq._make_field_must_exist_query(
                        field='embeddded.files.file_size'
                    )
                ]
            )
        ),
        sub_aggregation=aq._make_terms_aggregation(
            field='embedded.lab.name'
        )
    )
    assert fasa.to_dict() == {
        'aggs': {
            'Lab name terms on Experiments that have files with file_size': {
                'terms': {
                    'size': 200,
                    'field': 'embedded.lab.name',
                    'exclude': []
                }
            }
        },
        'filter': {
            'bool': {
                'filter': [
                    {'exists': {'field': 'embeddded.files.file_size'}}
                ],
                'must': [
                    {'terms': {'@type': ['Experiment']}}
                ]
            }
        }
    }


def test_searches_queries_abstract_query_factory_make_filter_and_sub_aggregation_bool(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser)
    fasa = aq._make_filter_and_sub_aggregation(
        title='Small processed versus raw files on first day of ENCODE4',
        filter_context=aq._make_bool_query(
            must=[
                aq._make_field_must_exist_query(
                    field='embedded.file_type'
                )
            ],
            should=[
                aq._make_must_equal_terms_query(
                    field='embedded.file_size',
                    terms=['1', '2', '3']
                )
            ],
            must_not=[
                aq._make_must_equal_terms_query(
                    field='embedded.@type',
                    terms=['Publication']
                ),
                aq._make_must_equal_terms_query(
                    field='embedded.award.rfa',
                    terms=['ENCODE2']
                )
            ],
            filter=[
                aq._make_must_equal_terms_query(
                    field='embedded.date_created',
                    terms=['05/01/2017']
                )
            ]
        ),
        sub_aggregation=aq._make_exists_aggregation(
            field='embedded.derived_from'
        )
    )
    assert fasa.to_dict() == {
        'aggs': {
            'Small processed versus raw files on first day of ENCODE4': {
                'filters': {
                    'filters': {
                        'no': {
                            'bool': {
                                'must_not': [{'exists': {'field': 'embedded.derived_from'}}]}
                        },
                        'yes': {
                            'exists': {'field': 'embedded.derived_from'}}}
                }
            }
        },
        'filter': {
            'bool': {
                'must_not': [
                    {'terms': {'embedded.@type': ['Publication']}},
                    {'terms': {'embedded.award.rfa': ['ENCODE2']}}
                ],
                'filter': [
                    {'terms': {'embedded.date_created': ['05/01/2017']}}
                ],
                'must': [
                    {'exists': {'field': 'embedded.file_type'}}
                ],
                'should': [
                    {'terms': {'embedded.file_size': ['1', '2', '3']}}]
            }
        }
    }


def test_searches_queries_abstract_query_factory_make_query_string_query(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser)
    qsq = aq._make_query_string_query(
        'my TEST',
        fields=['embedded.description'],
        default_operator='OR'
    )
    assert qsq.to_dict() == {
        'query_string': {
            'default_operator': 'OR',
            'fields': ['embedded.description'],
            'query': 'my TEST'
        }
    }


def test_searches_queries_abstract_query_factory_make_must_equal_terms_query(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser)
    mtq = aq._make_must_equal_terms_query(
        field='embedded.status',
        terms=['released']
    )
    assert mtq.to_dict() == {
        'terms': {
            'embedded.status': ['released']
        }
    }


def test_searches_queries_abstract_query_factory_make_field_must_exist_query(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser)
    meq = aq._make_field_must_exist_query(
        field='embedded.file_size'
    )
    assert meq.to_dict() == {
        'exists': {
            'field': 'embedded.file_size'
        }
    }


def test_searches_queries_abstract_query_factory_make_default_filters(params_parser_snovault_types):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser_snovault_types)
    df = aq._make_default_filters()
    assert df[0].to_dict() == {
        'terms': {
            'principals_allowed.view': ['system.Everyone']
        }
    }
    assert df[1].to_dict() == {
        'terms': {
            'embedded.@type': ['TestingSearchSchema']
        }
    }


def test_searches_queries_abstract_query_factory_make_default_filters_default_types(dummy_request):
    from snovault.elasticsearch.searches.parsers import ParamsParser
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    dummy_request.environ['QUERY_STRING'] = (
        'assay_title=Histone+ChIP-seq&award.project=Roadmap'
        '&assembly=GRCh38&biosample_ontology.classification=primary+cell'
        '&target.label=H3K27me3&biosample_ontology.classification%21=cell+line'
    )
    p = ParamsParser(dummy_request)
    aq = AbstractQueryFactory(p, default_item_types=['Snowflake', 'Pancake'])
    df = aq._make_default_filters()
    assert df[0].to_dict() == {
        'terms': {
            'principals_allowed.view': ['system.Everyone']
        }
    }
    assert df[1].to_dict() == {
        'terms': {
            'embedded.@type': [
                'Snowflake', 'Pancake'
            ]
        }
    }


def test_searches_queries_abstract_query_factory_make_terms_aggregation(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser)
    ta = aq._make_terms_aggregation(
        field='embedded.lab.name',
        exclude=['other lab'],
        size=14
    )
    assert ta.to_dict() == {
        'terms': {
            'exclude': ['other lab'],
            'field': 'embedded.lab.name',
            'size': 14
        }
    }


def test_searches_queries_abstract_query_factory_make_exists_aggregation(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser)
    eq = aq._make_exists_aggregation(
        field='embedded.file_available'
    )
    assert eq.to_dict() == {
        'filters': {
            'filters': {
                'no': {
                    'bool': {
                        'must_not': [
                            {'exists': {'field': 'embedded.file_available'}}
                        ]
                    }
                },
                'yes': {
                    'exists': {'field': 'embedded.file_available'}
                }
            }
        }
    }


def test_searches_queries_abstract_query_factory_add_query_string_query(dummy_request):
    from snovault.elasticsearch.searches.parsers import ParamsParser
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    dummy_request.environ['QUERY_STRING'] = (
        'searchTerm=chip-seq'
    )
    params_parser = ParamsParser(dummy_request)
    aq = AbstractQueryFactory(params_parser)
    aq.add_query_string_query()
    constructed_query = aq.search.to_dict()
    expected_query = {
        'query': {
            'query_string': {
                'default_operator': 'AND',
                'fields': [
                    '_all',
                    '*.uuid',
                    '*.md5sum',
                    '*.submitted_file_name',
                    'unique_keys.*'
                ],
                'query': '(chip-seq)'
            }
        }
    }
    assert (
        constructed_query['query']['query_string']['query']
        == expected_query['query']['query_string']['query']
    )
    assert (
        constructed_query['query']['query_string']['default_operator']
        == expected_query['query']['query_string']['default_operator']
    )
    assert (
        set(constructed_query['query']['query_string']['fields'])
        == set(expected_query['query']['query_string']['fields'])
    )


def test_searches_queries_abstract_query_factory_add_query_string_query_with_type(dummy_request):
    from snovault.elasticsearch.searches.parsers import ParamsParser
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    dummy_request.environ['QUERY_STRING'] = (
        'searchTerm=chip-seq&type=TestingSearchSchema&status=released'
    )
    params_parser = ParamsParser(dummy_request)
    aq = AbstractQueryFactory(params_parser)
    aq.add_query_string_query()
    constructed_query = aq.search.to_dict()
    expected_query = {
        'query': {
            'query_string': {
                'default_operator': 'AND',
                'fields': [
                    '_all',
                    '*.uuid',
                    '*.md5sum',
                    '*.submitted_file_name',
                    'unique_keys.*',
                    'embedded.accession',
                    'embedded.status'
                ],
                'query': '(chip-seq)'
            }
        }
    }
    assert (
        constructed_query['query']['query_string']['query']
        == expected_query['query']['query_string']['query']
    )
    assert (
        constructed_query['query']['query_string']['default_operator']
        == expected_query['query']['query_string']['default_operator']
    )
    assert (
        set(constructed_query['query']['query_string']['fields'])
        == set(expected_query['query']['query_string']['fields'])
    )


def test_searches_queries_abstract_query_factory_add_query_string_query_with_default_type(dummy_request):
    from snovault.elasticsearch.searches.parsers import ParamsParser
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    dummy_request.environ['QUERY_STRING'] = (
        'searchTerm=chip-seq'
    )
    params_parser = ParamsParser(dummy_request)
    aq = AbstractQueryFactory(
        params_parser,
        default_item_types=[
            'TestingSearchSchema'
        ]
    )
    aq.add_query_string_query()
    constructed_query = aq.search.to_dict()
    expected_query = {
        'query': {
            'query_string': {
                'default_operator': 'AND',
                'fields': [
                    '_all',
                    '*.uuid',
                    '*.md5sum',
                    '*.submitted_file_name',
                    'unique_keys.*',
                    'embedded.accession',
                    'embedded.status'
                ],
                'query': '(chip-seq)'
            }
        }
    }
    assert (
        constructed_query['query']['query_string']['query']
        == expected_query['query']['query_string']['query']
    )
    assert (
        constructed_query['query']['query_string']['default_operator']
        == expected_query['query']['query_string']['default_operator']
    )
    assert (
        set(constructed_query['query']['query_string']['fields'])
        == set(expected_query['query']['query_string']['fields'])
    )


def test_searches_queries_abstract_query_factory_add_query_string_query_no_search_term(dummy_request):
    from snovault.elasticsearch.searches.parsers import ParamsParser
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    dummy_request.environ['QUERY_STRING'] = (
        'type=TestingSearchSchema&status=released'
    )
    params_parser = ParamsParser(dummy_request)
    aq = AbstractQueryFactory(params_parser)
    aq.add_query_string_query()
    assert aq.search is None


def test_searches_queries_abstract_query_factory_add_must_equal_terms_filter(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser)
    aq._add_must_equal_terms_filter(
        field='status',
        terms=['released', 'archived']
    )
    assert aq.search.to_dict() == {
        'query': {
            'bool': {
                'filter': [
                    {
                        'terms': {
                            'status': [
                                'released',
                                'archived'
                            ]
                        }
                    }
                ]
            }
        }
    }


def test_searches_queries_abstract_query_factory_add_must_equal_terms_post_filter(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser)
    aq._add_must_equal_terms_post_filter(
        field='status',
        terms=['released', 'archived']
    )
    assert aq.search.to_dict() == {
        'query': {
            'match_all': {}
        },
        'post_filter': {
            'terms': {'status': ['released', 'archived']}}
    }


def test_searches_queries_abstract_query_factory_add_must_not_equal_terms_post_filter(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser)
    aq._add_must_not_equal_terms_post_filter(
        field='status',
        terms=['released', 'archived']
    )
    assert aq.search.to_dict() == {
        'query': {
            'match_all': {}
        },
        'post_filter': {
            'bool': {
                'filter': [
                    {'bool': {'must_not': [{'terms': {'status': ['released', 'archived']}}]}}]
            }
        }
    }


def test_searches_queries_abstract_query_factory_add_must_not_equal_terms_filter(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser)
    aq._add_must_not_equal_terms_filter(
        field='status',
        terms=['released', 'archived']
    )
    assert aq.search.to_dict() == {
        'query': {
            'bool': {
                'filter': [
                    {
                        'bool': {
                            'must_not': [
                                {
                                    'terms': {
                                        'status': [
                                            'released',
                                            'archived'
                                        ]
                                    }
                                }
                            ]
                        }
                    }
                ]
            }
        }
    }


def test_searches_queries_abstract_query_factory_add_field_must_exist_filter(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser)
    aq._add_field_must_exist_filter(
        'embedded.status'
    )
    assert aq.search.to_dict() == {
        'query': {
            'bool': {
                'filter': [{'exists': {'field': 'embedded.status'}}]
                }
        }
    }


def test_searches_queries_abstract_query_factory_add_field_must_exist_post_filter(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser)
    aq._add_field_must_exist_post_filter(
        'embedded.status'
    )
    assert aq.search.to_dict() == {
        'post_filter': {
            'bool': {
                'filter': [
                    {'exists': {'field': 'embedded.status'}}]
            }
        },
        'query': {
            'match_all': {}
        }
    }


def test_searches_queries_abstract_query_factory_add_field_must_exist_filter_multiple(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser)
    aq._add_field_must_exist_filter(
        'embedded.status'
    )
    aq._add_field_must_exist_filter(
        'embedded.lab'
    )
    assert aq.search.to_dict() == {
        'query': {
            'bool': {
                'filter': [
                    {'exists': {'field': 'embedded.status'}},
                    {'exists': {'field': 'embedded.lab'}}
                ]
            }
        }
    }


def test_searches_queries_abstract_query_factory_add_field_must_not_exist_filter(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser)
    aq._add_field_must_not_exist_filter(
        'embedded.file_size'
    )
    assert aq.search.to_dict() == {
        'query': {
            'bool': {
                'filter': [{'bool': {'must_not': [{'exists': {'field': 'embedded.file_size'}}]}}]
            }
        }
    }


def test_searches_queries_abstract_query_factory_add_field_must_not_exist_post_filter(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser)
    aq._add_field_must_not_exist_post_filter(
        'embedded.file_size'
    )
    assert aq.search.to_dict() == {
        'query': {
            'match_all': {}
        },
        'post_filter': {
            'bool': {
                'filter': [
                    {'bool': {'must_not': [{'exists': {'field': 'embedded.file_size'}}]}}]
            }
        }
    }


def test_searches_queries_abstract_query_factory_add_field_must_and_must_not_exist_filter(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser)
    aq._add_field_must_exist_filter(
        'embedded.status'
    )
    aq._add_field_must_not_exist_filter(
        'embedded.file_size'
    )
    assert aq.search.to_dict() == {
        'query': {
            'bool': {
                'filter': [
                    {'exists': {'field': 'embedded.status'}},
                    {'bool': {'must_not': [{'exists': {'field': 'embedded.file_size'}}]}}
                ]
            }
        }
    }


def test_searches_queries_abstract_query_factory_add_terms_aggregation(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser)
    aq._add_terms_aggregation('Statuses', 'embedded.status', size=10)
    assert aq.search.to_dict() == {
        'aggs': {
            'Statuses': {
                'terms': {
                    'exclude': [],
                    'field': 'embedded.status',
                    'size': 10,
                }
            }
        },
        'query': {'match_all': {}}
    }


def test_searches_queries_abstract_query_factory_add_terms_aggregation_with_exclusion(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser)
    aq._add_terms_aggregation('Statuses', 'embedded.status', exclude=['Item'])
    assert aq.search.to_dict() == {
        'aggs': {
            'Statuses': {
                'terms': {
                    'exclude': ['Item'],
                    'field': 'embedded.status',
                    'size': 200
                }
            }
        },
        'query': {'match_all': {}}
    }


def test_searches_queries_abstract_query_factory_add_exists_aggregation(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser)
    aq._add_exists_aggregation('Processed file', 'embedded.derived_from')
    assert aq.search.to_dict() == {
        'aggs': {
            'Processed file': {
                'filters': {
                    'filters': {
                        'no': {
                            'bool': {
                                'must_not': [
                                    {
                                        'exists': {
                                            'field': 'embedded.derived_from'
                                        }
                                    }
                                ]
                            }
                        },
                        'yes': {
                            'exists': {
                                'field': 'embedded.derived_from'
                            }
                        }
                    }
                }
            }
        },
        'query': {'match_all': {}}
    }


def test_searches_queries_abstract_query_factory_add_filters(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser)
    aq.add_filters()
    assert aq.search.to_dict() == {
        'query': {
            'bool': {
                'must': [
                    {'terms': {'principals_allowed.view': ['system.Everyone']}},
                    {'terms': {'embedded.@type': ['Experiment']}}]
            }
        }
    }


def test_searches_queries_abstract_query_factory_add_post_filters(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser)
    aq.add_post_filters()
    actual_must = aq.search.to_dict()['post_filter']['bool']['must']
    expected_must = [
        {'terms': {'target.label': ['H3K27me3']}},
        {'terms': {'assembly': ['GRCh38']}},
        {'terms': {'award.project': ['Roadmap']}},
        {'terms': {'assay_title': ['Histone ChIP-seq']}},
        {'terms': {'status': ['released']}},
        {'terms': {'biosample_ontology.classification': ['primary cell']}}
    ]
    assert all(e in actual_must for e in expected_must)
    actual_must_not = aq.search.to_dict()['post_filter']['bool']['must_not']
    expected_must_not = [
        {
            'terms': {
                'biosample_ontology.term_name': [
                    'naive thymus-derived CD4-positive, alpha-beta T cell'
                ]
            }
        },
        {
            'terms': {
                'biosample_ontology.classification': [
                    'cell line'
                ]
            }
        }
    ]
    assert all(e in actual_must_not for e in expected_must_not)


def test_searches_queries_abstract_query_factory_add_query_string_and_post_filters_wildcards(dummy_request):
    from snovault.elasticsearch.searches.parsers import ParamsParser
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    dummy_request.environ['QUERY_STRING'] = (
        'searchTerm=chip-seq&type=TestingSearchSchema&status=*&restricted!=*'
        '&no_file_available!=*&limit=10&field=@id&field=accession&lab.name=*'
        '&file_type=bam'
    )
    p = ParamsParser(dummy_request)
    aq = AbstractQueryFactory(p)
    aq.add_query_string_query()
    aq.add_post_filters()
    actual_query = aq.search.to_dict()
    expected_query = {
        'query': {
            'query_string': {
                'query': '(chip-seq)',
                'fields': [
                    '_all',
                    '*.uuid',
                    'unique_keys.*',
                    'embedded.accession',
                    'embedded.status',
                    '*.submitted_file_name',
                    '*.md5sum'
                ],
                'default_operator': 'AND'
            }
        },
        'post_filter': {
            'bool': {
                'must': [
                    {'terms': {'file_type': ['bam']}},
                    {'exists': {'field': 'lab.name'}},
                    {'exists': {'field': 'status'}}
                ],
                'must_not': [
                    {'exists': {'field': 'restricted'}},
                    {'exists': {'field': 'no_file_available'}}
                ]
            }
        }
    }
    assert (
        expected_query['query']['query_string']['query']
        == actual_query['query']['query_string']['query']
    )
    assert all(
        e in actual_query['post_filter']['bool']['must_not']
        for e in expected_query['post_filter']['bool']['must_not']
    )
    assert all(
        e in actual_query['post_filter']['bool']['must']
        for e in expected_query['post_filter']['bool']['must']
    )
    

def test_searches_queries_abstract_query_factory_add_source(params_parser):
    assert False 


def test_searches_queries_abstract_query_factory_add_aggregations_and_aggregation_filters(params_parser):
    assert False


def test_searches_queries_abstract_query_factory_build_query():
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory({})
    with pytest.raises(NotImplementedError):
        aq.build_query()


def test_searches_builders_basic_search_query_factory_init(params_parser):
    from snovault.elasticsearch.searches.queries import BasicSearchQueryFactory
    bsqf = BasicSearchQueryFactory(params_parser)
    assert isinstance(bsqf, BasicSearchQueryFactory)
    assert bsqf.params_parser == params_parser