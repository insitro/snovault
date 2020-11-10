import copy
import json
import requests
import sys
from os.path import expanduser


home = expanduser("~")
ALL_APP_QUERY = 'type=*'
base_path = f"{home}/.encoded/snovault-queries"


def read_json(path):
    with open(f"{path}.json", "r") as fh:
        return json.load(fh)


def write_json(d, path):
    with open(f"{path}.json", "w") as fh:
        json.dump(d, fh, indent=4, sort_keys=True)


def _fix_file_name(url_name):
    url_name = url_name.replace('.', '')
    url_name = url_name.replace(',', '')
    url_name = url_name.replace(' ', '_')
    return url_name


def fix_url_name(url_name):
    url_name = url_name.replace(' ', '+')
    url_name = url_name.replace(',', '%2C')
    return url_name


def get_app_res(query_string):
    app_host = 'http://localhost:6543'
    app_headers = {'Content-type': 'application/json'}
    app_url_base = f"{app_host}/search"
    app_query_base = 'format=json'
    # Calling a url to the app will save the es query used
    url = f"{app_url_base}?{app_query_base}&{query_string}"
    app_res = requests.get(url, headers=app_headers)
    app_res_dict = app_res.json()
    # Get res save name
    file_name = None
    filter_names = []
    type_names = []
    for filter_dict in app_res_dict['filters']:
        term = _fix_file_name(filter_dict['term'])
        if filter_dict['field'] == 'type':
            type_names.append(term)
        else:
            filter_names.append(term)
    # Save the app res
    if filter_names or type_names:
        type_names.sort()
        filter_names.sort()
        type_names.extend(filter_names)
        file_name = '_'.join(type_names)
        write_json(app_res_dict, f"{base_path}/auto-json/{file_name}_app_res")
        return url, app_res_dict, f"{base_path}/auto-json/{file_name}_query"
    print(app_res_dict)
    print(f"get app res failure on {query_string}")
    sys.exit()


def write_es_res(query_path):
    es_url = 'http://localhost:9201/snovault-resources/_search'
    es_headers = {'Content-type': 'application/json'}
    es_query_dict = read_json(query_path)
    es_res = requests.post(es_url, headers=es_headers, json=es_query_dict)
    es_res_dict = es_res.json()
    write_json(es_res_dict, f"{query_path}_es_res")
    return es_res_dict, 


def get_facet_urls(main_url, data_type):
    facet_urls = []
    app_res = requests.get(main_url, headers=app_headers)
    app_res_dict = app_res.json()
    write_json(app_res_dict, f"{base_path}/auto-json/{data_type}_app_res")
    for facet in app_res_dict['facets']:
        facet_name = facet['field']
        for term in facet['terms']:
            term_key = fix_url_name(term['key'])
            term_query = f"{facet_name}={term_key}"
            if term_query in main_url:
                continue
            facet_urls.append(f"{main_url}&{facet_name}={term_key}")
    return facet_urls


def _run_query(query_str):
    url, app_res, query_path = get_app_res(query_str)
    es_res = write_es_res(query_path)
    return url, app_res, query_path, es_res


facets_per_field_dict = {}
basic_url_queries = []

# Get all query and app res
url, app_res, query_path, es_res = _run_query(ALL_APP_QUERY)
basic_url_queries.append(ALL_APP_QUERY)

# Create followup urls from ALL_APP_QUERY response
followup_queries = []
for facet_type_dict in app_res['facets']:
    field_name = facet_type_dict['field']
    facets_per_field_dict[field_name] = []
    for facet_dict in facet_type_dict['terms']:
        field_value = fix_url_name(facet_dict['key'])
        followup_query = f"{field_name}={field_value}"
        if field_value not in facets_per_field_dict[field_name]:
            facets_per_field_dict[field_name].append(field_value)
        followup_queries.append(
            (field_name, facet_dict['key'], facet_dict['doc_count'], followup_query)
        )

# Run followup queries
followup_facet_queries = []
for qtuple in followup_queries:
    url, app_res, query_path, es_res = _run_query(qtuple[3])
    basic_url_queries.append(qtuple[3])
    # Create followup facet queries from followup query
    for facet_type_dict in app_res['facets']:
        field_name = facet_type_dict['field']
        if field_name not in facets_per_field_dict:
            facets_per_field_dict[field_name] = []
        for facet_dict in facet_type_dict['terms']:
            if field_name == 'type' and facet_dict['key'] in ['Item', qtuple[1]]:
                continue
            field_value = fix_url_name(facet_dict['key'])
            if field_value not in facets_per_field_dict[field_name]:
                facets_per_field_dict[field_name].append(field_value)
            followup_query = f"{qtuple[3]}&{field_name}={field_value}"
            followup_facet_queries.append(
                (f"qtuple[1]_{field_name}", facet_dict['key'], facet_dict['doc_count'], followup_query)
            )

# Run follow up facet queries
for qtuple in followup_facet_queries:
    url, app_res, query_path, es_res = _run_query(qtuple[3])
    basic_url_queries.append(qtuple[3])
write_json(basic_url_queries, f"{base_path}/urls/snovault_basic_queries")


# Clean up facets_per_field_dict
facets_per_field_dict_keys = list(facets_per_field_dict.keys())
facets_per_field_dict_keys.sort()
sorted_facets_per_field = {}
for key in facets_per_field_dict_keys:
    key_list = facets_per_field_dict[key]
    key_list.append('*')
    key_list.sort()
    sorted_facets_per_field[key] = key_list


# Create queries lists combining field keys with facets
field_queries_lists = []
for key, val in sorted_facets_per_field.items():
    new_list = []
    for v in val:
        new_list.append(f"{key}={v}")
    field_queries_lists.append(new_list)
write_json(field_queries_lists, f"{base_path}/urls/snovault_field_queries_lists")

sum_l = 0
for l in field_queries_lists:
    print(len(l), l)
    sum_l += len(l)
print(f"{sum_l} facets")

sys.exit()
