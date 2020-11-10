import json
import itertools
import sys
from os.path import expanduser


home = expanduser("~")
base_path = f"{home}/.encoded/snovault-queries"


def read_json(path):
    with open(f"{path}.json", "r") as fh:
        return json.load(fh)


def write_json(d, path):
    with open(f"{path}.json", "w") as fh:
        json.dump(d, fh, indent=4, sort_keys=True)

# Load facet data lists
field_queries_lists = read_json(f"{base_path}/urls/snovault_field_queries_lists")
sum_l = 0
for l in field_queries_lists:
    sum_l += len(l)
print(f"{sum_l} facets")

# Expand facet data lists int facet list combinations
product_lists = {}
max_len_of_queries = len(field_queries_lists)
for i in range(1, max_len_of_queries + 1):
    list_combos = list(itertools.combinations(field_queries_lists, i))
    product_lists[i] = list_combos
    print(f"i={i}: with {len(list_combos)} list to multiply")

# Expand facet lists combinations into flat urls. 11 million 
all_advanced_url_queries = set()
advanced_url_queries_dict = {}
for i in range(1, max_len_of_queries + 1):
    advanced_url_queries_dict[i] = set()
    for item in product_lists[i]:
        for query_parts in itertools.product(*item):
             advanced_url_queries_dict[i].add('&'.join(query_parts))
    all_advanced_url_queries.update(advanced_url_queries_dict[i])
    advanced_url_queries_dict[i] = list(advanced_url_queries_dict[i])
    write_path = f"{base_path}/urls/{i}_queries_{len(advanced_url_queries_dict[i])}"
    print(f"\twriting: {write_path}")
    write_json(advanced_url_queries_dict[i], write_path)
print('all', max_len_of_queries, len(all_advanced_url_queries))
