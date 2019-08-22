import time
from past.builtins import basestring
from pyramid.threadlocal import manager as threadlocal_manager
from snovault.interfaces import ROOT


def includeme(config):
    config.add_request_method(select_distinct_values)


def get_root_request():
    if threadlocal_manager.stack:
        return threadlocal_manager.stack[0]['request']


def ensurelist(value):
    if isinstance(value, basestring):
        return [value]
    return value


def simple_path_ids(obj, path):
    if isinstance(path, basestring):
        path = path.split('.')
    if not path:
        yield obj
        return
    name = path[0]
    remaining = path[1:]
    value = obj.get(name, None)
    if value is None:
        return
    if not isinstance(value, list):
        value = [value]
    for member in value:
        for result in simple_path_ids(member, remaining):
            yield result


def expand_path(request, obj, path):
    print('expand_path', 'start')
    if isinstance(path, basestring):
        print('expand_path', 'basestring')
        path = path.split('.')
    if not path:
        return
    name = path[0]
    remaining = path[1:]
    value = obj.get(name, None)
    if value is None:
        print('expand_path', 'value is None')
        return
    if isinstance(value, list):
        print('expand_path', 'value is list', 'start')
        for index, member in enumerate(value):
            print('expand_path', 'value is list', 'loop')
            if not isinstance(member, dict):
                print('expand_path', 'not member dict')
                start_time_sub = time.time()
                member = value[index] = request.embed(member, '@@object')
                print('expand_path', 'not member dict', '%s %.6f' % (member, time.time() - start_time_sub))
            print('expand_path', 'value is list', 'loop end')
            start_time = time.time()
            expand_path(request, member, remaining)
            print('expand_path', 'remaining', '%s %.6f' % (remaining, time.time() - start_time))
    else:
        print('expand_path', 'value is not list', 'start')
        if not isinstance(value, dict):
            print('expand_path', 'value is not list', 'not dict')
            start_time_sub = time.time()
            value = obj[name] = request.embed(value, '@@object')
            print('expand_path', 'value', '%s %.6f' % (value, time.time() - start_time_sub))
        print('expand_path', 'value is not list', 'post')
        start_time = time.time()
        expand_path(request, value, remaining)
        print('expand_path', 'value remaining', '%s %s %.6f' % (remaining, time.time() - start_time))


def _get_calculated_properties_from_paths(request, paths):
    root = request.registry[ROOT]
    calculated_fields = set()
    # Expect paths in form ['/item1/identifier/', '/item2/identifier/', ...].
    item_types = {
        root.collections.get(p.split('/')[1]).type_info.item_type
        for p in paths
        if len(p.split('/')) == 4 and p.split('/')[1] in root.collections
    }
    for item_type in item_types:
        item_cls = request.registry['types'].by_item_type.get(item_type).factory
        calculated_fields.update(
            root.collections.get(item_type).type_info.calculated_properties.props_for(item_cls).keys()
        )
    return list(calculated_fields)


def select_distinct_values(request, value_path, *from_paths):
    if isinstance(value_path, basestring):
        value_path = value_path.split('.')
    values = from_paths
    for name in value_path:
        calculated_properties = _get_calculated_properties_from_paths(request, values)
        # Don't waste time calculating properties if the field isn't calculated.
        frame = '@@object' if name in calculated_properties else '@@object?skip_calculated=true'
        objs = (request.embed(member, frame) for member in values)
        value_lists = (ensurelist(obj.get(name, [])) for obj in objs)
        values = {value for value_list in value_lists for value in value_list}
    return list(values)


def quick_deepcopy(obj):
    """Deep copy an object consisting of dicts, lists, and primitives.

    This is faster than Python's `copy.deepcopy` because it doesn't
    do bookkeeping to avoid duplicating objects in a cyclic graph.

    This is intended to work fine for data deserialized from JSON,
    but won't work for everything.
    """
    if isinstance(obj, dict):
        obj = {k: quick_deepcopy(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        obj = [quick_deepcopy(v) for v in obj]
    return obj


def mutated_schema(schema, mutator):
    """Apply a change to all levels of a schema.

    Returns a new schema rather than modifying the original.
    """
    schema = mutator(schema.copy())
    if 'items' in schema:
        schema['items'] = mutated_schema(schema['items'], mutator)
    if 'properties' in schema:
        schema['properties'] = schema['properties'].copy()
        for k, v in schema['properties'].items():
            schema['properties'][k] = mutated_schema(v, mutator)
    return schema
