import time
from .util import quick_deepcopy
from posixpath import join
from pyramid.compat import (
    native_,
    unquote_bytes_to_wsgi,
)
from pyramid.httpexceptions import HTTPNotFound
from .interfaces import CONNECTION
import logging
log = logging.getLogger(__name__)


def includeme(config):
    config.scan(__name__)
    config.add_renderer('null_renderer', NullRenderer)
    config.add_request_method(embed, 'embed')
    config.add_request_method(lambda request: set(), '_embedded_uuids', reify=True)
    config.add_request_method(lambda request: set(), '_linked_uuids', reify=True)
    config.add_request_method(lambda request: None, '__parent__', reify=True)


def make_subrequest(request, path):
    """ Make a subrequest

    Copies request environ data for authentication.

    May be better to just pull out the resource through traversal and manually
    perform security checks.
    """
    env = request.environ.copy()
    if path and '?' in path:
        path_info, query_string = path.split('?', 1)
        path_info = path_info
    else:
        path_info = path
        query_string = ''
    env['PATH_INFO'] = path_info
    env['QUERY_STRING'] = query_string
    subreq = request.__class__(env, method='GET', content_type=None,
                               body=b'')
    subreq.remove_conditional_headers()
    # XXX "This does not remove headers like If-Match"
    subreq.__parent__ = request
    return subreq


def embed(request, *elements, **kw):
    """ as_user=True for current user
    """
    start_time = time.time()
    # Should really be more careful about what gets included instead.
    # Cache cut response time from ~800ms to ~420ms.
    embed_cache = request.registry[CONNECTION].embed_cache
    as_user = kw.get('as_user')
    path = join(*elements)
    path = unquote_bytes_to_wsgi(native_(path))
    # log.debug('embed: %s', path)
    print('')
    print('embed.py:embed', 'start', path)
    if as_user is not None:
        print('embed.py:embed', 'call as_user _embed', path)
        result, embedded, linked = _embed(request, path, as_user)
    else:
        cached = embed_cache.get(path, None)
        if cached is None:
            print('embed.py:embed', 'call not cached _embed', path)
            cached = _embed(request, path)
            embed_cache[path] = cached
        result, embedded, linked = cached
        result = quick_deepcopy(result)
    request._embedded_uuids.update(embedded)
    request._linked_uuids.update(linked)
    print('')
    print('embed.py:embed', request.url, 'embedded uuids', embedded)
    print('embed.py:embed', request.url, 'linked uuids', linked)
    print('embed.py:embed', 'end', request.url, '%.6f' % (time.time() - start_time))
    return result


def _embed(request, path, as_user='EMBED'):
    start_time = time.time()
    print('embed.py:_embed', 'start', path)
    subreq = make_subrequest(request, path)
    subreq.override_renderer = 'null_renderer'
    if as_user is not True:
        if 'HTTP_COOKIE' in subreq.environ:
            del subreq.environ['HTTP_COOKIE']
        subreq.remote_user = as_user
    try:
        sub_start_time = time.time()
        print('embed.py:_embed', 'call invoke_subrequest', path)
        result = request.invoke_subrequest(subreq)
        if path == '/labs/peggy-farnham/':
            fjfj
        print('embed.py:_embed', 'call invoke_subrequest', path, '%.6f' % (time.time() - sub_start_time))
    except HTTPNotFound:
        raise KeyError(path)
    print('embed.py:_embed', 'end', path, '%.6f' % (time.time() - start_time))
    return result, subreq._embedded_uuids, subreq._linked_uuids


class NullRenderer:
    '''Sets result value directly as response.
    '''
    def __init__(self, info):
        pass

    def __call__(self, value, system):
        request = system.get('request')
        if request is None:
            return value
        request.response = value
        return None
