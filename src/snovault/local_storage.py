from redis import StrictRedis

from pyramid.view import view_config

from snovault.interfaces import LOCAL_STORAGE


def includeme(config):
    config.scan(__name__)
    config.add_route('local_storage', '/local_storage')
    config.registry[LOCAL_STORAGE] = LocalStoreClient()


@view_config(route_name='local_storage', request_method='GET')
def local_storage(request):
    return {'local_storage': str(request.registry[LOCAL_STORAGE])}


class LocalStoreClient():
    client = None
    
    def __init__(self, redis_db=0, local_store=None):
        if local_store:
            self.client = StrictRedis(
                charset="utf-8",
                decode_responses=True,
                db=redis_db,
                host='localhost',
                port=6379,
                socket_timeout=5,
            )
        else:
            # ephemeral storage objects.
            self.state = {}
            self.events = []
            self.items = {}

    def get_tag(self, num_bytes=8):
        return binascii.b2a_hex(urandom(num_bytes)).decode('utf-8')
   
    def hash_get(self, key):
        # Get the dict
        if self.client:
            return self.client.hgetall(key)
        return self.state

    def hash_set(self, key, hash_dict):
        # Stores a dict.  Allowed values for keys are limited
        if self.client:
            return self.client.hmset(key, hash_dict)
        self.state.update(hash_dict)

    def item_get(self, key):
        if self.client:
            return self.client.get(key)
        return self.items.get(key)
    
    def item_set(self, key, item):
        # Add item with key
        if self.client:
            return self.client.set(key, item)
        self.items[key] = item

    def list_add(self, key, item):
        # List is for storing item tags
        if self.client:
            return self.client.lpush(key, item)
        self.events.insert(0, item)
    
    def list_get(self, key, start, stop):
        # list get must have range, 0 to -1 min/max
        if self.client:
            return self.client.lrange(key, start, stop)
        if stop == 0:
            stop += 1
        if stop == -1:
            return self.events[start:]
        else:
            return self.events[start:stop]
