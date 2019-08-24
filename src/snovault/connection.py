import time
from past.builtins import basestring
from pyramid.decorator import reify
from uuid import UUID
from .cache import ManagerLRUCache
from .interfaces import (
    CONNECTION,
    STORAGE,
    TYPES,
)

def includeme(config):
    registry = config.registry
    registry[CONNECTION] = Connection(registry)


class UnknownItemTypeError(Exception):
    pass


class Connection(object):
    ''' Intermediates between the storage and the rest of the system
    '''
    def __init__(self, registry):
        self.registry = registry
        self.item_cache = ManagerLRUCache('snovault.connection.item_cache', 1000)
        self.unique_key_cache = ManagerLRUCache('snovault.connection.key_cache', 1000)
        embed_cache_capacity = int(registry.settings.get('embed_cache.capacity', 5000))
        self.embed_cache = ManagerLRUCache('snovault.connection.embed_cache', embed_cache_capacity)
    @reify
    def storage(self):
        return self.registry[STORAGE]

    @reify
    def types(self):
        return self.registry[TYPES]

    def get_by_uuid(self, uuid, default=None):
        if isinstance(uuid, basestring):
            try:
                uuid = UUID(uuid)
            except ValueError:
                return default
        elif not isinstance(uuid, UUID):
            raise TypeError(uuid)

        uuid = str(uuid)
        cached = self.item_cache.get(uuid)
        if cached is not None:
            return cached

        model = self.storage.get_by_uuid(uuid)
        if model is None:
            return default

        try:
            Item = self.types.by_item_type[model.item_type].factory
        except KeyError:
            raise UnknownItemTypeError(model.item_type)

        item = Item(self.registry, model)
        model.used_for(item)
        self.item_cache[uuid] = item
        return item

    def get_by_unique_key(self, unique_key, name, default=None, index=None):
        start_time = time.time()
        print('1 get_by_unique_key %0.6f' % (time.time() - start_time))
        pkey = (unique_key, name)
        print('2 get_by_unique_key %0.6f' % (time.time() - start_time))
        cached = self.unique_key_cache.get(pkey)
        print('3 get_by_unique_key %0.6f' % (time.time() - start_time))
        if cached is not None:
            print('4 get_by_unique_key %0.6f' % (time.time() - start_time))
            return self.get_by_uuid(cached)

        print('5 get_by_unique_key %0.6f' % (time.time() - start_time))
        model = self.storage.get_by_unique_key(unique_key, name, index=index)
        print('6 get_by_unique_key %0.6f' % (time.time() - start_time))
        if model is None:
            print('7 get_by_unique_key %0.6f' % (time.time() - start_time))
            return default
        print('8 get_by_unique_key %0.6f' % (time.time() - start_time))
        uuid = model.uuid
        print('9 get_by_unique_key %0.6f' % (time.time() - start_time))
        self.unique_key_cache[pkey] = uuid
        print('10 get_by_unique_key %0.6f' % (time.time() - start_time))
        cached = self.item_cache.get(uuid)
        print('11 get_by_unique_key %0.6f' % (time.time() - start_time))
        if cached is not None:
            return cached
        print('12 get_by_unique_key %0.6f' % (time.time() - start_time))
        try:
            print('13 get_by_unique_key %0.6f' % (time.time() - start_time))
            Item = self.types.by_item_type[model.item_type].factory
            print('14 get_by_unique_key %0.6f' % (time.time() - start_time))
        except KeyError:
            raise UnknownItemTypeError(model.item_type)

        print('15 get_by_unique_key %0.6f' % (time.time() - start_time))
        item = Item(self.registry, model)
        print('16 get_by_unique_key %0.6f' % (time.time() - start_time))
        model.used_for(item)
        print('17 get_by_unique_key %0.6f' % (time.time() - start_time))
        self.item_cache[uuid] = item
        print('18 get_by_unique_key %0.6f' % (time.time() - start_time))
        return item

    def get_rev_links(self, model, rel, *types):
        item_types = [self.types[t].item_type for t in types]
        return self.storage.get_rev_links(model, rel, *item_types)

    def __iter__(self, *types):
        if not types:
            item_types = self.types.by_item_type.keys()
        else:
            item_types = [self.types[t].item_type for t in types]
        for uuid in self.storage.__iter__(*item_types):
            yield uuid

    def __len__(self, *types):
        if not types:
            item_types = self.types.by_item_type.keys()
        else:
            item_types = [self.types[t].item_type for t in types]
        return self.storage.__len__(*item_types)

    def __getitem__(self, uuid):
        item = self.get_by_uuid(uuid)
        if item is None:
            raise KeyError(uuid)
        return item

    def create(self, type_, uuid):
        ti = self.types[type_]
        return self.storage.create(ti.item_type, uuid)

    def update(self, model, properties, sheets=None, unique_keys=None, links=None):
        self.storage.update(model, properties, sheets, unique_keys, links)
