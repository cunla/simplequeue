from datetime import datetime
from typing import List, Optional

from redis import Redis
from redis.client import Pipeline

from rq.utils import as_str

_MAX_KEYS = 1000


class SetRegistry:
    """A registry for a model. Store a set of keys for a model"""

    def __init__(self, connection: Redis, registry_key: str):
        self._connection = connection
        self._registry_key = registry_key

    def all(self) -> List[str]:
        """Returns all keys in the registry."""
        results = self._connection.smembers(self._registry_key)
        return [result.decode() for result in results]

    def add(self, key: str, pipeline: Optional[Pipeline] = None) -> None:
        """Adds a key to the registry."""
        connection = pipeline or self._connection
        connection.sadd(self._registry_key, key)

    def remove(self, key: str, pipeline: Optional[Pipeline] = None) -> None:
        """Removes a key from the registry."""
        connection = pipeline or self._connection
        connection.srem(self._registry_key, key)

    def cleanup(self):
        """Removes keys from the registry that don't exist in Redis."""
        keys = self.all()
        pipeline = self._pipeline()
        for key in keys:
            pipeline.exists(key)
        results = pipeline.execute()
        invalid_keys = [key for key, exists in zip(keys, results) if not exists]
        for i in range(0, len(invalid_keys), _MAX_KEYS):
            pipeline.srem(self._registry_key, *invalid_keys[i: i + _MAX_KEYS])
            pipeline.execute()
        pipeline.execute()

    def count(self):
        return len(self)

    def _pipeline(self):
        return self._connection.pipeline()

    def __contains__(self, item: str) -> bool:
        """Checks if a key exists in the registry."""
        return self._connection.sismember(self._registry_key, item) > 0

    def __len__(self):
        """Returns the number of keys in the registry."""
        return self._connection.scard(self._registry_key)


class KeyValueModel:
    registry_key: str = ":registry_key:"
    element_key_template: str = ":element:%s"
    fields_formatting: dict = {
        "created_at": lambda x: x.isoformat(),
        "updated_at": lambda x: x.isoformat(),
        "connection": lambda x: None,
    }

    def __init__(self, name: str, connection: Redis, created_at: Optional[datetime] = None):
        self.connection = connection
        self.name = name
        self.created_at: datetime = created_at or datetime.now()

    @property
    def _key(self) -> str:
        return self.element_key_template.format(self.name)

    @classmethod
    def all(cls, connection: Redis) -> List[str]:
        results = connection.lrange(cls.registry_key, 0, -1)
        return [result.decode() for result in results]

    @classmethod
    def exists(cls, name: str, connection: Redis) -> bool:
        return connection.exists(cls.element_key_template.format(name)) > 0

    @classmethod
    def get(cls, name: str, connection: Redis):
        raise NotImplementedError

    def save(self) -> bool:
        raise NotImplementedError

    def delete(self, name: str) -> bool:
        self.connection.lrem(self.registry_key, 0, name)
        res = self.connection.delete(self._key)
        return res > 0

    def get_field(self, field: str) -> Optional[str]:
        res = self.connection.hget(self._key, field)
        return as_str(res)

    def set_field(self, field: str, value: str) -> bool:
        return self.connection.hset(self._key, field, value) > 0


class HashModel(KeyValueModel):

    @classmethod
    def get(cls, name: str, connection: Redis) -> 'HashModel':
        res = connection.hgetall(cls.element_key_template.format(name))
        return cls(**res)

    def save(self) -> bool:
        res = self.__dict__.copy()
        self.connection.lpush(self.list_key, self._key)
        for field in self.fields_formatting:
            res[field] = self.fields_formatting[field](res[field])
        return self.connection.hset(self._key, mapping=res) > 0
