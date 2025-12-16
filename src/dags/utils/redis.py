import redis
from typing import Any, Optional

class RedisClient:
    def __init__(
        self,
        uri: str,
        **kwargs
    ):
        """
        Initialize the Redis client.

        :param uri: Redis connection URI.
        :param kwargs: Additional keyword arguments for redis.Redis.from_url.
        """
        self.uri = uri
        self.client: Optional[redis.Redis] = None
        self.client_kwargs = kwargs

    def connect(self):
        """Connect to the Redis client using the URI."""
        self.client = redis.Redis.from_url(self.uri, **self.client_kwargs)

    def disconnect(self):
        """Disconnect from the Redis client."""
        if self.client:
            self.client.close()
            self.client = None

    def write(self, key: Any, value: Any):
        if self.client is None:
            raise Exception("Redis connection not established.")
        self.client.set(key, value)

    def read(self, key: Any):
        if self.client is None:
            raise Exception("Redis connection not established.")
        return self.client.get(key)