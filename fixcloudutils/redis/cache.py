#  Copyright (c) 2023. Some Engineering
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU Affero General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU Affero General Public License for more details.
#
#  You should have received a copy of the GNU Affero General Public License
#  along with this program.  If not, see <http://www.gnu.org/licenses/>.
import base64
import hashlib
import logging
import pickle
from asyncio import Task, Queue, CancelledError, create_task
from datetime import datetime, timedelta
from typing import Any, Optional, TypeVar, Callable, ParamSpec, NewType

from attr import frozen
from prometheus_client import Counter
from redis.asyncio import Redis

from fixcloudutils.asyncio import stop_running_task
from fixcloudutils.asyncio.periodic import Periodic
from fixcloudutils.redis.pub_sub import RedisPubSubListener, RedisPubSubPublisher
from fixcloudutils.service import Service
from fixcloudutils.types import Json
from fixcloudutils.util import utc, uuid_str

log = logging.getLogger("fixcloudutils.redis.cache")
RedisKey = NewType("RedisKey", str)
P = ParamSpec("P")
T = TypeVar("T")
CacheHit = Counter("redis_cache", "Redis Cache", ["key", "stage"])


@frozen
class RedisCacheSet:
    key: RedisKey
    value: Any
    ttl: timedelta


@frozen
class RedisCacheEvict:
    key: RedisKey


# Actions that can be sent to a RedisCache queue listener
RedisAction = RedisCacheSet | RedisCacheEvict


@frozen
class RedisCacheEntry:
    key: Any
    redis_key: RedisKey
    value: Any
    deadline: datetime


class RedisCache(Service):
    def __init__(
        self,
        redis: Redis,
        key: str,
        *,
        ttl_memory: Optional[timedelta] = None,
        ttl_redis: Optional[timedelta] = None,
        cleaner_task_frequency: timedelta = timedelta(seconds=10),
    ) -> None:
        self.redis = redis
        self.key = key
        self.ttl_memory = ttl_memory or timedelta(minutes=5)
        self.ttl_redis = ttl_redis or timedelta(minutes=30)
        self.queue: Queue[RedisAction] = Queue(128)
        self.should_run = True
        self.started = False
        self.process_queue_task: Optional[Task[None]] = None
        self.event_listener = RedisPubSubListener(redis, f"cache_evict:{key}", self._handle_evict_message)
        self.event_publisher = RedisPubSubPublisher(redis, f"cache_evict:{key}", str(uuid_str()))
        self.local_cache: dict[Any, RedisCacheEntry] = {}
        self.cached_functions: dict[str, Any] = {}
        self.cleaner_task = Periodic("wipe_local_cache", self._wipe_outdated_from_local_cache, cleaner_task_frequency)

    async def start(self) -> None:
        if self.started:
            return
        self.started = True
        self.should_run = True
        await self.cleaner_task.start()
        self.process_queue_task = create_task(self._process_queue())

    async def stop(self) -> None:
        if not self.started:
            return
        self.should_run = False
        await stop_running_task(self.process_queue_task)
        await self.cleaner_task.stop()
        self.started = False

    async def evict(self, fn_name: str, key: str) -> None:
        log.info(f"{self.key}:{fn_name} Evict {key}")
        await self.queue.put(RedisCacheEvict(self._redis_key(fn_name, key)))

    def evict_with(self, fn: Callable[P, T]) -> Callable[P, T]:
        async def evict_fn(*args: Any, **kwargs: Any) -> None:
            key = self._redis_key(fn.__name__, None, *args, **kwargs)
            log.info(f"{self.key}:{fn.__name__} Evict args based key: {key}")
            await self.queue.put(RedisCacheEvict(key))

        return evict_fn  # type: ignore

    def call(
        self,
        fn: Callable[P, T],
        *,
        key: Optional[str] = None,
        ttl_memory: Optional[timedelta] = None,
        ttl_redis: Optional[timedelta] = None,
    ) -> Callable[P, T]:
        """
        This is the memoization function.
        If a value for this function call is available in the local cache, it will be returned.
        If a value for this function call is available in redis, it will be returned and added to the local cache.
        Otherwise, the function will be called and the result will be added to redis and the local cache.

        :param fn: The function that should be memoized.
        :param key: The key to use for memoization. If not provided, the function name and the arguments will be used.
        :param ttl_redis: The time to live for the redis entry. If not provided, the default ttl will be used.
        :param ttl_memory: The time to live for the local cache entry. If not provided, the default ttl will be used.
        :return: The result of the function call.
        """

        async def handle_call(*args: Any, **kwargs: Any) -> T:
            # check if the value is available in the local cache
            fns = fn.__name__
            local_cache_key = key or (fns, *args, *kwargs.values())
            if local_value := self.local_cache.get(local_cache_key):
                log.info(f"{self.key}:{fns} Serve result from local cache.")
                CacheHit.labels(self.key, "local").inc()
                return local_value.value  # type: ignore
            # check if the value is available in redis
            redis_key = self._redis_key(fns, key, *args, **kwargs)
            if redis_value := await self.redis.get(redis_key):
                log.info(f"{self.key}:{fns} Serve result from redis cache.")
                CacheHit.labels(self.key, "redis").inc()
                result: T = pickle.loads(base64.b64decode(redis_value))
                self._add_to_local_cache(local_cache_key, redis_key, result, ttl_memory or self.ttl_memory)
                return result
            # call the function
            result = await fn(*args, **kwargs)  # type: ignore
            CacheHit.labels(self.key, "call").inc()
            self._add_to_local_cache(local_cache_key, redis_key, result, ttl_memory or self.ttl_memory)
            await self.queue.put(RedisCacheSet(redis_key, result, ttl_redis or self.ttl_redis))
            return result

        return handle_call  # type: ignore

    async def _process_queue(self) -> None:
        """
        Local queue processor which will execute tasks in a separate execution context.
        """
        while self.should_run:
            try:
                entry = await self.queue.get()
                if isinstance(entry, RedisCacheSet):
                    log.info(f"{self.key}: Store cached value in redis as {entry.key}")
                    value = base64.b64encode(pickle.dumps(entry.value))
                    await self.redis.set(name=entry.key, value=value, ex=entry.ttl)
                elif isinstance(entry, RedisCacheEvict):
                    log.info(f"{self.key}: Delete cached value from redis key {entry.key}")
                    # delete the entry
                    await self.redis.delete(entry.key)
                    # inform all other cache instances to evict the key
                    await self.event_publisher.publish("evict", {"redis_key": entry.key})
                    # delete from local cache
                    self._remove_from_local_cache(entry.key)
                else:
                    log.warning(f"Unknown entry in queue: {entry}")
            except CancelledError:
                return  # ignore
            except Exception as ex:
                log.warning("Failed to process queue", exc_info=ex)

    async def _handle_evict_message(self, uid: str, at: datetime, publisher: str, kind: str, data: Json) -> None:
        """
        PubSub listener for evict messages.
        """
        log.info(f"Received message: {kind} {data} from {publisher} at {at} by {uid}")
        if kind == "evict" and (redis_key := data.get("redis_key")):
            # delete from local cache
            self._remove_from_local_cache(redis_key)
        else:  # pragma: no cover
            log.warning(f"Unknown message: {kind} {data}")

    async def _wipe_outdated_from_local_cache(self) -> None:
        """
        Periodically called by this cache instance to remove outdated entries from the local cache.
        """
        now = utc()
        for key, entry in list(self.local_cache.items()):
            if entry.deadline < now:
                log.info(f"Evicting {key} from local cache")
                del self.local_cache[key]

    def _add_to_local_cache(self, key: Any, redis_key: RedisKey, value: Any, ttl: timedelta) -> None:
        entry = RedisCacheEntry(key=key, redis_key=redis_key, value=value, deadline=utc() + ttl)
        self.local_cache[key] = entry

    def _remove_from_local_cache(self, redis_key: str) -> None:
        local_key: Optional[Any] = None
        for key, entry in self.local_cache.items():
            if entry.redis_key == redis_key:
                local_key = key
                break
        if local_key:
            log.info(f"Evicting {redis_key} from local cache")
            del self.local_cache[local_key]

    def _redis_key(self, fn_name: str, fn_key: Optional[str], *args: Any, **kwargs: Any) -> RedisKey:
        if fn_key is None:
            sha = hashlib.sha256()
            for a in args:
                sha.update(pickle.dumps(a))
            for k, v in kwargs.items():
                sha.update(pickle.dumps(k))
                sha.update(pickle.dumps(v))
            fn_key = sha.hexdigest()
        return RedisKey(f"cache:{self.key}:{fn_name}:{fn_key}")


class redis_cached:  # noqa
    """
    Decorator for caching function calls in redis.

    Usage:
    >>> redis = Redis()
    >>> cache = RedisCache(redis, "test", "test1")
    >>> @redis_cached(cache)
    ... async def f(a: int, b: int) -> int:
    ...     return a + b
    """

    def __init__(
        self,
        cache: RedisCache,
        *,
        key: Optional[str] = None,
        ttl_memory: Optional[timedelta] = None,
        ttl_redis: Optional[timedelta] = None,
    ):
        self.cache = cache
        self.key = key
        self.ttl_memory = ttl_memory
        self.ttl_redis = ttl_redis

    def __call__(self, fn: Callable[P, T]) -> Callable[P, T]:
        return self.cache.call(fn, key=self.key, ttl_memory=self.ttl_memory, ttl_redis=self.ttl_redis)
