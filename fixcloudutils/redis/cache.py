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
from functools import wraps
from typing import Any, Optional, TypeVar, Callable, ParamSpec, NewType, Hashable, Awaitable

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
    fn_name: str
    fn_key: str
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
    fn_key: str
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
        assert self.ttl_memory < self.ttl_redis, "ttl_memory must be smaller than ttl_redis"

    async def start(self) -> None:
        if self.started:
            return
        self.started = True
        self.should_run = True
        await self.cleaner_task.start()
        await self.event_listener.start()
        await self.event_publisher.start()
        self.process_queue_task = create_task(self._process_queue())

    async def stop(self) -> None:
        if not self.started:
            return
        self.should_run = False
        await self.event_publisher.stop()
        await self.event_listener.stop()
        await stop_running_task(self.process_queue_task)
        await self.cleaner_task.stop()
        self.started = False

    async def evict(self, key: str) -> None:
        log.debug(f"{self.key}: Evict {key}")
        await self.queue.put(RedisCacheEvict(self._redis_key(key)))

    def cache(
        self, key: str, *, ttl_memory: Optional[timedelta] = None, ttl_redis: Optional[timedelta] = None
    ) -> Callable[[Callable[P, T]], Callable[P, Awaitable[T]]]:
        """
        Use it as a decorator.
        ```
        rc = RedisCache(...)
        @rc.cache("my_key")
        def my_function():
            ...
        ```
        """

        def decorator(fn: Callable[P, T]) -> Callable[P, Awaitable[T]]:
            @wraps(fn)
            async def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
                return await self.call(fn, key, ttl_memory=ttl_memory, ttl_redis=ttl_redis)(*args, **kwargs)  # type: ignore # noqa

            return wrapper

        return decorator

    def call(
        self,
        fn: Callable[P, T],
        key: str,
        *,
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
            fn_key = self._fn_key(fns, *args, *kwargs.values())
            local_cache_key = (key, fn_key)
            if local_value := self.local_cache.get(local_cache_key):
                log.info(f"{self.key}:{fns}:{key} Serve result from local cache.")
                CacheHit.labels(self.key, "local").inc()
                return local_value.value  # type: ignore
            # check if the value is available in redis
            redis_key = self._redis_key(key)
            if redis_value := await self.redis.hget(redis_key, fn_key):  # type: ignore
                log.info(f"{self.key}:{fns}:{key} Serve result from redis cache.")
                CacheHit.labels(self.key, "redis").inc()
                result: T = pickle.loads(base64.b64decode(redis_value))
                self._add_to_local_cache(local_cache_key, redis_key, fn_key, result, ttl_memory or self.ttl_memory)
                return result
            # call the function
            result = await fn(*args, **kwargs)  # type: ignore
            CacheHit.labels(self.key, "call").inc()
            self._add_to_local_cache(local_cache_key, redis_key, fn_key, result, ttl_memory or self.ttl_memory)
            await self.queue.put(RedisCacheSet(redis_key, fns, fn_key, result, ttl_redis or self.ttl_redis))
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
                    log.info(f"{self.key}:{entry.fn_name} Store cached value in redis as {entry.key}:{entry.fn_key}")
                    value = base64.b64encode(pickle.dumps(entry.value)).decode("utf-8")
                    await self.redis.hset(name=entry.key, key=entry.fn_key, value=value)  # type: ignore
                    await self.redis.expire(name=entry.key, time=entry.ttl)
                elif isinstance(entry, RedisCacheEvict):
                    # delete the entry
                    if (await self.redis.delete(entry.key)) > 0:
                        log.info(f"{self.key}: Deleted cached value from redis key {entry.key}")
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
        log.debug(f"Received message: {kind} {data} from {publisher} at {at} by {uid}")
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

    def _add_to_local_cache(self, key: Any, redis_key: RedisKey, fn_key: str, value: Any, ttl: timedelta) -> None:
        entry = RedisCacheEntry(key=key, redis_key=redis_key, fn_key=fn_key, value=value, deadline=utc() + ttl)
        self.local_cache[key] = entry

    def _remove_from_local_cache(self, redis_key: RedisKey) -> None:
        entries = [key for key, entry in self.local_cache.items() if entry.redis_key == redis_key]
        if entries:
            log.info(f"Evicting {len(entries)} entries for key {redis_key} from local cache")
            for k in entries:
                del self.local_cache[k]

    def _redis_key(self, key: str) -> RedisKey:
        return RedisKey(f"cache:{self.key}:{key}")

    def _fn_key(self, fn_name: str, *args: Any, **kwargs: Any) -> str:
        counter = 0

        def object_hash(obj: Any) -> bytes:
            nonlocal counter
            if isinstance(obj, Hashable):
                counter += 1
                return f"{counter}:{hash(obj)}".encode("utf-8")
            else:
                return pickle.dumps(obj)

        if len(args) == 0 and len(kwargs) == 0:
            return fn_name

        sha = hashlib.sha256()
        sha.update(fn_name.encode("utf-8"))
        for a in args:
            sha.update(object_hash(a))
        for k, v in kwargs.items():
            sha.update(object_hash(k))
            sha.update(object_hash(v))
        return sha.hexdigest()
