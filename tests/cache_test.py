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
import os
from contextlib import AsyncExitStack
from datetime import timedelta

import pytest
from redis.asyncio import Redis

from conftest import eventually
from fixcloudutils.redis.cache import RedisCache


@pytest.mark.asyncio
@pytest.mark.skipif(os.environ.get("REDIS_RUNNING") is None, reason="Redis is not running")
async def test_cache(redis: Redis) -> None:
    t0 = timedelta(seconds=0.1)
    t1 = timedelta(seconds=0.5)
    t2 = timedelta(seconds=1)
    t3 = timedelta(seconds=60)

    async with AsyncExitStack() as stack:
        cache1 = await stack.enter_async_context(
            RedisCache(redis, "test", ttl_redis=t2, ttl_memory=t1, cleaner_task_frequency=t0)
        )
        cache2 = await stack.enter_async_context(
            RedisCache(redis, "test", ttl_redis=t2, ttl_memory=t1, cleaner_task_frequency=t0)
        )
        call_count = 0

        async def complex_function(a: int, b: int) -> int:
            nonlocal call_count
            call_count += 1
            return a + b

        async def local_cache_is_empty(cache: RedisCache) -> bool:
            return len(cache.local_cache) == 0

        key = cache1._redis_key(complex_function.__name__, None, 1, 2)
        assert await cache1.call(complex_function)(1, 2) == 3
        assert call_count == 1
        assert len(cache1.local_cache) == 1
        # should come from internal memory cache
        assert await cache1.call(complex_function)(1, 2) == 3
        assert call_count == 1
        await eventually(redis.exists, key, timeout=2)
        # should come from redis cache
        assert len(cache2.local_cache) == 0
        assert await cache2.call(complex_function)(1, 2) == 3
        assert call_count == 1
        assert len(cache2.local_cache) == 1
        # after ttl expires, the local cache is empty
        await eventually(local_cache_is_empty, cache1, timeout=1)
        await eventually(local_cache_is_empty, cache2, timeout=1)
        # after redis ttl the cache is evicted
        await eventually(redis.exists, key, fn=lambda x: not x, timeout=2)

        # calling this method again should trigger a new call and a new cache entry
        # we use a loner redis ttl to test the eviction of the redis cache
        for a in range(100):
            assert await cache1.call(complex_function, ttl_redis=t3)(a, 2) == a + 2
        assert call_count == 101
        assert len(cache1.local_cache) == 100
        for a in range(100):
            assert await cache1.call(complex_function)(a, 2) == a + 2
            assert await cache2.call(complex_function)(a, 2) == a + 2
        assert len(cache1.local_cache) == 100
        assert len(cache2.local_cache) == 100
        # no more calls are done
        assert call_count == 101

        # evict all entries should evict all messages in all caches
        for a in range(100):
            await cache1.evict_with(complex_function)(a, 2)
        await eventually(local_cache_is_empty, cache1, timeout=1)
        await eventually(local_cache_is_empty, cache2, timeout=1)
