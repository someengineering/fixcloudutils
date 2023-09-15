import asyncio
import os

import pytest
from redis.asyncio import Redis

from fixcloudutils.redis.lock import Lock


@pytest.mark.asyncio
@pytest.mark.skipif(os.environ.get("REDIS_RUNNING") is None, reason="Redis is not running")
async def test_lock(redis: Redis) -> None:
    holy_grail = [0]  # one task should only modify the holy grail at a time
    cond = asyncio.Event()  # mark the beginning of the test
    number = 0  # counts the number of concurrent tasks

    async def try_with_lock(num: int) -> str:
        nonlocal number

        async def perform_locked_action() -> str:
            print(f"[{num}] performing action")
            nonlocal holy_grail
            holy_grail[0] += 1
            holy_grail.append(num)
            assert len(holy_grail) == 2
            assert holy_grail[-1] == num
            assert len(holy_grail) == 2
            holy_grail.pop()
            print(f"[{num}] performing action done")
            return "done"

        lock = Lock(redis, "test_lock", 5)
        number += 1
        await cond.wait()  # wait for the test driver to start
        return await lock.with_lock(perform_locked_action())

    tasks = [asyncio.create_task(try_with_lock(num)) for num in range(10)]
    # wait for all tasks to start
    while number < 10:
        await asyncio.sleep(0.1)
    cond.set()
    await asyncio.gather(*tasks)
