from __future__ import annotations

import asyncio
import logging
from typing import TypeVar, Coroutine, Any

from redis.asyncio import Redis
from redis.asyncio.lock import Lock as RedisLock

from fixcloudutils.asyncio import stop_running_task

log = logging.getLogger(__file__)
T = TypeVar("T")


class Lock:
    # noinspection PyUnresolvedReferences
    """
    Redis based Lock extension.
    You cannot use this lock as context manager, but pass the function to be performed.
    The lock will be created/released when the action is performed.
    The action is canceled if the lock cannot be extended.

    Example:
    >>> async def perform_locked_action() -> str:
    >>>     print("Acquired the lock!")
    >>>     # do stuff here that needs controlled access
    >>>     return "done"
    >>> async def main() -> None:
    >>>    redis: Redis = ...
    >>>    lock = Lock(redis, "test_lock", 5)
    >>>    result = await lock.with_lock(perform_locked_action())
    """

    def __init__(self, redis: Redis, lock_name: str, timeout: float):
        self.redis = redis
        self.lock_name = "redlock__" + lock_name
        self.timeout = timeout

    async def with_lock(self, coro: Coroutine[T, None, Any]) -> T:
        """
        Use this method in a situation where the time it takes for the coroutine to run is unknown.
        This method will extend the lock time as long as the coroutine is running (every half of the auto_release_time).
        If the lock cannot be extended, the coroutine will be
        stopped and an ExtendUnlockedLock exception will be raised.

        :param coro: The coroutine to execute.
        :return: The result of the coroutine
        """

        async with self.lock() as lock:

            async def extend_lock() -> None:
                while True:
                    extend_time = self.timeout / 2
                    await asyncio.sleep(extend_time)
                    log.debug(f"Extend the lock {self.lock_name} for {self.timeout} seconds.")
                    await lock.extend(self.timeout)  # will throw in case the lock is not owned anymore

            # The extend_lock task will never return a result but only an exception
            # So we can take the first done, which will be either:
            # - the exception from extend_lock
            # - the exception from coro
            # - the result from coro
            done, pending = await asyncio.wait(
                [asyncio.create_task(extend_lock()), asyncio.create_task(coro)], return_when=asyncio.FIRST_COMPLETED
            )
            for task in pending:
                await stop_running_task(task)
            for task in done:
                return task.result()  # type: ignore
            raise Exception("You should never come here!")

    def lock(self) -> RedisLock:
        return self.redis.lock(name=self.lock_name, timeout=self.timeout)


if __name__ == "__main__":
    import time

    shift = 1694685084

    def show(*args: Any) -> None:
        t = int((time.time() - shift) * 1000)
        print(f"[{t}] ", *args)

    async def simple_check() -> None:
        redis = Redis.from_url("redis://localhost:6379")
        lock = Lock(redis, "test_lock", 5)

        async def perform_action() -> str:
            show("Acquired the lock!")
            for i in range(11):
                show("Performing work. Still locked.")
                await asyncio.sleep(1)
            return "{int(time.time() * 1000)} done"

        while True:
            try:
                result = await lock.with_lock(perform_action())
                show("Lock released. Result of locked action: ", result)
            except Exception as ex:
                show("GOT exception", ex)
            finally:
                await asyncio.sleep(1)

    logging.basicConfig(level=logging.DEBUG)
    asyncio.run(simple_check())
