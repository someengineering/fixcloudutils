import asyncio
from time import sleep

from fixcloudutils.asyncio.async_extensions import run_async


async def test_run_async() -> None:
    def io_bound_method() -> None:
        sleep(0.1)

    started = asyncio.get_event_loop().time()
    await asyncio.gather(*[run_async(io_bound_method) for _ in range(10)])
    duration = asyncio.get_event_loop().time() - started
    assert duration < 0.3, f"Sleeping 10 times 0.1 seconds in parallel should take ~ 0.1 seconds. Took {duration}s."
