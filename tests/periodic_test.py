import asyncio
from datetime import timedelta

import pytest

from fixcloudutils.asyncio.periodic import Periodic


@pytest.mark.asyncio
async def test_periodic() -> None:
    count = 0

    async def func() -> None:
        nonlocal count
        count += 1

    async def check_count() -> None:
        while count < 10:
            await asyncio.sleep(0.1)

    async with Periodic("test", func, timedelta(milliseconds=1)) as periodic:
        assert periodic.started
        await asyncio.wait_for(check_count(), timeout=2)
    assert count >= 10
