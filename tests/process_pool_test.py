#  Copyright (c) 2024. Some Engineering
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
import asyncio
import os
import re
from asyncio import Task
from concurrent.futures.process import BrokenProcessPool
from time import sleep
from typing import Tuple, List

import pytest

from fixcloudutils.asyncio.process_pool import AsyncProcessPool


def do_work(i: int) -> Tuple[str, int]:
    sleep(0.1)
    return f"GOT {i}", os.getpid()


def broken_work(i: int) -> Tuple[str, int]:
    if i == 3:
        os._exit(1)
    else:
        return do_work(i)


async def test_process_pool() -> None:
    tasks: List[Task[Tuple[str, int]]] = []
    pids = set()
    task: str
    async with AsyncProcessPool(max_workers=5) as pool:
        for num in range(5):
            create_task = asyncio.create_task(pool.submit(do_work, num))
            tasks.append(create_task)
        for task, pid in await asyncio.gather(*tasks):
            assert re.match(r"GOT [0-4]", task) is not None
            assert pid not in pids
            pids.add(pid)


async def test_process_pool_broken() -> None:
    tasks = []
    async with AsyncProcessPool(max_workers=5) as pool:
        for num in range(5):
            tasks.append(asyncio.create_task(pool.submit(broken_work, num)))
        for t in asyncio.as_completed(tasks):
            # one failing task will make the whole batch fail
            with pytest.raises(BrokenProcessPool):
                await t
        # we can still submit new tasks that get executed
        tasks = []
        for num in range(5):
            tasks.append(asyncio.create_task(pool.submit(do_work, num)))
        for task, pid in await asyncio.gather(*tasks):
            assert re.match(r"GOT [0-4]", task) is not None
