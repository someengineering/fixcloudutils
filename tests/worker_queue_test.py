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
from typing import Dict

from arq.connections import RedisSettings
from arq.worker import func

from fixcloudutils.redis.worker_queue import WorkerInstance, WorkDispatcher


async def example(ctx: Dict[str, str], num: int, txt: str) -> str:
    return f"test {num} {txt}"


async def test_worker(arq_settings: RedisSettings) -> None:
    async with WorkDispatcher(arq_settings, "test_queue") as dispatcher:
        async with WorkerInstance(
            redis_settings=arq_settings,
            queue_name="test_queue",
            functions=[func(example, name="example")],
        ):
            jobs = {num: await dispatcher.enqueue("example", num, "test") for num in range(10)}
            assert len(jobs) == 10
            for num, job in jobs.items():
                res = await job.result()
                assert res == f"test {num} test"
