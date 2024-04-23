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
import logging
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from datetime import timedelta
from functools import partial
from typing import Optional, Any, Callable, TypeVar

from fixcloudutils.service import Service

log = logging.getLogger(__name__)
T = TypeVar("T")


class AsyncProcessPool(Service):
    def __init__(self, timeout: Optional[timedelta] = None, **pool_args: Any):
        self.pool_args = pool_args
        self.executor: Optional[ProcessPoolExecutor] = None
        self.lock = asyncio.Lock()
        self.timeout = timeout.total_seconds() if timeout else None

    async def start(self) -> Any:
        async with self.lock:
            if not self.executor:
                self.executor = ProcessPoolExecutor(**self.pool_args)

    async def stop(self) -> None:
        async with self.lock:
            if self.executor:
                self.executor.shutdown(wait=True)
                self.executor = None

    async def submit(self, func: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        assert self.executor is not None, "Executor not started"
        loop = asyncio.get_running_loop()
        executor_id = id(self.executor)
        try:
            fn = func if not kwargs else partial(func, **kwargs)
            return await asyncio.wait_for(loop.run_in_executor(self.executor, fn, *args), timeout=self.timeout)  # type: ignore # noqa
        except BrokenProcessPool:  # every running task will raise this exception
            async with self.lock:
                if id(self.executor) == executor_id:  # does this exception comes from the current executor?
                    log.warning("A process in the pool died unexpectedly. Creating a new pool.", exc_info=True)
                    self.executor.shutdown(wait=False)
                    self.executor = ProcessPoolExecutor(**self.pool_args)
            raise
