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
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import asyncio
import logging
from asyncio import AbstractEventLoop, Task
from contextlib import suppress
from datetime import timedelta
from typing import (
    Any,
    Callable,
    Optional,
    Awaitable,
)

from fixcloudutils.service import Service

log = logging.getLogger("fix.periodic")


class Periodic(Service):
    """
    Periodic execution of a function based on a defined frequency that can be started and stopped.
    """

    def __init__(
        self,
        name: str,
        func: Callable[[], Any],
        frequency: timedelta,
        first_run: Optional[timedelta] = None,
        loop: Optional[AbstractEventLoop] = None,
    ):
        self.name = name
        self.func = func
        self.frequency = frequency
        self.first_run = first_run if first_run else frequency
        self._task: Optional[Task[None]] = None
        self._loop = loop

    @property
    def started(self) -> bool:
        return self._task is not None

    async def start(self) -> None:
        if self._task is None:
            # Start task to call func periodically:
            self._task = asyncio.ensure_future(self._run(), loop=self._loop)
            log.debug(f"Periodic task {self.name} has been started.")

    async def stop(self) -> None:
        # Stop task and await it stopped:
        if self._task is not None:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task

    async def _run(self) -> None:
        await asyncio.sleep(self.first_run.total_seconds())
        while True:
            log.debug(f"Execute periodic task {self.name}.")
            try:
                result = self.func()
                if isinstance(result, Awaitable):
                    await result
            except Exception as ex:
                log.error(
                    f"Periodic function {self.name} caught an exception: {ex}",
                    exc_info=ex,
                )
            await asyncio.sleep(self.frequency.total_seconds())
