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
