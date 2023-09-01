# fixcloudutils
# Copyright (C) 2023  Some Engineering
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
import os
from collections import defaultdict
from datetime import datetime
from typing import Dict

import pytest
from redis.asyncio import Redis

from fixcloudutils.redis.pub_sub import RedisPubSubPublisher, RedisPubSubListener
from fixcloudutils.service import Dependencies
from fixcloudutils.types import Json


@pytest.mark.asyncio
@pytest.mark.skipif(os.environ.get("REDIS_RUNNING") is None, reason="Redis is not running")
async def test_pub_sub(redis: Redis) -> None:
    message_counter: Dict[str, int] = defaultdict(int)

    async def handle_message(uid: str, at: datetime, publisher: str, kind: str, data: Json) -> None:
        message_counter[uid] += 1

    async def all_messages_arrived() -> bool:
        while True:
            # Expect 10 message ids and 10 messages per listener --> 100 messages
            if len(message_counter) == 10 and all(v == 10 for v in message_counter.values()):
                return True
            await asyncio.sleep(0.1)

    channel = "test_channel"
    deps = Dependencies()
    redis_publisher = deps.add("publisher", RedisPubSubPublisher(redis, channel, "test"))
    for a in range(10):
        deps.add(f"listener_{a}", RedisPubSubListener(redis, channel, handle_message))
    async with deps:
        for a in range(10):
            await redis_publisher.publish("test", {"foo": "bar", "num": a})
        await asyncio.wait_for(all_messages_arrived(), timeout=2)
