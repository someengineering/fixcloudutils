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
from contextlib import suppress
from datetime import timedelta
from functools import partial
from typing import Dict, Tuple, List

import pytest
from cattrs import unstructure, structure
from redis.asyncio import Redis

from conftest import ExampleData
from fixcloudutils.redis.event_stream import (
    RedisStreamPublisher,
    Backoff,
    RedisStreamListener,
)
from fixcloudutils.types import Json


@pytest.mark.asyncio
@pytest.mark.skipif(os.environ.get("REDIS_RUNNING") is None, reason="Redis is not running")
async def test_stream(redis: Redis) -> None:
    group_counter: Dict[int, int] = defaultdict(int)
    listener_counter: Dict[Tuple[int, int], int] = defaultdict(int)

    async def handle_message(group: int, uid: int, message: Json) -> None:
        # make sure we can read the message
        data = structure(message, ExampleData)
        assert data.bar == "foo"
        assert data.bla == [1, 2, 3]
        group_counter[group] += 1
        listener_counter[(group, uid)] += 1

    # clean slate
    await redis.delete("test-stream", "test-stream.listener", "test-stream.dlq")

    # create 3 listeners in 10 groups
    streams = [
        RedisStreamListener(
            redis,
            "test-stream",
            f"group-{group}",
            f"id{i}",
            partial(handle_message, group, i),
            timedelta(seconds=5),
        )
        for group in range(10)
        for i in range(3)
    ]
    for s in streams:
        await s.start()

    # publish 10 messages
    publisher = RedisStreamPublisher(redis, "test-stream", "test")
    for i in range(10):
        await publisher.publish("test_data", unstructure(ExampleData(i, "foo", [1, 2, 3])))

    # make sure 10 messages are in the stream
    assert (await redis.xlen("test-stream")) == 10

    # expect 10 messages per listener --> 100 messages
    async def check_all_arrived(expected_reader: int) -> bool:
        while True:
            if len(group_counter) == expected_reader and all(v == 10 for v in group_counter.values()):
                return True
            await asyncio.sleep(0.1)

    await asyncio.wait_for(check_all_arrived(10), timeout=2)

    listener_by_group: Dict[int, int] = defaultdict(int)
    for (group, lid), counter in listener_counter.items():
        listener_by_group[group] += counter
    assert all(v == 10 for v in listener_by_group.values())

    # stop all listeners
    for s in streams:
        await s.stop()

    # a new redis listener started later will receive all messages
    group_counter.clear()
    async with RedisStreamListener(
        redis, "test-stream", "other", "l1", partial(handle_message, 123, 123), timedelta(seconds=5)
    ):
        await asyncio.wait_for(check_all_arrived(1), timeout=2)
        pass

    # all messages are processed, cleanup should remove all of them (except the last one)
    assert (await redis.xlen("test-stream")) == 10
    await publisher.cleanup_processed_messages()
    assert (await redis.xlen("test-stream")) <= 1

    # emit a new message and call cleanup: the message is not cleaned up
    await publisher.publish("test_data", unstructure(ExampleData(42, "foo", [1, 2, 3])))
    await publisher.cleanup_processed_messages()
    assert (await redis.xlen("test-stream")) <= 2

    # don't leave any traces
    await redis.delete("test-stream", "test-stream.listener", "test-stream.dlq")


@pytest.mark.asyncio
@pytest.mark.skipif(os.environ.get("REDIS_RUNNING") is None, reason="Redis is not running")
async def test_stream_pending(redis: Redis) -> None:
    await redis.delete("test-stream", "test-stream.listener", "test-stream.dlq")
    arrived_messages: List[Json] = []

    # create listener
    with suppress(Exception):
        await redis.xgroup_create("test-stream", "test-stream_foo", id="0", mkstream=True)

    # publish 10 messages
    publisher = RedisStreamPublisher(redis, "test-stream", "test")
    for i in range(10):
        await publisher.publish("test_data", unstructure(ExampleData(i, "foo", [1, 2, 3])))

    # read the message and do not acknowledge
    await redis.xreadgroup("test-stream_foo", "bar", {"test-stream": ">"}, count=5)

    # expect n messages
    async def check_all_arrived(num_messages: int) -> bool:
        while True:
            if len(arrived_messages) >= num_messages:
                return True
            await asyncio.sleep(0.1)

    async def handle_message(message: Json) -> None:
        arrived_messages.append(message)

    listener = RedisStreamListener(redis, "test-stream", "foo", "bar", handle_message, timedelta(seconds=5))

    # call handle_pending explicitly to see if it works
    await listener._handle_pending_messages()
    await asyncio.wait_for(check_all_arrived(5), timeout=2)

    # The normal way of using the listener. This will handle pending messages (None) and then process all remaining.
    async with listener:
        await asyncio.wait_for(check_all_arrived(10), timeout=2)


@pytest.mark.asyncio
@pytest.mark.skipif(os.environ.get("REDIS_RUNNING") is None, reason="Redis is not running")
async def test_failure(redis: Redis) -> None:
    counter = 0

    async def handle_message(_: Json) -> None:
        nonlocal counter
        counter += 1
        raise Exception("boom")

    # clean slate
    await redis.delete("test-stream", "test-stream.listener", "test-stream.dlq")

    # a new redis listener started later will receive all messages
    async with RedisStreamListener(
        redis, "test-stream", "t1", "l1", handle_message, timedelta(seconds=5), backoff=Backoff(0, 0, 5)
    ):
        async with RedisStreamPublisher(redis, "test-stream", "test") as publisher:
            await publisher.publish("test_data", unstructure(ExampleData(1, "foo", [1, 2, 3])))

            # expect one in the dlq
            async def expect_dlq() -> None:
                while await redis.xlen("test-stream.dlq") != 1:
                    await asyncio.sleep(0.1)

            await asyncio.wait_for(expect_dlq(), timeout=2)
            assert counter == 6  # one invocation + five retries
