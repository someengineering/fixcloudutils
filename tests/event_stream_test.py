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
import os
from collections import defaultdict
from contextlib import suppress
from datetime import timedelta
from functools import partial
from typing import Dict, Tuple, List, Any

import pytest
from cattrs import unstructure, structure
from redis.asyncio import Redis

from conftest import ExampleData
from fixcloudutils.redis.event_stream import (
    RedisStreamPublisher,
    Backoff,
    RedisStreamListener,
    MessageContext,
)
from fixcloudutils.types import Json


@pytest.mark.asyncio
@pytest.mark.skipif(os.environ.get("REDIS_RUNNING") is None, reason="Redis is not running")
async def test_stream(redis: Redis) -> None:
    group_counter: Dict[int, int] = defaultdict(int)
    listener_counter: Dict[Tuple[int, int], int] = defaultdict(int)

    async def handle_message(group: int, uid: int, message: Json, _: MessageContext) -> None:
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
    publisher = RedisStreamPublisher(redis, "test-stream", "test", keep_processed_messages_for=timedelta(seconds=0))
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
async def test_stream_parallel(redis: Redis) -> None:
    counter: List[int] = [0]

    async def handle_message(group: int, uid: int, message: Json, _: MessageContext) -> None:
        # make sure we can read the message
        data = structure(message, ExampleData)
        assert data.bar == "foo"
        assert data.bla == [1, 2, 3]
        await asyncio.sleep(0.5)  # message takes time to be processed
        counter[0] += 1

    # clean slate
    await redis.delete("test-stream", "test-stream.listener", "test-stream.dlq")

    # create a single listener
    stream = RedisStreamListener(
        redis, "test-stream", "group", "id", partial(handle_message, 1, 1), timedelta(seconds=1), parallelism=10
    )
    await stream.start()

    messages_total = 10
    # publish 10 messages
    publisher = RedisStreamPublisher(redis, "test-stream", "test")
    for i in range(messages_total):
        await publisher.publish("test_data", unstructure(ExampleData(i, "foo", [1, 2, 3])))

    # make sure messages are in the stream
    assert (await redis.xlen("test-stream")) == messages_total

    # expect 10 messages per listener --> 100 messages
    async def check_all_arrived(expected_reader: int) -> bool:
        while True:
            if counter[0] == expected_reader:
                return True
            await asyncio.sleep(0.1)

    # processing must be parallel and we won't hit a timeout error
    # if the parallelism is not working then the processing will take 5 seconds
    # and the test will fail
    await asyncio.wait_for(check_all_arrived(messages_total), timeout=2)

    # messages must be acked and not be processed again
    await asyncio.sleep(1)
    assert counter[0] == messages_total

    # no tasks should be running once everything is processed
    assert len(stream._ongoing_tasks) == 0

    # stop all listeners
    await stream.stop()

    # don't leave any traces
    await redis.delete("test-stream", "test-stream.listener", "test-stream.dlq")


@pytest.mark.asyncio
@pytest.mark.skipif(os.environ.get("REDIS_RUNNING") is None, reason="Redis is not running")
async def test_stream_parallel_backpressure(redis: Redis) -> None:
    counter: List[int] = [0]

    async def handle_message(group: int, uid: int, message: Json, _: MessageContext) -> None:
        # make sure we can read the message
        data = structure(message, ExampleData)
        assert data.bar == "foo"
        assert data.bla == [1, 2, 3]
        await asyncio.sleep(0.15)  # message takes time to be processed
        counter[0] += 1

    # clean slate
    await redis.delete("test-stream", "test-stream.listener", "test-stream.dlq")

    # create a single listener
    stream = RedisStreamListener(
        redis, "test-stream", "group", "id", partial(handle_message, 1, 1), timedelta(seconds=1), parallelism=1
    )
    await stream.start()

    messages_total = 10
    # publish 10 messages
    publisher = RedisStreamPublisher(redis, "test-stream", "test")
    for i in range(messages_total):
        await publisher.publish("test_data", unstructure(ExampleData(i, "foo", [1, 2, 3])))

    # make sure messages are in the stream
    assert (await redis.xlen("test-stream")) == messages_total

    # expect 10 messages per listener --> 100 messages
    async def check_all_arrived(expected_reader: int) -> bool:
        while True:
            if counter[0] == expected_reader:
                return True
            await asyncio.sleep(0.1)

    # if the parallelism is full we should wait before enqueueing the next message
    # the total processing time should at least be 1.5 seconds (10 messages * 0.15 seconds)
    before = asyncio.get_running_loop().time()
    await asyncio.wait_for(check_all_arrived(messages_total), timeout=2)
    after = asyncio.get_running_loop().time()
    assert after - before >= 1.5

    # messages must be acked and not be processed again
    await asyncio.sleep(1)
    assert counter[0] == messages_total

    # no tasks should be running once everything is processed
    assert len(stream._ongoing_tasks) == 0

    # stop all listeners
    await stream.stop()

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

    async def handle_message(message: Json, context: Any) -> None:
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

    async def handle_message(message: Json, context: Any) -> None:
        nonlocal counter
        counter += 1
        raise Exception("boom")

    # clean slate
    await redis.delete("test-stream", "test-stream.listener", "test-stream.dlq")

    # a new redis listener started later will receive all messages
    async with RedisStreamListener(
        redis,
        "test-stream",
        "t1",
        "l1",
        handle_message,
        timedelta(seconds=5),
        backoff=defaultdict(lambda: Backoff(0, 0, 5)),
    ):
        async with RedisStreamPublisher(redis, "test-stream", "test") as publisher:
            await publisher.publish("test_data", unstructure(ExampleData(1, "foo", [1, 2, 3])))

            # expect one in the dlq
            async def expect_dlq() -> None:
                while await redis.xlen("test-stream.dlq") != 1:
                    await asyncio.sleep(0.1)

            await asyncio.wait_for(expect_dlq(), timeout=2)
            assert counter == 6  # one invocation + five retries
