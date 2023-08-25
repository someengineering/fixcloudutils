import asyncio
import os
from collections import defaultdict
from functools import partial
from typing import List, Dict

import pytest
from attrs import define
from cattrs import unstructure, structure
from pytest import fixture
from redis.asyncio import Redis
from redis.asyncio.retry import Retry
from redis.backoff import ExponentialBackoff

from fixcloudutils.redis.event_stream import RedisStreamListener, RedisStreamPublisher, Backoff
from fixcloudutils.types import Json


@fixture
def redis() -> Redis:
    return Redis(
        host="localhost",
        port=6379,
        decode_responses=True,
        retry=Retry(ExponentialBackoff(), 10),  # type: ignore
    )


@define
class ExampleData:
    foo: int
    bar: str
    bla: List[int]


@pytest.mark.asyncio
@pytest.mark.skipif(os.environ.get("REDIS_RUNNING") is None, reason="Redis is not running")
async def test_stream(redis: Redis) -> None:
    message_counter: Dict[str, int] = defaultdict(int)

    async def handle_message(uid: str, message: Json) -> None:
        print(uid, message)
        # make sure we can read the message
        data = structure(message, ExampleData)
        assert data.bar == "foo"
        assert data.bla == [1, 2, 3]
        message_counter[uid] += 1

    # clean slate
    await redis.delete("test-stream", "test-stream.listener", "test-stream.dlq")

    # create 10 listeners
    streams = [
        RedisStreamListener(redis, "test-stream", f"id{i}", partial(handle_message, f"id{i}:")) for i in range(10)
    ]
    for s in streams:
        await s.start()

    # publish 10 messages
    publisher = RedisStreamPublisher(redis, "test-stream", "test")
    for i in range(10):
        await publisher.publish_json("test_data", unstructure(ExampleData(i, "foo", [1, 2, 3])))

    # make sure 10 messages are in the stream
    assert (await redis.xlen("test-stream")) == 10

    # expect 10 messages per listener --> 100 messages
    async def check_all_arrived() -> bool:
        while True:
            if len(message_counter) >= 10 and all(v == 10 for v in message_counter.values()):
                return True
            await asyncio.sleep(0.1)

    await asyncio.wait_for(check_all_arrived(), timeout=2)

    # stop all listeners
    for s in streams:
        await s.stop()

    # a new redis listener started later will receive all messages
    async with RedisStreamListener(redis, "test-stream", "t1", partial(handle_message, "t1")):
        while message_counter["t1"] < 10:
            await asyncio.sleep(0.1)
    await asyncio.wait_for(check_all_arrived(), timeout=2)

    # all messages are processed, cleanup should remove all of them
    assert (await redis.xlen("test-stream")) == 10
    await publisher.cleanup_processed_messages()
    assert (await redis.xlen("test-stream")) == 0

    # emit a new message and call cleanup: the message is not cleaned up
    await publisher.publish_json("test_data", unstructure(ExampleData(42, "foo", [1, 2, 3])))
    await publisher.cleanup_processed_messages()
    assert (await redis.xlen("test-stream")) == 1

    # don't leave any traces
    await redis.delete("test-stream", "test-stream.listener", "test-stream.dlq")


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
    async with RedisStreamListener(redis, "test-stream", "t1", handle_message, backoff=Backoff(0, 0, 5)):
        async with RedisStreamPublisher(redis, "test-stream", "test") as publisher:
            await publisher.publish_json("test_data", unstructure(ExampleData(1, "foo", [1, 2, 3])))

            # expect one in the dlq
            async def expect_dlq() -> None:
                while await redis.xlen("test-stream.dlq") != 1:
                    await asyncio.sleep(0.1)

            await asyncio.wait_for(expect_dlq(), timeout=2)
            assert counter == 6  # one invocation + five retries
