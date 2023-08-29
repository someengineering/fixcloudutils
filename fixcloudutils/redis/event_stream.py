import asyncio
import json
import logging
import random
import re
import sys
import uuid
from asyncio import Task, CancelledError
from contextlib import suppress
from datetime import datetime, timedelta
from functools import partial
from typing import (
    Callable,
    Any,
    Union,
    Awaitable,
    Optional,
    TypeVar,
    Dict,
)

from attrs import define
from redis.asyncio import Redis

from fixcloudutils.asyncio.periodic import Periodic
from fixcloudutils.service import Service
from fixcloudutils.util import utc_str

log = logging.getLogger("collect.coordinator")
T = TypeVar("T")
Json = Dict[str, Any]
CommitTimeRE = re.compile(r"(\d{13})-.*")


def time_from_id(uid: str, default: int) -> int:
    if match := CommitTimeRE.match(uid):
        return int(match.group(1))
    return default


@define(frozen=True, slots=True)
class Backoff:
    base_delay: float
    maximum_delay: float
    retries: int

    def wait_time(self, attempt: int) -> float:
        delay: float = self.base_delay * (2**attempt + random.uniform(0, 1))
        return min(delay, self.maximum_delay)

    async def with_backoff(self, fn: Callable[[], Awaitable[T]], attempt: int = 0) -> T:
        try:
            return await fn()
        except Exception as e:
            if attempt < self.retries:
                delay = self.wait_time(attempt)
                log.warning(f"Got Exception in attempt {attempt}. Retry after {delay} seconds: {e}")
                await asyncio.sleep(delay)
                return await self.with_backoff(fn, attempt + 1)
            else:
                raise


NoBackoff = Backoff(0, 0, 0)


class RedisStreamListener(Service):
    def __init__(
        self,
        redis: Redis,
        stream: str,
        listener: str,
        message_processor: Callable[[Json], Union[Awaitable[Any], Any]],
        batch_size: int = 1000,
        wait_for_batch_ms: int = 1000,
        stop_on_fail: bool = False,
        backoff: Optional[Backoff] = Backoff(0.1, 10, 10),
    ) -> None:
        """
        Create a RedisStream client.
        :param redis: The redis client.
        :param stream: The name of the redis event stream.
        :param listener:  The name of this listener (used to store the last read event id).
        :param message_processor: The function to process the event message.
        :param batch_size: The number of events to read in one batch.
        :param wait_for_batch_ms: The time to wait for events in one batch.
        :param stop_on_fail: If True, the listener will stop if a failed event is retried too many times.
        :param backoff: The backoff strategy to use when retrying failed events.
        """
        self.redis = redis
        self.stream = stream
        self.listener = listener
        self.message_processor = message_processor
        self.batch_size = batch_size
        self.wait_for_batch_ms = wait_for_batch_ms
        self.stop_on_fail = stop_on_fail
        self.backoff = backoff or NoBackoff
        self.__should_run = True
        self.__listen_task: Optional[Task[Any]] = None

    async def listen(self) -> None:
        last = (await self.redis.hget(f"{self.stream}.listener", self.listener)) or 0  # type: ignore
        while self.__should_run:
            try:
                # wait for either batch_size messages or wait_for_batch_ms time whatever comes first
                res = await self.redis.xread(
                    {self.stream: last},
                    count=self.batch_size,
                    block=self.wait_for_batch_ms,
                )
                if res:
                    [[_, content]] = res  # safe, since we are reading from one stream
                    for rid, data in content:
                        await self.handle_message(data)
                        last = rid
                    # acknowledge all messages by committing the last read id
                    await self.redis.hset(f"{self.stream}.listener", self.listener, last)  # type: ignore
            except Exception as e:
                log.error(f"Failed to read from stream {self.stream}: {e}", exc_info=True)
                if self.stop_on_fail:
                    raise

    async def handle_message(self, message: Json) -> None:
        try:
            if "id" in message and "at" in message and "data" in message:
                mid = message["id"]
                at = message["at"]
                publisher = message["publisher"]
                data = json.loads(message["data"])
                log.debug(f"Received message {self.listener}: message {mid}, from {publisher}, at {at} data: {data}")
                await self.backoff.with_backoff(partial(self.message_processor, data))
            else:
                log.warning(f"Invalid message format: {message}. Ignore.")
        except Exception as e:
            if self.stop_on_fail:
                raise e
            else:
                log.error(f"Failed to process message {self.listener}: {message}. Error: {e}")
                # write the failed message to the dlq
                await self.redis.xadd(
                    f"{self.stream}.dlq",
                    {
                        "listener": self.listener,
                        "error": str(e),
                        "message": json.dumps(message),
                    },
                )

    async def start(self) -> Any:
        self.__should_run = True
        self.__listen_task = asyncio.create_task(self.listen())

    async def stop(self) -> Any:
        self.__should_run = False
        if self.__listen_task:
            self.__listen_task.cancel()
            with suppress(CancelledError):
                await self.__listen_task


class RedisStreamPublisher(Service):
    """
    Publish messages to a redis stream.
    :param redis: The redis client.
    :param stream: The name of the redis event stream.
    """

    def __init__(
        self,
        redis: Redis,
        stream: str,
        publisher_name: str,
        keep_unprocessed_messages_for: timedelta = timedelta(days=1),
    ) -> None:
        self.redis = redis
        self.stream = stream
        self.publisher_name = publisher_name
        self.keep_unprocessed_messages_for = keep_unprocessed_messages_for
        self.clean_process = Periodic(
            "clean_processed_messages",
            self.cleanup_processed_messages,
            timedelta(minutes=1),
            first_run=timedelta(milliseconds=10),
        )

    async def publish(self, kind: str, message: Json) -> None:
        to_send = {
            "id": str(uuid.uuid1()),
            "at": utc_str(),
            "publisher": self.publisher_name,
            "kind": kind,
            "data": json.dumps(message),
        }
        await self.redis.xadd(self.stream, to_send)  # type: ignore

    async def cleanup_processed_messages(self) -> int:
        log.debug("Cleaning up processed messages.")
        # get the earliest commit id over all listeners
        last_commit_messages = await self.redis.hgetall(self.stream + ".listener")  # type: ignore
        # in case, there are no listeners:
        latest = sys.maxsize if last_commit_messages else 0
        for listener, last_commit in last_commit_messages.items():
            latest = min(latest, time_from_id(last_commit, latest))
        # in case there is an inactive reader, make sure to only keep the last 7 days
        latest = max(
            latest,
            int((datetime.now() - self.keep_unprocessed_messages_for).timestamp() * 1000),
        )
        # iterate in batches over the stream and delete all messages that are older than the latest commit
        last = "0"
        cleaned_messages = 0
        while True:
            res = await self.redis.xread({self.stream: last}, count=5000)
            if not res:
                break
            to_delete = []
            for id_message in res:
                stream, messages = id_message
                for uid, message in messages:
                    if time_from_id(uid, sys.maxsize) <= latest:
                        to_delete.append(uid)
                    last = uid
            # delete all messages in one batch
            if to_delete:
                log.info(f"Deleting processed or old messages from stream: {len(to_delete)}")
                cleaned_messages += len(to_delete)
                await self.redis.xdel(self.stream, *to_delete)
        log.info(f"Cleaning up processed messages done. Cleaned {cleaned_messages} messages.")
        return cleaned_messages

    async def start(self) -> None:
        await self.clean_process.start()

    async def stop(self) -> None:
        await self.clean_process.stop()
