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
import json
import logging
import random
import re
import sys
import uuid
from asyncio import Task
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
    List,
)

from attrs import define
from redis.asyncio import Redis

from fixcloudutils.asyncio import stop_running_task
from fixcloudutils.asyncio.periodic import Periodic
from fixcloudutils.service import Service
from fixcloudutils.util import utc_str, parse_utc_str, utc

log = logging.getLogger("fix.event_stream")
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


@define(frozen=True, slots=True)
class MessageContext:
    id: str
    kind: str
    publisher: str
    sent_at: datetime
    received_at: datetime


class RedisStreamListener(Service):
    """
    Allows processing of messages from a redis stream in a group of readers.
    All messages can be processed by any group member, but only one group member will process one message.
    Once a batch is processed, all messages in the batch are acknowledged.

    Important:
    - stream name, group name and listener name must be stable over restarts.
    - the combination of stream + group + listener must be unique.
    """

    def __init__(
        self,
        redis: Redis,
        stream: str,
        group: str,
        listener: str,
        message_processor: Callable[[Json, MessageContext], Union[Awaitable[Any], Any]],
        consider_failed_after: timedelta,
        batch_size: int = 1000,
        stop_on_fail: bool = False,
        backoff: Optional[Backoff] = Backoff(0.1, 10, 10),
    ) -> None:
        """
        Create a RedisStream client.
        :param redis: The redis client.
        :param stream: The name of the redis event stream to read from.
        :param group: The name of the redis event stream group.
                      Messages are spread across all members of the group.
                      One message is only processed by one member of the group.
        :param listener:  The name of this listener (used to store the last read event id).
        :param message_processor: The function to process the event message.
        :param consider_failed_after: The time after which a message is considered failed and will be retried.
        :param batch_size: The number of events to read in one batch.
        :param stop_on_fail: If True, the listener will stop if a failed event is retried too many times.
        :param backoff: The backoff strategy to use when retrying failed events.
        """
        self.redis = redis
        self.stream = stream
        self.group = f"{stream}_{group}"  # avoid clashing group names by prefixing with stream name
        self.listener = listener
        self.message_processor = message_processor
        self.batch_size = batch_size
        self.stop_on_fail = stop_on_fail
        self.backoff = backoff or NoBackoff
        self.__should_run = True
        self.__listen_task: Optional[Task[Any]] = None
        # Check for messages that are not processed for a long time by any listener. Try to claim and process them.
        self.__outdated_messages_task = Periodic(
            "handle_outdated_messages",
            partial(self._handle_pending_messages, min_idle_time=consider_failed_after, ignore_delivery_count=True),
            consider_failed_after,
            first_run=timedelta(seconds=3),
        )
        self.__readpos = ">"

    async def _listen(self) -> None:
        while self.__should_run:
            try:
                messages = await self.redis.xreadgroup(
                    self.group, self.listener, {self.stream: self.__readpos}, count=self.batch_size, block=1000
                )
                self.__readpos = ">"

                await self._handle_stream_messages(messages)
            except Exception as e:
                log.error(f"Failed to read from stream {self.stream}: {e}", exc_info=True)
                if self.stop_on_fail:
                    raise

    async def _handle_stream_messages(self, messages: List[Any]) -> None:
        ids = []
        try:
            for stream, stream_messages in messages:
                log.debug(f"Handle {len(stream_messages)} messages from stream.")
                for uid, data in stream_messages:
                    await self._handle_single_message(data)
                    ids.append(uid)
        finally:
            if ids:
                # acknowledge all processed messages
                await self.redis.xack(self.stream, self.group, *ids)

    async def _handle_single_message(self, message: Json) -> None:
        try:
            if "id" in message and "at" in message and "data" in message:
                context = MessageContext(
                    id=message["id"],
                    kind=message["kind"],
                    publisher=message["publisher"],
                    sent_at=parse_utc_str(message["at"]),
                    received_at=utc(),
                )
                data = json.loads(message["data"])
                log.debug(f"Received message {self.listener}: message {context} data: {data}")
                await self.backoff.with_backoff(partial(self.message_processor, data, context))
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

    async def _handle_pending_messages(
        self,
        listener_name: Optional[str] = None,
        min_idle_time: Optional[timedelta] = None,
        ignore_delivery_count: bool = False,
    ) -> None:
        """
        This method is used in two cases:
        1. When a new listener is started, it will claim all messages for the same listener id.
           Assumption: the listener restarted and needs to process all messages that were not acknowledged.
        2. When a listener has claimed a message but did not finish processing it in time (no matter which listener id).
           We try to claim the message and process it.
        """
        while True:
            min_idle = int(min_idle_time.total_seconds() * 1000) if min_idle_time is not None else None
            # get all pending messages
            pending_messages = await self.redis.xpending_range(
                self.stream,
                self.group,
                "-",
                "+",
                consumername=listener_name,
                count=self.batch_size,
                idle=min_idle,
            )
            if len(pending_messages) == 0:
                break

            message_ids = [
                pm["message_id"] for pm in pending_messages if (pm["times_delivered"] < 10 or ignore_delivery_count)
            ]

            log.debug(f"Found {len(pending_messages)} pending messages and {len(message_ids)} for this listener.")

            # it is possible that claiming the message fails
            with suppress(Exception):
                messages = await self.redis.xclaim(self.stream, self.group, self.listener, min_idle or 0, message_ids)
                await self._handle_stream_messages([(self.stream, messages)])

    async def start(self) -> Any:
        self.__should_run = True
        # create the group if it does not exist. throws an exception if the group already exists, which is fine.
        with suppress(Exception):
            # id=0 all elements, id=$ all new elements
            await self.redis.xgroup_create(self.stream, self.group, id="0", mkstream=True)
            # if we come here, the group has been created. We should start reading from the beginning.
            self.__readpos = "0"

        async def read_all() -> None:
            await self._handle_pending_messages(listener_name=self.listener)  # messages claimed by this listener
            await self._listen()

        self.__listen_task = asyncio.create_task(read_all())
        await self.__outdated_messages_task.start()

    async def stop(self) -> Any:
        self.__should_run = False
        await self.__outdated_messages_task.stop()
        await stop_running_task(self.__listen_task)


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
        try:
            info_messages = await self.redis.xinfo_groups(self.stream)
        except Exception as e:
            log.info(f"Stream does not exist: {self.stream}. Ignore. {e}")
            return 0
        # in case, there are no listeners:
        latest = sys.maxsize if info_messages else 0
        for info in info_messages:
            last_commit = info["last-delivered-id"]
            latest = min(latest, time_from_id(last_commit, latest))
        # in case there is an inactive reader, make sure to only keep the unprocessed message time
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
                    if time_from_id(uid, sys.maxsize) < latest:
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
