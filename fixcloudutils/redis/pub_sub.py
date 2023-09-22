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
import re
import uuid
from asyncio import Task
from datetime import datetime
from typing import Any, Optional, Callable, Awaitable

from redis.asyncio import Redis
from redis.asyncio.client import PubSub

from fixcloudutils.asyncio import stop_running_task
from fixcloudutils.service import Service
from fixcloudutils.types import Json
from fixcloudutils.util import utc_str, parse_utc_str

# id, at, publisher, kind, data
MessageHandler = Callable[[str, datetime, str, str, Json], Awaitable[Any]]
log = logging.getLogger("fixcloudutils.redis.pub_sub")
redis_wildcard = re.compile(r"(?<!\\)[*?\[]")


class RedisPubSubListener(Service):
    def __init__(self, redis: Redis, channel: str, handler: MessageHandler) -> None:
        self.redis = redis
        self.channel = channel
        self.handler = handler
        self.pubsub: Optional[PubSub] = None
        self.reader: Optional[Task[Any]] = None

    async def start(self) -> None:
        async def read_messages(pubsub: PubSub) -> None:
            while True:
                msg = await pubsub.get_message(ignore_subscribe_messages=True)
                try:
                    if msg is not None:
                        data = json.loads(msg["data"])
                        for prop in ["id", "at", "publisher", "kind", "data"]:
                            if prop not in data:
                                log.error(f"Invalid message received: {msg}. Missing property {prop}")
                                continue
                        await self.handler(
                            data["id"], parse_utc_str(data["at"]), data["publisher"], data["kind"], data["data"]
                        )
                except Exception as ex:
                    log.exception(f"Error handling message {msg}: {ex}. Ignore.")

        ps = self.redis.pubsub()
        # If the channel name contains wildcards, we need to use psubscribe
        if redis_wildcard.search(self.channel) is not None:
            await ps.psubscribe(self.channel)
        else:
            await ps.subscribe(self.channel)
        self.reader = asyncio.create_task(read_messages(ps))
        self.pubsub = ps

    async def stop(self) -> None:
        await stop_running_task(self.reader)
        if self.pubsub:
            await self.pubsub.close()
            self.pubsub = None


class RedisPubSubPublisher(Service):
    """
    Publish messages to a redis stream.
    :param redis: The redis client.
    :param channel: The name of the redis event stream.
    """

    def __init__(self, redis: Redis, channel: str, publisher_name: str) -> None:
        self.redis = redis
        self.channel = channel
        self.publisher_name = publisher_name

    async def publish(self, kind: str, message: Json, channel: Optional[str] = None) -> None:
        to_send = {
            "id": str(uuid.uuid1()),
            "at": utc_str(),
            "publisher": self.publisher_name,
            "kind": kind,
            "data": message,
        }
        await self.redis.publish(channel or self.channel, json.dumps(to_send))
