import asyncio
import json
import logging
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
        await ps.psubscribe(self.channel)
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
