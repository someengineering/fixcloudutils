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

from typing import List, AsyncIterator

from arango.client import ArangoClient
from attr import define
from pytest import fixture
from redis.asyncio import Redis
from redis.asyncio.retry import Retry
from redis.backoff import ExponentialBackoff

from fixcloudutils.arangodb.async_arangodb import AsyncArangoDB


@fixture
async def redis() -> AsyncIterator[Redis]:
    backoff = ExponentialBackoff()  # type: ignore
    redis = Redis(host="localhost", port=6379, decode_responses=True, retry=Retry(backoff, 10))
    yield redis
    await redis.close(True)


@fixture
def arangodb() -> AsyncArangoDB:
    client = ArangoClient(hosts="http://localhost:8529")
    system_db = client.db()
    if not system_db.has_user("test"):
        system_db.create_user("test", "test", True)

    if not system_db.has_database("test"):
        system_db.create_database("test", [{"username": "test", "password": "test", "active": True}])
    db = client.db("test", username="test", password="test")
    return AsyncArangoDB(db)


@define(repr=True, eq=True, frozen=True)
class ExampleData:
    foo: int
    bar: str
    bla: List[int]
