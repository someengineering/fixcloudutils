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

import os

import pytest

from conftest import ExampleData
from fixcloudutils.arangodb.async_arangodb import AsyncArangoDB
from fixcloudutils.arangodb.entitydb import ArangoEntityDb


@pytest.mark.asyncio
@pytest.mark.skipif(os.environ.get("ARANGODB_RUNNING") is None, reason="ArangoDb is not running")
async def test_entity_db(arangodb: AsyncArangoDB) -> None:
    example_db = ArangoEntityDb(arangodb, "example", ExampleData, lambda x: str(x.bar))
    await example_db.create_update_schema()
    await example_db.wipe()

    data = [ExampleData(1, "a", [1, 2, 3]), ExampleData(2, "b", [4, 5, 6]), ExampleData(3, "c", [])]
    await example_db.update_many(data)
    assert {key async for key in example_db.keys()} == {"a", "b", "c"}
    assert sorted([t async for t in example_db.all()], key=lambda x: x.foo) == data
    assert await example_db.update(ExampleData(1, "a", [1])) == ExampleData(1, "a", [1])
    assert await example_db.get("a") == ExampleData(1, "a", [1])
    assert await example_db.delete("a") is True
    assert await example_db.delete("a") is False  # already deleted
    assert await example_db.delete_many(["b", "c"]) is True
    assert [key async for key in example_db.keys()] == []
