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
