from typing import List

from attr import define
from pytest import fixture
from redis.asyncio import Redis
from redis.backoff import ExponentialBackoff
from redis.asyncio.retry import Retry


@fixture
def redis() -> Redis:
    backoff = ExponentialBackoff()  # type: ignore
    return Redis(host="localhost", port=6379, decode_responses=True, retry=Retry(backoff, 10))


@define
class ExampleData:
    foo: int
    bar: str
    bla: List[int]
