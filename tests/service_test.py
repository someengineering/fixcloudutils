import pytest

from fixcloudutils.service import Service, Dependencies


class ExampleService(Service):
    def __init__(self) -> None:
        self.started = False

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.started = False


@pytest.mark.asyncio
async def test_dependencies() -> None:
    dep = Dependencies()
    example = dep.add("example", ExampleService())
    async with dep:
        assert example.started
    assert not example.started
