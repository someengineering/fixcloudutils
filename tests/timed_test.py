import prometheus_client

from fixcloudutils.asyncio.timed import timed


@timed("fixcloudutils", "test_async")
async def some_fn_async() -> int:
    return 23


@timed("fixcloudutils", "test", is_async=False)
def some_fn() -> int:
    return 23


def test_timed() -> None:
    for a in range(10):
        assert some_fn() == 23
    gl = prometheus_client.generate_latest().decode("utf-8")
    assert 'method_call_duration_bucket{le="0.005",module="fixcloudutils",name="test"} 10.0' in gl


async def test_async_timed() -> None:
    for a in range(10):
        assert await some_fn_async() == 23
    gl = prometheus_client.generate_latest().decode("utf-8")
    assert 'method_call_duration_bucket{le="0.005",module="fixcloudutils",name="test_async"} 10.0' in gl
