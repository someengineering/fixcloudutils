from asyncio import Task, CancelledError
from contextlib import suppress
from typing import Optional, Any


async def stop_running_task(task: Optional[Task[Any]]) -> None:
    if task is not None:
        if not task.done():
            task.cancel()
        with suppress(CancelledError):
            await task
