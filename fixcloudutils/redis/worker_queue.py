#  Copyright (c) 2024. Some Engineering
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
import asyncio
import logging
from contextlib import suppress
from datetime import datetime, timedelta
from typing import Any, Optional, List, Union

from arq import Worker
from arq.jobs import Job
from arq.worker import Function

from fixcloudutils.asyncio import stop_running_task
from fixcloudutils.service import Service
from arq.connections import RedisSettings, create_pool, ArqRedis

log = logging.getLogger(__name__)


class WorkerInstance(Service):

    def __init__(
        self, redis_settings: RedisSettings, queue_name: str, functions: List[Function], **worker_args: Any
    ) -> None:
        self.redis_settings = redis_settings
        self.queue_name = queue_name
        self.functions = functions
        self.redis_worker_task: Optional[asyncio.Task[Any]] = None
        self.should_run = False
        self.worker_args = worker_args

    async def start(self) -> Any:
        if not self.should_run:
            self.should_run = True
            self.redis_worker_task = asyncio.create_task(self._run_worker())

    async def stop(self) -> Any:
        self.should_run = False
        await stop_running_task(self.redis_worker_task)

    async def _run_worker(self) -> Any:
        while self.should_run:
            cleanup: Optional[Worker] = None
            try:
                async with await create_pool(self.redis_settings) as pool:
                    defaults = dict(  # can be overridden by worker_args
                        job_timeout=3600,  # 1 hour
                        health_check_interval=60,  # every minute
                        keep_result=180,  # 3 minutes
                        retry_jobs=False,
                        handle_signals=False,
                        log_results=False,
                        queue_name=self.queue_name,
                    )
                    worker_args = {**defaults, **self.worker_args}
                    worker = Worker(functions=self.functions, redis_pool=pool, **worker_args)
                    log.info(
                        f"Worker created for queue {self.queue_name} for functions "
                        f'{", ".join(a.name for a in self.functions)} with following args: {worker_args}'
                    )
                    cleanup = worker
                    await worker.async_run()
            except Exception:
                log.exception("Worker failed. Retry.", exc_info=True)
                await asyncio.sleep(1)
            finally:
                if cleanup:
                    with suppress(BaseException):
                        await cleanup.close()


class WorkDispatcher(Service):
    def __init__(
        self, redis_or_settings: Union[ArqRedis, RedisSettings], default_queue_name: Optional[str] = None
    ) -> None:
        self.redis_or_settings = redis_or_settings
        self.arq_redis: Optional[ArqRedis] = redis_or_settings if isinstance(redis_or_settings, ArqRedis) else None
        self.default_queue_name = default_queue_name

    async def start(self) -> Any:
        if self.arq_redis is None:
            if isinstance(self.redis_or_settings, ArqRedis):
                self.arq_redis = self.redis_or_settings
            else:
                self.arq_redis = await create_pool(self.redis_or_settings)
                await self.arq_redis.__aenter__()

    async def stop(self) -> Any:
        if self.arq_redis and isinstance(self.redis_or_settings, RedisSettings):
            await self.arq_redis.__aexit__(None, None, None)  # type: ignore
        self.arq_redis = None

    async def enqueue(
        self,
        function: str,
        *args: Any,
        _job_id: Optional[str] = None,
        _queue_name: Optional[str] = None,
        _defer_until: Optional[datetime] = None,
        _defer_by: Union[None, int, float, timedelta] = None,
        _expires: Union[None, int, float, timedelta] = None,
        _job_try: Optional[int] = None,
        **kwargs: Any,
    ) -> Job:
        assert self.arq_redis is not None, "Redis not started"
        result = await self.arq_redis.enqueue_job(
            function,
            *args,
            _job_id=_job_id,
            _queue_name=_queue_name or self.default_queue_name,
            _defer_until=_defer_until,
            _defer_by=_defer_by,
            _expires=_expires,
            _job_try=_job_try,
            **kwargs,
        )
        if result is None:
            raise AttributeError(f"Job with id {_job_id} already enqueued")
        return result
