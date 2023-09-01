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

from typing import Any, TypeVar, Dict, List, Type, AsyncContextManager

ServiceType = TypeVar("ServiceType", bound="Service")
T = TypeVar("T")


class Service(AsyncContextManager[Any]):
    async def start(self) -> Any:
        pass

    async def stop(self) -> None:
        pass

    async def __aenter__(self: ServiceType) -> ServiceType:
        await self.start()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.stop()


class Dependencies(Service):
    def __init__(self, **deps: Any) -> None:
        self.lookup: Dict[str, Any] = deps

    def add(self, name: str, service: T) -> "T":
        self.lookup[name] = service
        return service

    def extend(self, **deps: Any) -> "Dependencies":
        self.lookup = {**self.lookup, **deps}
        return self

    @property
    def services(self) -> List[AsyncContextManager[Any]]:
        return [v for _, v in self.lookup.items() if isinstance(v, AsyncContextManager)]

    def service(self, name: str, clazz: Type[T]) -> T:
        if isinstance(existing := self.lookup.get(name), clazz):
            return existing
        else:
            raise KeyError(f"Service {name} not found")

    async def start(self) -> None:
        for service in self.services:
            await service.__aenter__()

    async def stop(self) -> None:
        for service in reversed(self.services):
            await service.__aexit__(None, None, None)
