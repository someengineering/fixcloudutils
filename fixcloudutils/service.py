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
