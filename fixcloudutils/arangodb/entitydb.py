import logging
from abc import ABC, abstractmethod
import attrs

from typing import AsyncGenerator, Generic, TypeVar, Optional, Type, Callable, List

import cattrs
from arango import DocumentUpdateError, DocumentRevisionError

from fixcloudutils.arangodb.async_arangodb import AsyncArangoDB, OptimisticLockingFailed
from fixcloudutils.types import Json

log = logging.getLogger(__name__)

T = TypeVar("T")
K = TypeVar("K", bound=str)


class EntityDb(ABC, Generic[K, T]):
    @abstractmethod
    def keys(self) -> AsyncGenerator[K, None]:
        pass

    @abstractmethod
    def all(self) -> AsyncGenerator[T, None]:
        pass

    @abstractmethod
    async def update_many(self, elements: List[T]) -> None:
        pass

    @abstractmethod
    async def get(self, key: K) -> Optional[T]:
        pass

    @abstractmethod
    async def update(self, t: T) -> T:
        pass

    @abstractmethod
    async def delete(self, key: K) -> bool:
        pass

    @abstractmethod
    async def delete_value(self, value: T) -> None:
        pass

    @abstractmethod
    async def create_update_schema(self) -> None:
        pass

    @abstractmethod
    async def wipe(self) -> bool:
        pass


class ArangoEntityDb(EntityDb[K, T], ABC):
    def __init__(
        self,
        db: AsyncArangoDB,
        collection_name: str,
        t_type: Type[T],
        key_fn: Callable[[T], K],
        from_js: Optional[Callable[[Json, Type[T]], T]] = None,
        to_js: Optional[Callable[[T], Json]] = None,
    ):
        self.db = db
        self.collection_name = collection_name
        self.t_type = t_type
        self.key_of = key_fn
        self.from_js = from_js or cattrs.structure
        self.to_js = to_js or cattrs.unstructure

    async def keys(self) -> AsyncGenerator[K, None]:
        with await self.db.keys(self.collection_name) as cursor:
            for element in cursor:
                yield element

    async def all(self) -> AsyncGenerator[T, None]:
        with await self.db.all(self.collection_name) as cursor:
            for element in cursor:
                yield self.from_js(element, self.t_type)

    async def update_many(self, elements: List[T]) -> None:
        await self.db.insert_many(self.collection_name, [self.to_doc(e) for e in elements], overwrite=True)

    async def get(self, key: K) -> Optional[T]:
        result = await self.db.get(self.collection_name, key)
        return self.from_js(result, self.t_type) if result else None

    async def update(self, t: T) -> T:
        doc = self.to_doc(t)
        try:
            result = await self.db.update(self.collection_name, doc, merge=False)
        except DocumentRevisionError as ex:
            raise OptimisticLockingFailed(self.key_of(t)) from ex
        except DocumentUpdateError as ex:
            if ex.error_code == 1202:  # document not found
                result = await self.db.insert(self.collection_name, doc)  # type: ignore
            else:
                raise ex
        if hasattr(t, "revision") and "_rev" in result:
            if attrs.has(type(t)):
                t = attrs.evolve(t, revision=result["_rev"])  # type: ignore
            else:
                setattr(t, "revision", result["_rev"])
        return t

    async def delete(self, key: K) -> bool:
        return await self.db.delete(self.collection_name, key, ignore_missing=True, silent=True)  # type: ignore

    async def delete_many(self, keys: List[K]) -> bool:
        return await self.db.delete_many(self.collection_name, keys, silent=True)  # type: ignore

    async def delete_value(self, value: T) -> None:
        key = self.key_of(value)
        await self.db.delete(self.collection_name, key, ignore_missing=True)

    async def create_update_schema(self) -> None:
        name = self.collection_name
        db = self.db
        if not await db.has_collection(name):
            await db.create_collection(name)

    async def wipe(self) -> bool:
        return await self.db.truncate(self.collection_name)

    def to_doc(self, elem: T) -> Json:
        js = self.to_js(elem)
        js["_key"] = self.key_of(elem)
        return js
