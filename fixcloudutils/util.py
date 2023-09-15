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
import uuid
from datetime import datetime, timezone
from typing import Optional, TypeVar, Union, List, Any

from fixcloudutils.types import JsonElement, Json

T = TypeVar("T")
UTC_Date_Format = "%Y-%m-%dT%H:%M:%SZ"


def utc() -> datetime:
    return datetime.now(timezone.utc)


def utc_str(dt: Optional[datetime] = None) -> str:
    return (dt or utc()).strftime(UTC_Date_Format)


def parse_utc_str(s: str) -> datetime:
    return datetime.strptime(s, UTC_Date_Format).replace(tzinfo=timezone.utc)


def identity(o: T) -> T:
    return o


def value_in_path_get(element: JsonElement, path_or_name: Union[List[str], str], if_none: T) -> T:
    result = value_in_path(element, path_or_name)
    return result if result is not None and isinstance(result, type(if_none)) else if_none


def value_in_path(element: JsonElement, path_or_name: Union[List[str], str]) -> Optional[Any]:
    path = path_or_name if isinstance(path_or_name, list) else path_or_name.split(".")
    at = len(path)

    def at_idx(current: JsonElement, idx: int) -> Optional[Any]:
        if at == idx:
            return current
        elif current is None or not isinstance(current, dict) or path[idx] not in current:
            return None
        else:
            return at_idx(current[path[idx]], idx + 1)

    return at_idx(element, 0)


def set_value_in_path(element: JsonElement, path_or_name: Union[List[str], str], js: Optional[Json] = None) -> Json:
    path = path_or_name if isinstance(path_or_name, list) else path_or_name.split(".")
    at = len(path) - 1

    def at_idx(current: Json, idx: int) -> None:
        if at == idx:
            current[path[-1]] = element
        else:
            value = current.get(path[idx])
            if not isinstance(value, dict):
                value = {}
                current[path[idx]] = value
            at_idx(value, idx + 1)

    js = js if js is not None else {}
    at_idx(js, 0)
    return js


def del_value_in_path(element: JsonElement, path_or_name: Union[List[str], str]) -> JsonElement:
    path = path_or_name if isinstance(path_or_name, list) else path_or_name.split(".")
    pl = len(path) - 1

    def at_idx(current: JsonElement, idx: int) -> JsonElement:
        if current is None or not isinstance(current, dict) or path[idx] not in current:
            return element
        elif pl == idx:
            current.pop(path[-1], None)
            return element
        else:
            result = at_idx(current[path[idx]], idx + 1)
            if not current[path[idx]]:
                current[path[idx]] = None
            return result

    return at_idx(element, 0)


def uuid_str(from_object: Optional[Any] = None) -> str:
    if from_object:
        return str(uuid.uuid5(uuid.NAMESPACE_DNS, from_object))
    else:
        return str(uuid.uuid1())
