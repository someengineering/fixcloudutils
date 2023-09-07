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

from typing import Union, Mapping, Any, Sequence, Dict, NewType

# Ideally we would be able to define it like this:
# JsonElement = Union[ Mapping[str, JsonElement], Sequence[JsonElement], str, int, float, bool, None]
# Sadly python does not support recursive types yet, so we try to narrow it to:
JsonElement = Union[
    str,
    int,
    float,
    bool,
    None,
    Mapping[str, Any],
    Sequence[Union[Mapping[str, Any], Sequence[Any], str, int, float, bool, None]],
]
JsonArray = Sequence[JsonElement]
Json = Dict[str, Any]
GraphName = NewType("GraphName", str)
