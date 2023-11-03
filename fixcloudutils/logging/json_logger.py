#  Copyright (c) 2023. Some Engineering
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
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU Affero General Public License for more details.
#
#  You should have received a copy of the GNU Affero General Public License
#  along with this program.  If not, see <http://www.gnu.org/licenses/>.
import json
from logging import Formatter, LogRecord
from typing import Mapping, Optional, Dict, Callable


class JsonFormatter(Formatter):
    """
    Simple json log formatter.
    """

    def __init__(
        self,
        fmt_dict: Mapping[str, str],
        time_format: str = "%Y-%m-%dT%H:%M:%S",
        static_values: Optional[Dict[str, str]] = None,
        get_logging_context: Optional[Callable[[], Dict[str, str]]] = None,
    ) -> None:
        super().__init__()
        self.fmt_dict = fmt_dict
        self.time_format = time_format
        self.static_values = static_values or {}
        self.get_logging_context = get_logging_context
        self.__uses_time = "asctime" in self.fmt_dict.values()

    def usesTime(self) -> bool:  # noqa: N802
        return self.__uses_time

    def format(self, record: LogRecord) -> str:
        def prop(name: str) -> str:
            if name == "asctime":
                return self.formatTime(record, self.time_format)
            elif name == "message":
                return record.getMessage()
            else:
                return getattr(record, name, "n/a")

        message_dict = {fmt_key: prop(fmt_val) for fmt_key, fmt_val in self.fmt_dict.items()}
        message_dict.update(self.static_values)
        if get_context := self.get_logging_context:
            message_dict.update(get_context())
        if record.exc_info:
            if not record.exc_text:
                record.exc_text = self.formatException(record.exc_info)
        if record.exc_text:
            message_dict["exception"] = record.exc_text
        if record.stack_info:
            message_dict["stack_info"] = self.formatStack(record.stack_info)
        return json.dumps(message_dict, default=str)
