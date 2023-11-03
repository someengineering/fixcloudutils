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
import logging
import os
from logging import StreamHandler, basicConfig
from typing import Optional, List, Dict, Callable

from .json_logger import JsonFormatter
from .prometheus_counter import PrometheusLoggingCounter

__all__ = ["setup_logger", "JsonFormatter", "PrometheusLoggingCounter"]


def setup_logger(
    component: str,
    *,
    force: bool = True,
    level: Optional[int] = None,
    json_format: bool = True,
    count_logs: bool = True,
    log_format: Optional[str] = None,
    json_format_dict: Optional[Dict[str, str]] = None,
    get_logging_context: Optional[Callable[[], Dict[str, str]]] = None,
) -> List[StreamHandler]:  # type: ignore
    log_level = level or logging.INFO
    # override log output via env var
    plain_text = os.environ.get("LOG_TEXT", "false").lower() == "true"
    handler = PrometheusLoggingCounter(component) if count_logs else StreamHandler()
    if json_format and not plain_text:
        format_dict = json_format_dict or {
            "level": "levelname",
            "timestamp": "asctime",
            "message": "message",
            "logger": "name",
            "pid": "process",
            "thread": "threadName",
        }
        formatter = JsonFormatter(
            format_dict, static_values={"component": component}, get_logging_context=get_logging_context
        )
        handler.setFormatter(formatter)
        basicConfig(handlers=[handler], force=force, level=log_level)
    else:
        lf = log_format or "%(asctime)s %(levelname)s %(message)s"
        basicConfig(handlers=[handler], format=lf, datefmt="%Y-%m-%dT%H:%M:%S", force=force, level=log_level)
    return [handler]
