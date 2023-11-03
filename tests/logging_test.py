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

import prometheus_client
from typing import Dict

from fixcloudutils.logging import JsonFormatter, PrometheusLoggingCounter


def test_json_logging() -> None:
    def logging_context() -> Dict[str, str]:
        return {"foo": "bar"}

    format = JsonFormatter({"level": "levelname", "message": "message"}, get_logging_context=logging_context)
    record = logging.getLogger().makeRecord("test", logging.INFO, "test", 1, "test message", (), None)
    assert format.format(record) == '{"level": "INFO", "message": "test message", "foo": "bar"}'


def test_prometheus_counter() -> None:
    counter = PrometheusLoggingCounter("test")
    levels = {"CRITICAL": 50, "ERROR": 40, "WARNING": 30, "INFO": 20, "DEBUG": 10, "NOTSET": 0}
    for level in levels.values():
        record = logging.getLogger().makeRecord("test", level, "test", 1, "test message", (), None)
        counter.emit(record)
    gl = prometheus_client.generate_latest().decode("utf-8")
    for level_name in levels.keys():
        assert f'log_record_counter_total{{component="test",level="{level_name}"}}' in gl
