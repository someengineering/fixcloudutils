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

import logging
from typing import Optional, MutableMapping, Union, Tuple

from arango import HTTPClient
from arango.response import Response
from arango.typings import Headers
from requests import Session
from requests.adapters import HTTPAdapter, Retry
from requests_toolbelt import MultipartEncoder

log = logging.getLogger(__name__)


class ArangoHTTPClient(HTTPClient):
    def __init__(self, timeout: int, verify: Union[str, bool, None]):
        log.info(f"Create ArangoHTTPClient with timeout={timeout} and verify={verify}")
        self.timeout = timeout
        self.verify = verify

    def create_session(self, host: str) -> Session:
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"],
        )
        http_adapter = HTTPAdapter(max_retries=retry_strategy)
        session = Session()
        session.mount("https://", http_adapter)
        session.mount("http://", http_adapter)
        session.verify = self.verify
        return session

    def send_request(
        self,
        session: Session,
        method: str,
        url: str,
        headers: Optional[Headers] = None,
        params: Optional[MutableMapping[str, str]] = None,
        data: Union[str, MultipartEncoder, None] = None,
        auth: Optional[Tuple[str, str]] = None,
    ) -> Response:
        response = session.request(method, url, params, data, headers, auth=auth, timeout=self.timeout)
        return Response(method, response.url, response.headers, response.status_code, response.reason, response.text)
