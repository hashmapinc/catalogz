# Modifications © 2020 Hashmap, Inc
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import logging


class Interface:

    _catalog: dict = None
    _logger = logging.getLogger()
    _catalog_in_memory: bool = False

    def __init__(self, **kwargs):
        pass

    def register(self, entry_name: str, object_type: str, args: dict, **kwargs) -> None:
        raise NotImplementedError()

    def read_asset_configuration(self, entry_name: str, version: int = 1) -> dict:
        raise NotImplementedError

    def validate_entry_type(self, entry_name: str, asset_type: str) -> bool:
        raise NotImplementedError()

    def get_latest_version(self, entry_name):
        raise NotImplementedError()

    # ----------- Protected Methods Below ------------- #

    def _check_if_registered(self, entry_name: str) -> bool:
        if self._catalog.get(entry_name):
            return True
        return False

    def _load_catalog(self) -> None:
        raise NotImplementedError()
