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
import os
import yaml
from providah.factories.package_factory import PackageFactory as pf


class Catalog:
    _logger = logging.getLogger()
    _catalog: dict

    def __init__(self, **kwargs):

        # Create logger instance
        # self._catalog = None
        self._load_catalog()

    def read(self, entry_name: str, version: int = 1) -> dict:
        raise NotImplementedError()

    def register(self, entry_name: str, object_type: str, version: int, asset_configuration: dict) -> bool:
        raise NotImplementedError()

    def validate_entry_type(self, entry_name: str, asset_type: str) -> bool:
        raise NotImplementedError()

    def latest_version(self, entry_name: str) -> int:
        raise NotImplementedError()

    # ----------- Protected Methods Below ------------- #

    def _check_if_registered(self, entry_name: str) -> bool:
        raise NotImplementedError()

    def _load_catalog(self) -> None:
        raise NotImplementedError()

    def _update_catalog(self):
        raise NotImplementedError()
