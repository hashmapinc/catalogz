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
import datetime
import os
import yaml

from dataframez.catalogs.catalog import Catalog


class Local(Catalog):

    def __init__(self, location: str, name: str, **kwargs):
        self.__location = os.path.expandvars(location)
        self.__name = name
        super().__init__(**kwargs)

    def read(self, entry_name: str, version: int = 0) -> dict:

        if version == 0:
            version = self.latest_version(entry_name=entry_name)

        entries = self._catalog.get(entry_name)
        print(f'{entry_name} \n Entries: {entries}')
        entry = [entry for entry in entries['versions'] if entry['number'] == version][0]

        if not entry:
            error_message = f'dataframez: when attempting to read from catalog, {entry_name} did not exist. It is possible that version {version} is not ' \
                            f'there, but {entry_name} is.'
            self._logger.error(error_message)
            raise ValueError(error_message)

        return {
            'type': entries['type'],
            'config': entry['asset_configuration']
        }

    def latest_version(self, entry_name: str) -> int:
        if entry_name in self._catalog.keys():
            versions = self._catalog[entry_name]['versions']
            if isinstance(versions, dict):
                return versions['number']
            return max([version['number'] for version in versions])
        return 0

    def register(self, entry_name: str, object_type: str, version: int, asset_configuration: dict) -> bool:

        if self._check_if_registered(entry_name=entry_name):
            self._logger.info(f'Entry {entry_name} already exists. Creating a new version of the entry.')
            versions = self._catalog[entry_name]['versions']
            if isinstance(versions, dict):
                versions = [versions]
                self._catalog[entry_name]['versions'] = versions
            self._catalog[entry_name]['versions'].append(
                {
                    'number': version,
                    'asset_configuration': asset_configuration,
                    'create_timestamp': datetime.datetime.timestamp(datetime.datetime.utcnow())
                }
            )
        else:
            self._catalog[entry_name] = {
                'type': object_type,
                'versions': [
                    {
                        'number': version,
                        'asset_configuration': asset_configuration,
                        'create_timestamp': datetime.datetime.timestamp(datetime.datetime.utcnow())
                    }
                ]
            }

        self._update_catalog()

    def validate_entry_type(self, entry_name: str, asset_type: str) -> bool:
        if self._check_if_registered(entry_name=entry_name):
            return self._catalog.get(entry_name)['type'].lower() == asset_type
        return True

    def _load_catalog(self) -> None:

        catalog_path = os.path.join(self.__location, self.__name)

        if not os.path.exists(catalog_path):
            if not os.path.exists(self.__location):
                os.mkdir(self.__location)

            stream = open(catalog_path, 'w')
            stream.close()

        # Read catalog into memory
        with open(catalog_path, 'r') as stream:
            self._catalog = yaml.safe_load(stream)
            if not self._catalog:
                self._catalog = {}

    def _update_catalog(self):
        with open(os.path.join(self.__location, self.__name), 'w') as stream:
            _ = yaml.dump(self._catalog, stream)

    def _check_if_registered(self, entry_name: str) -> bool:
        if self._catalog.get(entry_name):
            return True
        return False
