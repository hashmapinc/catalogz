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
import os
import yaml
from providah.factories.package_factory import PackageFactory as pf

from dataframez.catalogs.interface import Interface


class Catalog:

    # This is an interface to the specific kind of catalog.
    __registry: Interface = None

    # Identifies when the catalog interface has been set - single pattern
    __activated = False

    @classmethod
    def read(cls, name: str) -> dict:

        cls.__activate()
        return cls.__registry[name]

    @classmethod
    def register(cls, name: str, object_type: str, version: int, asset_configuration: dict) -> bool:
        cls.__activate()
        cls.__registry.register(entry_name=name,**{
            'type': object_type,
            'version': version,
            'asset_configuration': asset_configuration,
        })

    @classmethod
    def validate_write_type(cls, entry_name: str, asset_type: str) -> bool:
        return cls.__registry.validate_entry_type(entry_name=entry_name, asset_type=asset_type)

    @classmethod
    def latest_version(cls, entry_name: str) -> int:
        return cls.__registry.get_latest_version(entry_name=entry_name)

    @classmethod
    def __activate(cls):

        config_path = os.path.join(os.getenv("HOME"), ".dataframez/config.yml")
        if not os.path.exists(config_path):
            catalog_location = os.path.join(os.getenv("HOME"), ".dataframez")
            os.mkdir(catalog_location)
            registry_configuration = {
                'type': 'local',
                'conf': {
                    'location': catalog_location,
                    'name': 'default.dfz'
                },
            }

            with open(config_path, 'w') as stream:
                _ = yaml.dump(cls.__active_catalog, stream)

        else:
            with open(config_path, 'r') as stream:
                registry_configuration = yaml.safe_load(stream)

        cls.__registry = pf.create(key=registry_configuration['type'],
                                   configuration=registry_configuration['conf'])

        cls.__activated = True
