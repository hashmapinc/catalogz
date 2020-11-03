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

import pandas as pd
import yaml
from providah.factories.package_factory import PackageFactory as pf

from dataframez.catalogs.catalog import Catalog


class __CatalogReader:
    __catalog: Catalog
    __initialized = False
    __readers: dict = {}
    __logger = logging.getLogger()
    __configuration_path: str = os.path.join(os.getenv("HOME"), '.dataframez/configuration.yml')

    @classmethod
    def __initialize(cls):

        if not cls.__initialized:
            cls.__configure_catalog()
            cls.__configure_writer_methods()
        cls.__initialized = True

    @classmethod
    def read(cls, entry_name: str, version: int = 0, **kwargs) -> pd.DataFrame:

        cls.__initialize()
        asset_info = cls.__catalog.read(entry_name=entry_name,
                                        version=version)

        return cls.__readers[asset_info['type']](asset_info,
                                                 **kwargs)

    @classmethod
    def __configure_catalog(self) -> None:
        # When a configuration already exists, load it
        with open(self.__configuration_path, 'r') as stream:
            registry_configuration = yaml.safe_load(stream)['configurations']['catalog']

        # Load the configuration
        self.__catalog = pf.create(key=registry_configuration['type'],
                                   configuration=registry_configuration['conf'])

    @classmethod
    def __configure_writer_methods(cls):

        # ----------- create local registry of all writers ---------- #
        # Load configuration
        with open(cls.__configuration_path, 'r') as config_stream:
            configuration = yaml.safe_load(stream=config_stream)['configurations']

        for key, value in configuration['writers'].items():
            if value['conf']['allowed']:
                cls.__readers[key.lower()] = pf.create(key=value['type'].lower(),
                                                       library='dataframez',
                                                       configuration=value['conf']).read


def __read_from_catalog(entry_name: str, version: int = 0) -> pd.DataFrame:

    return __CatalogReader.read(entry_name=entry_name, version=version)
