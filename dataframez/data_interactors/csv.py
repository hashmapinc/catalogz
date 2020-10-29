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

import pandas as pd
from dataframez.data_interactors.interface import Interface


class CSV(Interface):

    @classmethod
    def read(cls, entry_name: str, version: int = 1) -> pd.DataFrame:

        # Retrieve asset_configuration
        asset_configuration = cls._catalog.read_asset_configuration(entry_name=entry_name, version=1)

        # Read to DataFrame and return
        return pd.read_csv(asset_configuration['object_name'])

    @classmethod
    def write(cls, _df: pd.DataFrame, entry_name: str, **kwargs):
        # Load configuration and make sure that this mode is enabled.
        with open(os.path.join(os.getenv("HOME"), '.dataframez/configuration.yml')) as stream:
            configuration: dict = [conf for conf in yaml.safe_load(stream)['confs'] if conf['type'] == 'csv_writer'][0]

        if not configuration['allowed']:
            error_message = 'Writing to CSV is not currently supported'
            cls._logger.error(error_message)
            raise PermissionError(error_message)

        # Make sure you aren't trying to create a different version of this data resource with the same asset name using a different kind of persistence
        if not cls._catalog.validate_entry_type(entry_name=entry_name, asset_type='csv'):
            error_message = 'Cannot write asset as type CSV'
            cls._logger.error(error_message)
            raise ValueError(error_message)

        # Get the next version number
        version_number = cls._catalog.latest_version(entry_name=entry_name)

        # Create the assets persisted path name.
        kwargs['path_or_buf'] = os.path.join(configuration['path'],
                                             f'{entry_name}/version_{version_number}.csv')

        _df.to_csv(**kwargs)

        cls._catalog.register(name=entry_name,
                              object_type='csv',
                              version_number=version_number,
                              asset_configuration={
                                  'object_name': kwargs.get('path_or_buf'),
                              })
