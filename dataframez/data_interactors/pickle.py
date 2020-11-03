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

import pandas as pd
from dataframez.data_interactors.io import IO


class Pickle(IO):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.__path = kwargs.get('path')

    # ---------- Reading Capabilities ---------- #
    def read(self, asset_info: dict, **kwargs) -> pd.DataFrame:
        write_config: dict = asset_info['config']

        # Read to DataFrame and return
        kwargs['filepath_or_buffer'] = write_config['path']
        return pd.read_csv(**kwargs)

    # ---------- Reading Capabilities ---------- #
    def write(self, _df: pd.DataFrame, entry_name: str, **kwargs):

        # Make sure you aren't trying to create a different version of this data resource with the same asset name using a different kind of persistence
        if not self._catalog.validate_entry_type(entry_name=entry_name, asset_type='csv'):
            error_message = 'Cannot write asset as type CSV'
            self._logger.error(error_message)
            raise ValueError(error_message)

        # Get the next version number
        version_number = self._catalog.latest_version(entry_name=entry_name) + 1

        # Create the assets persisted path name.
        kwargs['path'] = os.path.join(self.__path,
                                      f'{entry_name}/version_{version_number}.pickle')

        # Write the data to teh specified target
        _df.to_csv(**kwargs)

        # Update the catalog entry.
        self._catalog.register(entry_name=entry_name,
                               object_type='csv',
                               version=version_number,
                               asset_configuration=kwargs)
