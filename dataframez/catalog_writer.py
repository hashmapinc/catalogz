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
import logging
import os

import pandas as pd
import yaml

from dataframez.catalog import Catalog


@pd.api.extensions.register_dataframe_accessor('dataframez')
class CatalogWriter:

    __logger = logging.getLogger()
    __catalog = Catalog

    def __init__(self, df: pd.DataFrame):
        self._df = df

    def to_csv(self,
               register_as: str,
               sep=',',
               na_rep='',
               float_format=None,
               columns=None,
               header=True,
               index=True,
               index_label=None,
               mode='w',
               encoding=None,
               compression='infer',
               quoting=None,
               quotechar='"',
               line_terminator=None,
               chunksize=None,
               date_format=None,
               doublequote=True,
               escapechar=None,
               decimal='.',
               errors='strict') -> None:

        # Load configuration and make sure that this mode is enabled.
        with open(os.path.join(os.getenv("HOME"),'.dataframez/configuration.yml')) as stream:
            configuration: dict= [conf for conf in yaml.safe_load(stream)['confs'] if conf['type'] == 'csv_writer']

        if not configuration['allowed']:
            error_message = 'Writing to CSV is not currently supported'
            self.__logger.error(error_message)
            raise PermissionError(error_message)

        # Make sure you aren't trying to create a different version of this data resource with the same asset name using a different kind of persistence
        if not self.__catalog.validate_write_type(entry_name=register_as, asset_type='csv'):
            error_message = 'Cannot write asset as type CSV'
            self.__logger.error(error_message)
            raise ValueError(error_message)

        # Get the next version number
        version_number = self.__catalog.latest_version(entry_name=register_as)

        # Create the assets persisted path name.
        path_or_buf = os.path.join(configuration['path'],
                                   f'{register_as}/version_{version_number}.csv')

        self._df.to_csv(path_or_buf, sep, na_rep, float_format, columns, header,
                        index, index_label, mode, encoding, compression, quoting,
                        quotechar, line_terminator, chunksize, date_format,
                        doublequote, escapechar, decimal, errors)

        self._catalog.register(name=register_as,
                               object_type='csv',
                               version_number = version_number,
                               asset_configuration={
                                   'object_name': path_or_buf,
                               })

    def to_pickle(self, register_as: str, path: str, compression: str='infer', protocol: int=5):
        pass
