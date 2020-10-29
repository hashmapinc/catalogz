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
import pandas as pd

from dataframez.catalogs.catalog import Catalog
from dataframez.data_interactors.csv import CSV


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

        CSV.write(_df=self._df, entry_name=register_as, **{'register_as': register_as,
                                                           'sep': sep,
                                                           'na_rep': na_rep,
                                                           'float_format': float_format,
                                                           'columns': columns,
                                                           'header': header,
                                                           'index': index,
                                                           'index_label': index_label,
                                                           'mode': mode,
                                                           'encoding': encoding,
                                                           'compression': compression,
                                                           'quoting': quoting,
                                                           'quotechar': quotechar,
                                                           'line_terminator': line_terminator,
                                                           'chunksize': chunksize,
                                                           'date_format': date_format,
                                                           'doublequote': doublequote,
                                                           'escapechar': escapechar,
                                                           'decimal': decimal,
                                                           })

    def to_pickle(self, register_as: str, path: str, compression: str='infer', protocol: int=5):
        pass
