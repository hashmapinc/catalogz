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


class Interface:

    _catalog = Catalog
    _logger = logging.getLogger()

    @classmethod
    def read(cls, entry_name: str, version: int = 1) -> pd.DataFrame:
        raise NotImplementedError()

    def write(self, _df: pd.DataFrame, entry_name: str, **kwargs):
        raise NotImplementedError()
