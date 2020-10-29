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

import pandas as pd

from dataframez.catalog import Catalog


def read_from_catalog(name: str) -> pd.DataFrame:
    catalog_entry = Catalog.read(name=name)

    if catalog_entry['type'] == 'gcs':
        args = catalog_entry['access_args']
        return pd.read_csv('gs://' + args['bucket'] + '/' + args['object'])


pd.from_catalog = read_from_catalog
