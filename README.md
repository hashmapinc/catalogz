<!---
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
-->
# dataframez

Extension to pandas to allow for simple cross-cloud-platform interactions with data, use of data versioning tools, and much more. The idea is to make it very simple for pandas users to interact with named data sources. 

A named data source is a source where the name can be used to retrieve the data without giving additional access criteria - such as would be necessary when accessing data through a database connection, connection to a cloud resource, and so on. This can be bothersome for a data scientist. Who wants to track where their data resides!!!

In modern environment a data catalog is often used to track data assets. But interacting with these catalogs is also bothersome. The use of a named asset abstracts the interface with such catalogs by providing all the necessary interactions with this 'catalog' to identify and retrieve teh data. Cataloging in this sense can also mean a data versioning utility. In gereal, however, this means that the catalog interactions of dataframez can work across many catalogs in tandem; that is, with enterprise catalog and data scientist catalogs at the same time. 

##About
Description + Intent
Configuration

##API
The intent has been to keep the API as simple as possible by minimally extending the pandas API and supporting, for the most part, the same functionality in terms of saving data outputs as is done in pandas.

### Reading from a Catalog
__pandas.from_catalog(name: str, version: int, **kwargs) -> pandas.DataFrame__
This method extends the read capabilities of pandas to read from a 'cataloged' asset. 

###Extended Write Capabilities

The write capabilities - to cataloged entrypoints - of pandas has been extended by providing capabilities in the pandas name space extension 'dataframez'. In this namespace standard pandas write methods are added - with the addition of an asset registration name in place of common persistence identifies like a path. In some cases default parameters are changed to make the seemless integration of read & write smooth.

In addition to the norm - additional methods have been added for specialized data source interactions.

Also, in order to discover cataloged resources, you can call the list_assets() method to retrieve a list of all asset names.
#### Supported Methods
* pandas.DataFrame.dataframez.to_csv
* pandas.DataFrame.dataframez.to_parquet
* pandas.DataFrame.dataframez.to_pickle

*NOTE: Through all of the write methods it should be noted that entry_name is used both in the name of the source and as the name of the entry in the catalog.*
#### to_csv(entry_name: str, **kwargs)
This will write the data out to a persistence storage as CSV format while logging the asset to a catalog with entry_name. kwargs represents the standard write parameters in pandas which can be used here in the same. 

*Make note that the default value of index_col has been changed to 0 to make sure that the write & read defaults are as seamless as possible.*

#### to_parquet(entry_name: str, **kwargs)
This will write the data out to a persistence storage as CSV format while logging the asset to a catalog with entry_name. kwargs represents the standard write parameters in pandas which can be used here in the same.

#### to_pickle(entry_name: str, **kwargs)
This will write the data out to a persistence storage as CSV format while logging the asset to a catalog with entry_name. kwargs represents the standard write parameters in pandas which can be used here in the same.

##Examples

__Reading and Writing__
```python
import pandas as pd 
import dataframez

df_to_write = pd.DataFrame.from_dict({'a': [1, 2, 3], 'b': [2, 3, 5]})

df_to_write.dataframez.to_parquet(entry_name='my_asset')

df_read_from_catalog = pd.from_catalog(entry_name='test_data_parquet')
```

__Getting list of Assets__
```python
import pandas as pd
import dataframez

asset_list = pd.list_assets()
```