# dataframez

Extension to pandas to allow for simple cross-cloud-platform interactions with data, use of data versioning tools, and much more. The idea is to make it very simple for pandas users to interact with named data sources. 

A named data source is a source where the name can be used to retrieve the data without giving additional access criteria - such as would be necessary when accessing data through a database connection, connection to a cloud resource, and so on. This can be bothersome for a data scientist. Who wants to track where their data resides!!!

In modern environment a data catalog is often used to track data assets. But interacting with these catalogs is also bothersome. The use of a named asset abstracts the interface with such catalogs by providing all the necessary interactions with this 'catalog' to identify and retrieve teh data. Cataloging in this sense can also mean a data versioning utility. In gereal, however, this means that the catalog interactions of dataframez can work across many catalogs in tandem; that is, with enterprise catalog and data scientist catalogs at the same time. 

##API

###pandas.from_catalog(name: str) -> pandas.DataFrame
This method extends the read capabilities of pandas to read from a 'cataloged' asset. 

###pandas.DataFrame.dataframez
This is a collection of methods placed under the dataframez namespace. The methods here are writer methods to write data to csv, parquet, ..., and various cloud provider data sources.

#### Supported Methods
* pandas.DataFrame.dataframez.to_csv
* pandas.DataFrame.dataframez.to_parquet
* pandas.DataFrame.dataframez.to_sql
* pandas.DataFrame.dataframez.to_gbq
* pandas.DataFrame.dataframez.to_pickle

##Examples