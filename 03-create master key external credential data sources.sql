-- create master key
CREATE MASTER KEY ENCRYPTION BY PASSWORD ='{PASSWORD}';


-- create credential to access storage account data lake
CREATE DATABASE SCOPED CREDENTIAL aw_access_cred
WITH IDENTITY = 'Managed Identity';

-- create external data source for pointing url to a storage container
-- create data source for silver layer to access the data

CREATE EXTERNAL DATA SOURCE source_silver
WITH
(
    LOCATION = 'https://adityaawstorage.dfs.core.windows.net/silver/',
    CREDENTIAL = aw_access_cred
)

-- create data source for gold layer to write the data
CREATE EXTERNAL DATA SOURCE source_gold
WITH
(
    LOCATION = 'https://adityaawstorage.dfs.core.windows.net/gold/',
    CREDENTIAL = aw_access_cred
)


-- create external file format
CREATE EXTERNAL FILE FORMAT format_parquet
WITH
(
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
)