-- create external tables in gold schema
CREATE EXTERNAL TABLE gold.ext_order
WITH
(
    LOCATION = 'external_order',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
)
AS
SELECT * FROM gold.order_view