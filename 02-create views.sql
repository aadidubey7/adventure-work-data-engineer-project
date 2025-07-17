------------------------------------------------------------------------------------------------
-- create views on transformed data (from silver layer) for gold layer 
------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------------
CREATE VIEW gold.order_view
AS
SELECT
    *
FROM
    OPENROWSET (
        BULK 'https://adityaawstorage.dfs.core.windows.net/silver/order/',
        FORMAT = 'PARQUET'
    ) AS order_view_query;