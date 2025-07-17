# Databricks notebook source
# MAGIC %md
# MAGIC **Connect data lake storage using APP**

# COMMAND ----------

from pyspark.sql.functions import month, year, col, concat_ws, split, to_timestamp, regexp_replace, count

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.adityaawstorage.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.adityaawstorage.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.adityaawstorage.dfs.core.windows.net", "{APP_CLIENT_ID}")
spark.conf.set("fs.azure.account.oauth2.client.secret.adityaawstorage.dfs.core.windows.net", "{APP_SECRET_KEY}")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.adityaawstorage.dfs.core.windows.net", "https://login.microsoftonline.com/{TENANT_OR_DIRECTORY_ID}/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC #### load data

# COMMAND ----------

df_calendar = spark.read.format('csv').option('header', True).option('inferSchema', True).load('abfss://bronze@adityaawstorage.dfs.core.windows.net/AdventureWorks_Calendar')
df_customers = spark.read.format('csv').option('header', True).option('inferSchema', True).load('abfss://bronze@adityaawstorage.dfs.core.windows.net/AdventureWorks_Customers')
df_categories = spark.read.format('csv').option('header', True).option('inferSchema', True).load('abfss://bronze@adityaawstorage.dfs.core.windows.net/AdventureWorks_Product_Categories')
df_products = spark.read.format('csv').option('header', True).option('inferSchema', True).load('abfss://bronze@adityaawstorage.dfs.core.windows.net/AdventureWorks_Products')
df_returns = spark.read.format('csv').option('header', True).option('inferSchema', True).load('abfss://bronze@adityaawstorage.dfs.core.windows.net/AdventureWorks_Returns')
df_sales = spark.read.format('csv').option('header', True).option('inferSchema', True).load('abfss://bronze@adityaawstorage.dfs.core.windows.net/AdventureWorks_Sales*')
df_territories = spark.read.format('csv').option('header', True).option('inferSchema', True).load('abfss://bronze@adityaawstorage.dfs.core.windows.net/AdventureWorks_Territories')
df_subcategories = spark.read.format('csv').option('header', True).option('inferSchema', True).load('abfss://bronze@adityaawstorage.dfs.core.windows.net/Product_Subcategories')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformations

# COMMAND ----------

# MAGIC %md
# MAGIC #### Calendar

# COMMAND ----------

df_calendar.display()

# COMMAND ----------

df_calendar_final = df_calendar.withColumn('Month', month(col('Date'))) \
    .withColumn('Year', year(col('Date')))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Customers

# COMMAND ----------

df_customers.display()

# COMMAND ----------

df_customers_final = df_customers.withColumn('Fullname', concat_ws(' ', col('Prefix'), col('FirstName'), col('LastName')))
df_customers_final.display()


# COMMAND ----------

# MAGIC %md
# MAGIC #### Products

# COMMAND ----------

df_products.display()

# COMMAND ----------

df_products_final = df_products.withColumn('ProductSKU', split(col('ProductSKU'), '-')[0])\
                                .withColumn('ProductName', split(col('ProductName'), ' ')[0])

df_products_final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### sales

# COMMAND ----------

df_sales.display()

# COMMAND ----------


df_sales_final = df_sales.withColumn('StockDate', to_timestamp(col('StockDate')))

# COMMAND ----------

df_sales_final = df_sales_final.withColumn('OrderNumber', regexp_replace(col('OrderNumber'), 'S', 'T'))

# COMMAND ----------

df_sales_final = df_sales_final.withColumn('multiply', col('OrderLineItem') * col('OrderQuantity'))

# COMMAND ----------

df_sales_final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### sales analysis - how many orders we receive every day

# COMMAND ----------

df_order = df_sales_final.groupBy('OrderDate').agg(count('OrderNumber').alias('total_order'))
df_order.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write data to silver layer

# COMMAND ----------

df_calendar_final.write.format('parquet').mode('overwrite').option('path', 'abfss://silver@adityaawstorage.dfs.core.windows.net/calendar').save()
df_customers_final.write.format('parquet').mode('overwrite').option('path', 'abfss://silver@adityaawstorage.dfs.core.windows.net/customers').save()
df_categories.write.format('parquet').mode('overwrite').option('path', 'abfss://silver@adityaawstorage.dfs.core.windows.net/categories').save()
df_products_final.write.format('parquet').mode('overwrite').option('path', 'abfss://silver@adityaawstorage.dfs.core.windows.net/products').save()
df_returns.write.format('parquet').mode('overwrite').option('path', 'abfss://silver@adityaawstorage.dfs.core.windows.net/returns').save()
df_territories.write.format('parquet').mode('overwrite').option('path', 'abfss://silver@adityaawstorage.dfs.core.windows.net/territories').save()
df_subcategories.write.format('parquet').mode('overwrite').option('path', 'abfss://silver@adityaawstorage.dfs.core.windows.net/subcategories').save()
df_sales_final.write.format('parquet').mode('overwrite').option('path', 'abfss://silver@adityaawstorage.dfs.core.windows.net/sales').save()
df_order.write.format('parquet').mode('overwrite').option('path', 'abfss://silver@adityaawstorage.dfs.core.windows.net/order').save()

# COMMAND ----------

