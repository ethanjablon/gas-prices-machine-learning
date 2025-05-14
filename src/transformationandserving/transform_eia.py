# Databricks notebook source
# MAGIC %md
# MAGIC We set up the data source connection first, using our secret scope.

# COMMAND ----------

storage_end_point = "csci422ejablonstorage.dfs.core.windows.net"
my_scope = "CSCI422-EJablon-secret-scope"
my_key = "StorageAccountKey"

spark.conf.set(
    "fs.azure.account.key." + storage_end_point,
    dbutils.secrets.get(scope=my_scope, key=my_key))

uri = "abfss://blob@csci422ejablonstorage.dfs.core.windows.net/"

# COMMAND ----------

# MAGIC %md
# MAGIC Cleaning gas prices data

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

# Read in gas prices
gas_prices_df = spark.read.options(delimiter=',', header=True).csv(uri+"gas prices.csv")

# Selecting relevant data to the analysis
gas_prices_df = gas_prices_df.select(['period', 'area-name', 'value', 'units'])

# We are interested in the 5 major EIA regions
areas_of_interest = [f'PADD {i}' for i in range(1,6)]
# Filter the data to only include the areas of interest
gas_prices_df = gas_prices_df.where(col('area-name').isin(areas_of_interest))

gas_prices_df = gas_prices_df.toDF('Period', 'Region', 'Price', 'Price Units')

display(gas_prices_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Cleaning up gas sales data

# COMMAND ----------

# Read in gas sales
gas_sales_df = spark.read.options(delimiter=',', header=True).csv(uri+"gas sales.csv")

# Filter data to include only Wholesale/Resale Volume
gas_sales_df = gas_sales_df.where(col('process-name') == 'Wholesale/Resale Volume by Refiners and Gas Plants')

# Selecting relevant data to the analysis
gas_sales_df = gas_sales_df.select(['period', 'area-name', 'value', 'units'])

# We are interested in the 5 major EIA regions
areas_of_interest = [f'PADD {i}' for i in range(1,6)]
# Filter the data to only include the areas of interest
gas_sales_df = gas_sales_df.where(col('area-name').isin(areas_of_interest))

gas_sales_df = gas_sales_df.toDF('Period', 'Region', 'Sales', 'Sales Units')

display(gas_sales_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Cleaning oil import data

# COMMAND ----------

# Read in oil imports
oil_imports_df = spark.read.options(delimiter=',', header=True).csv(uri+"oil imports.csv")

# Selecting relevant data to the analysis
oil_imports_df = oil_imports_df.select(['period', 'destinationId', 'quantity', 'quantity-units'])

# Destination ID of areas of interest
areas_of_interest = [f'PP_{i}' for i in range(1,6)]

# Filter the data to only include the areas of interest
oil_imports_df = oil_imports_df.where(oil_imports_df['destinationId'].isin(areas_of_interest))

from pyspark.sql import functions as fn

oil_imports_df = oil_imports_df.withColumn('destinationId',
                                            fn.concat(fn.lit("PADD "),
                                                      fn.regexp_extract(fn.col('destinationId'), r'\d+', 0)))

oil_imports_df = oil_imports_df.toDF('Period', 'Region', 'Import Quantity', 'Import Quantity Units')

display(oil_imports_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Now we join the EIA tables all together. We also replace "Period" with Month and Year

# COMMAND ----------

eia_df = gas_prices_df.join(
  gas_sales_df, on=['Period', 'Region'], how='left'
).join(
  oil_imports_df, on=['Period', 'Region'], how='left'
)

# Extracting year and month from period formatted as YYYY-MM
eia_df = eia_df.withColumn('Year', fn.substring(eia_df['Period'], 1, 4).cast('string'))
eia_df = eia_df.withColumn('Month', fn.substring(fn.col('Period'), 6, 2).cast('int')) # cast to int to remove extra 0s
eia_df = eia_df.drop('Period')

display(eia_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Now we send the data back to to csv again in the datalake.

# COMMAND ----------

eia_df.write.option('header', 'true').mode('overwrite').csv(uri + 'eia_data.csv')
