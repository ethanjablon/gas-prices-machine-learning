# Databricks notebook source
# MAGIC %md
# MAGIC # Data source connection setup
# MAGIC We set up the data source connection first, using our secret scope.

# COMMAND ----------

storage_end_point = "gaspricesmlstorage.dfs.core.windows.net"
my_scope = "CSCI422-EJablon-secret-scope"
my_key = "StorageAccountKey"

spark.conf.set(
    "fs.azure.account.key." + storage_end_point,
    dbutils.secrets.get(scope=my_scope, key=my_key)
)

uri = f"abfss://blob@gaspricesmlstorage.dfs.core.windows.net/"

# COMMAND ----------

# MAGIC %md
# MAGIC # Retrieve both data files into dataframes

# COMMAND ----------

eia_df = spark.read.format('csv').option('header', 'true').load(uri + 'eia_data.csv')

display(eia_df)

ghcnm_df = spark.read.format('csv').option('header', 'true').load(uri + 'ghcnm_data.csv')

display(ghcnm_df)

# COMMAND ----------

joined_df = eia_df.join(ghcnm_df, on=['Region', 'Year', 'Month'], how='left')

display(joined_df)

# COMMAND ----------

joined_df.write.option('header', 'true').mode('overwrite').csv(uri + 'transformed_aggregated_data.csv')
