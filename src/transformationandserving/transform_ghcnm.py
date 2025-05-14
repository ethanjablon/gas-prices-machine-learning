# Databricks notebook source
# MAGIC %md
# MAGIC # Data source connection setup
# MAGIC We set up the data source connection first, using our secret scope.

# COMMAND ----------

storage_end_point = "csci422ejablonstorage.dfs.core.windows.net"
my_scope = "CSCI422-EJablon-secret-scope"
my_key = "StorageAccountKey"

spark.conf.set(
    "fs.azure.account.key." + storage_end_point,
    dbutils.secrets.get(scope=my_scope, key=my_key)
)

uri = "abfss://blob@csci422ejablonstorage.dfs.core.windows.net/"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Imports
# MAGIC Here we import packages that are used multiple times, and make sure ones that are not installed by default are installed.

# COMMAND ----------

!pip install geopandas

from pyspark.sql import DataFrame
from pyspark.sql import functions as fn
from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %md
# MAGIC We need to parse the raw text files into a more usable structure with columns and rows.
# MAGIC
# MAGIC To do this, we apply the schema from the GHCNm documentation.
# MAGIC
# MAGIC The GHCNm data is divided into a .dat file and a .inv file. We will be joining the two to better serve the PowerBI analysis.

# COMMAND ----------

ghcnm_filename = "ghcnm.tavg.v4.0.1.20241209.qcf"

# COMMAND ----------

# MAGIC %md
# MAGIC # Transforming the .inv file
# MAGIC First we transform the .inv file. We will map the longitude/latitude values to the five regions of interest and filter out the rows that are not of interest.

# COMMAND ----------

inv_df = spark.read.text(uri + ghcnm_filename + '.inv')

inv_df = inv_df.select(
    [inv_df.value.substr(1,-1+11+1).alias('ID'),
    inv_df.value.substr(13,-13+20+1).alias('LATITUDE'),
    inv_df.value.substr(22,-22+30+1).alias('LONGITUDE'),
    inv_df.value.substr(32,-32+37+1).alias('STNELEV'),
    inv_df.value.substr(39,-39+68+1).alias('NAME')]
)

display(inv_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reverse geocoding with geopandas and a government shapefile in Azure datalake
# MAGIC We define a function to map latitude and longitude to US states using geopandas.

# COMMAND ----------

# We use geopandas for the reverse geocoding step to avoid distributed computing problems
import pandas as pd
import geopandas as gpd

local_shp_dir = "/dbfs/tmp/shapefiles/"
shp_name = "tl_2024_us_state"

dbutils.fs.cp(uri + shp_name, local_shp_dir + shp_name, recurse=True)

# Load the shapefile into a GeoDataFrame
states_gdf = gpd.read_file(f'{local_shp_dir}{shp_name}/{shp_name}.shp')
states_gdf = states_gdf.to_crs(epsg=4326) # maps to the standard Coordinate Reference System for GPS

states_gdf = states_gdf.reset_index(drop=True)

@udf
def get_state(lat, lon):
    lat = float(str(lat).strip()) # strings to floats
    lon = float(str(lon).strip())
    point = gpd.points_from_xy([lon], [lat])[0] # converts lat/lon to shapely Point
    matches_gdf = states_gdf.loc[states_gdf.geometry.contains(point)] # gets rows of GeoDataFrame containing point
    return matches_gdf.iloc[0]['NAME'] if not matches_gdf.empty else None

inv_df = inv_df.withColumn('USSTATE',
                           get_state(inv_df['LATITUDE'], inv_df['LONGITUDE']))
inv_df = inv_df.where(col('USSTATE').isNotNull())

display(inv_df)

# COMMAND ----------

# MAGIC %md
# MAGIC We map the US states to the PADD regions of interest, and drop rows that do not correspond to any of these regions.

# COMMAND ----------

# PADD 1 (East Coast)
PADD_1A = {'Connecticut', 'Maine', 'Massachusetts', 'New Hampshire', 'Rhode Island', 'Vermont'}
PADD_1B = {'Delaware', 'District of Columbia', 'Maryland', 'New Jersey', 'New York', 'Pennsylvania'}
PADD_1C = {'Florida', 'Georgia', 'North Carolina', 'South Carolina', 'Virginia', 'West Virginia'}
PADD_1 = PADD_1A | PADD_1B | PADD_1C # unions the three sets

# PADD 2 (Midwest)
PADD_2 = {'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky', 'Michigan', 'Minnesota', 'Missouri', 'Nebraska', 'North Dakota', 'Ohio', 'Oklahoma', 'South Dakota', 'Tennessee', 'Wisconsin'}

# PADD 3 (Gulf Coast)
PADD_3 = {'Alabama', 'Arkansas', 'Louisiana', 'Mississippi', 'New Mexico', 'Texas'}

# PADD 4 (Rocky Mountain)
PADD_4 = {'Colorado', 'Idaho', 'Montana', 'Utah', 'Wyoming'}

# PADD 5 (West Coast)
PADD_5 = {'Alaska', 'Arizona', 'California', 'Hawaii', 'Nevada', 'Oregon', 'Washington'}

@udf
def region(state):
    if state in PADD_1:
        return 'PADD 1'
    elif state in PADD_2:
        return 'PADD 2'
    elif state in PADD_3:
        return 'PADD 3'
    elif state in PADD_4:
        return 'PADD 4'
    elif state in PADD_5:
        return 'PADD 5'
    else:
        return None

inv_df = inv_df.withColumn('PADDREGION',
                           region(inv_df.USSTATE))
inv_df = inv_df.drop('LATITUDE', 'LONGITUDE', 'USSTATE')

inv_df = inv_df[col('PADDREGION').isNotNull()]

display(inv_df)


# COMMAND ----------

# MAGIC %md
# MAGIC # Transforming the .dat file
# MAGIC First we read it in.

# COMMAND ----------

import itertools

dat_df = spark.read.text(uri + ghcnm_filename + '.dat')

dat_df = dat_df.select(
    [dat_df.value.substr(1,-1+11+1).alias('ID'),
    dat_df.value.substr(12,-12+15+1).alias('YEAR'),
    dat_df.value.substr(16,-16+19+1).alias('ELEMENT')]
    # Includes VALUE and the 3 FLAGs. These will be split after melting.
    + [dat_df.value.substr(20+8*i,4+3+1).alias(f'MONTH{i+1}') for i in range(12)]
)

'''
    + list(itertools.chain.from_iterable(
        (dat_df.value.substr(20+8*i,4+1).alias(f'VALUE{i}'),
         dat_df.value.substr(20+8*i + 5,1).alias(f'DMFLAG{i}'),
         dat_df.value.substr(20+8*i + 6,1).alias(f'QCFLAG{i}'),
         dat_df.value.substr(20+8*i + 7,1).alias(f'DSFLAG{i}')) for i in range(1, 13)
    ))
'''

display(dat_df)

# COMMAND ----------

# MAGIC %md
# MAGIC We will immediately remove any records from before 1970 because they are irrelevant.

# COMMAND ----------

dat_df = dat_df.where(col('YEAR').cast('int') >= 1970)

# COMMAND ----------

# MAGIC %md
# MAGIC The following function allows us to melt the dataframe.

# COMMAND ----------

# This function was shown to us in class

# Function from https://www.taintedbits.com/2018/03/25/reshaping-dataframe-using-pivot-and-melt-in-apache-spark-and-pandas/
from pyspark.sql.functions import array, col, explode, lit, struct
from pyspark.sql import DataFrame
from typing import Iterable

def melt_df(
        df: DataFrame,
        id_vars: Iterable[str], value_vars: Iterable[str],
        var_name: str="variable", value_name: str="value") -> DataFrame:
    """Convert :class:`DataFrame` from wide to long format."""

    # Create array<struct<variable: str, value: ...>>
    _vars_and_vals = array(*(
        struct(lit(c).alias(var_name), col(c).alias(value_name))
        for c in value_vars))

    # Add to the DataFrame and explode
    _tmp = df.withColumn("_vars_and_vals", explode(_vars_and_vals))

    cols = id_vars + [
            col("_vars_and_vals")[x].alias(x) for x in [var_name, value_name]]
    return _tmp.select(*cols)

# COMMAND ----------

# MAGIC %md
# MAGIC Now we actually do the melting.

# COMMAND ----------

# The target format is ID, YEAR, ELEMENT, MONTH, DATA
dat_df = melt_df(dat_df,   # Data frame
                 ['ID', 'YEAR', 'ELEMENT'], # Columns to keep
                 dat_df.columns[3:], # Columns to convert to long
                 'MONTH', # Name of new column that used to be the column header.
                 'DATA') # Name of new column containing the value.

dat_df = dat_df.withColumn('MONTH', fn.trim(fn.substring(col('MONTH'),6,2)))

display(dat_df)

# COMMAND ----------

# MAGIC %md
# MAGIC We split up the DATA column into VALUE and the 3 FLAGS.

# COMMAND ----------

dat_df = dat_df.withColumn('VALUE', fn.trim(fn.substring(col('DATA'),1,5)))\
               .withColumn('DMFLAG', fn.trim(fn.substring(col('DATA'),6,1)))\
               .withColumn('QCFLAG', fn.trim(fn.substring(col('DATA'),7,1)))\
               .withColumn('DSFLAG', fn.trim(fn.substring(col('DATA'),8,1)))\
               .drop('DATA')

display(dat_df)

# COMMAND ----------

# MAGIC %md
# MAGIC We filter out any data with imperfect flags.

# COMMAND ----------

dat_df = dat_df[dat_df['DMFLAG'] == '']\
               [dat_df['QCFLAG'] == '']

dat_df = dat_df.drop('DMFLAG', 'QCFLAG', 'DSFLAG')

display(dat_df)

# COMMAND ----------

# MAGIC %md
# MAGIC We map the VALUE column to the corresponding celsius measurement.

# COMMAND ----------

dat_df = dat_df.withColumn('CELSIUSTEMP',
                           fn.col('VALUE').cast('double') / 100)\
               .drop('VALUE')

display(dat_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Joining .dat and .inv

# COMMAND ----------

# MAGIC %md
# MAGIC Now we join this data with the .inv data to create a merged dataframe, melt the dataframe so that the 12 VALUE fields are replaced by a single month field and value field, and aggregate temperature by region.

# COMMAND ----------

ghcnm_df = inv_df.join(dat_df, on='ID')

display(ghcnm_df)

# COMMAND ----------

# MAGIC %md
# MAGIC We aggregate average temp by region.

# COMMAND ----------

ghcnm_df = ghcnm_df.groupBy('PADDREGION', 'YEAR', 'MONTH')\
                   .agg(fn.avg('CELSIUSTEMP').alias('REGIONALAVGTEMP'))

display(ghcnm_df)

# COMMAND ----------

# MAGIC %md
# MAGIC We rename columns to match the schema of the EIA file.

# COMMAND ----------

ghcnm_df = ghcnm_df.toDF('Region', 'Year', 'Month', 'Regional Average Temp')

display(ghcnm_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # We write the transformed data back to the Azure datalake.

# COMMAND ----------

ghcnm_df.write.option('header', 'true').mode('overwrite').csv(uri + 'ghcnm_data.csv')
