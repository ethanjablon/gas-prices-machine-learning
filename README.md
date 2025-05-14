# CSCI 422 Gas Prices Project Overview - Ethan Jablon

The goal of this project is to identify factors that cause gas price fluctuations, which are not widely understood and often seem unpredictable. By identifying these factors, the project seeks to improve understanding of gas price fluctuations.

## Questions to answer
To what extent do each of the following affect gas prices:
- Gasoline consumption
- Crude oil imports
- Climate conditions

Also, what is the earliest predictor of a gas price shift?

## Datasets
To answer these questions, four datasets were chosen.

Three data sets came from the Energy Information Administration (EIA)'s [EIA OpenData](https://www.eia.gov/opendata/). Each was retrieved with a *monthly* timescale.
**1. Gasoline prices**
Average gasoline price in units of $/gal for the month, by region.
**2. Gasoline sales**
Average quantity of gasoline sold each month, in Mgal/D (million gallons per day), also by region.
**3. Crude oil imports**
Quantity of gas imported over the course of the month, in units of thousand barrels. Specifically, we are interested in *light, sweet* crude oil, as this is the type of oil most commonly used for gasoline for cars.

The fourth data set is for weather data. The data set that was chosen initially was from the National Centers for Environmental Information (NCEI)'s [U.S. Climate Normals](https://www.ncei.noaa.gov/products/land-based-station/us-climate-normals). However, this data set did not contain location data for the weather stations whose climate normals it reported. This required a pivot to (GHCNm)

The dataset we are using contains values averaged over a 30-year period (1991-2020) of many different climate measures on a monthly scale.

From this dataset, we are interested in only the average temperature, because temperature is a reasonable singular representative of climate.

### Regions of interest
Because most data was sourced from EIA, the analysis was divided by the (PADD regions)[https://www.eia.gov/tools/glossary/index.php?id=petroleum%20administration%20for%20defense%20district#:~:text=petroleum%20administration%20for%20defense%20district,and%207%20encompass%20U.S.%20territories.]:
PADD 1 (East Coast):
- PADD 1A (New England): Connecticut, Maine, Massachusetts, New Hampshire, Rhode Island, and Vermont.
- PADD 1B (Central Atlantic): Delaware, District of Columbia, Maryland, New Jersey, New York, and Pennsylvania.
- PADD 1C (Lower Atlantic): Florida, Georgia, North Carolina, South Carolina, Virginia, and West Virginia.
PADD 2 (Midwest): Illinois, Indiana, Iowa, Kansas, Kentucky, Michigan, Minnesota, Missouri, Nebraska, North Dakota, Ohio, Oklahoma, South Dakota, Tennessee, and Wisconsin.
PADD 3 (Gulf Coast): Alabama, Arkansas, Louisiana, Mississippi, New Mexico, and Texas.
PADD 4 (Rocky Mountain): Colorado, Idaho, Montana, Utah, and Wyoming.
PADD 5 (West Coast): Alaska, Arizona, California, Hawaii, Nevada, Oregon, and Washington.

The regions of interest were PADD 1, 2, 3, 4, and 5.

# Technical Solution

## Ingestion
All data is stored in a single blob container (named "blob") in an Azure datalake storage account.

The three data sets from EIA were ingested via REST API requests. Due to a limit on request size, the data was requested by year and concatenated as Pandas dataframes before being sent to the Azure datalake as a .csv file

The NCEI data set was ingested via direct upload to the Azure storage container. This was the only option because the data set is not API-accessible. Since the data set is static, and will not be updated until 2030, this option is perfectly viable. It is stored in the datalake in .csv format.

## Transformation
All data was transformed using Pyspark, with notebooks run in an Azure Databricks cluster. The data was pared down to only the relevant columns and rows to the analysis.

The resulting data from the EIA sources had the following columns, in order:
- Region
- Price
- Price Units
- Sales
- Sales Units
- Import Quantity
- Import Quantity Units
- Year
- Month

Price data was ensured to be non-null for each row, because it is the main interest of our analysis, but Sales and Import data were allowed to be null. By doing this, we avoided restricting our analysis in either category because of lack of data in another.

## Serving
Data was served to PowerBI directly from Azure blob storage. To avoid making dates overly specific, I did not use a DateTime datatype in the served data, but rather left the handling of the dates to the analysis.

## Analysis
A PowerBI report was generated from the served dataset.