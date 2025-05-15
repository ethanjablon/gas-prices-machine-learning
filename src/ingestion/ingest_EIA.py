import requests
import json
import pandas as pd
# pip install azure-identity
from azure.identity import DefaultAzureCredential
# pip install azure-storage-file-datalake
from azure.storage.filedatalake import DataLakeServiceClient
from src.azure import azure_connection_setup
import time

# get API key from config file
with open("EIAKey.config") as f:
    eia_key=f.readline()

# get client for blob container
blob_client = azure_connection_setup.get_storage_container_client()

# Makes request to EIA and converts it into a Pandas dataframe
def eia_request_to_dataframe(request_url, request_params):
    # make API request
    api_response = requests.get(
        url=request_url,
        params=request_params
    )
    # convert response to json datatype
    json_data = json.loads(api_response.content)
    try:
        # convert json to pandas and return
        return pd.DataFrame(json_data['response']['data'])
    except KeyError as ke:
        # error if 'response' or 'data' are invalid keys
        raise KeyError(str(ke) + '\nEither "response" or "data" not found. Request returned the following: ' + str(json_data))

# Makes requests for each year and concatenates the dataframes.
# Workaround for EIA request size limit of 5000 rows.
def eia_monthly_request_multiyear(request_url, request_params, start_year, end_year):
    dataframe = None
    for i in range(start_year, end_year + 1):
        request_params["start"] = str(i) + "-01"
        request_params["end"] = str(i) + "-12"
        # make request for year i and concatenate to the dataframe
        dataframe = pd.concat([dataframe, eia_request_to_dataframe(request_url, request_params)])
    return dataframe

# setting up shared request parameters
# that will be the same for all three datasets
shared_params = {
    "api_key": eia_key,
    "frequency": "monthly",
    "sort[0][column]": "period", # sort by time
    "sort[0][direction]": "asc", # ascending
    "offset": 0,
    "length": 5000
}

print('Retrieving gas price data from EIA API...')
# Get the data for the first dataset, gasoline prices.
gas_prices_url = "https://api.eia.gov/v2/petroleum/pri/gnd/data/"
gas_prices_params = shared_params.copy()
gas_prices_params["facets[product][0]"] = "EPM0"
gas_prices_params["data[0]"] = "value"
gas_prices_df = eia_monthly_request_multiyear(
    gas_prices_url,
    gas_prices_params,
    1990,
    2025
)

print('Retrieving gasoline consumption/sales data from EIA API...')
# Get the data for gasoline consumption/sales
gas_sales_url = "https://api.eia.gov/v2/petroleum/cons/refmg/data/"
gas_sales_params = shared_params.copy()
gas_sales_params["facets[product][0]"] = "EPM0"
gas_sales_params["data[0]"] = "value"
gas_sales_df = eia_monthly_request_multiyear(
    gas_sales_url,
    gas_sales_params,
    1990,
    2025
)

print('Retrieving crude oil imports data from EIA API...')
# Get the data for crude oil imports
oil_imports_url = "https://api.eia.gov/v2/crude-oil-imports/data/"
oil_imports_params = shared_params.copy()
oil_imports_params["data[0]"] = "quantity"
oil_imports_params["facets[gradeId][]"] = "LSW"
oil_imports_df = eia_monthly_request_multiyear(
    oil_imports_url,
    oil_imports_params,
    2009,
    2025
)

print('printing size of gasprices and sales dataframes')
print(gas_prices_df.shape)
print(gas_sales_df.shape)

# Upload data to blob storage
print('Uploading gas prices data to blob storage.')
gas_prices_file_client = blob_client.get_file_client('gas prices.csv')
gas_prices_file_client.upload_data(gas_prices_df.to_csv().encode('utf-8'),
                                  overwrite=True,
                                  chunk_size=4 * 1024 * 1024) # 4MB chunk size

print('Uploading gas sales data to blob storage.')
gas_sales_file_client = blob_client.get_file_client('gas sales.csv')
gas_sales_file_client.upload_data(gas_sales_df.to_csv().encode('utf-8'),
                                  overwrite=True,
                                  chunk_size=4 * 1024 * 1024) # 4MB chunk size

time.sleep(5)

print('Uploading oil imports data to blob storage.')
oil_imports_file_client = blob_client.get_file_client('oil imports.csv')
oil_imports_file_client.upload_data(oil_imports_df.to_csv().encode('utf-8'),
                                  overwrite=True,
                                  chunk_size=4 * 1024 * 1024) # 4MB chunk size
