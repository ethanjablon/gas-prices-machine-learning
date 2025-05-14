from src.azure import azure_connection_setup
from io import StringIO
import os
import pandas as pd
from ydata_profiling import ProfileReport

# get client for blob container
blob_client = azure_connection_setup.get_storage_container_client()

# retrieve data sets from Azure and place into dataframes
gas_prices_df = pd.read_csv(
    StringIO(
        blob_client.get_file_client('gas prices.csv').download_file().readall().decode("utf-8")
    )
)
gas_sales_df = pd.read_csv(
    StringIO(
        blob_client.get_file_client('gas sales.csv').download_file().readall().decode("utf-8")
    )
)
oil_imports_df = pd.read_csv(
    StringIO(
        blob_client.get_file_client('oil imports.csv').download_file().readall().decode("utf-8")
    )
)
climate_norms_df = pd.read_csv(
    StringIO(
        blob_client.get_file_client('mly-normal-allall.csv').download_file().readall().decode("utf-8")
    )
)

# destination path for profile reports
IngestionAnalysis_path = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'SupplementaryInfo', 'IngestionAnalysis'
)

# create profile reports
ProfileReport(
    gas_prices_df, title="Gas prices dataset profile report"
).to_file(
    os.path.join(IngestionAnalysis_path, "Gas prices dataset profile report.html")
)
ProfileReport(
    gas_sales_df, title="Gas sales dataset profile report"
).to_file(
    os.path.join(IngestionAnalysis_path, "Gas sales dataset profile report.html")
)
ProfileReport(
    oil_imports_df, title="Oil imports dataset profile report"
).to_file(
    os.path.join(IngestionAnalysis_path, "Oil imports dataset profile report.html")
)
ProfileReport(
    climate_norms_df, title="Climate normals dataset profile report"
).to_file(
    os.path.join(IngestionAnalysis_path, "Climate normals dataset profile report.html")
)
