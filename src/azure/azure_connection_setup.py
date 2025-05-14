from src.azure import azure_config_reader
from azure.storage.filedatalake import DataLakeServiceClient
import xml.etree.ElementTree as et

# get API key from config file
with open("ADLSSASToken.config") as f:
    sas_token=f.readline()

def get_service_client_sas(account_name: str, sas_token: str) -> DataLakeServiceClient:
    '''Gets an ADLS client via SAS token passed as parameter'''
    account_url = f"https://{account_name}.dfs.core.windows.net"

    # The SAS token string can be passed in as credential param or appended to the account URL
    service_client = DataLakeServiceClient(account_url, credential=sas_token)

    return service_client

def get_storage_account_client():
    """Gets the ADLS client via SAS token stored in ADLSSASToken.config"""
    return get_service_client_sas(azure_config_reader.get('StorageAccountName'), sas_token)

def get_storage_container_client():
    """Gets the ADLS blob storage client specifically"""
    return get_storage_account_client().get_file_system_client(azure_config_reader.get('StorageContainerName'))