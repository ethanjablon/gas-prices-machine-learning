import xml.etree.ElementTree as et
import os

azure_config_path = os.path.join(os.path.dirname(__file__), 'AzureConfig.xml')
root = et.parse(azure_config_path).getroot()

def get(element_name: str):
    '''
    Gets the entry in the AzureConfig.xml file with the name element_name.
    Causes an error if there is no such element.
    '''
    return root.find(element_name).text