
<# This Powershell script creates a Databricks instance in the Azure resource group
 # and adds the storage account access key to the key vault.
 # It must be in the same directory as AzureConfig.xml to work.
 # 
 # The user is required to manually create:
 #  - A compute cluster
 #  - Secret scope backed by the key vault for Databricks to access the storage account key
 #  - Github connection to run transformation scripts
 #>


<# Set up Azure connection. #>
# Module installations 
$module = 'Az'
if (-not (Get-InstalledModule -Name $module -ErrorAction SilentlyContinue)) {
    Install-Module -Name $module -Force
    Write-Output ('Installed {0} module.' -f $module)
}
else {
    Write-Output ('{0} module found.' -f $module)
}

$module = 'AzureAD'
if (-not (Get-InstalledModule -Name $module -ErrorAction SilentlyContinue)) {
    Install-Module -Name $module -AllowClobber -Force
    Write-Output ('Installed {0} module.' -f $module)
}
else {
    Write-Output ('{0} module found.' -f $module)
}

Import-Module Az.Storage
Import-Module Az.Sql

Connect-AzAccount


<# Assign parameters from config file. #>
# Note - following assumes execution is in the folder containing the config file.
$ParameterFile=([xml](Get-Content -Path .\AzureConfig.xml )).root

$SubscriptionName=$ParameterFile.SubscriptionName # Name of subscription in Azure

$AuthorizedUser=$ParameterFile.AuthorizedUser # Name of authorized user for key vault
$ResourceGroupName=$ParameterFile.ResourceGroupName # Name of resource group to be created
$Location=$ParameterFile.Location # Region of Azure Resource Group
$DatabricksName=$ParameterFile.DatabricksWorkspaceName # Name of Databricks workspace
$KeyVaultName=$ParameterFile.KeyVaultName # Name of Key Vault to be created
$StorageAccountName=$ParameterFile.StorageAccountName # Name of storage account to be created
#$StorageContainerName=$ParameterFile.StorageContainerName # Name of storage container to be created
#$SQLServerName=$ParameterFile.SQLServerName #Name of SQL Server to be created
#$DatabaseName=$ParameterFile.DatabaseName #Name of database to be created for storing usage data in SQL Server
#$ServerAdmin=$ParameterFile.ServerAdmin #Azure user name of server admin

# Set the subscription context
# Note - this is the name of the subscription, not the ID.
Write-Host 'Setting subscription context'
Set-AzContext -SubscriptionName $SubscriptionName

# Checks the Azure context
Write-Host 'Checking subscription context'
Get-AzContext

Write-Host 'Getting user'
Get-AzADUser -UserPrincipalName $AuthorizedUser

# get ObjectId of the authorized user for the role assignment
$userObjectId = (Get-AzADUser -UserPrincipalName $AuthorizedUser).Id

# give myself permission to manage secrets in the key vault
Write-Host 'Giving user role-based permission to manage key vault'
New-AzRoleAssignment -ObjectId $userObjectId `
  -RoleDefinitionName "Key Vault Administrator" `
  -Scope "/subscriptions/e2cca51e-caac-4aee-978e-260ba21f10e9/resourceGroups/gas-prices-ml/providers/Microsoft.KeyVault/vaults/gas-prices-ml-kv"

# Register the storage resource provider
Write-Host 'Registering storage resource provider'
Register-AzResourceProvider -ProviderNamespace Microsoft.Databricks
# Create the Databricks workspace
Write-Host 'Creating Databricks workspace'
New-AzDatabricksWorkspace -Name $DatabricksName -ResourceGroupName $ResourceGroupName -Location $Location -ManagedResourceGroupName databricks-group -Sku standard

$storageAccountKey = `
    (Get-AzStorageAccountKey `
    -ResourceGroupName $ResourceGroupName `
    -Name $StorageAccountName).Value[0]

$secureKey = ConvertTo-SecureString $storageAccountKey -AsPlainText -Force

Write-Host $storageAccountKey

Write-Host 'Adding storage account key to key vault'
Set-AzKeyVaultSecret -VaultName $KeyVaultName -Name 'StorageAccountKey' -SecretValue $secureKey
