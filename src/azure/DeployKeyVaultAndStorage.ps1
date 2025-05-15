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

$SubscriptionName=$ParameterFile.SubscriptionName #Name of subscription in Azure

$AuthorizedUser=$ParameterFile.AuthorizedUser # Name of authorized user for key vault
$ResourceGroupName=$ParameterFile.ResourceGroupName # Name of resource group to be created
$Location=$ParameterFile.Location # Region of Azure Resource Group
$KeyVaultName=$ParameterFile.KeyVaultName # Name of Key Vault to be created
$StorageAccountName=$ParameterFile.StorageAccountName # Name of storage account to be created
$StorageContainerName=$ParameterFile.StorageContainerName # Name of storage container to be created


# Set the subscription context
# Note - this is the name of the subscription, not the ID.
Set-AzContext -SubscriptionName $SubscriptionName

$currentContext = Get-AzContext
Write-Host "Active Subscription: $($currentContext.Subscription.Name)"
Write-Host "Active Subscription ID: $($currentContext.Subscription.Id)"

<# Create Azure resources #>
Write-Host 'Creating resource group'
New-AzResourceGroup -Name $ResourceGroupName -Location $Location

Write-Host 'Creating key vault'
Write-Host $KeyVaultName
New-AzKeyVault -VaultName $KeyVaultName -ResourceGroupName $ResourceGroupName -Location $Location 
Set-AzKeyVaultAccessPolicy -VaultName $KeyVaultName -UserPrincipalName $AuthorizedUser -PermissionsToSecrets get,set,list

Write-Host 'Creating storage account'
# Register the storage resource provider
Register-AzResourceProvider -ProviderNamespace Microsoft.Storage
$params = @{'ResourceGroupName'=$ResourceGroupName
            'Name'=$StorageAccountName
            'Location'=$Location
            'SkuName'='Standard_LRS'
            'Kind'='StorageV2'
            'AllowBlobPublicAccess'=$false
            'EnableHierarchicalNamespace'=$true
            'AccessTier'='Hot'
            'Debug'=$false}
New-AzStorageAccount @params # splatting the params

Write-Host 'Creating blob container in storage account'
# Get storage account context
$storageAccountContext = (Get-AzStorageAccount -ResourceGroupName $ResourceGroupName -Name $StorageAccountName).Context
# Creat blob container
$params = @{'Name'=$StorageContainerName
            'Permission'='Off' # No public access
            'Context'=$storageAccountContext} # inside of storage account
New-AzStorageContainer @params