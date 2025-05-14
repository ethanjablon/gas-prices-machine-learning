
<# This Powershell script sets up a SQL database in the Azure datalake.
 # It must be in the same directory as AzureConfig.xml to work.
 #
 # It is in the archive directory because the current solution does not use SQL,
 # but if future changes create a need for SQL this file will be helpful for setup.
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

#$SubscriptionName=$ParameterFile.SubscriptionName # Name of subscription in Azure

$AuthorizedUser=$ParameterFile.AuthorizedUser # Name of authorized user for key vault
$ResourceGroupName=$ParameterFile.ResourceGroupName # Name of resource group to be created
$Location=$ParameterFile.Location # Region of Azure Resource Group
$KeyVaultName=$ParameterFile.KeyVaultName # Name of Key Vault to be created
#$StorageAccountName=$ParameterFile.StorageAccountName # Name of storage account to be created
#$StorageContainerName=$ParameterFile.StorageContainerName # Name of storage container to be created
$SQLServerName=$ParameterFile.SQLServerName #Name of SQL Server to be created
$DatabaseName=$ParameterFile.DatabaseName #Name of database to be created for storing usage data in SQL Server
$ServerAdmin=$ParameterFile.ServerAdmin #Azure user name of server admin


Get-AzKeyVaultSecret -VaultName $KeyVaultName

# SQL Admin cred set up
$AdminPassword = Read-Host "Enter SQL Admin password (must meet Azure SQL strong password requirements): " -AsSecureString
$SQLServerAdminUserName = ConvertTo-SecureString $ServerAdmin -AsPlainText -Force
$SQLServerCredentials = New-Object System.Management.Automation.PSCredential ($ServerAdmin,$AdminPassword)


# Add secret password and username to key vault
Set-AzKeyVaultSecret -VaultName $KeyVaultName -Name 'SQLServerAdminUserName' -SecretValue $SQLServerAdminUserName
Set-AzKeyVaultSecret -VaultName $KeyVaultName -Name 'SQLServerAdminPassword' -SecretValue $AdminPassword

# Create new SQL server  
Write-Host "Creating SQL Server"
New-AzSqlServer -ResourceGroupName $ResourceGroupName -Location $Location -ServerName $SQLServerName -ServerVersion "12.0" -SqlAdministratorCredentials $SQLServerCredentials

# Firewall Rules
New-AzSqlServerFirewallRule -ResourceGroupName $ResourceGroupName -ServerName $SQLServerName -AllowAllAzureIPs

# Create database for server.  Default to Basic tier.
Write-Host "Creating database"
New-AzSqlDatabase -ResourceGroupName $ResourceGroupName -ServerName $SQLServerName -DatabaseName $DatabaseName -Edition Basic