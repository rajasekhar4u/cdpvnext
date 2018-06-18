#enter the cosmos filedetails
$cosmoshttp = "https://cosmos11.osdinfra.net/cosmos/Marketing.Test"
$sourcefilePath = "/local/CDPvNext/DataModels/OneGDC/Processing/Intermediate/OneMarketo/v1/"
$fileName = "Leads"
$tableName = $fileName

#converting STREAM file to TSV file and place in the same location
$sourcePath=$sourcefilePath + "/" + $filename + ".ss"
$destinationPath="/local/HDIClusters/cdpspark/tmp" + $sourcefilePath + "/" + $fileName + "/" + $fileName + ".tsv"
#$CosmosJob = Submit-CosmosScopeJob -ScriptPath "C:\Users\rachinth\documents\visual studio 2015\Projects\OneGDCTesting\OneGDCTesting\LeadActivities_Pivoted.script" -Vc vc://cosmos11/Marketing.Test -Parameters @{ sourcePath = $sourcePath ; destinationPath = $destinationPath}

#Create columns for Hive schema
$cosmoshttppath = $cosmoshttp + $sourcePath
$fileMetaData = Get-CosmosStreamMetadata $cosmoshttppath
$colms1 = $fileMetaData.Schema.Columns | ForEach-Object{"," + $_.Name }
$colms1[0] = $colms1[0].Trim(",")



$colms2 = $fileMetaData.Schema.Columns | ForEach-Object{"," + $_.Name + " string"}
$colms2[0] = $colms2[0].Trim(",")



$colms3 = $fileMetaData.Schema.Columns | ForEach-Object{"," + $_.Name + " " + $_.DataTypeName}
$colms3[0] = $colms3[0].Trim(",")


#Prepare Hive Query
#      Linux-based HDInsight
#$queryString = "CREATE EXTERNAL TABLE cdpvnextdev." + $tableName + "(" + $colms +") ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE LOCATION 'adl://marketing-test-c11.azuredatalakestore.net/local/HDIClusters/cdpspark/tmp" + $sourcefilePath + "/" + $fileName + "';"
#$queryString