// Databricks notebook source
// MAGIC %run /CDPvNext/UserDefinedFunctions

// COMMAND ----------

// MAGIC %sql
// MAGIC set spark.databricks.io.skipping.enabled=true;
// MAGIC set spark.databricks.io.cache.enabled=true;
// MAGIC set spark.databricks.optimiser.dynamicPartitionPruning=true;
// MAGIC set spark.databricks.optimiser.aggregatePushdown.enable=false;

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE DATABASE IF NOT EXISTS ADLS;
// MAGIC DROP TABLE IF EXISTS ADLS.LeadActivitiesTxt;
// MAGIC CREATE EXTERNAL TABLE IF NOT EXISTS ADLS.LeadActivitiesTxt(
// MAGIC  LeadActivities_Id string
// MAGIC ,LeadId Int
// MAGIC ,ActivityDate timestamp
// MAGIC ,ActivityTypeId Int
// MAGIC ,IsActivityTypePrimaryAttributePII boolean
// MAGIC ,primaryAttributeValueId Int
// MAGIC ,primaryAttributeValue string
// MAGIC ,primaryAttributeValue_PII int
// MAGIC ,ActivityTypeAttributeName string
// MAGIC ,IsActivityTypeAttributeNamePII boolean
// MAGIC ,ActivityTypeAttributeDataType string
// MAGIC ,ActivityTypeAttributeValue string
// MAGIC ,ActivityTypeAttributeValue_PII string
// MAGIC ,AuditDate timestamp
// MAGIC ,ActivityTypeAttributeValue_PIIHash double
// MAGIC ,primaryAttributeValue_PIIHash  double
// MAGIC ,InitializationVector string
// MAGIC ,DeltaDateTime timestamp
// MAGIC ,DatabasePartitionId int
// MAGIC ,RowProcessTime timestamp
// MAGIC ,TaskExecutionId int
// MAGIC ,LeadActivities_IdKey double
// MAGIC ,ActivityTypeAttributeNameKey double
// MAGIC ,ReceivedDate timestamp
// MAGIC ,CollectionBatchKey double
// MAGIC ,IntegrationBatchKey double
// MAGIC ,IncidentId string
// MAGIC ,IncidentTimeStamp timestamp)
// MAGIC     COMMENT 'LeadActivitiesTxt Raw data in Txt format'
// MAGIC     location 'dbfs:/mnt/CDPvNext/RAW/LeadActivitiesTxt'
// MAGIC     ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
// MAGIC     STORED AS TEXTFILE;

// COMMAND ----------

// MAGIC %sql 
// MAGIC select count(*) from ADLS.LeadActivitiesTxt;

// COMMAND ----------

spark.sql("select * from ADLS.LeadActivitiesTxt").write.mode("overwrite").format("com.databricks.spark.csv").option("path","dbfs:/mnt/CDPvNext/ADLS/LeadActivitiesCsv").saveAsTable("ADLS.LeadActivitiesCsv")

// COMMAND ----------

// MAGIC %sql 
// MAGIC select count(*) from ADLS.LeadActivitiesCsv;

// COMMAND ----------

spark.sql("select * from ADLS.LeadActivitiesTxt").write.mode("overwrite").option("path","dbfs:/mnt/CDPvNext/ADLS/LeadActivitiesPrqt").saveAsTable("ADLS.LeadActivitiesPrqt")

// COMMAND ----------

// MAGIC %sql 
// MAGIC select count(*) from ADLS.LeadActivitiesPrqt;

// COMMAND ----------

spark.sql("select * from ADLS.LeadActivitiesTxt").write.mode("overwrite").saveAsTable("DBFS.LeadActivitiesPrqt")

// COMMAND ----------

// MAGIC %sql 
// MAGIC select count(*) from DBFS.LeadActivitiesPrqt;

// COMMAND ----------

val ADLS_TXT_Raw = spark.sql("""
SELECT LeadActivities_Id,
       LeadId,           
       ActivityDate,
       ActivityTypeId,
       IsActivityTypePrimaryAttributePII,
       primaryAttributeValueId,
       IF(IsActivityTypePrimaryAttributePII == true, primaryAttributeValue_PII, primaryAttributeValue) AS primaryAttributeValue,
       IF(ActivityTypeAttributeName==null OR ActivityTypeAttributeName=='', null, regexp_replace(ActivityTypeAttributeName,' ','')) AS ActivityTypeAttributeName,
       IsActivityTypeAttributeNamePII,
       ActivityTypeAttributeDataType,
       IF(IsActivityTypeAttributeNamePII == true , ActivityTypeAttributeValue_PII , ActivityTypeAttributeValue )AS ActivityTypeAttributeValue,
       AuditDate,
       DeltaDateTime
    FROM ADLS.LeadActivitiesTxt""")
val ADLS_TXT = getLatestRecs(ADLS_TXT_Raw,List("LeadActivities_Id","ActivityTypeAttributeName"),List("DeltaDateTime")).cache()


// COMMAND ----------

val ADLS_Parquet_Raw = spark.sql("""
SELECT LeadActivities_Id,
       LeadId,           
       ActivityDate,
       ActivityTypeId,
       IsActivityTypePrimaryAttributePII,
       primaryAttributeValueId,
       IF(IsActivityTypePrimaryAttributePII == true, primaryAttributeValue_PII, primaryAttributeValue) AS primaryAttributeValue,
       IF(ActivityTypeAttributeName==null OR ActivityTypeAttributeName=='', null, regexp_replace(ActivityTypeAttributeName,' ','')) AS ActivityTypeAttributeName,
       IsActivityTypeAttributeNamePII,
       ActivityTypeAttributeDataType,
       IF(IsActivityTypeAttributeNamePII == true , ActivityTypeAttributeValue_PII , ActivityTypeAttributeValue )AS ActivityTypeAttributeValue,
       AuditDate,
       DeltaDateTime
    FROM ADLS.LeadActivitiesPrqt""")
val ADLS_Parquet = getLatestRecs(ADLS_Parquet_Raw,List("LeadActivities_Id","ActivityTypeAttributeName"),List("DeltaDateTime")).cache()

// COMMAND ----------

val DBFS_Parquet_Raw = spark.sql("""
SELECT LeadActivities_Id,
       LeadId,           
       ActivityDate,
       ActivityTypeId,
       IsActivityTypePrimaryAttributePII,
       primaryAttributeValueId,
       IF(IsActivityTypePrimaryAttributePII == true, primaryAttributeValue_PII, primaryAttributeValue) AS primaryAttributeValue,
       IF(ActivityTypeAttributeName==null OR ActivityTypeAttributeName=='', null, regexp_replace(ActivityTypeAttributeName,' ','')) AS ActivityTypeAttributeName,
       IsActivityTypeAttributeNamePII,
       ActivityTypeAttributeDataType,
       IF(IsActivityTypeAttributeNamePII == true , ActivityTypeAttributeValue_PII , ActivityTypeAttributeValue )AS ActivityTypeAttributeValue,
       AuditDate,
       DeltaDateTime
    FROM DBFS.LeadActivitiesPrqt""")
val DBFS_Parquet = getLatestRecs(DBFS_Parquet_Raw,List("LeadActivities_Id","ActivityTypeAttributeName"),List("DeltaDateTime")).cache()

// COMMAND ----------

// MAGIC %sql
// MAGIC Drop table if Exists ADLS.ADLS_TXT_ADLS_TXT;
// MAGIC Drop table if Exists DBFS.ADLS_TXT_DBFS_TXT;
// MAGIC Drop table if Exists ADLS.ADLS_TXT_ADLS_Parquet;
// MAGIC Drop table if Exists DBFS.ADLS_TXT_DBFS_Parquet;
// MAGIC Drop table if Exists ADLS.ADLS_Parquet_ADLS_TXT;
// MAGIC Drop table if Exists DBFS.ADLS_Parquet_DBFS_TXT;
// MAGIC Drop table if Exists ADLS.ADLS_Parquet_ADLS_Parquet;
// MAGIC Drop table if Exists DBFS.ADLS_Parquet_DBFS_Parquet;
// MAGIC Drop table if Exists ADLS.DBFS_Parquet_ADLS_TXT;
// MAGIC Drop table if Exists DBFS.DBFS_Parquet_DBFS_TXT;
// MAGIC Drop table if Exists ADLS.DBFS_Parquet_ADLS_Parquet;
// MAGIC Drop table if Exists DBFS.DBFS_Parquet_DBFS_Parquet;

// COMMAND ----------

// MAGIC %sql
// MAGIC drop table DBFS.ADLS_Parquet_DBFS_TXT

// COMMAND ----------

//ADLS_TXT_ADLS_TXT
ADLS_TXT.write.mode("overwrite").format("com.databricks.spark.csv").option("path","dbfs:/mnt/CDPvNext/ADLS/ADLS_TXT_ADLS_TXT").saveAsTable("ADLS.ADLS_TXT_ADLS_TXT")

// COMMAND ----------

//ADLS_TXT_DBFS_TXT
ADLS_TXT.write.mode("overwrite").format("com.databricks.spark.csv").saveAsTable("DBFS.ADLS_TXT_DBFS_TXT")

// COMMAND ----------

//ADLS_TXT_ADLS_Parquet
ADLS_TXT.write.mode("overwrite").option("path","dbfs:/mnt/CDPvNext/ADLS/ADLS_TXT_ADLS_TXT").saveAsTable("ADLS.ADLS_TXT_ADLS_Parquet")

// COMMAND ----------

//ADLS_TXT_DBFS_Parquet
ADLS_TXT.write.mode("overwrite").saveAsTable("DBFS.ADLS_TXT_DBFS_Parquet")

// COMMAND ----------

//ADLS_Parquet_ADLS_TXT
ADLS_Parquet.write.mode("overwrite").format("com.databricks.spark.csv").option("path","dbfs:/mnt/CDPvNext/ADLS/ADLS_Parquet_ADLS_TXT").saveAsTable("ADLS.ADLS_Parquet_ADLS_TXT")

// COMMAND ----------

//ADLS_Parquet_DBFS_TXT
ADLS_Parquet.write.mode("overwrite").format("com.databricks.spark.csv").saveAsTable("DBFS.ADLS_Parquet_DBFS_TXT")

// COMMAND ----------

// MAGIC %sql
// MAGIC clear cache;

// COMMAND ----------

//ADLS_Parquet_ADLS_Parquet
ADLS_Parquet.write.mode("overwrite").option("path","dbfs:/mnt/CDPvNext/ADLS/ADLS_Parquet_ADLS_Parquet").saveAsTable("ADLS.ADLS_Parquet_ADLS_Parquet")

// COMMAND ----------

//ADLS_Parquet_DBFS_Parquet
ADLS_Parquet.write.mode("overwrite").saveAsTable("DBFS.ADLS_Parquet_DBFS_Parquet")

// COMMAND ----------

//DBFS_Parquet_ADLS_TXT
DBFS_Parquet.write.mode("overwrite").format("com.databricks.spark.csv").option("path","dbfs:/mnt/CDPvNext/ADLS/DBFS_Parquet_ADLS_TXT").saveAsTable("ADLS.DBFS_Parquet_ADLS_TXT")

// COMMAND ----------

//DBFS_Parquet_DBFS_TXT
DBFS_Parquet.write.mode("overwrite").format("com.databricks.spark.csv").saveAsTable("DBFS.DBFS_Parquet_DBFS_TXT")

// COMMAND ----------

//DBFS_Parquet_ADLS_Parquet
DBFS_Parquet.write.mode("overwrite").option("path","dbfs:/mnt/CDPvNext/ADLS/DBFS_Parquet_ADLS_Parquet").saveAsTable("ADLS.DBFS_Parquet_ADLS_Parquet")

// COMMAND ----------

//DBFS_Parquet_DBFS_Parquet
DBFS_Parquet.write.mode("overwrite").saveAsTable("DBFS.DBFS_Parquet_DBFS_Parquet")

// COMMAND ----------



// COMMAND ----------

//ADLS_TXT_ADLS_TXT
ADLS_TXT.write.mode("overwrite").format("com.databricks.spark.csv").option("path","dbfs:/mnt/CDPvNext/ADLS/ADLS_TXT_ADLS_TXT").saveAsTable("ADLS.ADLS_TXT_ADLS_TXT")
//ADLS_TXT_DBFS_TXT
ADLS_TXT.write.mode("overwrite").format("com.databricks.spark.csv").saveAsTable("DBFS.ADLS_TXT_DBFS_TXT")
//ADLS_TXT_ADLS_Parquet
ADLS_TXT.write.mode("overwrite").option("path","dbfs:/mnt/CDPvNext/ADLS/ADLS_TXT_ADLS_TXT").saveAsTable("ADLS.ADLS_TXT_ADLS_Parquet")
//ADLS_TXT_DBFS_Parquet
ADLS_TXT.write.mode("overwrite").saveAsTable("DBFS.ADLS_TXT_DBFS_Parquet")
//ADLS_Parquet_ADLS_TXT
ADLS_Parquet.write.mode("overwrite").format("com.databricks.spark.csv").option("path","dbfs:/mnt/CDPvNext/ADLS/ADLS_Parquet_ADLS_TXT").saveAsTable("ADLS.ADLS_Parquet_ADLS_TXT")
//ADLS_Parquet_DBFS_TXT
ADLS_Parquet.write.mode("overwrite").format("com.databricks.spark.csv").saveAsTable("DBFS.ADLS_Parquet_DBFS_TXT")
//ADLS_Parquet_ADLS_Parquet
ADLS_Parquet.write.mode("overwrite").option("path","dbfs:/mnt/CDPvNext/ADLS/ADLS_Parquet_ADLS_Parquet").saveAsTable("ADLS.ADLS_Parquet_ADLS_Parquet")
//ADLS_Parquet_DBFS_Parquet
ADLS_Parquet.write.mode("overwrite").saveAsTable("DBFS.ADLS_Parquet_DBFS_Parquet")
//DBFS_Parquet_ADLS_TXT
DBFS_Parquet.write.mode("overwrite").format("com.databricks.spark.csv").option("path","dbfs:/mnt/CDPvNext/ADLS/DBFS_Parquet_ADLS_TXT").saveAsTable("ADLS.DBFS_Parquet_ADLS_TXT")
//DBFS_Parquet_DBFS_TXT
DBFS_Parquet.write.mode("overwrite").format("com.databricks.spark.csv").saveAsTable("DBFS.DBFS_Parquet_DBFS_TXT")
//DBFS_Parquet_ADLS_Parquet
DBFS_Parquet.write.mode("overwrite").option("path","dbfs:/mnt/CDPvNext/ADLS/DBFS_Parquet_ADLS_Parquet").saveAsTable("ADLS.DBFS_Parquet_ADLS_Parquet")
//DBFS_Parquet_DBFS_Parquet
DBFS_Parquet.write.mode("overwrite").saveAsTable("DBFS.DBFS_Parquet_DBFS_Parquet")

// COMMAND ----------

