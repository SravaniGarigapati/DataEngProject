# Databricks notebook source
df_hosa=spark.read.format("csv").option("header", "true").load("/mnt/bronze/hosb/patients.csv")
df_hosa.createOrReplaceTempView("df_hosa")
df_hosa.write.mode("overwrite").option("header","true").parquet("/mnt/bronze/hosb/patients")


# COMMAND ----------

from pyspark.sql import SparkSession, functions as f
df_hosa=spark.read.parquet("/mnt/bronze/hosa/patients")

df_hosa_dsc=df_hosa.withColumn("datasource",f.lit("hos-a"))
df_hosa_dsc.createOrReplaceTempView("patients_hosa")

#Reading Hospital B patient data 
df_hosb=spark.read.parquet("/mnt/bronze/hosb/patients")

df_hosb_dsc=df_hosb.withColumn("datasource",f.lit("hos-b"))
df_hosa_dsc.createOrReplaceTempView("patients_hosb")

# df_merged = df_hosa_dsc.unionByName(df_hosb_dsc)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW cdm_patients AS
# MAGIC SELECT 
# MAGIC   CONCAT(SRC_PatientID, '-', datasource) AS Patient_Key, 
# MAGIC   *
# MAGIC FROM (
# MAGIC   SELECT 
# MAGIC     PatientID AS SRC_PatientID,
# MAGIC     FirstName,
# MAGIC     LastName,
# MAGIC     MiddleName,
# MAGIC     SSN,
# MAGIC     PhoneNumber,
# MAGIC     Gender,
# MAGIC     DOB,
# MAGIC     Address,
# MAGIC     ModifiedDate,
# MAGIC     datasource
# MAGIC   FROM patients_hosa
# MAGIC   
# MAGIC   UNION ALL
# MAGIC   
# MAGIC   SELECT 
# MAGIC     PatientID AS SRC_PatientID,
# MAGIC     FirstName,
# MAGIC     LastName,
# MAGIC     MiddleName,
# MAGIC     SSN,
# MAGIC     PhoneNumber,
# MAGIC     Gender,
# MAGIC     DOB,
# MAGIC     Address,
# MAGIC     ModifiedDate,
# MAGIC     datasource
# MAGIC   FROM patients_hosb
# MAGIC );
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cdm_patients

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW quality_checks AS
# MAGIC SELECT 
# MAGIC     Patient_Key,
# MAGIC     SRC_PatientID,
# MAGIC     FirstName,
# MAGIC     LastName,
# MAGIC     MiddleName,
# MAGIC     SSN,
# MAGIC     PhoneNumber,
# MAGIC     Gender,
# MAGIC     DOB,
# MAGIC     Address,
# MAGIC     ModifiedDate AS SRC_ModifiedDate,
# MAGIC     datasource,
# MAGIC     CASE 
# MAGIC         WHEN SRC_PatientID IS NULL 
# MAGIC           OR DOB IS NULL 
# MAGIC           OR FirstName IS NULL 
# MAGIC           OR LOWER(FirstName) = 'null' THEN TRUE
# MAGIC         ELSE FALSE
# MAGIC     END AS is_quarantined
# MAGIC FROM cdm_patients;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.patients (
# MAGIC     Patient_Key STRING,
# MAGIC     SRC_PatientID STRING,
# MAGIC     FirstName STRING,
# MAGIC     LastName STRING,
# MAGIC     MiddleName STRING,
# MAGIC     SSN STRING,
# MAGIC     PhoneNumber STRING,
# MAGIC     Gender STRING,
# MAGIC     DOB DATE,
# MAGIC     Address STRING,
# MAGIC     SRC_ModifiedDate TIMESTAMP,
# MAGIC     datasource STRING,
# MAGIC     is_quarantined BOOLEAN,
# MAGIC     inserted_date TIMESTAMP,
# MAGIC     modified_date TIMESTAMP,
# MAGIC     is_current BOOLEAN
# MAGIC )
# MAGIC USING DELTA;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver.patients AS target
# MAGIC USING quality_checks AS source
# MAGIC ON target.Patient_Key = source.Patient_Key
# MAGIC AND target.is_current = true
# MAGIC
# MAGIC WHEN MATCHED AND (
# MAGIC     target.SRC_PatientID != source.SRC_PatientID OR
# MAGIC     target.FirstName != source.FirstName OR
# MAGIC     target.LastName != source.LastName OR
# MAGIC     target.MiddleName != source.MiddleName OR
# MAGIC     target.SSN != source.SSN OR
# MAGIC     target.PhoneNumber != source.PhoneNumber OR
# MAGIC     target.Gender != source.Gender OR
# MAGIC     target.DOB != source.DOB OR
# MAGIC     target.Address != source.Address OR
# MAGIC     target.SRC_ModifiedDate != source.SRC_ModifiedDate OR
# MAGIC     target.datasource != source.datasource OR
# MAGIC     target.is_quarantined != source.is_quarantined
# MAGIC )
# MAGIC THEN UPDATE SET
# MAGIC     target.is_current = false,
# MAGIC     target.modified_date = current_timestamp();
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver.patients AS target
# MAGIC USING quality_checks AS source
# MAGIC ON target.Patient_Key = source.Patient_Key
# MAGIC AND target.is_current = true
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT (
# MAGIC     Patient_Key,
# MAGIC     SRC_PatientID,
# MAGIC     FirstName,
# MAGIC     LastName,
# MAGIC     MiddleName,
# MAGIC     SSN,
# MAGIC     PhoneNumber,
# MAGIC     Gender,
# MAGIC     DOB,
# MAGIC     Address,
# MAGIC     SRC_ModifiedDate,
# MAGIC     datasource,
# MAGIC     is_quarantined,
# MAGIC     inserted_date,
# MAGIC     modified_date,
# MAGIC     is_current
# MAGIC )
# MAGIC VALUES (
# MAGIC     source.Patient_Key,
# MAGIC     source.SRC_PatientID,
# MAGIC     source.FirstName,
# MAGIC     source.LastName,
# MAGIC     source.MiddleName,
# MAGIC     source.SSN,
# MAGIC     source.PhoneNumber,
# MAGIC     source.Gender,
# MAGIC     source.DOB,
# MAGIC     source.Address,
# MAGIC     source.SRC_ModifiedDate,
# MAGIC     source.datasource,
# MAGIC     source.is_quarantined,
# MAGIC     current_timestamp(),
# MAGIC     current_timestamp(),
# MAGIC     true
# MAGIC );
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*),Patient_Key from silver.patients
# MAGIC group by patient_key
# MAGIC order by 1 desc