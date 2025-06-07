# Databricks notebook source
# from pyspark.sql import SparkSession, functions as f

# #Reading Hospital  Encounters data 
# encounters_df=spark.read.parquet("/mnt/bronze/hospital/encounters")
# encounters=encounters_df.withColumn("datasource",f.lit("hosp"))
# encounters.createOrReplaceTempView("encounters")
# encounters.display()

# COMMAND ----------

df_hosa=spark.read.format("csv").option("header", "true").load("/mnt/bronze/hosa/encounters.csv")
df_hosa.createOrReplaceTempView("df_hosa")
df_hosa.write.mode("overwrite").option("header","true").parquet("/mnt/bronze/hosa/encounters")

# COMMAND ----------

from pyspark.sql import SparkSession, functions as f

#Reading Hospital  Encounters data 
df_hosa=spark.read.parquet("/mnt/bronze/hosa/encounters")
df_hosa_dsc=df_hosa.withColumn("datasource",f.lit("hos-a"))

df_hosb=spark.read.parquet("/mnt/bronze/hosb/encounters")
df_hosb_dsc=df_hosb.withColumn("datasource",f.lit("hos-b"))

df_merged = df_hosa_dsc.unionByName(df_hosb_dsc)
df_merged.createOrReplaceTempView("encounters")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a cleaned temporary view for quality checks
# MAGIC CREATE OR REPLACE TEMP VIEW quality_checks AS
# MAGIC SELECT 
# MAGIC   CONCAT(EncounterID, '-', datasource) AS EncounterID,
# MAGIC   EncounterID AS SRC_EncounterID,
# MAGIC   PatientID,
# MAGIC   EncounterDate,
# MAGIC   EncounterType,
# MAGIC   ProviderID,
# MAGIC   DepartmentID,
# MAGIC   ProcedureCode,
# MAGIC   InsertedDate AS SRC_InsertedDate,
# MAGIC   ModifiedDate AS SRC_ModifiedDate,
# MAGIC   datasource,
# MAGIC   CASE 
# MAGIC     WHEN EncounterID IS NULL OR PatientID IS NULL THEN TRUE
# MAGIC     ELSE FALSE
# MAGIC   END AS is_quarantined
# MAGIC FROM encounters;
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from quality_checks

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create the silver.encounters table if it doesn't exist
# MAGIC CREATE TABLE IF NOT EXISTS silver.encounters (
# MAGIC   EncounterID         STRING,
# MAGIC   SRC_EncounterID     STRING,
# MAGIC   PatientID           STRING,
# MAGIC   EncounterDate       DATE,
# MAGIC   EncounterType       STRING,
# MAGIC   ProviderID          STRING,
# MAGIC   DepartmentID        STRING,
# MAGIC   ProcedureCode       INTEGER,
# MAGIC   SRC_InsertedDate    DATE,
# MAGIC   SRC_ModifiedDate    DATE,
# MAGIC   datasource          STRING,
# MAGIC   is_quarantined      BOOLEAN,
# MAGIC   audit_insertdate    TIMESTAMP,
# MAGIC   audit_modifieddate  TIMESTAMP,
# MAGIC   is_current          BOOLEAN
# MAGIC )
# MAGIC USING DELTA;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Update old records in silver.encounters (SCD Type 2)
# MAGIC MERGE INTO silver.encounters AS target
# MAGIC USING quality_checks AS source
# MAGIC ON target.EncounterID = source.EncounterID
# MAGIC    AND target.is_current = true
# MAGIC WHEN MATCHED AND (
# MAGIC      target.SRC_EncounterID     != source.SRC_EncounterID OR
# MAGIC      target.PatientID           != source.PatientID OR
# MAGIC      target.EncounterDate       != source.EncounterDate OR
# MAGIC      target.EncounterType       != source.EncounterType OR
# MAGIC      target.ProviderID          != source.ProviderID OR
# MAGIC      target.DepartmentID        != source.DepartmentID OR
# MAGIC      target.ProcedureCode       != source.ProcedureCode OR
# MAGIC      target.SRC_InsertedDate    != source.SRC_InsertedDate OR
# MAGIC      target.SRC_ModifiedDate    != source.SRC_ModifiedDate OR
# MAGIC      target.datasource          != source.datasource OR
# MAGIC      target.is_quarantined      != source.is_quarantined
# MAGIC ) THEN UPDATE SET
# MAGIC      target.is_current = false,
# MAGIC      target.audit_modifieddate = current_timestamp();
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert new records into silver.encounters (SCD Type 2 logic)
# MAGIC MERGE INTO silver.encounters AS target
# MAGIC USING quality_checks AS source
# MAGIC ON target.EncounterID = source.EncounterID
# MAGIC    AND target.is_current = true
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT (
# MAGIC   EncounterID,
# MAGIC   SRC_EncounterID,
# MAGIC   PatientID,
# MAGIC   EncounterDate,
# MAGIC   EncounterType,
# MAGIC   ProviderID,
# MAGIC   DepartmentID,
# MAGIC   ProcedureCode,
# MAGIC   SRC_InsertedDate,
# MAGIC   SRC_ModifiedDate,
# MAGIC   datasource,
# MAGIC   is_quarantined,
# MAGIC   audit_insertdate,
# MAGIC   audit_modifieddate,
# MAGIC   is_current
# MAGIC )
# MAGIC VALUES (
# MAGIC   source.EncounterID,
# MAGIC   source.SRC_EncounterID,
# MAGIC   source.PatientID,
# MAGIC   source.EncounterDate,
# MAGIC   source.EncounterType,
# MAGIC   source.ProviderID,
# MAGIC   source.DepartmentID,
# MAGIC   source.ProcedureCode,
# MAGIC   source.SRC_InsertedDate,
# MAGIC   source.SRC_ModifiedDate,
# MAGIC   source.datasource,
# MAGIC   source.is_quarantined,
# MAGIC   current_timestamp(),
# MAGIC   current_timestamp(),
# MAGIC   true
# MAGIC );
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select SRC_EncounterID,datasource,count(patientid) from  silver.encounters
# MAGIC group by all
# MAGIC order by 3 desc