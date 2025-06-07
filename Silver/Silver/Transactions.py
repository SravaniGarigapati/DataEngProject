# Databricks notebook source
df_hosa=spark.read.format("csv").option("header", "true").load("/mnt/bronze/hosa/transactions.csv")
df_hosa.createOrReplaceTempView("df_hosa")
df_hosa.write.mode("overwrite").option("header","true").parquet("/mnt/bronze/hosa/transactions")

# COMMAND ----------

from pyspark.sql import SparkSession, functions as f

#Reading Hospital A departments data 
df_hosa=spark.read.parquet("/mnt/bronze/hosa/transactions")
df_hosa_dsc=df_hosa.withColumn("datasource",f.lit("hos-a"))

#Reading Hospital B departments data 
df_hosb=spark.read.parquet("/mnt/bronze/hosb/transactions")
df_hosb_dsc=df_hosb.withColumn("datasource",f.lit("hos-b"))

#union two departments dataframes
df_merged = df_hosa_dsc.unionByName(df_hosb_dsc)
display(df_merged)

df_merged.createOrReplaceTempView("transactions")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create or replace temporary view for quality checks on transactions
# MAGIC CREATE OR REPLACE TEMP VIEW quality_checks AS
# MAGIC SELECT 
# MAGIC     CONCAT(TransactionID, '-', datasource) AS TransactionID,
# MAGIC     TransactionID AS SRC_TransactionID,
# MAGIC     EncounterID,
# MAGIC     PatientID,
# MAGIC     ProviderID,
# MAGIC     DeptID,
# MAGIC     VisitDate,
# MAGIC     ServiceDate,
# MAGIC     PaidDate,
# MAGIC     VisitType,
# MAGIC     Amount,
# MAGIC     AmountType,
# MAGIC     PaidAmount,
# MAGIC     ClaimID,
# MAGIC     PayorID,
# MAGIC     ProcedureCode,
# MAGIC     ICDCode,
# MAGIC     LineOfBusiness,
# MAGIC     MedicaidID,
# MAGIC     MedicareID,
# MAGIC     InsertDate AS SRC_InsertDate,
# MAGIC     ModifiedDate AS SRC_ModifiedDate,
# MAGIC     datasource,
# MAGIC     CASE 
# MAGIC         WHEN EncounterID IS NULL 
# MAGIC           OR PatientID IS NULL 
# MAGIC           OR TransactionID IS NULL 
# MAGIC           OR VisitDate IS NULL 
# MAGIC         THEN TRUE
# MAGIC         ELSE FALSE
# MAGIC     END AS is_quarantined
# MAGIC FROM transactions;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create the silver.transactions Delta table if it doesn't exist
# MAGIC CREATE TABLE IF NOT EXISTS silver.transactions (
# MAGIC   TransactionID STRING,
# MAGIC   SRC_TransactionID STRING,
# MAGIC   EncounterID STRING,
# MAGIC   PatientID STRING,
# MAGIC   ProviderID STRING,
# MAGIC   DeptID STRING,
# MAGIC   VisitDate DATE,
# MAGIC   ServiceDate DATE,
# MAGIC   PaidDate DATE,
# MAGIC   VisitType STRING,
# MAGIC   Amount DOUBLE,
# MAGIC   AmountType STRING,
# MAGIC   PaidAmount DOUBLE,
# MAGIC   ClaimID STRING,
# MAGIC   PayorID STRING,
# MAGIC   ProcedureCode INTEGER,
# MAGIC   ICDCode STRING,
# MAGIC   LineOfBusiness STRING,
# MAGIC   MedicaidID STRING,
# MAGIC   MedicareID STRING,
# MAGIC   SRC_InsertDate DATE,
# MAGIC   SRC_ModifiedDate DATE,
# MAGIC   datasource STRING,
# MAGIC   is_quarantined BOOLEAN,
# MAGIC   audit_insertdate TIMESTAMP,
# MAGIC   audit_modifieddate TIMESTAMP,
# MAGIC   is_current BOOLEAN
# MAGIC )
# MAGIC USING DELTA;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Update old records to implement SCD Type 2
# MAGIC MERGE INTO silver.transactions AS target
# MAGIC USING quality_checks AS source
# MAGIC ON target.TransactionID = source.TransactionID AND target.is_current = true
# MAGIC
# MAGIC WHEN MATCHED AND (
# MAGIC   target.SRC_TransactionID != source.SRC_TransactionID OR
# MAGIC   target.EncounterID != source.EncounterID OR
# MAGIC   target.PatientID != source.PatientID OR
# MAGIC   target.ProviderID != source.ProviderID OR
# MAGIC   target.DeptID != source.DeptID OR
# MAGIC   target.VisitDate != source.VisitDate OR
# MAGIC   target.ServiceDate != source.ServiceDate OR
# MAGIC   target.PaidDate != source.PaidDate OR
# MAGIC   target.VisitType != source.VisitType OR
# MAGIC   target.Amount != source.Amount OR
# MAGIC   target.AmountType != source.AmountType OR
# MAGIC   target.PaidAmount != source.PaidAmount OR
# MAGIC   target.ClaimID != source.ClaimID OR
# MAGIC   target.PayorID != source.PayorID OR
# MAGIC   target.ProcedureCode != source.ProcedureCode OR
# MAGIC   target.ICDCode != source.ICDCode OR
# MAGIC   target.LineOfBusiness != source.LineOfBusiness OR
# MAGIC   target.MedicaidID != source.MedicaidID OR
# MAGIC   target.MedicareID != source.MedicareID OR
# MAGIC   target.SRC_InsertDate != source.SRC_InsertDate OR
# MAGIC   target.SRC_ModifiedDate != source.SRC_ModifiedDate OR
# MAGIC   target.datasource != source.datasource OR
# MAGIC   target.is_quarantined != source.is_quarantined
# MAGIC )
# MAGIC THEN
# MAGIC UPDATE SET
# MAGIC   target.is_current = false,
# MAGIC   target.audit_modifieddate = current_timestamp();
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert new record to implement SCD Type 2
# MAGIC MERGE INTO silver.transactions AS target
# MAGIC USING quality_checks AS source
# MAGIC ON target.TransactionID = source.TransactionID AND target.is_current = true
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT (
# MAGIC   TransactionID,
# MAGIC   SRC_TransactionID,
# MAGIC   EncounterID,
# MAGIC   PatientID,
# MAGIC   ProviderID,
# MAGIC   DeptID,
# MAGIC   VisitDate,
# MAGIC   ServiceDate,
# MAGIC   PaidDate,
# MAGIC   VisitType,
# MAGIC   Amount,
# MAGIC   AmountType,
# MAGIC   PaidAmount,
# MAGIC   ClaimID,
# MAGIC   PayorID,
# MAGIC   ProcedureCode,
# MAGIC   ICDCode,
# MAGIC   LineOfBusiness,
# MAGIC   MedicaidID,
# MAGIC   MedicareID,
# MAGIC   SRC_InsertDate,
# MAGIC   SRC_ModifiedDate,
# MAGIC   datasource,
# MAGIC   is_quarantined,
# MAGIC   audit_insertdate,
# MAGIC   audit_modifieddate,
# MAGIC   is_current
# MAGIC )
# MAGIC VALUES (
# MAGIC   source.TransactionID,
# MAGIC   source.SRC_TransactionID,
# MAGIC   source.EncounterID,
# MAGIC   source.PatientID,
# MAGIC   source.ProviderID,
# MAGIC   source.DeptID,
# MAGIC   source.VisitDate,
# MAGIC   source.ServiceDate,
# MAGIC   source.PaidDate,
# MAGIC   source.VisitType,
# MAGIC   source.Amount,
# MAGIC   source.AmountType,
# MAGIC   source.PaidAmount,
# MAGIC   source.ClaimID,
# MAGIC   source.PayorID,
# MAGIC   source.ProcedureCode,
# MAGIC   source.ICDCode,
# MAGIC   source.LineOfBusiness,
# MAGIC   source.MedicaidID,
# MAGIC   source.MedicareID,
# MAGIC   source.SRC_InsertDate,
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
# MAGIC select * from silver.transactions