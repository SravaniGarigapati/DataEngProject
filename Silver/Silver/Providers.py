# Databricks notebook source
df_hosa=spark.read.format("csv").option("header", "true").load("/mnt/bronze/hosa/providers.csv")
df_hosa.createOrReplaceTempView("df_hosa")
df_hosa.write.mode("overwrite").option("header","true").parquet("/mnt/bronze/hosa/providers")

# COMMAND ----------

from pyspark.sql import SparkSession, functions as f
df_hosa=spark.read.parquet("/mnt/bronze/hosa/providers")
df_hosa_dsc=df_hosa.withColumn("datasource",f.lit("hos-a"))

#Reading Hospital B departments data 
df_hosb=spark.read.parquet("/mnt/bronze/hosb/providers")
df_hosb_dsc=df_hosb.withColumn("datasource",f.lit("hos-b"))

#union two departments dataframes
df_merged = df_hosa_dsc.unionByName(df_hosb_dsc)
display(df_merged)

df_merged.createOrReplaceTempView("providers")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.providers (
# MAGIC     ProviderID STRING,
# MAGIC     FirstName STRING,
# MAGIC     LastName STRING,
# MAGIC     Specialization STRING,
# MAGIC     DeptID STRING,
# MAGIC     NPI LONG,
# MAGIC     datasource STRING,
# MAGIC     is_quarantined BOOLEAN
# MAGIC )
# MAGIC USING DELTA;
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC truncate table silver.providers

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO silver.providers
# MAGIC SELECT DISTINCT
# MAGIC     ProviderID,
# MAGIC     FirstName,
# MAGIC     LastName,
# MAGIC     Specialization,
# MAGIC     DeptID,
# MAGIC     CAST(NPI AS INT) AS NPI,
# MAGIC     datasource,
# MAGIC     CASE 
# MAGIC         WHEN ProviderID IS NULL OR DeptID IS NULL THEN TRUE
# MAGIC         ELSE FALSE
# MAGIC     END AS is_quarantined
# MAGIC FROM providers;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.providers