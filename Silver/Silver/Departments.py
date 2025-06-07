# Databricks notebook source
df_hosa=spark.read.format("csv").option("header", "true").load("/mnt/bronze/hosb/departments.csv")
df_hosa.createOrReplaceTempView("df_hosa")
df_hosa.write.mode("overwrite").option("header","true").parquet("/mnt/bronze/hosb/departments")


# COMMAND ----------

from pyspark.sql import SparkSession, functions as f

#Reading Hospital A departments data 
df_hosa=spark.read.parquet("/mnt/bronze/hosa/departments")
df_hosa_dsc=df_hosa.withColumn("datasource",f.lit("hos-a"))

#Reading Hospital B departments data 
df_hosb=spark.read.parquet("/mnt/bronze/hosb/departments")
df_hosb_dsc=df_hosb.withColumn("datasource",f.lit("hos-b"))

#union two departments dataframes
df_merged = df_hosa_dsc.unionByName(df_hosb_dsc)

# Create the dept_id column and rename deptid to src_dept_id
df_merged = df_merged.withColumn("SRC_Dept_id", f.col("deptid")) \
                     .withColumn("Dept_id", f.concat(f.col("DeptID"),f.lit('-'), f.col("datasource"))) \
                     .drop("DeptID")

df_merged.createOrReplaceTempView("departments")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS databricksdev.silver
# MAGIC COMMENT 'This schema stores cleaned data';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create the Silver table only if it doesn't exist
# MAGIC CREATE TABLE IF NOT EXISTS silver.departments (
# MAGIC   Dept_Id STRING,
# MAGIC   SRC_Dept_Id STRING,
# MAGIC   Name STRING,
# MAGIC   datasource STRING,
# MAGIC   is_quarantined BOOLEAN
# MAGIC )
# MAGIC USING DELTA;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Clear previous data
# MAGIC TRUNCATE TABLE silver.departments;
# MAGIC

# COMMAND ----------

df_merged.createOrReplaceTempView("departments")

# COMMAND ----------

df_merged.show()

# COMMAND ----------

from pyspark.sql.functions import when, col

# Transform data and add is_quarantined column
df_transformed = df_merged.select(
    col("Dept_Id"),
    col("SRC_Dept_Id"),
    col("Name"),
    col("datasource"),
    when(
        col("SRC_Dept_Id").isNull() | col("Name").isNull(), True
    ).otherwise(False).alias("is_quarantined")
)

# Write to silver.departments (append mode to mimic INSERT INTO)
df_transformed.write.format("delta").mode("append").saveAsTable("silver.departments")


# COMMAND ----------

# MAGIC %sql
# MAGIC -- View the results
# MAGIC SELECT * FROM silver.departments;
# MAGIC