# Databricks notebook source
storage_account_name = "pythonoqmm"
storage_account_access_key = "stgAb+W2UIHuXV3elbtAKRrubLsweoYvo4DZ6Mxc3h6/YtT+fdlPtPpfw0C8vKbyQc41FogKek+POaAuo2iCYw=="

# COMMAND ----------

file_location = "wasbs://pythonmm@pythonoqmm.blob.core.windows.net/OutletTv.csv"
detfile_location = "wasbs://pythonmm@pythonoqmm.blob.core.windows.net/tvdetails.csv"

file_type = "csv"

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)

# COMMAND ----------

df = spark.read.format(file_type).option("inferSchema", "true").option("header","true").load(file_location)
#Adding Second Source

# COMMAND ----------

detdf = spark.read.format(file_type).option("inferSchema", "true").option("header","true").load(detfile_location)

# COMMAND ----------

display(df.select("title"))

# COMMAND ----------

# MAGIC %md
# MAGIC Creating Temp View
# MAGIC Since this table is registered as a temp view, it will be available only to this notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.

# COMMAND ----------

df.createOrReplaceTempView("TVView")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Filtering All TVs, without humax

# COMMAND ----------

df = df.filter('Title NOT LIKE "HUMAX%"')

# COMMAND ----------

from pyspark.sql import functions as F

df = df.withColumn('Company', F.split(df.Title, ' ')[0])
df = df.withColumn('INCH',F.regexp_extract(df.Title,'(\\d\\d)',1))
df = df.withColumn('ProductID',F.regexp_extract(df.link,'[I][D]\\d*',0))
#DiscountNumber
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Joining Two Dataframes

# COMMAND ----------

#df.write.mode("OVERWRITE").saveAsTable("TVS")
dfDetailed = df.join(detdf, on=['ProductID'], how='inner')
display(dfDetailed)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from TVView

# COMMAND ----------


