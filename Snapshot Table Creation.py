# Databricks notebook source
# MAGIC %md
# MAGIC Run the first table to set the initial snapshot, and then execute the DLT pipeline.
# MAGIC
# MAGIC Run the 2nd table to overwrite the initial table.

# COMMAND ----------

# DBTITLE 1,Creates the base snapshot table
subs = [("ryan", 'active', '2023-01-01', 'NA'), ("kam", 'active', '2023-01-01', 'NA'), ("sara", 'active', '2023-01-01', 'NA'), ("jake", 'inactive', '2023-01-01', '2023-12-12'), ("joe", 'inactive', '2023-01-01', '2023-12-12')]
columns = ["external_id","subscribe_status","subscribe_dt","end_sub_dt"]
spark.createDataFrame(data=subs, schema = columns).write.format("delta").mode("overwrite").saveAsTable("ryant_catalog.braze_snapshot.subscription")

# COMMAND ----------

# DBTITLE 1,Secondary snapshot
#note ryan is updated from active to inactive, and newly inserted rows of joe + francis
subs = [("ryan", 'inactive', '2023-01-01', '2024-03-12'), ("kam", 'active', '2023-01-01', 'NA'), ("sara", 'active', '2023-01-01', 'NA'), ("jake", 'inactive', '2023-01-01', '2023-12-12'), ("joe", 'inactive', '2023-01-01', '2023-12-12'), ("francis", 'inactive', '2023-01-01', '2023-12-12')]
columns = ["external_id","subscribe_status","subscribe_dt","end_sub_dt"]
spark.createDataFrame(data=subs, schema = columns).write.format("delta").mode("overwrite").saveAsTable("ryant_catalog.braze_snapshot.subscription")
