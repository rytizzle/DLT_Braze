# Databricks notebook source
# MAGIC %md
# MAGIC ### This DLT Pipeline notebook utilizes the [APPLY CHANGES INTO SNAPSHOT](https://docs.google.com/document/d/e/2PACX-1vQ6349MI2k32LTQ_cq5o28WX4U0SOnfE0NGwwkl1yv01Z-krUogZAPb9y2x9QgAEWRultqOYxaSf1td/pub) feature to compare a most recent snapshot of a table to a previous version. The APPLY CHANGES INTO SNAPSHOT target is a streaming table and enables SCD2. 
# MAGIC
# MAGIC The key is the column or combination of columns that uniquely identify a row in the snapshot data. This is used to identify which row has changed in the new snapshot.
# MAGIC
# MAGIC
# MAGIC New rows are treated as inserts, and changed rows to existing keys are treated as updates. From here we are able to structure these inserts and updates, into a payload for Braze. For inserts, selected columns are constructed into a payload. For updates, only the columns that updated are added to the payload.
# MAGIC
# MAGIC
# MAGIC
# MAGIC #NOTE
# MAGIC - If you are running this pipeline for the first time, RUN VALIDATE FIRST to generate & render the DAG. Then select tables for refresh the snapshot_target_table to hydrate the initial state of streaming stable. Subsequent runs of the pipeline will compared to the current snapshot to the previous snapshot and identify incremental changes.

# COMMAND ----------

import dlt
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from pyspark.sql.functions import when
from pyspark.sql.functions import udf, col, create_map, expr, array, array_contains, size, aggregate, map_concat, when, array_remove, to_json, lit, current_timestamp, count
from pyspark.sql.types import MapType, StringType

from datetime import datetime, time


# COMMAND ----------

# DBTITLE 1,Creates target streaming table that tracks scd2 from the snapshots
dlt.create_streaming_table("snapshot_target_table")

# COMMAND ----------

# DBTITLE 1,Define the base table that is being create or replaced
source_table = "ryant_catalog.braze_snapshot.subscription"

# COMMAND ----------

# DBTITLE 1,Sets the snapshot version and apply changes logic for the target streaming table
initial_version = int(datetime.now().timestamp())

def next_snapshot_and_version(version):
	if version is None or version < initial_version:
		return (spark.read.table(source_table), initial_version)
	else:
		return None

dlt.apply_changes_from_snapshot(
  target = "snapshot_target_table",
  snapshot_and_version = next_snapshot_and_version,
  keys = ["external_id"],
  stored_as_scd_type = 2,
  track_history_except_column_list = ["external_id"]
)


# COMMAND ----------

# DBTITLE 1,Filters by changed or added entries to the base table
@dlt.table(
  comment = "MV that has entries with the latest version timestamp"
)

def most_recent_version():
  return (
    dlt.read('snapshot_target_table')
    .filter((col('__START_AT') == initial_version) | (col('__END_AT') == initial_version))
    .dropDuplicates()
    )
  


# COMMAND ----------

# DBTITLE 1,Find newly inserted rows from the filtered base table
#Grouped by external_id as that is the unique identifier for the source table. New rows should only have 1 entry per unique identifier.
@dlt.table(
  comment = "split by inserts"
)

def inserts_tbl():
  df = dlt.read('most_recent_version')
  df = df.groupBy('external_id').agg(count("*").alias('count')).filter(col('count') == 1).join(df, 'external_id')
  
  return df

# COMMAND ----------

# DBTITLE 1,Apply logic to wrap the columns into Braze payload
@dlt.table(
  comment = "inserts payload table"
)

def inserts_payload():

  map_col = create_map(
  lit('subscribe_status'), col('subscribe_status'), 
  lit('subscribe_dt'), col('subscribe_dt'),
  lit('end_sub_dt'), col('end_sub_dt')
  )
  inserts_df = dlt.read('inserts_tbl') \
    .withColumn('payload', to_json(map_col)) \
    .withColumn('updated_at', current_timestamp()) \
    .select(col('external_id'), col('payload'), col('updated_at'))

  return inserts_df

# COMMAND ----------

# DBTITLE 1,Find updated rows in the filtered base table
#Updated rows will have 2 entries per unique identifier 
@dlt.table(
  comment = "split by updates"
)

def updates_tbl():
  df = dlt.read('most_recent_version')
  df = df.groupBy('external_id').agg(count("*").alias('count')).filter(col('count') > 1).join(df, 'external_id')
  
  return df

# COMMAND ----------

# DBTITLE 1,Apply logic to wrap updated fields into a payload for Braze
#captures changed columns and wraps into a json payload
@dlt.table(
  comment = "update payload table"
)

def update_payload():
  window = Window.partitionBy('external_id').orderBy("__END_AT")

  df = dlt.read('updates_tbl')
  df2 = df.withColumn('row_num', row_number().over(window))
  df3 = df2.withColumn('row_info', when(col('row_num') == 2, 'previous').when(col('row_num') == 1, 'current'))

  dfX = df3.filter(col('row_info') == 'previous').alias('x')
  dfY = df3.filter(col('row_info') == 'current').alias('y')

  # Create select condition conditions: when pre update is not equal to update, return the value 
  conditions_ = [
    when(col("x." + c) != col("y." + c), lit(c)).otherwise("").alias("condition_" + c)
    for c in dfX.columns if c not in ['external_id', 'row_info', '__START_AT', '__END_AT', 'row_num']
  ]

  select_expr =[
                  col("external_id"), 
                  *[col("y." + c).alias("y_" + c) for c in dfY.columns if c != 'external_id'],
                  array_remove(array(*conditions_), "").alias("changed_columns")
  ]

  # Self join preimage vs. post image
  changed_df = dfX.join(dfY, ["external_id"]).select(*select_expr)

  # Rename columns 
  columns_to_rename = [col_name for col_name in changed_df.columns if "y_" in col_name]

  for col_name in columns_to_rename:
      new_col_name = col_name.replace("y_", "")
      changed_df = changed_df.withColumnRenamed(col_name, new_col_name)

  #Create mapping to columns that have changed and format for Braze
  new_df = changed_df. \
    withColumn('changed_arr', 
                array(*[create_map([lit(c), col(c)]) for c in changed_df.drop('external_id', 'changed_columns').columns])
                ). \
    withColumn('changed_arr2', 
                expr('filter(changed_arr, x -> array_contains(changed_columns, map_keys(x)[0]))')
                ). \
    withColumn('new_col',
                aggregate(expr('slice(changed_arr2, 2, size(changed_arr2))'),
                              col('changed_arr2')[0],
                              lambda x, y: map_concat(x, y)
                              )
                  ) \
    .withColumn('payload', to_json(col('new_col'))) \
    .withColumn('updated_at', current_timestamp())
    
  return new_df.select(col('external_id'), col('payload'), col('updated_at'))


# COMMAND ----------

# DBTITLE 1,Union the inserts and update payload to be appended to a delta table in a notebook task
#union the 2 tables

@dlt.table(
  comment = 'Appended to another another table'
)

def stg_payload():
  df1 = dlt.read('inserts_payload')
  df2 = dlt.read('update_payload')
  return df1.union(df2)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC Appendix

# COMMAND ----------

# @dlt.table
# def append_only():
#   df = dlt.readStream('stg_payload')
#   return df
#   # return spark.readStream.format("delta").load('stg_payload')

# COMMAND ----------

# # #union the 2 tables
# @dlt.table(
#   comment = 'test tehe'
# )
# def sync_to_braze():
#   # stg_df = spark.read.table('ryant_catalog.braze_snapshot.stg_payload')
#   stg_df = dlt.read('stg_payload')
#   stg_df.write.format("delta").mode("append").saveAsTable('ryant_catalog.braze_snapshot.sync_to_braze')

# COMMAND ----------



# COMMAND ----------


