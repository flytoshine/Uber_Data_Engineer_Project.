# Databricks notebook source
import pandas as pd
df = pd.read_json("https://dluberprojectsdevs.blob.core.windows.net/raw/ingestion/map_cities?sp=r&st=2026-08-09T17:24:09Z&se=2026-08-10T01:39:09Z&spr=https&sv=2026-02-06&sr=c&sig=xQbwDpJ7r6Ev8aMLbFcZuWNfyWk7idPSd87ajSQuhWg%3D")

df_spark = spark.createDataFrame(df)
display(df_spark)

# COMMAND ----------

import pandas as pd

files= [
{"file":"map_cities"},
{"file":"map_cancellation_reasons"},
{"file":"map_payment_methods"},
{"file":"map_ride_statuses"},
{"file":"map_vehicle_makes"},
{"file":"map_vehicle_types"}
]

for file in files:


    url = f"https://dluberprojectsdevs.blob.core.windows.net/raw/ingestion/{file['file']}?sp=r&st=2026-08-11T08:49:43Z&se=2026-08-11T17:04:43Z&spr=https&sv=2026-02-06&sr=c&sig=s7yeTEh9P3stc3TsOKLo8zLj0Q1Pe7nYsLffNCZhwKg%3D"


    df = pd.read_json(url)
    df_spark = spark.createDataFrame(df)
    
    df_spark.write.format("delta")\
            .mode("overwrite")\
            .option("overwriteSchema", "true")\
            .saveAsTable(f"uber.bronze.{file['file']}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM uber.bronze.map_cities