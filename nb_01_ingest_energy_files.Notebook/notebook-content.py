# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "c7dafc67-7342-46d3-8fe6-8e2691aa7f7e",
# META       "default_lakehouse_name": "lh_energy_ops",
# META       "default_lakehouse_workspace_id": "247d8f13-46ae-4a63-b09f-1f3a77454f58",
# META       "known_lakehouses": [
# META         {
# META           "id": "c7dafc67-7342-46d3-8fe6-8e2691aa7f7e"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.functions import col, to_timestamp

readings_path = "Files/input/energy/equipment_readings.csv"
reference_path = "Files/input/energy/equipment_reference.csv"

readings_df = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv(readings_path)
    .withColumn("ReadingTime", to_timestamp(col("ReadingTime")))
)

reference_df = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv(reference_path)
)

readings_df.write.mode("overwrite").format("delta").saveAsTable("bronze_equipment_readings")
reference_df.write.mode("overwrite").format("delta").saveAsTable("ref_equipment")

display(readings_df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
