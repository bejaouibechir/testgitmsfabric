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

from pyspark.sql.functions import col, lit, round, when

readings_df = spark.table("bronze_equipment_readings")
reference_df = spark.table("ref_equipment").select(
    "EquipmentId",
    "EquipmentName",
    "Region",
    "NominalPowerKW",
    "CriticalTemperatureC",
    "CriticalVibrationMmS",
)

health_df = (
    readings_df.alias("r")
    .join(reference_df.alias("e"), on="EquipmentId", how="left")
    .withColumn("LoadPct", round((col("PowerKW") / col("NominalPowerKW")) * 100, 2))
    .withColumn(
        "TemperatureStatus",
        when(col("TemperatureC") >= col("CriticalTemperatureC"), lit("CRITICAL"))
        .when(col("TemperatureC") >= col("CriticalTemperatureC") - 10, lit("WARNING"))
        .otherwise(lit("NORMAL")),
    )
    .withColumn(
        "VibrationStatus",
        when(col("VibrationMmS") >= col("CriticalVibrationMmS"), lit("CRITICAL"))
        .when(col("VibrationMmS") >= col("CriticalVibrationMmS") * 0.75, lit("WARNING"))
        .otherwise(lit("NORMAL")),
    )
    .withColumn(
        "HealthStatus",
        when((col("TemperatureStatus") == "CRITICAL") | (col("VibrationStatus") == "CRITICAL"), lit("CRITICAL"))
        .when((col("TemperatureStatus") == "WARNING") | (col("VibrationStatus") == "WARNING"), lit("WARNING"))
        .otherwise(lit("NORMAL")),
    )
)

health_df.write.mode("overwrite").format("delta").saveAsTable("gold_equipment_health")

display(health_df.select(
    "EquipmentId",
    "EquipmentName",
    "Site",
    "EnergyType",
    "PowerKW",
    "LoadPct",
    "TemperatureC",
    "VibrationMmS",
    "HealthStatus"
))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Feature: OVERLOAD alert when LoadPct > 95%
from pyspark.sql.functions import col, when, lit

health_df = health_df.withColumn(
    "HealthStatus",
    when(col("LoadPct") > 95, lit("OVERLOAD")).otherwise(col("HealthStatus"))
)

health_df.write.mode("overwrite").format("delta").saveAsTable("gold_equipment_health")

display(
    health_df.select("EquipmentId", "EquipmentName", "LoadPct", "HealthStatus")
             .orderBy("LoadPct", ascending=False)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
