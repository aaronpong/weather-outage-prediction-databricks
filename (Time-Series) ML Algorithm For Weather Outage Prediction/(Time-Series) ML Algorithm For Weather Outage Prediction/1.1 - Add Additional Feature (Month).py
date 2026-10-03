# Databricks notebook source
from pyspark.sql.functions import month, col, when

weather_outage_data = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("s3://databricks-bucket-ece606/datasets/processed/Energy-OE/Combined_Summary.csv")

weather_outage_data = weather_outage_data \
    .withColumn('season', 
        when((col('month').isin(12, 1, 2)), 0)  # Winter
        .when((col('month').isin(3, 4, 5)), 1)  # Spring
        .when((col('month').isin(6, 7, 8)), 2)  # Summer
        .when((col('month').isin(9, 10, 11)), 3)  # Fall
    ) \
    .drop('month')

weather_outage_data.display()

# COMMAND ----------

from pyspark.sql.functions import month, col, when
from pyspark.ml.feature import VectorAssembler

# Extract month and drop date_of_restoration column
df_time_features = weather_outage_data.withColumn('month', month('date_event_began')) \
    .drop('date_of_restoration')

df_time_features.display()

# Create seasons based on months
df_time_features = weather_outage_data.withColumn('month', month('date_event_began')) \
    .withColumn('season', 
        when((col('month').isin(12, 1, 2)), 0)  # Winter
        .when((col('month').isin(3, 4, 5)), 1)  # Spring
        .when((col('month').isin(6, 7, 8)), 2)  # Summer
        .when((col('month').isin(9, 10, 11)), 3)  # Fall
    ) \
    .drop('date_of_restoration', 'month')

# Create feature vector with seasons instead of months
feature_columns = ['temp_min', 'temp_max', 'wind_speed', 'precipitation', 'season']
assembler = VectorAssembler(
    inputCols=feature_columns,
    outputCol='features'
)
df_features = assembler.transform(df_time_features)

# COMMAND ----------

output_path = "s3://databricks-bucket-ece606/datasets/processed/Energy-OE/Combined_Summary.csv"
df_time_features.coalesce(1) \
    .write \
    .format("csv") \
    .mode("overwrite") \
    .option("header", "true") \
    .save(output_path)

# COMMAND ----------

