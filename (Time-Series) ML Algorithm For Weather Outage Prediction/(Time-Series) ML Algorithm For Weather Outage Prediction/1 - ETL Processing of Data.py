# Databricks notebook source
dbutils.fs.ls("s3a://databricks-bucket-ece606/datasets/raw/")

# COMMAND ----------

dbutils.fs.ls("s3a://databricks-bucket-ece606/datasets/raw/Energy-EIA")

# COMMAND ----------

dbutils.fs.ls("s3a://databricks-bucket-ece606/datasets/raw/Energy-OE")

# COMMAND ----------

# DBTITLE 1,Clean 2013 csv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, regexp_extract

# Initialize Spark session
spark = SparkSession.builder.appName("Clean 2013 Energy Outages").getOrCreate()

# Define paths
input_path = "s3a://databricks-bucket-ece606/datasets/raw/Energy-OE/2013_Annual_Summary.csv"
output_path = "s3a://databricks-bucket-ece606/datasets/cleaned/Energy-OE/2013_Annual_Summary.csv"

# Read the CSV without assuming first row as header
raw_df = spark.read.csv(input_path)

# Get rid of first row with unnamed columns and last citation row
df = raw_df.filter(~col("_c0").contains("OE-417") & ~col("_c0").contains("Source"))

df = df.withColumn("_c0", 
    when(regexp_extract("_c0", "(\\d{4}-\\d{2}-\\d{2})", 1) != "", 
         regexp_extract("_c0", "(\\d{4}-\\d{2}-\\d{2})", 1))
    .when(regexp_extract("_c0", "(\\d{2}/\\d{2}/\\d{4})", 1) != "",
          regexp_extract("_c0", "(\\d{2}/\\d{2}/\\d{4})", 1))
    .otherwise(col("_c0"))
)

# Rename columns manually
df = df.select(
    col("_c0").alias("date_event_began"),
    col("_c2").alias("date_of_restoration"),
    col("_c4").alias("area_affected"),
    col("_c5").alias("nerc_region"),
    col("_c6").alias("event_type"),
    col("_c7").alias("demand_loss_mw"),
    col("_c8").alias("customers_affected")
)

# Filter out the month headers and any remaining header rows
df_cleaned = df

# Clean up the Unknown values
df_cleaned = df_cleaned.select(
    col("date_event_began"),
    col("date_of_restoration"),
    col("area_affected"),
    col("nerc_region"),
    col("event_type"),
    when(col("demand_loss_mw") == "Unknown", None)
    .when(col("demand_loss_mw") == "", None)
    .otherwise(col("demand_loss_mw")).alias("demand_loss_mw"),
    when(col("customers_affected") == "Unknown", None)
    .when(col("customers_affected") == "", None)
    .otherwise(col("customers_affected")).alias("customers_affected")
)

# Write the cleaned data back to S3
df_cleaned.write.mode("overwrite").option("header", "true").csv(output_path)

print("2013 data cleaned and saved successfully!")

print("\nSample of cleaned data:")
df_cleaned.show(5)

print("\nCleaned data schema:")
df_cleaned.printSchema()

# COMMAND ----------

df_cleaned.display()

# COMMAND ----------

# DBTITLE 1,Clean 2014 csv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, regexp_extract

# Initialize Spark session
spark = SparkSession.builder.appName("Clean 2013 Energy Outages").getOrCreate()

# Define paths
input_path = "s3a://databricks-bucket-ece606/datasets/raw/Energy-OE/2014_Annual_Summary.csv"
output_path = "s3a://databricks-bucket-ece606/datasets/cleaned/Energy-OE/2014_Annual_Summary.csv"

# Read the CSV without assuming first row as header
raw_df = spark.read.csv(input_path)

# Get rid of first row with unnamed columns and last citation row
df = raw_df.filter(~col("_c0").contains("OE-417") & ~col("_c0").contains("Source"))

df = df.withColumn("_c0", 
    when(regexp_extract("_c0", "(\\d{4}-\\d{2}-\\d{2})", 1) != "", 
         regexp_extract("_c0", "(\\d{4}-\\d{2}-\\d{2})", 1))
    .when(regexp_extract("_c0", "(\\d{2}/\\d{2}/\\d{4})", 1) != "",
          regexp_extract("_c0", "(\\d{2}/\\d{2}/\\d{4})", 1))
    .otherwise(col("_c0"))
)

# Rename columns manually
df = df.select(
    col("_c0").alias("date_event_began"),
    col("_c2").alias("date_of_restoration"),
    col("_c4").alias("area_affected"),
    col("_c5").alias("nerc_region"),
    col("_c6").alias("event_type"),
    col("_c7").alias("demand_loss_mw"),
    col("_c8").alias("customers_affected")
)

# Filter out the month headers and any remaining header rows
df_cleaned = df

# Clean up the Unknown values
df_cleaned = df_cleaned.select(
    col("date_event_began"),
    col("date_of_restoration"),
    col("area_affected"),
    col("nerc_region"),
    col("event_type"),
    when(col("demand_loss_mw") == "Unknown", None)
    .when(col("demand_loss_mw") == "", None)
    .otherwise(col("demand_loss_mw")).alias("demand_loss_mw"),
    when(col("customers_affected") == "Unknown", None)
    .when(col("customers_affected") == "", None)
    .otherwise(col("customers_affected")).alias("customers_affected")
)

# Write the cleaned data back to S3
df_cleaned.write.mode("overwrite").option("header", "true").csv(output_path)

print("2013 data cleaned and saved successfully!")

print("\nSample of cleaned data:")
df_cleaned.show(5)

print("\nCleaned data schema:")
df_cleaned.printSchema()

# COMMAND ----------

df_cleaned.display()

# COMMAND ----------

# DBTITLE 1,Clean 2015 csv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, regexp_extract

# Initialize Spark session
spark = SparkSession.builder.appName("Clean 2015 Energy Outages").getOrCreate()

# Define paths
input_path = "s3a://databricks-bucket-ece606/datasets/raw/Energy-OE/2015_Annual_Summary.csv"
output_path = "s3a://databricks-bucket-ece606/datasets/cleaned/Energy-OE/2015_Annual_Summary.csv"

# Read the CSV without assuming first row as header
raw_df = spark.read.csv(input_path)

df = raw_df.filter(
    ~col("_c0").contains("OE-417") & 
    ~col("_c0").contains("Source") &
    ~col("_c0").contains("Month") &
    col("_c1").isNotNull()
)

df = df.withColumn("_c1", 
    when(regexp_extract("_c1", "(\\d{4}-\\d{2}-\\d{2})", 1) != "", 
         regexp_extract("_c1", "(\\d{4}-\\d{2}-\\d{2})", 1))
    .when(regexp_extract("_c1", "(\\d{2}/\\d{2}/\\d{4})", 1) != "",
          regexp_extract("_c1", "(\\d{2}/\\d{2}/\\d{4})", 1))
    .otherwise(col("_c1"))
)

df = df.select(
    col("_c1").alias("date_event_began"),
    col("_c3").alias("date_of_restoration"),
    col("_c5").alias("area_affected"),
    col("_c6").alias("nerc_region"),
    col("_c8").alias("event_type"),
    when(col("_c9") == "Unknown", None)
    .when(col("_c9") == "", None)
    .otherwise(col("_c9")).alias("demand_loss_mw"),
    when(col("_c10") == "Unknown", None)
    .when(col("_c10") == "", None)
    .otherwise(col("_c10")).alias("customers_affected")
)

# Filter out rows that don't have proper dates
df_cleaned = df

# Write the cleaned data back to S3
df_cleaned.write.mode("overwrite").option("header", "true").csv(output_path)

print("2015 data cleaned and saved successfully!")

print("\nSample of cleaned data:")
df_cleaned.show(5)

print("\nCleaned data schema:")
df_cleaned.printSchema()

# COMMAND ----------

df_cleaned.display()

# COMMAND ----------

# DBTITLE 1,Clean 2016 csv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, regexp_extract

# Initialize Spark session
spark = SparkSession.builder.appName("Clean 2016 Energy Outages").getOrCreate()

# Define paths
input_path = "s3a://databricks-bucket-ece606/datasets/raw/Energy-OE/2016_Annual_Summary.csv"
output_path = "s3a://databricks-bucket-ece606/datasets/cleaned/Energy-OE/2016_Annual_Summary.csv"

# Read the CSV without assuming first row as header
raw_df = spark.read.csv(input_path)

df = raw_df.filter(
    ~col("_c0").contains("OE-417") & 
    ~col("_c0").contains("Source") &
    ~col("_c0").contains("Month") & 
    col("_c1").isNotNull()
)

df = df.withColumn("_c1", 
    when(regexp_extract("_c1", "(\\d{4}-\\d{2}-\\d{2})", 1) != "", 
         regexp_extract("_c1", "(\\d{4}-\\d{2}-\\d{2})", 1))
    .when(regexp_extract("_c1", "(\\d{2}/\\d{2}/\\d{4})", 1) != "",
          regexp_extract("_c1", "(\\d{2}/\\d{2}/\\d{4})", 1))
    .otherwise(col("_c1"))
)

df = df.select(
    col("_c1").alias("date_event_began"),
    col("_c3").alias("date_of_restoration"),
    col("_c5").alias("area_affected"),
    col("_c6").alias("nerc_region"),
    col("_c8").alias("event_type"),
    when(col("_c9") == "Unknown", None)
    .when(col("_c9") == "", None)
    .otherwise(col("_c9")).alias("demand_loss_mw"),
    when(col("_c10") == "Unknown", None)
    .when(col("_c10") == "", None)
    .otherwise(col("_c10")).alias("customers_affected")
)

# Filter out rows that don't have proper dates
df_cleaned = df

# Write the cleaned data back to S3
df_cleaned.write.mode("overwrite").option("header", "true").csv(output_path)

print("2015 data cleaned and saved successfully!")

print("\nSample of cleaned data:")
df_cleaned.show(5)

print("\nCleaned data schema:")
df_cleaned.printSchema()

# COMMAND ----------

df_cleaned.display()

# COMMAND ----------

# DBTITLE 1,Clean 2017 csv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, regexp_extract

# Initialize Spark session
spark = SparkSession.builder.appName("Clean 2017 Energy Outages").getOrCreate()

# Define paths
input_path = "s3a://databricks-bucket-ece606/datasets/raw/Energy-OE/2017_Annual_Summary.csv"
output_path = "s3a://databricks-bucket-ece606/datasets/cleaned/Energy-OE/2017_Annual_Summary.csv"

# Read the CSV without assuming first row as header
raw_df = spark.read.csv(input_path)

df = raw_df.filter(
    ~col("_c0").contains("OE-417") & 
    ~col("_c0").contains("Source") &
    ~col("_c0").contains("Month") &
    col("_c1").isNotNull()
)

# Extract just the date part from the timestamp format (YYYY-MM-DD) or date format (MM/DD/YYYY)
df = df.withColumn("_c1", 
    when(regexp_extract("_c1", "(\\d{4}-\\d{2}-\\d{2})", 1) != "", 
         regexp_extract("_c1", "(\\d{4}-\\d{2}-\\d{2})", 1))
    .when(regexp_extract("_c1", "(\\d{2}/\\d{2}/\\d{4})", 1) != "",
          regexp_extract("_c1", "(\\d{2}/\\d{2}/\\d{4})", 1))
    .otherwise(col("_c1"))
)

df = df.select(
    col("_c1").alias("date_event_began"),
    col("_c3").alias("date_of_restoration"),
    col("_c5").alias("area_affected"),
    col("_c6").alias("nerc_region"),
    col("_c8").alias("event_type"),
    when(col("_c9") == "Unknown", None)
    .when(col("_c9") == "", None)
    .otherwise(col("_c9")).alias("demand_loss_mw"),
    when(col("_c10") == "Unknown", None)
    .when(col("_c10") == "", None)
    .otherwise(col("_c10")).alias("customers_affected")
)

df_cleaned = df

# Write the cleaned data back to S3
df_cleaned.write.mode("overwrite").option("header", "true").csv(output_path)

print("2017 data cleaned and saved successfully!")

print("\nSample of cleaned data:")
df_cleaned.show(5)

print("\nCleaned data schema:")
df_cleaned.printSchema()

# COMMAND ----------

df_cleaned.display()

# COMMAND ----------

# DBTITLE 1,Clean 2018 csv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, regexp_extract

# Initialize Spark session
spark = SparkSession.builder.appName("Clean 2018 Energy Outages").getOrCreate()

# Define paths
input_path = "s3a://databricks-bucket-ece606/datasets/raw/Energy-OE/2018_Annual_Summary.csv"
output_path = "s3a://databricks-bucket-ece606/datasets/cleaned/Energy-OE/2018_Annual_Summary.csv"

raw_df = spark.read.csv(input_path)

df = raw_df.filter(
    ~col("_c0").contains("OE-417") & 
    ~col("_c0").contains("Source") &
    ~col("_c0").contains("Month") &
    col("_c1").isNotNull()
)

# Extract just the date part from the timestamp format (YYYY-MM-DD) or date format (MM/DD/YYYY)
df = df.withColumn("_c1", 
    when(regexp_extract("_c1", "(\\d{4}-\\d{2}-\\d{2})", 1) != "", 
         regexp_extract("_c1", "(\\d{4}-\\d{2}-\\d{2})", 1))
    .when(regexp_extract("_c1", "(\\d{2}/\\d{2}/\\d{4})", 1) != "",
          regexp_extract("_c1", "(\\d{2}/\\d{2}/\\d{4})", 1))
    .otherwise(col("_c1"))
)

df = df.select(
    col("_c1").alias("date_event_began"),
    col("_c3").alias("date_of_restoration"),
    col("_c5").alias("area_affected"),
    col("_c6").alias("nerc_region"),
    col("_c8").alias("event_type"),
    when(col("_c9") == "Unknown", None)
    .when(col("_c9") == "", None)
    .otherwise(col("_c9")).alias("demand_loss_mw"),
    when(col("_c10") == "Unknown", None)
    .when(col("_c10") == "", None)
    .otherwise(col("_c10")).alias("customers_affected")
)

df_cleaned = df

# Write the cleaned data back to S3
df_cleaned.write.mode("overwrite").option("header", "true").csv(output_path)

print("data cleaned and saved successfully!")

print("\nSample of cleaned data:")
df_cleaned.show(5)

print("\nCleaned data schema:")
df_cleaned.printSchema()

# COMMAND ----------

df_cleaned.display()

# COMMAND ----------

# DBTITLE 1,Clean 2019 csv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, regexp_extract

# Initialize Spark session
spark = SparkSession.builder.appName("Clean 2019 Energy Outages").getOrCreate()

# Define paths
input_path = "s3a://databricks-bucket-ece606/datasets/raw/Energy-OE/2019_Annual_Summary.csv"
output_path = "s3a://databricks-bucket-ece606/datasets/cleaned/Energy-OE/2019_Annual_Summary.csv"

# Read the CSV without assuming first row as header
raw_df = spark.read.csv(input_path)

df = raw_df.filter(
    ~col("_c0").contains("OE-417") & 
    ~col("_c0").contains("Source") &
    ~col("_c0").contains("Month") &
    col("_c1").isNotNull()
)

# Extract just the date part from the timestamp format (YYYY-MM-DD) or date format (MM/DD/YYYY)
df = df.withColumn("_c1", 
    when(regexp_extract("_c1", "(\\d{4}-\\d{2}-\\d{2})", 1) != "", 
         regexp_extract("_c1", "(\\d{4}-\\d{2}-\\d{2})", 1))
    .when(regexp_extract("_c1", "(\\d{2}/\\d{2}/\\d{4})", 1) != "",
          regexp_extract("_c1", "(\\d{2}/\\d{2}/\\d{4})", 1))
    .otherwise(col("_c1"))
)

df = df.select(
    col("_c1").alias("date_event_began"),
    col("_c3").alias("date_of_restoration"),
    col("_c5").alias("area_affected"),
    col("_c6").alias("nerc_region"),
    col("_c8").alias("event_type"),
    when(col("_c9") == "Unknown", None)
    .when(col("_c9") == "", None)
    .otherwise(col("_c9")).alias("demand_loss_mw"),
    when(col("_c10") == "Unknown", None)
    .when(col("_c10") == "", None)
    .otherwise(col("_c10")).alias("customers_affected")
)

df_cleaned = df

# Write the cleaned data back to S3
df_cleaned.write.mode("overwrite").option("header", "true").csv(output_path)

print("data cleaned and saved successfully!")

print("\nSample of cleaned data:")
df_cleaned.show(5)

print("\nCleaned data schema:")
df_cleaned.printSchema()

# COMMAND ----------

df_cleaned.display()

# COMMAND ----------

# DBTITLE 1,Clean 2020 csv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, regexp_extract

# Initialize Spark session
spark = SparkSession.builder.appName("Clean 2020 Energy Outages").getOrCreate()

# Define paths
input_path = "s3a://databricks-bucket-ece606/datasets/raw/Energy-OE/2020_Annual_Summary.csv"
output_path = "s3a://databricks-bucket-ece606/datasets/cleaned/Energy-OE/2020_Annual_Summary.csv"

# Read the CSV without assuming first row as header
raw_df = spark.read.csv(input_path)

df = raw_df.filter(
    ~col("_c0").contains("OE-417") & 
    ~col("_c0").contains("Source") &
    ~col("_c0").contains("Month") &
    col("_c1").isNotNull()
)

# Extract just the date part from the timestamp format (YYYY-MM-DD) or date format (MM/DD/YYYY)
df = df.withColumn("_c1", 
    when(regexp_extract("_c1", "(\\d{4}-\\d{2}-\\d{2})", 1) != "", 
         regexp_extract("_c1", "(\\d{4}-\\d{2}-\\d{2})", 1))
    .when(regexp_extract("_c1", "(\\d{2}/\\d{2}/\\d{4})", 1) != "",
          regexp_extract("_c1", "(\\d{2}/\\d{2}/\\d{4})", 1))
    .otherwise(col("_c1"))
)

df = df.select(
    col("_c1").alias("date_event_began"),
    col("_c3").alias("date_of_restoration"),
    col("_c5").alias("area_affected"),
    col("_c6").alias("nerc_region"),
    col("_c8").alias("event_type"),
    when(col("_c9") == "Unknown", None)
    .when(col("_c9") == "", None)
    .otherwise(col("_c9")).alias("demand_loss_mw"),
    when(col("_c10") == "Unknown", None)
    .when(col("_c10") == "", None)
    .otherwise(col("_c10")).alias("customers_affected")
)

df_cleaned = df

# Write the cleaned data back to S3
df_cleaned.write.mode("overwrite").option("header", "true").csv(output_path)

print("data cleaned and saved successfully!")

print("\nSample of cleaned data:")
df_cleaned.show(5)

print("\nCleaned data schema:")
df_cleaned.printSchema()

# COMMAND ----------

df_cleaned.display()

# COMMAND ----------

# DBTITLE 1,Clean 2021 csv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, regexp_extract

# Initialize Spark session
spark = SparkSession.builder.appName("Clean 2021 Energy Outages").getOrCreate()

# Define paths
input_path = "s3a://databricks-bucket-ece606/datasets/raw/Energy-OE/2021_Annual_Summary.csv"
output_path = "s3a://databricks-bucket-ece606/datasets/cleaned/Energy-OE/2021_Annual_Summary.csv"

# Read the CSV without assuming first row as header
raw_df = spark.read.csv(input_path)

df = raw_df.filter(
    ~col("_c0").contains("OE-417") & 
    ~col("_c0").contains("Source") &
    ~col("_c0").contains("Month") &
    col("_c1").isNotNull()
)

# Extract just the date part from the timestamp format (YYYY-MM-DD) or date format (MM/DD/YYYY)
df = df.withColumn("_c1", 
    when(regexp_extract("_c1", "(\\d{4}-\\d{2}-\\d{2})", 1) != "", 
         regexp_extract("_c1", "(\\d{4}-\\d{2}-\\d{2})", 1))
    .when(regexp_extract("_c1", "(\\d{2}/\\d{2}/\\d{4})", 1) != "",
          regexp_extract("_c1", "(\\d{2}/\\d{2}/\\d{4})", 1))
    .otherwise(col("_c1"))
)

df = df.select(
    col("_c1").alias("date_event_began"),
    col("_c3").alias("date_of_restoration"),
    col("_c5").alias("area_affected"),
    col("_c6").alias("nerc_region"),
    col("_c8").alias("event_type"),
    when(col("_c9") == "Unknown", None)
    .when(col("_c9") == "", None)
    .otherwise(col("_c9")).alias("demand_loss_mw"),
    when(col("_c10") == "Unknown", None)
    .when(col("_c10") == "", None)
    .otherwise(col("_c10")).alias("customers_affected")
)

df_cleaned = df

# Write the cleaned data back to S3
df_cleaned.write.mode("overwrite").option("header", "true").csv(output_path)

print("data cleaned and saved successfully!")

print("\nSample of cleaned data:")
df_cleaned.show(5)

print("\nCleaned data schema:")
df_cleaned.printSchema()

# COMMAND ----------

df_cleaned.display()

# COMMAND ----------

# DBTITLE 1,Clean 2022 csv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, regexp_extract

# Initialize Spark session
spark = SparkSession.builder.appName("Clean 2022 Energy Outages").getOrCreate()

# Define paths
input_path = "s3a://databricks-bucket-ece606/datasets/raw/Energy-OE/2022_Annual_Summary.csv"
output_path = "s3a://databricks-bucket-ece606/datasets/cleaned/Energy-OE/2022_Annual_Summary.csv"

# Read the CSV without assuming first row as header
raw_df = spark.read.csv(input_path)

df = raw_df.filter(
    ~col("_c0").contains("OE-417") & 
    ~col("_c0").contains("Source") &
    ~col("_c0").contains("Month") & 
    col("_c1").isNotNull()
)

# Extract just the date part from the timestamp format (YYYY-MM-DD) or date format (MM/DD/YYYY)
df = df.withColumn("_c1", 
    when(regexp_extract("_c1", "(\\d{4}-\\d{2}-\\d{2})", 1) != "", 
         regexp_extract("_c1", "(\\d{4}-\\d{2}-\\d{2})", 1))
    .when(regexp_extract("_c1", "(\\d{2}/\\d{2}/\\d{4})", 1) != "",
          regexp_extract("_c1", "(\\d{2}/\\d{2}/\\d{4})", 1))
    .otherwise(col("_c1"))
)

df = df.select(
    col("_c1").alias("date_event_began"),
    col("_c3").alias("date_of_restoration"),
    col("_c5").alias("area_affected"),
    col("_c6").alias("nerc_region"),
    col("_c8").alias("event_type"),
    when(col("_c9") == "Unknown", None)
    .when(col("_c9") == "", None)
    .otherwise(col("_c9")).alias("demand_loss_mw"),
    when(col("_c10") == "Unknown", None)
    .when(col("_c10") == "", None)
    .otherwise(col("_c10")).alias("customers_affected")
)

df_cleaned = df

# Write the cleaned data back to S3
df_cleaned.write.mode("overwrite").option("header", "true").csv(output_path)

print("data cleaned and saved successfully!")

print("\nSample of cleaned data:")
df_cleaned.show(5)

print("\nCleaned data schema:")
df_cleaned.printSchema()

# COMMAND ----------

df_cleaned.display()

# COMMAND ----------

# DBTITLE 1,Clean 2023 csv
from pyspark.sql.functions import col, when, regexp_extract

input_path = "s3a://databricks-bucket-ece606/datasets/raw/Energy-OE/2023_Annual_Summary.csv"
output_path = "s3a://databricks-bucket-ece606/datasets/cleaned/Energy-OE/2023_Annual_Summary.csv"

# Read the CSV without assuming the first row as header
raw_df = spark.read.csv(input_path)

# Get rid of the unnamed header row and citation footer
df = raw_df.filter(
    ~col("_c0").contains("OE-417") &
    ~col("_c0").contains("Source") &
    ~col("_c0").contains("Event Month") &
    col("_c1").isNotNull()
)

# Filter out rows where "event_type" contains "Unnamed"
df = df.filter(~col("_c8").contains("Unnamed"))

# Extract just the date part from the timestamp format (YYYY-MM-DD) or date format (MM/DD/YYYY)
df = df.withColumn("_c1",
    when(regexp_extract("_c1", "(\\d{4}-\\d{2}-\\d{2})", 1) != "",
        regexp_extract("_c1", "(\\d{4}-\\d{2}-\\d{2})", 1))
    .when(regexp_extract("_c1", "(\\d{2}/\\d{2}/\\d{4})", 1) != "",
        regexp_extract("_c1", "(\\d{2}/\\d{2}/\\d{4})", 1))
    .otherwise(col("_c1"))
)

df = df.select(
    col("_c1").alias("date_event_began"),
    col("_c3").alias("date_of_restoration"),
    col("_c5").alias("area_affected"),
    col("_c6").alias("nerc_region"),
    col("_c8").alias("event_type"),
    when(col("_c9") == "Unknown", None)
        .when(col("_c9") == "", None)
        .otherwise(col("_c9")).alias("demand_loss_mw"),
    when(col("_c10") == "Unknown", None)
        .when(col("_c10") == "", None)
        .otherwise(col("_c10")).alias("customers_affected")
)

# Write the cleaned data back to S3
df.write.mode("overwrite").option("header", "true").csv(output_path)

print("2023 data cleaned and saved successfully!")


# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, when, year
import os

# Initialize Spark session
spark = SparkSession.builder.appName("Combine All Energy Outages").getOrCreate()

# Base path for input files
base_path = "s3a://databricks-bucket-ece606/datasets/cleaned/Energy-OE/"

# Get all years (2013-2023)
years = range(2013, 2024)

def standardize_dates(df):
    return df.withColumn("date_event_began",
        when(col("date_event_began").rlike("^\\d{4}-\\d{2}-\\d{2}"), 
            to_date(col("date_event_began"), "yyyy-MM-dd"))
        .when(col("date_event_began").rlike("^\\d{2}/\\d{2}/\\d{4}"), 
            to_date(col("date_event_began"), "MM/dd/yyyy"))
        .otherwise(to_date(col("date_event_began"), "MM/dd/yyyy"))
    ).withColumn("date_of_restoration",
        when(col("date_of_restoration").rlike("^\\d{4}-\\d{2}-\\d{2}"), 
            to_date(col("date_of_restoration"), "yyyy-MM-dd"))
        .when(col("date_of_restoration").rlike("^\\d{2}/\\d{2}/\\d{4}"), 
            to_date(col("date_of_restoration"), "MM/dd/yyyy"))
        .otherwise(to_date(col("date_of_restoration"), "MM/dd/yyyy"))
    )

# Read and combine all dataframes
all_dfs = []
for current_year in years:
    try:
        # Construct path for each year
        file_path = f"{base_path}{current_year}_Annual_Summary.csv/"
        
        # Read the CSV
        df = spark.read.option("header", "true").csv(file_path)
        
        # Clean dates
        df = standardize_dates(df)
        
        # Add to list
        all_dfs.append(df)
        print(f"Successfully processed {current_year} data")
    except Exception as e:
        print(f"Error processing {current_year} data: {str(e)}")

# Combine all dataframes
combined_df = all_dfs[0]
for df in all_dfs[1:]:
    combined_df = combined_df.union(df)

# Filter out rows with null event_type
combined_df = combined_df.filter(
    col("event_type").isNotNull() & 
    col("date_event_began").isNotNull()
)

# Write the combined and cleaned data back to S3
output_path = f"{base_path}combined_energy_outages_2013_2023.csv"
# combined_df.write.mode("overwrite").option("header", "true").csv(output_path)

print("\nData combined, cleaned, and saved successfully!")

print("\nSample of combined data:")
combined_df.show(5)

print("\nCombined data schema:")
combined_df.printSchema()

print("\nTotal number of rows:", combined_df.count())
print("\nEvents by year:")
combined_df.groupBy(year("date_event_began").alias("year")).count().orderBy("year").show()

print("\nNumber of events by type:")
combined_df.groupBy("event_type").count().orderBy("count", ascending=False).show(truncate=False)

# COMMAND ----------

print(combined_df.count())
combined_df.display()

# COMMAND ----------

from pyspark.sql.functions import explode, split, regexp_replace, trim, col, udf
from pyspark.sql.types import ArrayType, StringType

def extract_states_udf():
    def extract_states(area):
        if not area or area == '-':
            return ['Unknown']
            
        state_patterns = {
            'alabama': 'AL', 
            'alaska': 'AK',
            'arizona': 'AZ',
            'arkansas': 'AR',
            'california': 'CA',
            'colorado': 'CO',
            'connecticut': 'CT',
            'delaware': 'DE',
            'district of columbia': 'DC',
            'd.c.': 'DC',
            'dc': 'DC',
            'florida': 'FL',
            'georgia': 'GA',
            'hawaii': 'HI',
            'idaho': 'ID',
            'illinois': 'IL',
            'indiana': 'IN',
            'iowa': 'IA',
            'kansas': 'KS',
            'kentucky': 'KY',
            'louisiana': 'LA',
            'maine': 'ME',
            'maryland': 'MD',
            'massachusetts': 'MA',
            'michigan': 'MI',
            'minnesota': 'MN',
            'mississippi': 'MS',
            'missouri': 'MO',
            'montana': 'MT',
            'nebraska': 'NE',
            'nevada': 'NV',
            'new hampshire': 'NH',
            'new jersey': 'NJ',
            'new mexico': 'NM',
            'new york': 'NY',
            'north carolina': 'NC',
            'north dakota': 'ND',
            'ohio': 'OH',
            'oklahoma': 'OK',
            'oregon': 'OR',
            'pennsylvania': 'PA',
            'puerto rico': 'PR',
            'rhode island': 'RI',
            'south carolina': 'SC',
            'south dakota': 'SD',
            'tennessee': 'TN',
            'texas': 'TX',
            'utah': 'UT',
            'vermont': 'VT',
            'virginia': 'VA',
            'washington': 'WA',
            'west virginia': 'WV',
            'wisconsin': 'WI',
            'wyoming': 'WY'
        }
        
        # Normalize the input string
        area = area.lower()
        # Replace various connectors with semicolons for consistent splitting
        area = area.replace(' and ', ';')
        area = area.replace(',', ';')
        area = area.replace(':', ';')
        
        # Split on semicolon and clean up each part
        parts = [part.strip() for part in area.split(';') if part.strip()]
        
        # Extract states from each part
        states = set()
        for part in parts:
            # Check for ALL state matches in each part
            for state_name, abbrev in state_patterns.items():
                if state_name in part:
                    states.add(abbrev)
                    
        return list(states) if states else ['Unknown']
    
    return udf(extract_states, ArrayType(StringType()))

df_with_states = combined_df \
    .withColumn("states", extract_states_udf()(col("area_affected"))) \
    .withColumn("state", explode("states")) \
    .drop("states")

print(f"Original row count: {combined_df.count()}")
print(f"New row count: {df_with_states.count()}")

# COMMAND ----------

df_with_states.display()

# COMMAND ----------

type(df_with_states)

# COMMAND ----------

# DBTITLE 1,NOAA
import requests
import pandas as pd
from datetime import datetime
import time
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType

def get_weather_data(date, state, api_token):
    """Get daily weather data for a state."""
    state_codes = {
        'AL': '01', 'AK': '02', 'AZ': '04', 'AR': '05', 'CA': '06', 'CO': '08', 'CT': '09',
        'DC': '11', 'DE': '10', 'FL': '12', 'GA': '13', 'HI': '15', 'ID': '16', 'IL': '17', 
        'IN': '18', 'IA': '19', 'KS': '20', 'KY': '21', 'LA': '22', 'ME': '23', 'MD': '24', 
        'MA': '25', 'MI': '26', 'MN': '27', 'MS': '28', 'MO': '29', 'MT': '30', 'NE': '31', 
        'NV': '32', 'NH': '33', 'NJ': '34', 'NM': '35', 'NY': '36', 'NC': '37', 'ND': '38', 
        'OH': '39', 'OK': '40', 'OR': '41', 'PA': '42', 'PR': '72', 'RI': '44', 'SC': '45', 
        'SD': '46', 'TN': '47', 'TX': '48', 'UT': '49', 'VT': '50', 'VA': '51', 'WA': '53', 
        'WV': '54', 'WI': '55', 'WY': '56'
    }
    
    state_code = state_codes.get(state)
    if not state_code:
        print(f"No state code for {state}")
        return None
        
    url = "https://www.ncdc.noaa.gov/cdo-web/api/v2/data"
    headers = {'token': api_token}
    params = {
        'datasetid': 'GHCND',
        'locationid': f'FIPS:{state_code}',
        'startdate': date,
        'enddate': date,
        'units': 'standard',
        'datatypeid': 'TMAX,TMIN,PRCP,AWND',
        'limit': 500
    }
    
    try:
        response = requests.get(url, headers=headers, params=params)
        if response.ok:
            data = response.json().get('results', [])
            if data:
                weather = {
                    'temp_max': next((d['value'] for d in data if d['datatype'] == 'TMAX'), None),
                    'temp_min': next((d['value'] for d in data if d['datatype'] == 'TMIN'), None),
                    'precipitation': next((d['value'] for d in data if d['datatype'] == 'PRCP'), None),
                    'wind_speed': next((d['value'] for d in data if d['datatype'] == 'AWND'), None)
                }
                print(f"Got weather data for {state} on {date}: {weather}")
                return weather
            else:
                print(f"No data found for {state} on {date}")
                return None
        else:
            print(f"API error: {response.status_code} for {state} on {date}")
            return None
    except Exception as e:
        print(f"Error getting weather for {state} on {date}: {e}")
        return None

def enrich_with_weather_data(spark, df_with_states, api_token, batch_size=100):
    """Process the full dataset in batches."""
    from datetime import datetime
    from pyspark.sql.functions import to_date
    
    # Get unique date-state combinations to avoid duplicates
    unique_combos = df_with_states.select('date_event_began', 'state').distinct().toPandas()
    total_rows = len(unique_combos)
    print(f"Processing {total_rows} unique date-state combinations")
    
    results = []
    for idx, row in unique_combos.iterrows():
        if idx % 10 == 0:
            print(f"Progress: {idx}/{total_rows} ({idx/total_rows*100:.1f}%)")
            
        # Format the date for the API
        date = row['date_event_began'].strftime('%Y-%m-%d')
        state = row['state']
        
        weather = get_weather_data(date, state, api_token)
        if weather:
            results.append({
                'date_event_began': date,
                'state': state,
                **weather
            })
        time.sleep(0.2)
    
    # Create final DataFrame
    if results:
        weather_df = spark.createDataFrame(results)
        
        # Convert date string to date type
        weather_df = weather_df.withColumn("date_event_began", to_date("date_event_began"))
        
        print("\nCount of weather records before join:")
        print(weather_df.count())
        print("\nSample of weather data before join:")
        weather_df.show(5)
        
        # Join with original data
        result_df = df_with_states.join(
            weather_df,
            ["date_event_began", "state"]
        )
        
        print("\nWeather data enrichment complete!")
        return result_df
    else:
        print("No weather data found!")
        return None

api_token = 'IttbgagHIFHdEJEwOXFPXZVwzoCNSEEJ'
# Process full dataset
full_results = enrich_with_weather_data(spark, df_with_states, api_token)

if full_results is not None:
   print("\nFinal joined results:")
   full_results.select("date_event_began", "state", "temp_max", "temp_min", "precipitation", "wind_speed").show()
   
   print("\nWeather Data Statistics:")
   full_results.select("temp_max", "temp_min", "precipitation", "wind_speed").describe().show()

# COMMAND ----------

full_results.display()

# COMMAND ----------

cleaned_df = full_results.filter(
    (col("state").isNotNull()) & 
    (col("state") != "Unknown") & 
    (col("temp_min").isNotNull())
)

cleaned_df.display()

# COMMAND ----------

from pyspark.sql.functions import col, when, lower

# Create new column weather_outage based on if 'weather' appears in event_type
final_df = cleaned_df.withColumn(
    "weather_outage",
    when(lower(col("event_type")).contains("weather"), 1)
    .otherwise(0)
)

final_df.display()

# COMMAND ----------

output_path = "s3://databricks-bucket-ece606/datasets/processed/Energy-OE/Combined_Summary.csv"
final_df.coalesce(1) \
    .write \
    .format("csv") \
    .mode("overwrite") \
    .option("header", "true") \
    .save(output_path)

# COMMAND ----------

# DBTITLE 1,Free version (Rate limited)
import requests
import pandas as pd
from datetime import datetime
import time
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType
from concurrent.futures import ThreadPoolExecutor, as_completed

def get_weather_data(date, state):
    """Get daily weather data for a state using Open-Meteo API (free, no API key required)."""
    # State coordinates (approximate centroids)
    state_coords = {
        'AL': (32.7794, -86.8287), 'AK': (64.0685, -152.2782), 'AZ': (34.2744, -111.6602),
        'DC': (38.9072, -77.0369), 'PR': (18.2208, -66.5901),
        'AR': (34.8938, -92.4426), 'CA': (37.1841, -119.4696), 'CO': (38.9972, -105.5478),
        'CT': (41.6219, -72.7273), 'DE': (38.9896, -75.5050), 'FL': (28.6305, -82.4497),
        'GA': (32.6415, -83.4426), 'HI': (20.2927, -156.3737), 'ID': (44.3509, -114.6130),
        'IL': (40.0417, -89.1965), 'IN': (39.8942, -86.2816), 'IA': (42.0751, -93.4960),
        'KS': (38.4937, -98.3804), 'KY': (37.5347, -85.3021), 'LA': (31.0689, -91.9968),
        'ME': (45.3695, -69.2428), 'MD': (39.0550, -76.7909), 'MA': (42.2596, -71.8083),
        'MI': (44.3467, -85.4102), 'MN': (46.2807, -94.3053), 'MS': (32.7364, -89.6678),
        'MO': (38.3566, -92.4580), 'MT': (47.0527, -109.6333), 'NE': (41.5378, -99.7951),
        'NV': (39.3289, -116.6312), 'NH': (43.6805, -71.5811), 'NJ': (40.1907, -74.6728),
        'NM': (34.4071, -106.1126), 'NY': (42.9538, -75.5268), 'NC': (35.5557, -79.3877),
        'ND': (47.4501, -100.4659), 'OH': (40.2862, -82.7937), 'OK': (35.5889, -97.4943),
        'OR': (43.9336, -120.5583), 'PA': (40.8781, -77.7996), 'RI': (41.6762, -71.5562),
        'SC': (33.9169, -80.8964), 'SD': (44.4443, -100.2263), 'TN': (35.8580, -86.3505),
        'TX': (31.4757, -99.3312), 'UT': (39.3055, -111.6703), 'VT': (44.0687, -72.6658),
        'VA': (37.5215, -78.8537), 'WA': (47.3826, -120.4472), 'WV': (38.6409, -80.6227),
        'WI': (44.6243, -89.9941), 'WY': (42.9957, -107.5512)
    }
    
    if state not in state_coords:
        print(f"No coordinates found for {state}")
        return None
        
    lat, lon = state_coords[state]
    
    # Open-Meteo API endpoint (free, no API key needed)
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        'latitude': lat,
        'longitude': lon,
        'start_date': date,
        'end_date': date,
        'daily': 'temperature_2m_max,temperature_2m_min,precipitation_sum,windspeed_10m_max',
        'temperature_unit': 'fahrenheit',
        'windspeed_unit': 'mph',
        'precipitation_unit': 'inch',
        'timezone': 'America/New_York'
    }
    
    try:
        response = requests.get(url, params=params)
        if response.ok:
            data = response.json()
            if data.get('daily'):
                daily = data['daily']
                weather = {
                    'temp_max': daily['temperature_2m_max'][0],
                    'temp_min': daily['temperature_2m_min'][0],
                    'precipitation': daily['precipitation_sum'][0],
                    'wind_speed': daily['windspeed_10m_max'][0]
                }
                print(f"Got weather data for {state} on {date}: {weather}")
                return weather
            else:
                print(f"No data found for {state} on {date}")
                return None
        else:
            print(f"API error: {response.status_code} for {state} on {date}")
            return None
    except Exception as e:
        print(f"Error getting weather for {state} on {date}: {e}")
        return None

def process_batch(batch_data):
    """Process a batch of date-state combinations."""
    results = []
    for date, state in batch_data:
        weather = get_weather_data(date, state)
        if weather:
            results.append({
                'date': date,
                'state': state,
                **weather
            })
        time.sleep(0.05)  # Very mild rate limiting - Open-Meteo is quite generous
    return results

def enrich_with_weather_data(spark, df_with_states, batch_size=100, max_workers=10):
    """Process the full dataset using parallel requests."""
    
    # Get unique date-state combinations
    unique_combos = df_with_states.select('date_event_began', 'state').distinct().toPandas()
    total_rows = len(unique_combos)
    print(f"Processing {total_rows} unique date-state combinations")
    
    # Prepare batches
    date_state_pairs = list(zip(unique_combos['date_event_began'], unique_combos['state']))
    batches = [date_state_pairs[i:i + batch_size] for i in range(0, len(date_state_pairs), batch_size)]
    
    all_results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all batches
        future_to_batch = {
            executor.submit(process_batch, batch): batch 
            for batch in batches
        }
        
        # Process completed batches
        for idx, future in enumerate(as_completed(future_to_batch)):
            try:
                results = future.result()
                all_results.extend(results)
                print(f"Completed batch {idx + 1}/{len(batches)} ({(idx + 1)/len(batches)*100:.1f}%)")
                
                # Show intermediate stats for the batch
                if results:
                    batch_df = pd.DataFrame(results)
                    print(f"Batch {idx + 1} stats:\n{batch_df.describe()}")
            except Exception as e:
                print(f"Error processing batch: {e}")
    
    # Create final DataFrame
    if all_results:
        # Define schema for the weather data
        weather_schema = StructType([
            StructField("date", StringType(), True),
            StructField("state", StringType(), True),
            StructField("temp_max", DoubleType(), True),
            StructField("temp_min", DoubleType(), True),
            StructField("precipitation", DoubleType(), True),
            StructField("wind_speed", DoubleType(), True)
        ])
        
        # Convert to PySpark DataFrame
        weather_df = spark.createDataFrame(all_results, schema=weather_schema)
        
        # Join with original data
        result_df = df_with_states.join(
            weather_df,
            (df_with_states.date_event_began == weather_df.date) & 
            (df_with_states.state == weather_df.state),
            'left'
        ).drop(weather_df.date, weather_df.state)
        
        print("\nWeather data enrichment complete!")
        return result_df
    else:
        print("No weather data found!")
        return None

def main(spark, df_with_states):
    """Main function to run the weather data enrichment."""
    # Configure the number of batches and workers (can be adjusted based on needs)
    batch_size = 100  # Process 100 requests per batch
    max_workers = 10  # Run 10 parallel workers
    
    full_results = enrich_with_weather_data(
        spark, 
        df_with_states, 
        batch_size=batch_size, 
        max_workers=max_workers
    )
    
    if full_results is not None:
        full_results.cache()
        print("\nSample of Final Results:")
        full_results.select(
            "date_event_began", 
            "state", 
            "temp_max", 
            "temp_min", 
            "precipitation", 
            "wind_speed"
        ).show(5)
        
        # Show statistics
        print("\nWeather Data Statistics:")
        full_results.select(
            "temp_max", 
            "temp_min", 
            "precipitation", 
            "wind_speed"
        ).describe().show()
        
        # Save as a new table
        # full_results.write.mode("overwrite").saveAsTable("outages_with_weather")
        # print("\nSaved enriched data as 'outages_with_weather' table")
        
        return full_results
    return None

results = main(spark, df_with_states)

# COMMAND ----------

df_verification = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("s3://databricks-bucket-ece606/datasets/processed/Energy-OE/Combined_Summary.csv")

# COMMAND ----------

df_verification.display()

# COMMAND ----------

