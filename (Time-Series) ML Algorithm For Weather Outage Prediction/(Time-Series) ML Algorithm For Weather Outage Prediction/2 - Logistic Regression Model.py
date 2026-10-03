# Databricks notebook source
outage_data = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("s3://databricks-bucket-ece606/datasets/processed/Energy-OE/Combined_Summary.csv")

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql.functions import coalesce, lit

# COMMAND ----------

outage_data_standardized = outage_data.withColumn(
    "wind_speed", 
    coalesce(outage_data["wind_speed"], lit(0.0))
).dropna(subset=['temp_min', 'temp_max', 'weather_outage'])

# COMMAND ----------

# MAGIC %md
# MAGIC Data Visualizations From Gold Data

# COMMAND ----------

display(outage_data_standardized)

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator

feature_columns = ['temp_min', 'temp_max', 'wind_speed', 'precipitation', "season"]
assembler = VectorAssembler(
    inputCols=feature_columns,
    outputCol='features'
)
df_features = assembler.transform(outage_data_standardized)

# Split into training (80%) and test (20%) sets
train_data, test_data = df_features.randomSplit([0.8, 0.2], seed=42)

# Create and train logistic regression model
lr = LogisticRegression(
    featuresCol='features',
    labelCol='weather_outage',
    maxIter=10
)
model = lr.fit(train_data)

predictions = model.transform(test_data)

evaluator = BinaryClassificationEvaluator(
    labelCol='weather_outage',
    metricName='areaUnderROC'
)
auc_score = evaluator.evaluate(predictions)

print(f"AUC Score on test set: {auc_score}")

coefficients = list(zip(feature_columns, model.coefficients))
print("\nFeature Coefficients:")
for feature, coef in coefficients:
    print(f"{feature}: {coef}")

predictions.select('probability', 'prediction', 'weather_outage').show(10)

# COMMAND ----------

from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql.functions import col

evaluator_accuracy = MulticlassClassificationEvaluator(
    labelCol='weather_outage',
    predictionCol='prediction',
    metricName='accuracy'
)
accuracy = evaluator_accuracy.evaluate(predictions)

evaluator_precision = MulticlassClassificationEvaluator(
    labelCol='weather_outage',
    predictionCol='prediction',
    metricName='weightedPrecision'
)
precision = evaluator_precision.evaluate(predictions)

evaluator_recall = MulticlassClassificationEvaluator(
    labelCol='weather_outage',
    predictionCol='prediction',
    metricName='weightedRecall'
)
recall = evaluator_recall.evaluate(predictions)

print(f"Accuracy: {accuracy * 100:.2f}%")
print(f"AUC Score: {auc_score:.4f}")
print(f"Precision: {precision:.4f}")
print(f"Recall: {recall:.4f}")

print("\nConfusion Matrix:")
predictions.groupBy('weather_outage', 'prediction').count().orderBy('weather_outage', 'prediction').show()

# COMMAND ----------

