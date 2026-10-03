# Databricks notebook source
outage_data = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("s3://databricks-bucket-ece606/datasets/processed/Energy-OE/Combined_Summary.csv")

# COMMAND ----------

from pyspark.sql.functions import coalesce, lit

outage_data_standardized = outage_data.withColumn(
    "wind_speed", 
    coalesce(outage_data["wind_speed"], lit(0.0))
).dropna(subset=['temp_min', 'temp_max', 'weather_outage'])

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator

feature_columns = ['temp_min', 'temp_max', 'wind_speed', 'precipitation', 'season']
assembler = VectorAssembler(
    inputCols=feature_columns,
    outputCol='features'
)
df_features = assembler.transform(outage_data_standardized)

# Split into training (80%) and test (20%) sets
train_data, test_data = df_features.randomSplit([0.8, 0.2], seed=42)

# Create and train Random Forest model
rf = RandomForestClassifier(
    featuresCol='features',
    labelCol='weather_outage',
    numTrees=100,  # number of trees in the forest
    maxDepth=5     # maximum depth of each tree
)

rf_model = rf.fit(train_data)

rf_predictions = rf_model.transform(test_data)

evaluator_auc = BinaryClassificationEvaluator(
    labelCol='weather_outage',
    metricName='areaUnderROC'
)
rf_auc = evaluator_auc.evaluate(rf_predictions)

evaluator_accuracy = MulticlassClassificationEvaluator(
    labelCol='weather_outage',
    predictionCol='prediction',
    metricName='accuracy'
)
rf_accuracy = evaluator_accuracy.evaluate(rf_predictions)

evaluator_precision = MulticlassClassificationEvaluator(
    labelCol='weather_outage',
    predictionCol='prediction',
    metricName='weightedPrecision'
)
rf_precision = evaluator_precision.evaluate(rf_predictions)

evaluator_recall = MulticlassClassificationEvaluator(
    labelCol='weather_outage',
    predictionCol='prediction',
    metricName='weightedRecall'
)
rf_recall = evaluator_recall.evaluate(rf_predictions)

print(f"Random Forest Results:")
print(f"Accuracy: {rf_accuracy * 100:.2f}%")
print(f"AUC Score: {rf_auc:.4f}")
print(f"Precision: {rf_precision:.4f}")
print(f"Recall: {rf_recall:.4f}")

print("\nRandom Forest Confusion Matrix:")
rf_predictions.groupBy('weather_outage', 'prediction').count().orderBy('weather_outage', 'prediction').show()

feature_importance = list(zip(feature_columns, rf_model.featureImportances))
print("\nFeature Importances:")
for feature, importance in feature_importance:
    print(f"{feature}: {importance:.4f}")

# COMMAND ----------

