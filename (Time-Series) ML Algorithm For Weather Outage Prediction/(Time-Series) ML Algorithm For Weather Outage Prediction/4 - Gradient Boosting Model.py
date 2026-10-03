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
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator

feature_columns = ['temp_min', 'temp_max', 'wind_speed', 'precipitation', 'season']
assembler = VectorAssembler(
    inputCols=feature_columns,
    outputCol='features'
)
df_features = assembler.transform(outage_data_standardized)

# Split into training (80%) and test (20%) sets
train_data, test_data = df_features.randomSplit([0.8, 0.2], seed=42)

# Create and train Gradient Boosting model
gbt = GBTClassifier(
    featuresCol='features',
    labelCol='weather_outage',
    maxIter=100,      # number of iterations
    maxDepth=5,       # maximum depth of each tree
    stepSize=0.1      # learning rate
)

gbt_model = gbt.fit(train_data)

gbt_predictions = gbt_model.transform(test_data)

evaluator_auc = BinaryClassificationEvaluator(
    labelCol='weather_outage',
    metricName='areaUnderROC'
)
gbt_auc = evaluator_auc.evaluate(gbt_predictions)

evaluator_accuracy = MulticlassClassificationEvaluator(
    labelCol='weather_outage',
    predictionCol='prediction',
    metricName='accuracy'
)
gbt_accuracy = evaluator_accuracy.evaluate(gbt_predictions)

evaluator_precision = MulticlassClassificationEvaluator(
    labelCol='weather_outage',
    predictionCol='prediction',
    metricName='weightedPrecision'
)
gbt_precision = evaluator_precision.evaluate(gbt_predictions)

evaluator_recall = MulticlassClassificationEvaluator(
    labelCol='weather_outage',
    predictionCol='prediction',
    metricName='weightedRecall'
)
gbt_recall = evaluator_recall.evaluate(gbt_predictions)

print(f"Gradient Boosting Results:")
print(f"Accuracy: {gbt_accuracy * 100:.2f}%")
print(f"AUC Score: {gbt_auc:.4f}")
print(f"Precision: {gbt_precision:.4f}")
print(f"Recall: {gbt_recall:.4f}")

print("\nGradient Boosting Confusion Matrix:")
gbt_predictions.groupBy('weather_outage', 'prediction').count().orderBy('weather_outage', 'prediction').show()

feature_importance = list(zip(feature_columns, gbt_model.featureImportances))
print("\nFeature Importances:")
for feature, importance in feature_importance:
    print(f"{feature}: {importance:.4f}")

# COMMAND ----------

