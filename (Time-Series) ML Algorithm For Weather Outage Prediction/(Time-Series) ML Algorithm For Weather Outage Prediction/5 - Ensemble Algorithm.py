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
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier, GBTClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator
from pyspark.sql.functions import when, col, row_number
from pyspark.sql.window import Window

feature_columns = ['temp_min', 'temp_max', 'wind_speed', 'precipitation', 'season']
assembler = VectorAssembler(
    inputCols=feature_columns,
    outputCol='features'
)
df_features = assembler.transform(outage_data_standardized)

train_data, test_data = df_features.randomSplit([0.8, 0.2], seed=42)

# 1. Train Logistic Regression and get predictions
lr = LogisticRegression(featuresCol='features', labelCol='weather_outage', maxIter=10)
lr_model = lr.fit(train_data)
lr_predictions = lr_model.transform(test_data)
lr_preds = lr_predictions.select('prediction').withColumnRenamed('prediction', 'lr_pred')

# 2. Train Random Forest and get predictions
rf = RandomForestClassifier(featuresCol='features', labelCol='weather_outage', numTrees=100, maxDepth=5)
rf_model = rf.fit(train_data)
rf_predictions = rf_model.transform(test_data)
rf_preds = rf_predictions.select('prediction').withColumnRenamed('prediction', 'rf_pred')

# 3. Train Gradient Boosting and get predictions
gbt = GBTClassifier(featuresCol='features', labelCol='weather_outage', maxIter=100, maxDepth=5, stepSize=0.1)
gbt_model = gbt.fit(train_data)
gbt_predictions = gbt_model.transform(test_data)
gbt_preds = gbt_predictions.select('weather_outage', 'prediction').withColumnRenamed('prediction', 'gbt_pred')

# Add index to each prediction DataFrame
w = Window.orderBy(lit(1))
lr_preds = lr_preds.withColumn('idx', row_number().over(w))
rf_preds = rf_preds.withColumn('idx', row_number().over(w))
gbt_preds = gbt_preds.withColumn('idx', row_number().over(w))

# Join predictions using the index
ensemble_predictions = gbt_preds.join(lr_preds, 'idx').join(rf_preds, 'idx')

# Create ensemble prediction (majority voting)
ensemble_predictions = ensemble_predictions.withColumn(
    'ensemble_prediction',
    when((col('lr_pred') + col('rf_pred') + col('gbt_pred')) >= 2, 1.0).otherwise(0.0)
)

evaluator_accuracy = MulticlassClassificationEvaluator(
    labelCol='weather_outage',
    predictionCol='ensemble_prediction',
    metricName='accuracy'
)
ensemble_accuracy = evaluator_accuracy.evaluate(ensemble_predictions)

evaluator_precision = MulticlassClassificationEvaluator(
    labelCol='weather_outage',
    predictionCol='ensemble_prediction',
    metricName='weightedPrecision'
)
ensemble_precision = evaluator_precision.evaluate(ensemble_predictions)

evaluator_recall = MulticlassClassificationEvaluator(
    labelCol='weather_outage',
    predictionCol='ensemble_prediction',
    metricName='weightedRecall'
)
ensemble_recall = evaluator_recall.evaluate(ensemble_predictions)

print(f"Ensemble Voting Results:")
print(f"Accuracy: {ensemble_accuracy * 100:.2f}%")
print(f"Precision: {ensemble_precision:.4f}")
print(f"Recall: {ensemble_recall:.4f}")

print("\nEnsemble Confusion Matrix:")
ensemble_predictions.groupBy('weather_outage', 'ensemble_prediction').count().orderBy('weather_outage', 'ensemble_prediction').show()

print("\nSample Predictions from Each Model:")
ensemble_predictions.select('weather_outage', 'lr_pred', 'rf_pred', 'gbt_pred', 'ensemble_prediction').show(10)

# COMMAND ----------

