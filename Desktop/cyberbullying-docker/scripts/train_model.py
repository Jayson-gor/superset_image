from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF
from pyspark.ml.classification import LinearSVC, NaiveBayes, LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# Initialize Spark
spark = SparkSession.builder \
    .appName("Cyberbullying Detection") \
    .getOrCreate()

# Load data from HDFS
df = spark.read.csv("hdfs://localhost:9000/input/cyberbullying_dataset.csv", header=True, inferSchema=True)

# Assume columns: 'text' (content), 'label' (0=neutral, 1=bullying)
df = df.na.drop()

# Preprocessing pipeline
tokenizer = Tokenizer(inputCol="text", outputCol="words")
remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
hashingTF = HashingTF(inputCol="filtered_words", outputCol="raw_features")
idf = IDF(inputCol="raw_features", outputCol="features")

# Models
svm = LinearSVC(labelCol="label", featuresCol="features")
nb = NaiveBayes(labelCol="label", featuresCol="features")

# Pipeline
pipeline_svm = Pipeline(stages=[tokenizer, remover, hashingTF, idf, svm])
pipeline_nb = Pipeline(stages=[tokenizer, remover, hashingTF, idf, nb])

# Split data
train, test = df.randomSplit([0.8, 0.2], seed=42)

# Train models
model_svm = pipeline_svm.fit(train)
model_nb = pipeline_nb.fit(train)

# Evaluate
evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
pred_svm = model_svm.transform(test)
pred_nb = model_nb.transform(test)

print("SVM Accuracy:", evaluator.evaluate(pred_svm))
print("Naive Bayes Accuracy:", evaluator.evaluate(pred_nb))

# Save models
model_svm.save("/app/models/svm_model")
model_nb.save("/app/models/nb_model")

spark.stop()