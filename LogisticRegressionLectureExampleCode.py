# Slide 1

from pyspark.ml.classification import LogisticRegression
#Load data
data = spark.read.format("libsvm").load("data/mllib/sample_libsvm_data.txt")
#Create training and test set
training, test = data.randomSplit ([0.7,0.3], seed=123)
#Define the Logistic Regression instance
lr = LogisticRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)
#Fit the model
lrModel = lr.fit(training)
# Print coefficients and intercept
print("Coefficients: "+ str(lrModel.coefficients))
print(" Intercept: " +str(lrModel.intercept))

# Slide 2

# Extract the summary from the returned model
trainingSummary = lrModel.summary()
#Obtain the loss per iteration.
objectiveHistory = trainingSummary.objectiveHistory()
for loss in objectiveHistory:
print(loss)
#Obtain the ROC as a dataframe and areaUnderROC.
roc = trainingSummary.roc
roc.show()
# Show only the FPR column
roc.select("FPR").show()
# Area Under ROC
print(trainingSummary.areaUnderROC)

# Slide 3

From pyspark.ml.evaluation import MulticlassClassificationEvaluator
#Make predictions.
predictions = lrModel.transform(test)
#Select example rows to display.
predictions.show(5)
#Select (prediction, true label) and compute test error.
evaluator = MulticlassClassificationEvaluator(labelCol="label",
predictionCol="prediction",
metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print("Test Error = {}".format (1.0 - accuracy))

# Slide 4 Multinomial Logistic Regression Example

# We can also use the multinomial family for binary classification
mlr = LogisticRegression(
maxIter=10,
regParam=0.3,
elasticNetParam=0.8,
family="multinomial")
# Fit the model
mlrModel = mlr.fit(training)
# Print the coefficients and intercepts for logistic regression with multinomial
family
print("Multinomial coefficients: \n" + str(mlrModel.coefficientMatrix))
print("Multinomial intercepts: \n" + str(mlrModel.interceptVector))