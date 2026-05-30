#!/usr/bin/env python3
import sys

import spark
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.feature import IndexToString
from pyspark.ml import Pipeline

# read the csv file kdd.data

# Split into 10 datasets
# Train 10 models
# Predict 10 results
# gather accuracy - max, min, average, and standard deviation

# kdd.data - is csv

# https://xkcd.com/221/ - 4 is overused
random_seed = 221
data = spark.read.format("csv").load("data/kdd.data")
training_data = []
test_data = []
#Split the data into training and test sets (30% held out for testing)
for i in range(10):
    training_data[i], test_data[i] = data.randomSplit([0.7, 0.3], seed=random_seed + i)

dt = DecisionTreeClassifier(labelCol="indexedLabel",
featuresCol="indexedFeatures")



