#!/usr/bin/env python3
import sys

import numpy
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import IndexToString, StringIndexer, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType

PREDICTION_COLUMN = "prediction"
FEATURE_VECTOR_COLUMN = "features"
LABEL_COLUMN = "label"

# https://xkcd.com/221/ - 4 is overused
RANDOM_SEED = 221

def main():
    if len(sys.argv) != 4:
        print("Usage: DecisionTree.py <num_runs> <input_file> <output_dir>")
        sys.exit(-1)

    num_runs = int(sys.argv[1])
    input_path = sys.argv[2]
    output_path = sys.argv[3]

    spark = SparkSession.builder.appName("DecisionTree").getOrCreate()

    # generated using to_schema.sh
    # Updated 32, 33 to LongType.
    schema = StructType([
        StructField("duration", LongType()),
        StructField("protocol_type", LongType()),
        StructField("service", LongType()),
        StructField("flag", LongType()),
        StructField("src_bytes", LongType()),
        StructField("dst_bytes", LongType()),
        StructField("land", LongType()),
        StructField("wrong_fragment", LongType()),
        StructField("urgent", LongType()),
        StructField("hot", LongType()),
        StructField("num_failed_logins", LongType()),
        StructField("logged_in", LongType()),
        StructField("num_compromised", LongType()),
        StructField("root_shell", LongType()),
        StructField("su_attempted", LongType()),
        StructField("num_root", LongType()),
        StructField("num_file_creations", LongType()),
        StructField("num_shells", LongType()),
        StructField("num_access_files", LongType()),
        StructField("num_outbound_cmds", LongType()),
        StructField("is_host_login", LongType()),
        StructField("is_guest_login", LongType()),
        StructField("count", LongType()),
        StructField("srv_count", LongType()),
        StructField("serror_rate", DoubleType()),
        StructField("srv_serror_rate", DoubleType()),
        StructField("rerror_rate", DoubleType()),
        StructField("srv_rerror_rate", DoubleType()),
        StructField("same_srv_rate", DoubleType()),
        StructField("diff_srv_rate", DoubleType()),
        StructField("srv_diff_host_rate", DoubleType()),
        StructField("dst_host_count", LongType()),
        StructField("dst_host_srv_count", LongType()),
        StructField("dst_host_same_srv_rate", DoubleType()),
        StructField("dst_host_diff_srv_rate", DoubleType()),
        StructField("dst_host_same_src_port_rate", DoubleType()),
        StructField("dst_host_srv_diff_host_rate", DoubleType()),
        StructField("dst_host_serror_rate", DoubleType()),
        StructField("dst_host_srv_serror_rate", DoubleType()),
        StructField("dst_host_rerror_rate", DoubleType()),
        StructField("dst_host_srv_rerror_rate", DoubleType()),
        StructField("attack", StringType())
    ])
    raw_dataframe = spark.read.schema(schema).csv(input_path, header=False)
    feature_columns = raw_dataframe.columns
    feature_columns.remove("attack")

    label_indexer = StringIndexer(inputCol="attack", outputCol=LABEL_COLUMN)
    feature_vector_assembler = VectorAssembler(inputCols=feature_columns, outputCol=FEATURE_VECTOR_COLUMN)
    decision_tree = DecisionTreeClassifier(labelCol=LABEL_COLUMN, featuresCol=FEATURE_VECTOR_COLUMN)
    pipeline = Pipeline(stages = [label_indexer, feature_vector_assembler, decision_tree])

    training_data = [None] * num_runs
    test_data = [None] * num_runs
    # Split the data into training and test sets (30% held out for testing)
    # Make sure each split has a different, deterministic seed.
    for i in range(num_runs):
        training_data[i], test_data[i] = raw_dataframe.randomSplit([0.7, 0.3], seed=RANDOM_SEED + i)

    # Train num_runs models
    trained_models = []
    for train in training_data:
        trained_models.append(pipeline.fit(train))

    print(f"DecisionTree Structure:\n{trained_models[0].stages[-1].toDebugString}")
    
    # Predict the results of the 10 trees.
    train_predictions = []
    for (model, train) in zip(trained_models, training_data):
        train_predictions.append(model.transform(train))

    predictions = []
    for (model, test) in zip(trained_models, test_data):
        predictions.append(model.transform(test))

    # Print some output, just because.
    predictions[0].select(LABEL_COLUMN, PREDICTION_COLUMN).show(5)

    # Report the 10 results.
    # remove max(Accuracy), min(accuracy), average(accuracy) std_dev(accuracy)
    # Get the accuracies.
    evaluator = MulticlassClassificationEvaluator(labelCol=LABEL_COLUMN,
                                                  predictionCol=PREDICTION_COLUMN,
                                                  metricName="accuracy")
    train_accuracies = []
    for prediction in train_predictions:
        train_accuracies.append(evaluator.evaluate(prediction))
    # Python _sometimes_ has type safety. But only _sometimes_
    train_accuracy_max = float(numpy.max(train_accuracies))
    train_accuracy_min = float(numpy.min(train_accuracies))
    train_accuracy_average = float(numpy.average(train_accuracies))
    train_accuracy_stddev = float(numpy.std(train_accuracies))

    test_accuracies = []
    for prediction in predictions:
        test_accuracies.append(evaluator.evaluate(prediction))
    accuracy_max = float(numpy.max(test_accuracies))
    accuracy_min = float(numpy.min(test_accuracies))
    accuracy_average = float(numpy.average(test_accuracies))
    accuracy_stddev = float(numpy.std(test_accuracies))

    data = [
        ("train", train_accuracy_max, train_accuracy_min, train_accuracy_average, train_accuracy_stddev),
        ("test", accuracy_max, accuracy_min, accuracy_average, accuracy_stddev)
    ]
    out_schema = StructType([
        StructField("name", StringType(), False),
        StructField("max", DoubleType(), False),
        StructField("min", DoubleType(), False),
        StructField("average", DoubleType(), False),
        StructField("stddev", DoubleType(), False)
    ])
    output_dataframe = spark.createDataFrame(data, schema=out_schema)

    # output_dataframe = spark.createDataFrame(data, schema=["max", "min", "average", "stddev"])
    output_dataframe.coalesce(1).write.mode("errorifexists").option("header", "true").csv(output_path)

    result_rows = []
    run = 0
    for (train, test, train_accuracy, test_accuracy ) in zip(training_data, test_data, train_accuracies, test_accuracies) :
        result_rows.append( (run + 1, RANDOM_SEED + run, train.count(), test.count(), train_accuracy, test_accuracy))
        run += 1

    result_schema = StructType([
        StructField("run", LongType(), False),
        StructField("seed", LongType(), False),
        StructField("train_count", LongType(), False),
        StructField("test_count", LongType(), False),
        StructField("train_accuracy", DoubleType(), False),
        StructField("test_accuracy", DoubleType(), False),
    ])
    results_df = spark.createDataFrame(result_rows, schema=result_schema)
    results_df.coalesce(1).write.mode("append").option("header", "true").csv(output_path)

    spark.stop()

if __name__ == "__main__":
    main()


