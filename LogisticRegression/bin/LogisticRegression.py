from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, trim
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
import sys
import time



# Columns 1, 2, and 3 are categorical:
# protocol_type, service, flag.
NUM_FEATURES = 41

COLUMN_NAMES = [f"f{i}" for i in range(NUM_FEATURES)] + ["class_label"]

CATEGORICAL_COLS = ["f1", "f2", "f3"]
NUMERIC_COLS = [f"f{i}" for i in range(NUM_FEATURES) if f"f{i}" not in CATEGORICAL_COLS]


def parse_args():
    args = sys.argv[1:]

    if len(args) < 3:
        raise ValueError(
            "Expected: <num_runs> <input_path(s)> <output_dir>"
        )

    num_runs = int(args[0])
    output_dir = args[-1]
    input_paths = args[1:-1]

    return num_runs, input_paths, output_dir


def load_kdd_data(spark, input_paths):
    raw_df = (
        spark.read
        .option("header", "false")
        .option("inferSchema", "true")
        .csv(input_paths)
    )

    raw_column_count = len(raw_df.columns)
    print(f"Raw column count: {raw_column_count}")

    if raw_column_count != 42:
        raise ValueError(
            f"Expected 42 columns for KDD data, but found {raw_column_count}."
    )
    df = raw_df.toDF(*COLUMN_NAMES)

    # Convert class label into binary label:
    # normal. = 0, everything else = 1.
    df = df.withColumn(
    "label",
    when(trim(col("class_label")).isin("normal.", "normal"), 0.0).otherwise(1.0)
    )

    # Cast numeric features to double.
    for feature_col in NUMERIC_COLS:
        df = df.withColumn(feature_col, col(feature_col).cast("double"))

    return df


def build_pipeline():
    stages = []

    indexed_cols = []
    encoded_cols = []

    for cat_col in CATEGORICAL_COLS:
        indexed_col = f"{cat_col}_idx"
        encoded_col = f"{cat_col}_vec"

        indexer = StringIndexer(
            inputCol=cat_col,
            outputCol=indexed_col,
            handleInvalid="keep"
        )

        encoder = OneHotEncoder(
            inputCols=[indexed_col],
            outputCols=[encoded_col],
            handleInvalid="keep"
        )

        stages.append(indexer)
        stages.append(encoder)

        indexed_cols.append(indexed_col)
        encoded_cols.append(encoded_col)

    assembler = VectorAssembler(
        inputCols=NUMERIC_COLS + encoded_cols,
        outputCol="features",
        handleInvalid="skip"
    )

    lr = LogisticRegression(
        featuresCol="features",
        labelCol="label",
        predictionCol="prediction",
        maxIter=100,
        regParam=0.0,
        elasticNetParam=0.0,
        family="binomial"
    )

    stages.append(assembler)
    stages.append(lr)

    return Pipeline(stages=stages)


def evaluate_accuracy(predictions):
    evaluator = MulticlassClassificationEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="accuracy"
    )

    return evaluator.evaluate(predictions)


def main():
    num_runs, input_paths, output_dir = parse_args()

    spark = (
        SparkSession.builder
        .appName("LogisticRegression")
        .getOrCreate()
    )


    spark.sparkContext.setLogLevel("WARN")

    print("Loading KDD data...")
    df = load_kdd_data(spark, input_paths)

    print("Dataset checks:")

    print(f"Total rows: {df.count()}")

    print("Binary label counts:")
    df.groupBy("label").count().show()

    print("Original class label counts:")
    df.groupBy("class_label", "label").count().orderBy("class_label").show(100, truncate=False)

    total_rows = df.count()
    print(f"Rows loaded: {total_rows}")

    pipeline = build_pipeline()

    result_rows = []

    for run in range(num_runs):
        seed = run + 1

        print(f"Starting run {run + 1}/{num_runs} with seed={seed}")

        train_df, test_df = df.randomSplit([0.7, 0.3], seed=seed)

        start_time = time.time()

        model = pipeline.fit(train_df)

        train_predictions = model.transform(train_df)
        test_predictions = model.transform(test_df)

        train_accuracy = evaluate_accuracy(train_predictions)
        test_accuracy = evaluate_accuracy(test_predictions)

        elapsed_seconds = time.time() - start_time

        result_rows.append(
            (
                run + 1,
                seed,
                train_df.count(),
                test_df.count(),
                train_accuracy,
                test_accuracy,
                elapsed_seconds,
            )
        )

        print(
            f"Run {run + 1}: "
            f"train_accuracy={train_accuracy:.4f}, "
            f"test_accuracy={test_accuracy:.4f}, "
            f"time={elapsed_seconds:.2f}s"
        )

    results_df = spark.createDataFrame(
        result_rows,
        [
            "run",
            "seed",
            "train_count",
            "test_count",
            "train_accuracy",
            "test_accuracy",
            "elapsed_seconds",
        ]
    )

    (
        results_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(output_dir)
    )

    print(f"Wrote results to {output_dir}")

    spark.stop()


if __name__ == "__main__":
    main()
