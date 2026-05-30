from pyspark.sql import SparkSession
import sys


def main():
    spark = (
        SparkSession.builder
        .appName("LogisticRegressionDummy")
        .getOrCreate()
    )

    args = sys.argv[1:]

    if len(args) < 3:
        raise ValueError(
            "Expected: <num_runs> <input_path(s)> <output_dir>"
        )

    num_runs = int(args[0])
    output_dir = args[-1]
    input_paths = args[1:-1]

    print("Dummy LogisticRegression Spark job started.")
    print(f"num_runs = {num_runs}")
    print(f"input_paths = {input_paths}")
    print(f"output_dir = {output_dir}")

    # Read the first input file just to prove Spark can access it.
    input_df = spark.read.text(input_paths[0])

    row_count = input_df.count()
    first_row = input_df.first()[0] if row_count > 0 else ""

    result_rows = [
        ("status", "ok"),
        ("num_runs", str(num_runs)),
        ("num_input_paths", str(len(input_paths))),
        ("first_input_path", input_paths[0]),
        ("input_row_count", str(row_count)),
        ("first_row_preview", first_row[:80]),
    ]

    result_df = spark.createDataFrame(result_rows, ["field", "value"])

    (
        result_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(output_dir)
    )

    spark.stop()


if __name__ == "__main__":
    main()