from pyspark.sql import SparkSession
import sys

def main():
    if len(sys.argv) != 3:
        print("Usage: SparkWordCount.py <input_file> <output_dir>")
        sys.exit(-1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]

    spark = SparkSession.builder.appName("SparkWordCount").getOrCreate()
    sc = spark.sparkContext

    lines = sc.textFile(input_path)

    word_counts = (
        lines
        .flatMap(lambda line: line.split(" "))
        .map(lambda word: (word, 1))
        .reduceByKey(lambda a, b: a + b)
    )

    # Optional formatting like the Java commented block
    # formatted = word_counts.map(lambda t: f"{t[0]} appears {t[1]}")

    word_counts.repartition(1).saveAsTextFile(output_path)
    spark.stop()

if __name__ == "__main__":
    main()
