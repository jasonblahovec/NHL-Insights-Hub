import pyspark.sql.functions as f
import pyspark
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NHL Data Ingestion to GCP")
    parser.add_argument("--input_bucket", type=str, help="a GCS Bucket")
    parser.add_argument("--output_bucket", type=str, help="a GCS Bucket")
    args = parser.parse_args()

    spark = pyspark.sql.SparkSession.builder \
        .appName("") \
        .getOrCreate()

    # Disabling Arrow due to configuration duplication in the template
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")

    # Reading the data from the specified Google Cloud Storage location
    df = spark.read.format("parquet").load("gs://{input_bucket}/2021_2022/plays_sparse_detail")
    df.repartition(10).write.format("parquet").mode("overwrite").save("gs://{output_bucket}/2021_2022/plays_sparse_detail")
    
    df1 = spark.read.format("parquet").load("gs://{input_bucket}/2022_2023/plays_sparse_detail")
    df1.repartition(10).write.format("parquet").mode("overwrite").save("gs://{output_bucket}/2022_2023/plays_sparse_detail")

    df2 = spark.read.format("parquet").load("gs://{input_bucket}/2021_2022/corsi")
    df2.repartition(10).write.format("parquet").mode("overwrite").save("gs://{output_bucket}/2021_2022/corsi")
    
    df3 = spark.read.format("parquet").load("gs://{input_bucket}/2022_2023/corsi")
    df3.repartition(10).write.format("parquet").mode("overwrite").save("gs://{output_bucket}/2022_2023/corsi")

    df4 = spark.read.format("parquet").load("gs://{input_bucket}/2021_2022/raw_forwards")
    df4.repartition(10).write.format("parquet").mode("overwrite").save("gs://{output_bucket}/2021_2022/raw_forwards")
    
    df5 = spark.read.format("parquet").load("gs://{input_bucket}/2022_2023/raw_forwards")
    df5.repartition(10).write.format("parquet").mode("overwrite").save("gs://{output_bucket}/2022_2023/raw_forwards")

    df6 = spark.read.format("parquet").load("gs://{input_bucket}/2021_2022/raw_defense")
    df6.repartition(10).write.format("parquet").mode("overwrite").save("gs://{output_bucket}/2021_2022/raw_defense")
    
    df7 = spark.read.format("parquet").load("gs://{input_bucket}/2022_2023/raw_defense")
    df7.repartition(10).write.format("parquet").mode("overwrite").save("gs://{output_bucket}/2022_2023/raw_defense")

    df8 = spark.read.format("parquet").load("gs://{input_bucket}/2021_2022/raw_goalies")
    df8.repartition(10).write.format("parquet").mode("overwrite").save("gs://{output_bucket}/2021_2022/raw_goalies")
    
    df9 = spark.read.format("parquet").load("gs://{input_bucket}/2022_2023/raw_goalies")
    df9.repartition(10).write.format("parquet").mode("overwrite").save("gs://{output_bucket}/2022_2023/raw_goalies")

    df10 = spark.read.format("csv").load("gs://{input_bucket}/ea_goaltending.csv", header = True)
    df10.repartition(10).write.format("parquet").mode("overwrite").save("gs://{output_bucket}/ea_goaltending")
    
    df11 = spark.read.format("csv").load("gs://{input_bucket}/ea_skaters.csv", header = True)
    df11.repartition(10).write.format("parquet").mode("overwrite").save("gs://{output_bucket}/ea_skaters")

    spark.stop()
