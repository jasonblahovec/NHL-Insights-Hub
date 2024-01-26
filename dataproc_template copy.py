import pyspark.sql.functions as f
import pyspark


if __name__ == "__main__":

    spark = pyspark.sql.SparkSession.builder \
        .appName("") \
        .getOrCreate()
    
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")

    spark.stop()