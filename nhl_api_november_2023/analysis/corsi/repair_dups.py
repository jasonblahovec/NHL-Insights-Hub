import pyspark.sql.functions as f
import pandas as pd
import argparse
import pyspark


if __name__ == "__main__":
    spark = pyspark.sql.SparkSession.builder \
        .appName("dedupe_temp") \
        .getOrCreate()
    
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")

    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
    # df_plays_input = spark.read.format("parquet").load(f"gs://nhl_seasons/2021_2022/plays_with_onice").where(f.expr("game_id not between 2021020500 and 2021020900"))
    # df_forwards_input = spark.read.format("parquet").load(f"gs://nhl_seasons/2021_2022/raw_forwards").where(f.expr("game_id not between 2021020500 and 2021020900"))
    # df_defense_input = spark.read.format("parquet").load(f"gs://nhl_seasons/2021_2022/raw_defense").where(f.expr("game_id not between 2021020500 and 2021020900"))
    # df_goalies_input = spark.read.format("parquet").load(f"gs://nhl_seasons/2021_2022/raw_goalies").where(f.expr("game_id not between 2021020500 and 2021020900"))
    # df_rawplays = spark.read.format("parquet").load(f"gs://nhl_seasons/2021_2022/raw_plays").where(f.expr("game_id not between 2021020500 and 2021020900"))


    # df_plays_input.write.format("parquet").save("gs://nhl_seasons/2021_2022/temp/plays_with_onice")
    # df_forwards_input.write.format("parquet").save("gs://nhl_seasons/2021_2022/temp/raw_forwards")
    # df_defense_input.write.format("parquet").save("gs://nhl_seasons/2021_2022/temp/raw_defense")
    # df_goalies_input.write.format("parquet").save("gs://nhl_seasons/2021_2022/temp/raw_goalies")
    # df_rawplays.write.format("parquet").save("gs://nhl_seasons/2021_2022/temp/raw_plays")

    df_plays_input = spark.read.format("parquet").load(f"gs://nhl_seasons/2021_2022/temp/plays_with_onice").where(f.expr("game_id not between 2021020500 and 2021020900"))
    df_forwards_input = spark.read.format("parquet").load(f"gs://nhl_seasons/2021_2022/temp/raw_forwards").where(f.expr("game_id not between 2021020500 and 2021020900"))
    df_defense_input = spark.read.format("parquet").load(f"gs://nhl_seasons/2021_2022/temp/raw_defense").where(f.expr("game_id not between 2021020500 and 2021020900"))
    df_goalies_input = spark.read.format("parquet").load(f"gs://nhl_seasons/2021_2022/temp/raw_goalies").where(f.expr("game_id not between 2021020500 and 2021020900"))
    df_rawplays = spark.read.format("parquet").load(f"gs://nhl_seasons/2021_2022/temp/raw_plays").where(f.expr("game_id not between 2021020500 and 2021020900"))


    df_plays_input.write.format("parquet").save("gs://nhl_seasons/2021_2022/plays_with_onice")
    df_forwards_input.write.format("parquet").save("gs://nhl_seasons/2021_2022/raw_forwards")
    df_defense_input.write.format("parquet").save("gs://nhl_seasons/2021_2022/raw_defense")
    df_goalies_input.write.format("parquet").save("gs://nhl_seasons/2021_2022/raw_goalies")
    df_rawplays.write.format("parquet").save("gs://nhl_seasons/2021_2022/raw_plays")


    spark.stop()