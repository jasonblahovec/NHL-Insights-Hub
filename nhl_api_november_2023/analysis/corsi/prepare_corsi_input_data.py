import pyspark.sql.functions as f
import pandas as pd
import argparse
import pyspark


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NHL Data Ingestion to GCP")
    parser.add_argument("--input_bucket_name", type=str, help="a GCS Bucket")
    parser.add_argument("--input_fs_plays", type=str, help="location of ingested NHL Plays output from ingest_nhl_plays")
    parser.add_argument("--input_fs_forwards", type=str, help="location of ingested NHL Forwards data from ingest_nhl_plays")
    parser.add_argument("--input_fs_defense", type=str, help="location of ingested NHL Deffense data from ingest_nhl_plays")
    parser.add_argument("--output_bucket_name", type=str, help="a GCS Bucket")
    parser.add_argument("--output_fs_plays", type=str, help="output location for partitoned plays")
    parser.add_argument("--output_fs_forwards", type=str, help="output location for partitoned forwards")
    parser.add_argument("--output_fs_defense", type=str, help="output location for partitoned defense")
    parser.add_argument("--hm_count", type=str, help="number of hashmod partitions")
    args = parser.parse_args()

    spark = pyspark.sql.SparkSession.builder \
        .appName("NHL Corsi Analysis") \
        .getOrCreate()
    
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")

    input_bucket_name = args.input_bucket_name
    input_fs_plays = args.input_fs_plays
    input_fs_forwards = args.input_fs_forwards
    input_fs_defense = args.input_fs_defense
    output_bucket_name = args.output_bucket_name
    output_fs_plays = args.output_fs_plays
    output_fs_forwards = args.output_fs_forwards
    output_fs_defense = args.output_fs_defense
    hm_count = args.hm_count

    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
    df_plays_input = spark.read.format("parquet").load(f"gs://{input_bucket_name}/{input_fs_plays}").withColumn("hashmod", f.expr(f"abs(hash(cast(game_id as float))) % {hm_count}"))
    df_forwards_input = spark.read.format("parquet").load(f"gs://{input_bucket_name}/{input_fs_forwards}").withColumn("hashmod", f.expr(f"abs(hash(cast(game_id as float))) % {hm_count}"))
    df_defense_input = spark.read.format("parquet").load(f"gs://{input_bucket_name}/{input_fs_defense}").withColumn("hashmod", f.expr(f"abs(hash(cast(game_id as float))) % {hm_count}"))

    df_plays_input.distinct().repartition('hashmod').write.format("parquet").save(f"gs://{output_bucket_name}/{output_fs_plays}", mode = 'overwrite')

    df_forwards_input.distinct().repartition('hashmod').write.format("parquet").save(f"gs://{output_bucket_name}/{output_fs_forwards}", mode = 'overwrite')

    df_defense_input.distinct().repartition('hashmod').write.format("parquet").save(f"gs://{output_bucket_name}/{output_fs_defense}", mode = 'overwrite')

    spark.stop()