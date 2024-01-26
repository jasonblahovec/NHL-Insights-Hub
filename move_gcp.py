import pyspark.sql.functions as f
import pyspark


if __name__ == "__main__":

    spark = pyspark.sql.SparkSession.builder \
        .appName("") \
        .getOrCreate()
    
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")

    tables = [
        [f"gs://nhl-api-output/output_plays/plays_sparse_detail", f"gs://nhl-seasons/2022_2023/plays_sparse_detail"]
        ,[f"gs://nhl-api-output/output_plays/plays_with_onice", f"gs://nhl-seasons/2022_2023/plays_with_onice"]
        ,[f"gs://nhl-api-output/output_plays/raw_plays", f"gs://nhl-seasons/2022_2023/raw_plays"]
        ,[f"gs://nhl-api-output/output_plays/corsi/input_parquet/partitioned_forwards", f"gs://nhl-seasons/2022_2023/corsi/input_parquet/partitioned_forwards"]
        ,[f"gs://nhl-api-output/output_plays/corsi/input_parquet/partitioned_defense", f"gs://nhl-seasons/2022_2023/corsi/input_parquet/partitioned_defense"]
        ,[f"gs://nhl-api-output/output_plays/corsi/input_parquet/partitioned_goalies", f"gs://nhl-seasons/2022_2023/corsi/input_parquet/partitioned_goalies"]
        ,[f"gs://nhl-api-output/output_plays/corsi/full_season/team_player_game_detail", f"gs://nhl-seasons/2022_2023/corsi/full_season/team_player_game_detail"]
        ,[f"gs://nhl-api-output/output_plays/corsi/full_season/team_player_summary", f"gs://nhl-seasons/2022_2023/corsi/full_season/team_player_summary"]
    ]

    for tbs in tables:
        spark.read.format("parquet").load(tbs[0]) \
            .write.format("parquet").save(tbs[1], mode = 'overwrite')

    spark.stop()


