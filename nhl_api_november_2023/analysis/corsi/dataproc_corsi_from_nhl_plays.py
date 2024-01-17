import pyspark.sql.functions as f
import pandas as pd
import argparse
import pyspark

class PlayerGameCorsi():

    def get_teams(self):
        self.teams_key = self.df_skater_toi.groupBy("side").agg( \
            f.countDistinct('game_id').alias("games_played") \
                ,f.sum('toi_s').alias("toi_s") \
                    ,(f.sum('toi_s')/f.countDistinct('game_id')).alias('toipg')
                )
        # self.teams_key.display()

    def set_team(self,_val):
        self.player_team = _val

    def show_players(self):
        self.team_players_key = self.df_skater_toi.where(f.expr(f"side = '{self.player_team}'")).groupBy("playerid","side",f.expr("name['default'] as name")).agg( \
            f.countDistinct('game_id').alias("games_played") \
                ,f.sum('toi_s').alias("toi_s") \
                    ,(f.sum('toi_s')/f.countDistinct('game_id')).alias('toipg')
                )
        
        # self.team_players_key.display()

    def set_player_id(self,_val):
        self.player_id = _val

    def __init__(self, fs_plays, fs_forwards, fs_defense):
        self.fs_plays = fs_plays
        self.fs_forwards = fs_forwards
        self.fs_defense = fs_defense

        # Input Files to DF:
        self.df_plays = spark.read.format("parquet").load(f"gs://{bucket_name}/{self.fs_plays}")
        self.df_forwards = spark.read.format("parquet").load(f"gs://{bucket_name}/{self.fs_forwards}")
        self.df_defense = spark.read.format("parquet").load(f"gs://{bucket_name}/{self.fs_defense}")

        self.df_plays.createOrReplaceTempView('nhl_plays')

        self.df_skater_toi = self.query_player_timeonice_plusminus(self.df_forwards, self.df_defense)

    @staticmethod
    def query_player_timeonice_plusminus(df_forwards, df_defense):
        df_forwards_toi = df_forwards[['game_id','playerid', 'side', 'name','plusminus', 'powerplaytoi', 'shorthandedtoi', 'toi']] \
            .withColumn("toi_s", f.expr("(split(toi,':')[0]*60)+split(toi,':')[1]")) \
                .withColumn("powerplaytoi_s", f.expr("(split(powerplaytoi,':')[0]*60)+split(powerplaytoi,':')[1]")) \
                .withColumn("shorthandedtoi_s", f.expr("(split(shorthandedtoi,':')[0]*60)+split(shorthandedtoi,':')[1]")) \
                    .withColumn("evenstrengthtoi_s",f.expr("toi_s - powerplaytoi_s - shorthandedtoi_s"))


        df_defense_toi = df_defense[['game_id','playerid', 'side', 'name','plusminus', 'powerplaytoi', 'shorthandedtoi', 'toi']] \
            .withColumn("toi_s", f.expr("(split(toi,':')[0]*60)+split(toi,':')[1]")) \
                .withColumn("powerplaytoi_s", f.expr("(split(powerplaytoi,':')[0]*60)+split(powerplaytoi,':')[1]")) \
                .withColumn("shorthandedtoi_s", f.expr("(split(shorthandedtoi,':')[0]*60)+split(shorthandedtoi,':')[1]")) \
                    .withColumn("evenstrengthtoi_s",f.expr("toi_s - powerplaytoi_s - shorthandedtoi_s"))

        return df_forwards_toi.unionByName(df_defense_toi)

    @staticmethod
    def player_corsi_by_game(df_plays, player_team, player_id):
        def get_player_corsi(player_team, player_id):
            df_corsi_base = spark.sql(f"""
                                    SELECT 
                                        '{player_team}' as for_team
                                        ,game_id
                                        ,away_team, home_team, type, shooter_team, home_shooter
                                        ,count(1) as count
                                        FROM nhl_plays
                                        WHERE (away_team = '{player_team}' OR home_team = '{player_team}')
                                        AND type IN ('GOAL', 'SHOT', 'BLOCK', 'MISS')
                                        AND (ARRAY_CONTAINS(away_onice_ids, '{player_id}') or ARRAY_CONTAINS(home_onice_ids, '{player_id}'))
                                        group by 
                                        game_id
                                        ,away_team, home_team, type, shooter_team, home_shooter
                                        order by 
                                        game_id, type""").withColumn("for_against", f.expr("case when for_team = shooter_team then 'for' else 'against' end"))
            df_corsi_base.createOrReplaceTempView('corsi_base')

            return spark.sql("""
                                    SELECT
                                        game_id,
                                        SUM(CASE WHEN type = 'SHOT' AND for_against = 'for' THEN count ELSE 0 END) AS SHOTS_FOR,
                                        SUM(CASE WHEN type = 'SHOT' AND for_against = 'against' THEN count ELSE 0 END) AS SHOTS_AGAINST,
                                        SUM(CASE WHEN type = 'GOAL' AND for_against = 'for' THEN count ELSE 0 END) AS GOALS_FOR,
                                        SUM(CASE WHEN type = 'GOAL' AND for_against = 'against' THEN count ELSE 0 END) AS GOALS_AGAINST,
                                        SUM(CASE WHEN type = 'MISS' AND for_against = 'for' THEN count ELSE 0 END) AS MISSES_FOR,
                                        SUM(CASE WHEN type = 'MISS' AND for_against = 'against' THEN count ELSE 0 END) AS MISSES_AGAINST,
                                        SUM(CASE WHEN type = 'BLOCK' AND for_against = 'for' THEN count ELSE 0 END) AS BLOCKS_FOR,
                                        SUM(CASE WHEN type = 'BLOCK' AND for_against = 'against' THEN count ELSE 0 END) AS BLOCKS_AGAINST
                                    FROM corsi_base
                                    GROUP BY game_id
                                """).withColumn("corsi_for",f.expr("(shots_for+goals_for+misses_for+blocks_for)")) \
                                .withColumn("corsi_against",f.expr("(shots_against+goals_against+misses_against+blocks_against)")) \
                                    .withColumn("corsi",f.expr('corsi_for-corsi_against')) \
                                    .withColumn("corsi_for_pct", f.expr("corsi_for/(corsi_for+corsi_against)"))
            

        def get_relative_corsi(player_team, player_id):
            df_rel_corsi_base = spark.sql(f"""
                                SELECT 
                                    '{player_team}' as for_team
                                    ,game_id
                                    ,away_team, home_team, type, shooter_team, home_shooter
                                    ,count(1) as count
                                    FROM nhl_plays
                                    WHERE (away_team = '{player_team}' OR home_team = '{player_team}')
                                    AND type IN ('GOAL', 'SHOT', 'BLOCK', 'MISS')
                                    AND (NOT ARRAY_CONTAINS(away_onice_ids, '{player_id}') and NOT ARRAY_CONTAINS(home_onice_ids, '{player_id}'))
                                    group by 
                                    game_id
                                    ,away_team, home_team, type, shooter_team, home_shooter
                                    order by 
                                    game_id, type""").withColumn("for_against", f.expr("case when for_team = shooter_team then 'for' else 'against' end"))


            df_rel_corsi_base.createOrReplaceTempView('rel_corsi_base')

            return spark.sql("""
                                    SELECT
                                        game_id,
                                        SUM(CASE WHEN type = 'SHOT' AND for_against = 'for' THEN count ELSE 0 END) AS SHOTS_FOR,
                                        SUM(CASE WHEN type = 'SHOT' AND for_against = 'against' THEN count ELSE 0 END) AS SHOTS_AGAINST,
                                        SUM(CASE WHEN type = 'GOAL' AND for_against = 'for' THEN count ELSE 0 END) AS GOALS_FOR,
                                        SUM(CASE WHEN type = 'GOAL' AND for_against = 'against' THEN count ELSE 0 END) AS GOALS_AGAINST,
                                        SUM(CASE WHEN type = 'MISS' AND for_against = 'for' THEN count ELSE 0 END) AS MISSES_FOR,
                                        SUM(CASE WHEN type = 'MISS' AND for_against = 'against' THEN count ELSE 0 END) AS MISSES_AGAINST,
                                        SUM(CASE WHEN type = 'BLOCK' AND for_against = 'for' THEN count ELSE 0 END) AS BLOCKS_FOR,
                                        SUM(CASE WHEN type = 'BLOCK' AND for_against = 'against' THEN count ELSE 0 END) AS BLOCKS_AGAINST
                                    FROM rel_corsi_base
                                    GROUP BY game_id
                                """).withColumn("rel_corsi_for",f.expr("(shots_for+goals_for+misses_for+blocks_for)")) \
                                .withColumn("rel_corsi_against",f.expr("(shots_against+goals_against+misses_against+blocks_against)")) \
                                    .withColumn("rel_corsi",f.expr('rel_corsi_for-rel_corsi_against')) \
                                    .withColumn("rel_corsi_for_pct", f.expr("rel_corsi_for/(rel_corsi_for+rel_corsi_against)"))

        df_player_corsi = get_player_corsi(player_team, player_id)
        df_relative_corsi = get_relative_corsi(player_team, player_id)

        df_corsi_out = df_player_corsi[['game_id','corsi_for', 'corsi_against','corsi','corsi_for_pct', 'goals_for', 'goals_against']].join(        
        df_relative_corsi[['game_id','rel_corsi_for', 'rel_corsi_against','rel_corsi','rel_corsi_for_pct']],"game_id", "inner") \
            .withColumn("corsi_for_relative_pct", f.expr("corsi_for_pct - rel_corsi_for_pct"))

        return df_corsi_out

    def run_analysis(self):
        # Create a reference by game_id and player_id of time on ice and plus minus, to be appended to final output for Corsi/60.
        df_skater_toi = self.query_player_timeonice_plusminus(self.df_forwards, self.df_defense)

        # Run Corsi Analysis for request player and team
        df_corsi_out = self.player_corsi_by_game(self.df_plays, self.player_team, self.player_id)

        # Join Corsi Output to TOI info for _per_60 calculation:
        df_corsi_final = df_corsi_out.join(df_skater_toi.where(f.expr(f"playerid = {self.player_id}")),"game_id","inner") \
            .withColumn("corsi_for_per_es60", f.expr("corsi_for *(3600/evenstrengthtoi_s)")) \
            .withColumn("corsi_against_per_es60", f.expr("corsi_against *(3600/evenstrengthtoi_s)")) \
            .withColumn("corsi_per_es60", f.expr("corsi_for_per_es60-corsi_against_per_es60"))
            
        return df_corsi_final.withColumn("player_id", f.lit(self.player_id))
    
    def run_team_analysis(self):        
        out_records = []
        for i,player_record in enumerate(self.team_players_key.collect()[:]):
            print(player_record.playerid, player_record.name, player_record.games_played, player_record.toipg)
            self.set_player_id(player_record.playerid)
            df_player_game_corsi = self.run_analysis()
            if i==0:
                df_output = df_player_game_corsi
            else:
                df_output = df_output.union(df_player_game_corsi)
            rdd_out = df_player_game_corsi.agg(
                f.avg('corsi_for').alias('corsi_for')
                ,f.avg('corsi_against').alias('corsi_against')
                ,f.avg('corsi').alias('corsi')
                ,f.avg('corsi_for_pct').alias('corsi_for_pct')
                ,f.avg('goals_for').alias('goals_for')
                ,f.avg('goals_against').alias('goals_against')
                ,f.avg('rel_corsi_for').alias('rel_corsi_for')
                ,f.avg('rel_corsi_against').alias('rel_corsi_against')
                ,f.avg('rel_corsi').alias('rel_corsi')
                ,f.avg('rel_corsi_for_pct').alias('rel_corsi_for_pct')
                ,f.avg('corsi_for_relative_pct').alias('corsi_for_relative_pct')
                ,f.sum('plusminus').alias('plusminus')
                ,f.avg('toi_s').alias('toi_s')
                ,f.avg('evenstrengthtoi_s').alias('evenstrengthtoi_s')
                ,f.avg('corsi_for_per_es60').alias('corsi_for_per_es60')
                ,f.avg('corsi_against_per_es60').alias('corsi_against_per_es60')
                ,f.avg('corsi_per_es60').alias('average_corsi_per_60')
                ).collect()
            out_records = out_records + [[
                player_record.playerid
                , player_record.name
                , player_record.games_played
                , player_record.toipg
                , rdd_out[0].corsi_for
                , rdd_out[0].corsi_against
                , rdd_out[0].corsi
                , rdd_out[0].corsi_for_pct
                , rdd_out[0].goals_for
                , rdd_out[0].goals_against
                , rdd_out[0].rel_corsi_for
                , rdd_out[0].rel_corsi_against
                , rdd_out[0].rel_corsi
                , rdd_out[0].rel_corsi_for_pct
                , rdd_out[0].corsi_for_relative_pct
                , rdd_out[0].plusminus
                , rdd_out[0].toi_s
                , rdd_out[0].evenstrengthtoi_s
                , rdd_out[0].corsi_for_per_es60
                , rdd_out[0].corsi_against_per_es60
                , rdd_out[0].average_corsi_per_60
                ]]
            

        return spark.createDataFrame(pd.DataFrame(out_records, columns = [
                'playerid'
                , 'name'
                , 'games_played'
                , 'toipg'
                , 'corsi_for'
                , 'corsi_against'
                , 'corsi'
                , 'corsi_for_pct'
                , 'goals_for'
                , 'goals_against'
                , 'rel_corsi_for'
                , 'rel_corsi_against'
                , 'rel_corsi'
                , 'rel_corsi_for_pct'
                , 'corsi_for_relative_pct'
                , 'plusminus'
                , 'toi_s'
                , 'evenstrengthtoi_s'
                , 'corsi_for_per_es60'
                , 'corsi_against_per_es60'
                , 'average_corsi_per_60'
        ])).withColumn("team", f.lit(self.player_team))

    def run_all_team_analysis(self):
        for _team_i, team in enumerate(self.teams_key.collect()[0:2]):
            self.set_team(team.side)

            # must update df players key inside this function.:
            self.show_players()

            if _team_i==0:
                df_all_team_output = self.run_team_analysis()
            else:
                df_all_team_output = df_all_team_output.union(self.run_team_analysis())
        return df_all_team_output


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NHL Data Ingestion to GCP")
    parser.add_argument("--bucket_name", type=str, help="a GCS Bucket")
    parser.add_argument("--fs_plays", type=str, help="location of ingested NHL Plays output from ingest_nhl_plays")
    parser.add_argument("--fs_forwards", type=str, help="location of ingested NHL Forwards data from ingest_nhl_plays")
    parser.add_argument("--fs_defense", type=str, help="location of ingested NHL Deffense data from ingest_nhl_plays")
    parser.add_argument("--output_location", type=str, help="location of ingested NHL Deffense data from ingest_nhl_plays")
    args = parser.parse_args()

    spark = pyspark.sql.SparkSession.builder \
        .appName("NHL Corsi Analysis") \
        .getOrCreate()
    
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")

    bucket_name = args.bucket_name
    fs_plays = args.fs_plays
    fs_forwards = args.fs_forwards
    fs_defense = args.fs_defense
    output_location = args.output_location

    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")

    
    corsi = PlayerGameCorsi(fs_plays, fs_forwards, fs_defense)
    df_all_team_result = corsi.run_all_team_analysis()

    # Save the result as a Parquet file
    df_all_team_result.write.format("parquet").save(f"gs://{bucket_name}/{output_location}/plays_sparse_detail", mode = 'overwrite')

    spark.stop()
