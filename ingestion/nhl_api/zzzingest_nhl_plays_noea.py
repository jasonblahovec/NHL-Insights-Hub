# Databricks notebook source
# MAGIC %md # Parse HTML INPUT

# COMMAND ----------

import requests
import re
import os
import pandas as pd
from bs4 import BeautifulSoup
import pyspark.sql.functions as f
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
from pyspark.sql import Row
from pyspark.sql.types import ArrayType, IntegerType, StringType
from pyspark.sql.types import *

def append_nonea_onice_info(df_plays, df_forwards, df_defense, df_goalies):
    # Define a UDF to split the array into odd-indexed and even-indexed elements
    def split_array_udf(arr):
        odd_elements = arr[::2]  # Selecting elements with odd indexes
        even_elements = arr[1::2]  # Selecting elements with even indexes
        return (odd_elements, even_elements)
        

    # Register the UDF
    split_array = udf(split_array_udf, ArrayType(ArrayType(StringType())))

    # Use the UDF to create new columns
    df = df_plays.withColumn("away_onice_numbers", split_array("away_onice_array1")[0]) \
        .withColumn("away_onice_positions", split_array("away_onice_array1")[1]) \
            .withColumn("home_onice_numbers", split_array("home_onice_array1")[0]) \
        .withColumn("home_onice_positions", split_array("home_onice_array1")[1])
    # Define a UDF to concatenate even and odd indexed elements
    def concatenate_elements_udf(odd_elements, even_elements):
        concatenated_elements = [str(odd) + str(even) for odd, even in zip(odd_elements, even_elements)]
        return concatenated_elements

    # Register the UDF
    concatenate_elements = udf(concatenate_elements_udf, ArrayType(StringType()))

    # Use the UDF to create a new column with concatenated elements
    df = df.withColumn("away_onice_codes", concatenate_elements("away_onice_positions", "away_onice_numbers")) \
        .withColumn("home_onice_codes", concatenate_elements("home_onice_positions", "home_onice_numbers")) \
            .withColumn("game_id", f.expr("cast(game_id as int)"))

    df_codes = df.drop('away_onice_array1').drop('home_onice_array1').drop('away_onice_numbers').drop('home_onice_numbers').drop('away_onice_positions').drop('home_onice_positions') \
        .withColumn("game_id", f.expr("cast(game_id as int)"))

    away_exploded_df = df.select([x for x in df_codes.columns if x not in ['away_onice_codes','home_onice_codes']]+[ f.explode_outer("away_onice_codes").alias("away_onice_code")])
    home_exploded_df = df.select([x for x in df_codes.columns if x not in ['away_onice_codes','home_onice_codes']]+[ f.explode_outer("home_onice_codes").alias("home_onice_code")])

    df_forwards_key = df_forwards.withColumn("code",f.expr("concat(position,sweaterNumber)"))[['game_id','side','code','playerId',"default_last_name","default_first_init"]]
    df_defense_key = df_defense.withColumn("code",f.expr("concat(position,sweaterNumber)"))[['game_id','side','code','playerId',"default_last_name","default_first_init"]]
    df_goalies_key = df_goalies.withColumn("code",f.expr("concat(position,sweaterNumber)"))[['game_id','side','code','playerId',"default_last_name","default_first_init"]]

    df_ea = df_forwards_key.union(df_defense_key.union(df_goalies_key))
    home_joined_df = home_exploded_df.join(
        df_ea,
        "game_id",
        "left"
    ).where(f.expr("side = home_team and (code = home_onice_code or home_onice_code is null)"))

    away_joined_df = away_exploded_df.join(
        df_ea,
        "game_id",
        "left"
    ).where(f.expr("side = away_team and (code = away_onice_code or away_onice_code is null)"))

    away_missing_info_cleanup = away_joined_df.where(f.expr("away_onice_code is null")) \
        .withColumn("side", f.expr("cast(null as string)")) \
            .withColumn("code", f.expr("cast(null as string)")) \
                .withColumn("playerId", f.expr("cast(null as string)")) \
                    .withColumn("default_last_name", f.expr("cast(null as string)")) \
                        .withColumn("default_first_init", f.expr("cast(null as string)")).distinct()

    home_missing_info_cleanup = home_joined_df.where(f.expr("home_onice_code is null")) \
        .withColumn("side", f.expr("cast(null as string)")) \
            .withColumn("code", f.expr("cast(null as string)")) \
                .withColumn("playerId", f.expr("cast(null as string)")) \
                    .withColumn("default_last_name", f.expr("cast(null as string)")) \
                        .withColumn("default_first_init", f.expr("cast(null as string)")).distinct()

    away_completed_info = away_joined_df.where(f.expr("away_onice_code is not null"))
    home_completed_info = home_joined_df.where(f.expr("home_onice_code is not null"))

    away_joined_df_dedupe = away_completed_info.union(away_missing_info_cleanup)
    home_joined_df_dedupe = home_completed_info.union(home_missing_info_cleanup)

    home_pivoted_df = home_joined_df_dedupe.groupBy(home_joined_df_dedupe.columns[0:12]).agg( \
        f.collect_list("home_onice_code").alias('home_onice_code') \
            ,f.collect_list("default_last_name").alias('home_onice_names') \
            ,f.collect_list("playerId").alias('home_onice_ids')\
            ,f.collect_list("default_first_init").alias('home_onice_firstinit') \
            )
    away_pivoted_df = away_joined_df_dedupe.groupBy(away_joined_df_dedupe.columns[0:12]).agg( \
        f.collect_list("away_onice_code").alias('away_onice_code') \
            ,f.collect_list("default_last_name").alias('away_onice_names') \
            ,f.collect_list("playerId").alias('away_onice_ids')\
            ,f.collect_list("default_first_init").alias('away_onice_firstinit') \
            )

    return home_pivoted_df.join(away_pivoted_df,away_joined_df.columns[0:12], "full")

def zzz_append_onice_info(df_plays, df_forwards, df_defense, df_goalies):
    #     # Define a UDF to split the array into odd-indexed and even-indexed elements
    #     def split_array_udf(arr):
    #         odd_elements = arr[::2]  # Selecting elements with odd indexes
    #         even_elements = arr[1::2]  # Selecting elements with even indexes
    #         return (odd_elements, even_elements)
            

    #     # Register the UDF
    #     split_array = udf(split_array_udf, ArrayType(ArrayType(StringType())))

    #     # Use the UDF to create new columns
    #     df = df_plays.withColumn("away_onice_numbers", split_array("away_onice_array1")[0]) \
    #         .withColumn("away_onice_positions", split_array("away_onice_array1")[1]) \
    #             .withColumn("home_onice_numbers", split_array("home_onice_array1")[0]) \
    #         .withColumn("home_onice_positions", split_array("home_onice_array1")[1])



    #     # Define a UDF to concatenate even and odd indexed elements
    #     def concatenate_elements_udf(odd_elements, even_elements):
    #         concatenated_elements = [str(odd) + str(even) for odd, even in zip(odd_elements, even_elements)]
    #         return concatenated_elements

    #     # Register the UDF
    #     concatenate_elements = udf(concatenate_elements_udf, ArrayType(StringType()))

    #     # Use the UDF to create a new column with concatenated elements
    #     df = df.withColumn("away_onice_codes", concatenate_elements("away_onice_positions", "away_onice_numbers")) \
    #         .withColumn("home_onice_codes", concatenate_elements("home_onice_positions", "home_onice_numbers")) \
    #             .withColumn("game_id", f.expr("cast(game_id as int)"))
    #     df_codes = df.drop('away_onice_array1').drop('home_onice_array1').drop('away_onice_numbers').drop('home_onice_numbers').drop('away_onice_positions').drop('home_onice_positions') \
    #         .withColumn("game_id", f.expr("cast(game_id as int)"))

    #     away_exploded_df = df.select([x for x in df_codes.columns if x not in ['away_onice_codes','home_onice_codes']]+[ f.explode("away_onice_codes").alias("away_onice_code")])
    #     home_exploded_df = df.select([x for x in df_codes.columns if x not in ['away_onice_codes','home_onice_codes']]+[ f.explode("home_onice_codes").alias("home_onice_code")])

    #     df_forwards_key = df_forwards.withColumn("code",f.expr("concat(position,sweaterNumber)"))[['game_id','side','code','playerId',"default_last_name","default_first_init"]]
    #     df_defense_key = df_defense.withColumn("code",f.expr("concat(position,sweaterNumber)"))[['game_id','side','code','playerId',"default_last_name","default_first_init"]]
    #     df_goalies_key = df_goalies.withColumn("code",f.expr("concat(position,sweaterNumber)"))[['game_id','side','code','playerId',"default_last_name","default_first_init"]]

    #     df_ea = df_forwards_key.union(df_defense_key.union(df_goalies_key))
    #     home_joined_df = home_exploded_df.join(
    #         df_ea,
    #         "game_id",
    #         "left"
    #     ).where(f.expr("side = home_team and code = home_onice_code"))

    #     away_joined_df = away_exploded_df.join(
    #         df_ea,
    #         "game_id",
    #         "left"
    #     ).where(f.expr("side = away_team and code = away_onice_code"))

    #     # Define the struct type
    #     ea_stats_array_g_schema = StructType([
    #         StructField('glove_high', StringType(), True),
    #         StructField('glove_low', StringType(), True),
    #         StructField('_5_hole', StringType(), True),
    #         StructField('stick_high', StringType(), True),
    #         StructField('stick_low', StringType(), True),
    #         StructField('shot_recovery', StringType(), True),
    #         StructField('aggression', StringType(), True),
    #         StructField('agility', StringType(), True),
    #         StructField('speed', StringType(), True),
    #         StructField('positioning', StringType(), True),
    #         StructField('breakaway', StringType(), True),
    #         StructField('vision', StringType(), True),
    #         StructField('poke_check', StringType(), True),
    #         StructField('rebound_control', StringType(), True),
    #         StructField('passing', StringType(), True)
    #     ])

    #     ea_stats_array_s_schema = StructType([StructField('deking', StringType(), True), StructField('hand_eye', StringType(), True), StructField('passing', StringType(), True), StructField('puck_control', StringType(), True), StructField('slap_shot_accuracy', StringType(), True), StructField('slap_shot_power', StringType(), True), StructField('wrist_shot_accuracy', StringType(), True), StructField('wrist_shot_power', StringType(), True), StructField('acceleration', StringType(), True), StructField('agility', StringType(), True), StructField('balance', StringType(), True), StructField('endurance', StringType(), True), StructField('speed', StringType(), True), StructField('discipline', StringType(), True), StructField('off_awareness', StringType(), True), StructField('aggression', StringType(), True), StructField('body_checking', StringType(), True), StructField('durability', StringType(), True), StructField('strength', StringType(), True), StructField('fighting_skill', StringType(), True), StructField('def_awareness', StringType(), True), StructField('faceoffs', StringType(), True), StructField('shot_blocking', StringType(), True), StructField('stick_checking', StringType(), True)])

    #     df_ea_skaters = spark.read.format("csv").load("/FileStore/NHL_PLAYS_DB/bq_results_20231206_191017_1701889832565.csv", header = True, sep = ',').where(f.expr("lower(card_type) = 'base'")).where(f.expr("gameyear = 'NHL23' and card_type = 'base'"))[['gameyear','team','player_first_name','player_last_name','player_id','overall','player_type','age',f.expr("(deking, hand_eye, passing, puck_control, slap_shot_accuracy, slap_shot_power, wrist_shot_accuracy, wrist_shot_power, acceleration, agility, balance, endurance, speed, discipline, off_awareness, aggression, body_checking, durability, strength, fighting_skill, def_awareness, faceoffs, shot_blocking, stick_checking) as ea_stats_array")]]
    #     df_ea_goaltenders = spark.read.format("csv").load("/FileStore/NHL_PLAYS_DB/bq_results_20231206_191052_1701889859685.csv", header = True, sep = ',').where(f.expr("lower(card_type) = 'base'")).where(f.expr("gameyear = 'NHL23' and card_type = 'base'"))[['gameyear','team','player_first_name','player_last_name','player_id','overall',f.col('goalie_type').alias('player_type'),'age',f.expr("(glove_high, glove_low, _5_hole, stick_high, stick_low, shot_recovery, aggression, agility, speed, positioning, breakaway, vision, poke_check, rebound_control, passing) as ea_stats_array_g")]]

    #     skater_home_joined_df = home_joined_df.where(f.expr("left(home_onice_code,1)!='G'")).alias('home').join(df_ea_skaters.alias('eask'), f.expr("(lower(home.side) = lower(eask.team) and lower(eask.player_last_name) = lower(home.default_last_name)) or (lower(eask.player_last_name) = lower(home.default_last_name) and lower(home.default_first_init)= lower(left(eask.player_first_name,1)))"),"left")
    #     goal_home_joined_df = home_joined_df.where(f.expr("left(home_onice_code,1)=='G'")).alias('home').join(df_ea_goaltenders.alias('eag'), f.expr("(lower(home.side) = lower(eag.team) and lower(eag.player_last_name) = lower(home.default_last_name)) or (lower(eag.player_last_name) = lower(home.default_last_name) and lower(home.default_first_init)= lower(left(eag.player_first_name,1)))"),"left")
    #     skater_away_joined_df = away_joined_df.where(f.expr("left(away_onice_code,1)!='G'")).alias('away').join(df_ea_skaters.alias('eask'), f.expr("(lower(away.side) = lower(eask.team) and lower(eask.player_last_name) = lower(away.default_last_name)) or (lower(eask.player_last_name) = lower(away.default_last_name) and lower(away.default_first_init)= lower(left(eask.player_first_name,1)))"),"left")
    #     goal_away_joined_df = away_joined_df.where(f.expr("left(away_onice_code,1)=='G'")).alias('away').join(df_ea_goaltenders.alias('eag'), f.expr("(lower(away.side) = lower(eag.team) and lower(eag.player_last_name) = lower(away.default_last_name)) or (lower(eag.player_last_name) = lower(away.default_last_name) and lower(away.default_first_init)= lower(left(eag.player_first_name,1)))"),"left")

    #     skater_home_joined_df = skater_home_joined_df.withColumn("ea_stats_array_g", f.lit(None).cast(StructType(ea_stats_array_g_schema))).withColumnRenamed('ea_stats_array','ea_stats_array_s')
    #     goal_home_joined_df = goal_home_joined_df.withColumn("ea_stats_array_s", f.lit(None).cast(StructType(ea_stats_array_s_schema))).drop('ea_stats_array')
    #     home_joined_df = skater_home_joined_df.unionByName(goal_home_joined_df)

    #     skater_away_joined_df = skater_away_joined_df.withColumn("ea_stats_array_g", f.lit(None).cast(StructType(ea_stats_array_g_schema))).withColumnRenamed('ea_stats_array','ea_stats_array_s')
    #     goal_away_joined_df = goal_away_joined_df.withColumn("ea_stats_array_s", f.lit(None).cast(StructType(ea_stats_array_s_schema))).drop('ea_stats_array')
    #     away_joined_df = skater_away_joined_df.unionByName(goal_away_joined_df)

    #     agg_cols = ['away_onice_code','default_last_name','playerId','default_first_init','player_id','overall','player_type','ea_stats_array_s','ea_stats_array_g']
    #     home_pivoted_df = home_joined_df.groupBy(home_joined_df.columns[0:12]+['gameyear']).agg( \
    #         f.collect_list("home_onice_code").alias('home_onice_code') \
    #             ,f.collect_list("default_last_name").alias('home_onice_names') \
    #             ,f.collect_list("playerId").alias('home_onice_ids')\
    #             ,f.collect_list("default_first_init").alias('home_onice_firstinit') \
    #             ,f.collect_list("player_id").alias('home_ea_playerid') \
    #             ,f.collect_list("overall").alias('home_ea_overall') \
    #             ,f.collect_list("player_type").alias('home_ea_playertype') \
    #             ,f.collect_list("age").alias('home_ea_age') 
    #             ,f.collect_list("ea_stats_array_s").alias('home_ea_skater_detail')
    #             ,f.collect_list("ea_stats_array_g").alias('home_ea_goalie_detail'))
    #     away_pivoted_df = away_joined_df.groupBy(away_joined_df.columns[0:12]+['gameyear']).agg( \
    #         f.collect_list("away_onice_code").alias('away_onice_code') \
    #             ,f.collect_list("default_last_name").alias('away_onice_names') \
    #             ,f.collect_list("playerId").alias('away_onice_ids')\
    #             ,f.collect_list("default_first_init").alias('away_onice_firstinit') \
    #             ,f.collect_list("player_id").alias('away_ea_playerid') \
    #             ,f.collect_list("overall").alias('away_ea_overall') \
    #             ,f.collect_list("player_type").alias('away_ea_playertype') \
    #             ,f.collect_list("ea_stats_array_s").alias('away_ea_skater_detail')
    #             ,f.collect_list("ea_stats_array_g").alias('away_ea_goalie_detail'))

    #     return home_pivoted_df.join(away_pivoted_df,away_joined_df.columns[0:12]+['gameyear'], "inner")
    pass

def extract_player_stats(api_url):
    out = {}
    response = requests.get(api_url)
    response.raise_for_status()
    json_response = response.json()
    # Initialize empty lists to store parsed values
    forwards_data = []
    defense_data = []
    goalies_data = []

    # Define the schema for the DataFrame
    schema = ['playerId', 'sweaterNumber', 'name', 'position', 'goals', 'assists', 'points', 'plusMinus', 'pim', 'hits', 'blockedShots', 'powerPlayGoals', 'powerPlayPoints', 'shorthandedGoals', 'shPoints', 'shots', 'faceoffs', 'faceoffWinningPctg', 'toi', 'powerPlayToi', 'shorthandedToi']

    goalie_schema = ['playerId', 'sweaterNumber', 'name', 'position', 'evenStrengthShotsAgainst', 'powerPlayShotsAgainst', 'shorthandedShotsAgainst', 'saveShotsAgainst', 'savePctg', 'evenStrengthGoalsAgainst', 'powerPlayGoalsAgainst', 'shorthandedGoalsAgainst', 'pim', 'goalsAgainst', 'toi']

    # Function to align columns and data
    def align_data_with_schema(data, schema):
        aligned_data = []
        for player_values in data:
            # Create a dictionary with aligned data
            aligned_player = dict(zip(schema, player_values))
            aligned_data.append(aligned_player)
        return aligned_data

    game_away_team = json_response['awayTeam']
    game_home_team = json_response['homeTeam']

    # Iterate over 'awayTeam' and 'homeTeam'
    for side in ['awayTeam', 'homeTeam']:
        tm = game_away_team['abbrev']
        if side == 'homeTeam':
            tm = game_home_team['abbrev']
        # print(tm)
        
        forwards_data = []
        defense_data = []
        goalies_data = []

        side_info = json_response['boxscore']['playerByGameStats'][side]
        side_forwards = side_info['forwards']
        side_defense = side_info['defense']
        side_goalies = side_info['goalies']

        # Parse values for forwards
        for player in side_forwards:
            # Fill missing values with None
            player_values = [player.get(key, None) for key in schema]
            forwards_data.append(player_values)

        # Parse values for defense
        for player in side_defense:
            # Fill missing values with None
            player_values = [player.get(key, None) for key in schema]
            defense_data.append(player_values)

        # Parse values for goalies
        goalie_schema = list(set(key for player in side_goalies for key in player.keys()))
        for player in side_goalies:
            # Fill missing values with None based on the dynamically generated schema
            player_values = [player.get(key, None) for key in goalie_schema]
            goalies_data.append(player_values)


        forwards_df = spark.createDataFrame(align_data_with_schema(forwards_data, schema)).withColumn('game_id',f.lit(json_response['id'])).withColumn('side',f.lit(tm))
        defense_df = spark.createDataFrame(align_data_with_schema(defense_data, schema)).withColumn('game_id',f.lit(json_response['id'])).withColumn('side',f.lit(tm))
        goalies_df = spark.createDataFrame(align_data_with_schema(goalies_data, goalie_schema)).withColumn('game_id',f.lit(json_response['id'])).withColumn('side',f.lit(tm))
        
        if side == 'awayTeam':
            away_forwards_df = forwards_df
            away_defense_df = defense_df
            away_goalies_df = goalies_df
        else:
            home_forwards_df = forwards_df
            home_defense_df = defense_df
            home_goalies_df = goalies_df

    return away_forwards_df.union(home_forwards_df), away_defense_df.union(home_defense_df), away_goalies_df.union(home_goalies_df)

def get_completed_game_boxscore(api_url):
    try:
        response = requests.get(api_url)
        response.raise_for_status()  # Raise an HTTPError for bad responses

        # Parse the JSON data
        json_response = response.json()
        game_id = json_response['id']
        game_season = json_response['season']
        game_type = json_response['gameType']
        game_date = json_response['gameDate']
        game_venue = json_response['venue']
        game_start_time_utc = json_response['startTimeUTC']
        game_eastern_utc_offset = json_response['easternUTCOffset']
        game_venue_utc_offset = json_response['venueUTCOffset']

        game_state = json_response['gameState']
        game_schedule_state = json_response['gameScheduleState']
        game_period = json_response['period']
        game_period_number = json_response['periodDescriptor']

        game_away_team = json_response['awayTeam']
        game_home_team = json_response['homeTeam']

        game_clock = json_response['clock']
        game_outcome = json_response['gameOutcome']
        return [game_id, game_season, game_type, game_date, game_venue, game_away_team, game_home_team, game_start_time_utc, game_eastern_utc_offset, game_venue_utc_offset,game_schedule_state,game_period, game_outcome['lastPeriodType']] \
            , json_response['tvBroadcasts'], json_response['boxscore']
    except requests.exceptions.RequestException as e:
        print(f"Error making the request: {e}")

def get_document_text(url):
    response = requests.get(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        links = soup.find_all('a')
        for link in links:
            print(link.get('href'))
        return soup.get_text()
    else:
        return None
    
def playbyplaydf(playbyplay_text):
    text = playbyplay_text
    play_key = re.findall(r'\d+\s+\d+\s+[\w\s]*\s+\d+:\d+\d:\d+\d', text)
    plays = []
    div = 5*2
    for i in range(0,len(play_key)):
        play_id = play_key[i].replace('\n',' ')
        if i<len(play_key)-1:
            play_text = re.sub(r'\n+', '\n', text.split(play_key[i])[1].split(play_key[i+1])[0])
        else:
            play_text = re.sub(r'\n+', '\n', text.split(play_key[i])[1])
        play_id_tuple = play_id.split(' ')
        colon_index = play_id_tuple[-1].find(":")
        if colon_index != -1 and colon_index + 3 < len(play_id_tuple[-1]):
            result = [play_id_tuple[-1][0:colon_index + 3],play_id_tuple[-1][colon_index + 3:]]
        else:
            result = "Invalid input string"
        play_id_tuple[-1] = result
        play_array = [x for x in play_text.split('\n') if x != '\xa0']

        on_ice_array = play_array[3:-1]

        end_index = [i for i,x in enumerate(on_ice_array) if 'copyright' in x.lower()]
        if end_index == []:
            pass
        else:
            on_ice_array = on_ice_array[:end_index[0]]

        play_index = play_id_tuple[0]
        play_period = play_id_tuple[1]
        play_status = play_id_tuple[2]
        play_time_elapsed = play_id_tuple[3][0]
        play_time = play_id_tuple[3][1]
        play_type = play_array[1]
        play_detail = play_array[2]
        home_onice = 6
        away_onice = 6

        play_record = [GAMEID, ps_away_team.abbrev, ps_home_team.abbrev,play_index,play_period,play_status,play_type,play_detail,play_time,play_time_elapsed,away_onice, home_onice, on_ice_array]
        plays = plays + [play_record]
        last_div = div
    df_plays = spark.createDataFrame(pd.DataFrame(plays, columns = ['game_id', 'away_team', 'home_team', 'index', 'period', 'status', 'type','detail','time','time_elapsed', 'away_onice', 'home_onice','on_ice']))


    def convert_time_to_seconds(df):
        return df.withColumn("time_s",f.expr("cast(cast(split(time,':')[0] as float)*60 + cast(split(time,':')[1] as float) as int)")).withColumn("time_elapsed_s",f.expr("cast((cast(period as float)-1)*1200 + cast(split(time_elapsed,':')[0] as float)*60 + cast(split(time_elapsed,':')[1] as float) as int)")).withColumn("check",f.expr("time_s+time_elapsed_s"))

    df_plays = convert_time_to_seconds(df_plays)

    df_plays_final = df_plays.withColumn("position_of_G", f.expr("array_position(on_ice, 'G')")) \
        .withColumn("away_onice_array", f.expr("slice(on_ice, 1, position_of_G)")) \
            .withColumn("home_onice_array", f.expr("slice(on_ice, position_of_G + 1, size(on_ice))")) \
                .drop('away_onice').drop('home_onice').drop('time_s').drop('check').drop('position_of_G')

    def convert_time_to_seconds(df):
        return df.withColumn("time_s",f.expr("cast(cast(split(time,':')[0] as float)*60 + cast(split(time,':')[1] as float) as int)")).withColumn("time_elapsed_s",f.expr("cast((cast(period as float)-1)*1200 + cast(split(time_elapsed,':')[0] as float)*60 + cast(split(time_elapsed,':')[1] as float) as int)")).withColumn("check",f.expr("time_s+time_elapsed_s"))

    df_plays = convert_time_to_seconds(df_plays)

    df_plays_final = df_plays.withColumn("position_of_G", f.expr("array_position(on_ice, 'G')")) \
        .withColumn("away_onice_array", f.expr("slice(on_ice, 1, position_of_G)")) \
        .withColumn("home_onice_array", f.expr("slice(on_ice, position_of_G + 1, size(on_ice))")) \
        .drop('away_onice').drop('home_onice').drop('time_s').drop('check').drop('position_of_G')

    df_plays_final = df_plays_final.withColumn("num_on_ice", f.expr("cast(size(on_ice)/2 as int)"))

    df = df_plays_final

    df_result = df.withColumn(
        "indices_of_D",
        f.expr("FILTER(transform(on_ice, (element, i) -> IF(element = 'D', i, NULL)), x -> x IS NOT NULL)")
    )
    df_result = df_result.withColumn("away_adv", f.expr(f"IF(array_contains(indices_of_D, num_on_ice), true, null)"))
    df_result = df_result.withColumn("home_adv", f.expr(f"IF(array_contains(indices_of_D, num_on_ice), false, null)"))

    # Add conditions to set columns to empty arrays if num_on_ice < 10
    df_result = df_result.withColumn(
        "away_onice_array1",
        f.expr("case when num_on_ice < 10 then array() when size(home_onice_array) = 0 then slice(on_ice, 1, num_on_ice+1) else away_onice_array end ")
    )
    df_result = df_result.withColumn(
        "home_onice_array1",
        f.expr("case when num_on_ice < 10 then array() when size(home_onice_array) = 0 then slice(on_ice, num_on_ice + 2, size(on_ice)) else home_onice_array end")
    )

    return df_result.drop('indices_of_D').drop('away_adv').drop('home_adv')


def clean_name_col(df):
    return df \
        .withColumn('default_first_init',f.expr("split(upper(name['default']),'. ')[0]")) \
        .withColumn('default_last_name',f.expr("split(upper(name['default']),'. ')[1]")).drop('name')

# COMMAND ----------

errors = []
for _i,GAMEID in enumerate(range(2022020300,2022020400)):
    try:
        api_url = f'https://api-web.nhle.com/v1/gamecenter/{GAMEID}/boxscore'
        forwards_df, defense_df, goalies_df = extract_player_stats(api_url)
        game_record, tv_braodcasts, boxscore = get_completed_game_boxscore(api_url)
        game_record[5]['game_id'] = f'{GAMEID}'
        game_record[5]['game_team_side'] = 'away'
        game_record[6]['game_id'] = f'{GAMEID}'
        game_record[6]['game_team_side'] = 'home'
        ps_away_team = pd.Series(game_record[5])
        ps_home_team = pd.Series(game_record[6])

        df_gameinfo = spark.createDataFrame(pd.DataFrame([game_record[0:5]+game_record[7:]], columns = ['game_id', 'game_season', 'game_type', 'game_date', 'game_venue','game_start_time_utc', 'game_eastern_utc_offset', 'game_venue_utc_offset','game_schedule_state','game_period', 'game_last_period_type']))

        df_teams = spark.createDataFrame(pd.DataFrame([ps_away_team])).union(
        spark.createDataFrame(pd.DataFrame([ps_home_team])))

        playbyplay_link = boxscore['gameReports']['playByPlay']
        roster_link = boxscore['gameReports']['rosters']

        playbyplay_text = get_document_text(playbyplay_link)
        roster_text = get_document_text(roster_link)

        df = playbyplaydf(playbyplay_text)
        if _i == 0:
            df_out = df.drop('away_onice_array').drop('home_onice_array')
            df_out_forwards = forwards_df
            df_out_defense = defense_df
            df_out_goalies = goalies_df
        else:
            df_out = df_out.union(df.drop('away_onice_array').drop('home_onice_array'))
            df_out_forwards = df_out_forwards.union(forwards_df)
            df_out_defense = df_out_defense.union(defense_df)
            df_out_goalies = df_out_goalies.union(goalies_df)
    except:
        errors = errors + [GAMEID]

print(errors)

df_out.write.format("parquet").save("/FileStore/NHL_PLAYS_DB/raw_plays", mode = 'overwrite')
df_out_forwards.write.format("parquet").save("/FileStore/NHL_PLAYS_DB/raw_forwards", mode = 'overwrite')
df_out_defense.write.format("parquet").save("/FileStore/NHL_PLAYS_DB/raw_defense", mode = 'overwrite')
df_out_goalies.write.format("parquet").save("/FileStore/NHL_PLAYS_DB/raw_goalies", mode = 'overwrite')

df_plays = spark.read.format("parquet").load("/FileStore/NHL_PLAYS_DB/raw_plays")
df_forwards = clean_name_col(spark.read.format("parquet").load("/FileStore/NHL_PLAYS_DB/raw_forwards"))
df_defense = clean_name_col(spark.read.format("parquet").load("/FileStore/NHL_PLAYS_DB/raw_defense"))
df_goalies = clean_name_col(spark.read.format("parquet").load("/FileStore/NHL_PLAYS_DB/raw_goalies"))
df_plays_with_onice = append_nonea_onice_info(df_plays, df_forwards, df_defense, df_goalies)
df_plays_with_onice.write.format("parquet").save("/FileStore/NHL_PLAYS_DB/plays_with_onice", mode = 'append')

# COMMAND ----------

# The final 4 digits identify the specific game number. For regular season and preseason games, this ranges from 0001 to the number of games played. (1353 for seasons with 32 teams (2022 - Present), 1271 for seasons with 31 teams (2017 - 2020) and 1230 for seasons with 30 teams). For playoff games, the 2nd digit of the specific number gives the round of the playoffs, the 3rd digit specifies the matchup, and the 4th digit specifies the game (out of 7).

# COMMAND ----------

# MAGIC %md # By-Stat treatments

# COMMAND ----------

# MAGIC %md ### src

# COMMAND ----------

def append_lineup_info(df_stat, skater_action, action_label):
    df_stat = df_stat.withColumn(f'{action_label}_team', f.expr(f"substr(split({action_label}, ' ')[0],0,3)"))
    df_stat = df_stat.withColumn(f"{action_label}_jn", f.expr(f"trim(replace(split({action_label}, ' ')[1],'#',''))"))
    df_stat = df_stat.withColumn(f"{action_label}_lastname", f.expr(f"trim(split({action_label}, ' ')[2])"))
    df_stat = df_stat.withColumn(f"home_{action_label}", f.expr(f"case when trim({action_label}_team) = trim(home_team) then true else false end"))
    df_stat = df_stat.withColumn(f"home_{action_label}_onice_jn", f.expr(f"transform(home_onice_code, v -> regexp_replace(v, '[^0-9]', ''))"))
    df_stat = df_stat.withColumn(f"away_{action_label}_onice_jn", f.expr(f"transform(away_onice_code, v -> regexp_replace(v, '[^0-9]', ''))"))
    df_stat = df_stat.withColumn(f"{action_label}_onice_index", f.expr(f"array_position(case when home_{action_label} then home_{action_label}_onice_jn else away_{action_label}_onice_jn end, {action_label}_jn )"))
    return df_stat

def get_faceoff_detail(df_batch):
    def identify_array_position(homeaway, df):
        df = df.withColumn(f"{homeaway}_onice_jn", f.expr(f"transform({homeaway}_onice_code, v -> regexp_replace(v, '[^0-9]', ''))"))
        df = df.withColumn(f"{homeaway}_fo_jn", f.regexp_replace(df[f"fo_{homeaway}_player"], "[^0-9]", ""))
        df = df.withColumn(f"{homeaway}_onice_index", f.expr(f"array_position({homeaway}_onice_jn, {homeaway}_fo_jn)"))
        return df

    df = df_batch.where(f.expr("type = 'FAC'"))
    df = df.withColumn("fo_result", f.expr("split(detail,'-')[0]"))
    df = df.withColumn("fo_personnel", f.expr("split(detail,'-')[1]"))
    df = df.withColumn("fo_winning_team", f.expr("split(fo_result,' won ')[0]"))
    df = df.withColumn("fo_zone", f.expr("split(fo_result,' won ')[1]")).drop('fo_result')
    df = df.withColumn("fo_away_player", f.expr("split(fo_personnel,' vs ')[0]"))
    df = df.withColumn("fo_home_player", f.expr("split(fo_personnel,' vs ')[1]")).drop('fo_personnel')
    df = identify_array_position(homeaway = "away", df = df)
    df = identify_array_position(homeaway = "home", df = df)
    return df

def get_hit_detail(df_batch):
    df = df_batch.where(f.expr("type = 'HIT'"))    
    df = df.withColumn("hit_result", f.expr("split(detail, ',')[0]"))
    df = df.withColumn("hit_location", f.expr("split(detail, ',')[1]"))
    df = df.withColumn("hitter", f.expr("split(hit_result, ' HIT ')[0]"))
    df = df.withColumn("hittee", f.expr("split(hit_result, ' HIT ')[1]"))
    return df

def get_block_detail(df_batch):
    df = df_batch.where(f.expr("type = 'BLOCK'"))
    df = df.withColumn("shooter", f.expr("split(detail, ' BLOCKED BY ')[0]"))
    df = df.withColumn("_blocker", f.expr("split(split(detail, ' BLOCKED BY ')[1],',')"))
    df = df.withColumn("block_location", f.expr("trim(_blocker[2])"))
    df = df.withColumn("shot_type", f.expr("trim(_blocker[1])"))
    df = df.withColumn("blocker", f.expr("trim(_blocker[0])"))
    return df.drop('_blocker')
    
def get_miss_detail(df_batch):
    df = df_batch.where(f.expr("type = 'MISS'"))
    df = df.withColumn("miss_array", f.expr("split(detail, ', ')"))
    df = df.withColumn("shooter", f.expr("miss_array[0]"))
    df = df.withColumn("shot_type", f.expr("miss_array[1]"))
    df = df.withColumn("shot_result", f.expr("miss_array[2]"))
    df = df.withColumn("shot_zone", f.expr("miss_array[3]"))
    df = df.withColumn("shot_distance", f.expr("miss_array[4]"))
    return df.drop('miss_array')

def get_giveaway_detail(df_batch):
    df = df_batch.where(f.expr("type = 'GIVE'"))    
    df = df.withColumn("give_array", f.expr("split(replace(detail,'GIVEAWAY - ',' '),',')"))
    df = df.withColumn("giveaway_player", f.expr("trim(give_array[0])"))
    df = df.withColumn("giveaway_zone", f.expr("trim(give_array[1])"))
    return df.drop('give_array')

def get_penalty_detail(df_batch):
    df = df_batch.where(f.expr("type = 'PENL'")) 
    df = df.withColumn('penalty_array',f.expr("split(detail, 'Drawn By:')"))
    df = df.withColumn('penalty_drawn_by',f.expr("trim(penalty_array[1])"))
    df = df.withColumn('penalty_offense_info',f.expr("trim(penalty_array[0])")).drop('penalty_array')
    df = df.withColumn('penalty_zone',f.expr("trim(split(penalty_offense_info,',')[1])"))
    df = df.withColumn('penalty_detail',f.expr("trim(split(penalty_offense_info,',')[0])")).drop('penalty_offense_info')
    df = df.withColumn("maj_flag", f.when(f.col("penalty_detail").contains("(maj)"), 1).otherwise(0))

    # Extract the duration in the format (X min) and create a new column
    duration_pattern = r"\((\d+\s*min)\)"
    df = df.withColumn("penalty_duration", f.regexp_extract(f.col("penalty_detail"), duration_pattern, 1))

    replace_pattern = r"\(maj\)|\(\d+\s*min\)"
    df = df.withColumn("penalty_detail", f.regexp_replace(f.col("penalty_detail"), replace_pattern, ''))

    player_info_pattern = r"([A-Z]{3})\s*#(\d{1,2})\s*([A-Za-z]+)"
    df = df.withColumn("penalty_committed_by", f.regexp_extract(f.col("penalty_detail"), player_info_pattern, 0))
    df = df.withColumn("penalty_detail", f.regexp_replace(f.trim(f.col("penalty_detail")), player_info_pattern, ''))

    return df

def get_goal_detail(df_batch):
    df = df_batch.where(f.expr("type = 'GOAL'")) 
    df = df.withColumn('goal_array',f.expr("split(detail, 'Assists:')"))
    df = df.withColumn('assist_array',f.expr("split(goal_array[1], '; ')"))
    df = df.withColumn('goal_scorer_info',f.expr("split(goal_array[0],',')")).drop('goal_array')
    df = df.withColumn('goal_scorer',f.expr("goal_scorer_info[0]"))
    df = df.withColumn('goal_scorer_shottype',f.expr("goal_scorer_info[1]"))
    df = df.withColumn('goal_scorer_shotzone',f.expr("goal_scorer_info[2]"))
    df = df.withColumn('goal_scorer_shotdist',f.expr("goal_scorer_info[3]"))
    df = df.withColumn('goal_scorer_team',f.expr("substr(goal_scorer,0,3)"))

    df = df.withColumn('primary_assist_player',f.expr("concat(goal_scorer_team,assist_array[0])"))
    df = df.withColumn('secondary_assist_player',f.expr("case when size(assist_array)>1 then concat(goal_scorer_team,' ',assist_array[1]) else null end "))
    pattern = r"\((\d+)\)"
    df = df.withColumn("goal_scorer_num_goals", f.regexp_extract("goal_scorer", pattern, 1).cast(IntegerType()))
    df = df.withColumn("primary_assistor_num_assists", f.regexp_extract("primary_assist_player", pattern, 1).cast(IntegerType()))
    df = df.withColumn("secondary_assistor_num_assists", f.regexp_extract("secondary_assist_player", pattern, 1).cast(IntegerType()))
    df = df.withColumn("goal_scorer", f.regexp_replace("goal_scorer", pattern, ""))
    df = df.withColumn("primary_assist_player", f.regexp_replace("primary_assist_player", pattern, ""))
    df = df.withColumn("secondary_assist_player", f.regexp_replace("secondary_assist_player", pattern, ""))
    return df

def get_takeaway_detail(df_batch):
    df = df_batch.where(f.expr("type = 'TAKE'"))    
    df = df.withColumn("take_array", f.expr("split(replace(detail,'TAKEAWAY - ',' '),',')"))
    df = df.withColumn("takeaway_player", f.expr("trim(take_array[0])"))
    df = df.withColumn("takeaway_zone", f.expr("trim(take_array[1])"))
    return df.drop('take_array')

def get_shot_detail(df_batch):
    df = df_batch.where(f.expr("type = 'SHOT'"))
    df = df.withColumn("shot_array", f.expr("split(detail, 'ONGOAL -')"))
    df = df.withColumn("shooting_team", f.expr("substr(shot_array[0],0,3)"))
    df = df.withColumn("shot_detail", f.expr("split(trim(shot_array[1]),',')")).drop('shot_array')
    df = df.withColumn("shooter", f.expr("concat(shooting_team,' ',shot_detail[0])"))
    df = df.withColumn("shot_type", f.expr("shot_detail[1]"))
    df = df.withColumn("shot_zone", f.expr("shot_detail[2]"))
    df = df.withColumn("shot_distance", f.expr("shot_detail[3]"))
    return df.drop('shot_detail')


# COMMAND ----------

df_plays_with_onice = spark.read.format("parquet").load("/FileStore/NHL_PLAYS_DB/plays_with_onice")

# COMMAND ----------

# MAGIC %md ### FaceOff

# COMMAND ----------

df_faceoff_detail = get_faceoff_detail(df_plays_with_onice)
df_preunion_face = df_faceoff_detail[df_plays_with_onice.columns+['fo_winning_team', 'fo_zone', 'fo_away_player', 'fo_home_player', 'away_onice_index', 'home_onice_index']]


# COMMAND ----------

# MAGIC %md ### Hits

# COMMAND ----------

df_hit_detail = get_hit_detail(df_plays_with_onice)
df_hitter = append_lineup_info(df_stat = df_hit_detail, skater_action = 'hitter', action_label = 'hitter')
df_hits_full = append_lineup_info(df_stat = df_hitter, skater_action = 'hittee', action_label = 'hittee')
df_preunion_hits = df_hits_full[df_plays_with_onice.columns+['hit_location','hitter', 'hittee', 'hitter_team', 'hittee_team', 'home_hitter', 'hitter_onice_index', 'hittee_onice_index']]

# COMMAND ----------

# MAGIC %md ### Blocked Shots

# COMMAND ----------

df_blocks = get_block_detail(df_plays_with_onice)
df_blocker = append_lineup_info(df_stat = df_blocks, skater_action = 'blocker', action_label = 'blocker')
df_blocks_full = append_lineup_info(df_stat = df_blocker, skater_action = 'shooter', action_label = 'shooter')
df_preunion_block = df_blocks_full[df_plays_with_onice.columns+['shooter', 'blocker', 'block_location', 'shot_type', 'blocker_team','shooter_team', 'home_blocker', 'home_shooter', 'shooter_onice_index', 'blocker_onice_index']]

# COMMAND ----------

# MAGIC %md ### Missed Shots

# COMMAND ----------

df_miss = get_miss_detail(df_plays_with_onice)
df_miss_full = append_lineup_info(df_stat = df_miss, skater_action = 'shooter', action_label = 'shooter')
df_preunion_miss = df_miss_full[df_plays_with_onice.columns+['shooter', 'shot_type', 'shot_result', 'shot_zone', 'shot_distance', 'shooter_team', 'home_shooter','shooter_onice_index']]


# COMMAND ----------

# MAGIC %md ### Giveaway

# COMMAND ----------

df_give = get_giveaway_detail(df_plays_with_onice)
df_give_full = append_lineup_info(df_stat = df_give, skater_action = 'giveaway_player', action_label = 'giveaway_player')
df_preunion_give = df_give_full[df_plays_with_onice.columns+['giveaway_player', 'giveaway_zone', 'giveaway_player_team', 'home_giveaway_player' ,'giveaway_player_onice_index']]


# COMMAND ----------

# MAGIC %md ### Penalty

# COMMAND ----------

df_plays_with_onice.where(f.expr("type = 'DELPEN'")).display()

# COMMAND ----------

df_penl = get_penalty_detail(df_plays_with_onice)
df_offendor = append_lineup_info(df_stat = df_penl, skater_action = 'penalty_committed_by', action_label = 'penalty_committed_by')
df_penl_full = append_lineup_info(df_stat = df_offendor, skater_action = 'penalty_drawn_by', action_label = 'penalty_drawn_by')
df_preunion_penl = df_penl_full[df_plays_with_onice.columns+['penalty_drawn_by', 'penalty_committed_by', 'penalty_committed_by_team','penalty_zone', 'penalty_detail', 'maj_flag', 'penalty_duration', 'home_penalty_committed_by', 'penalty_committed_by_onice_index', 'penalty_drawn_by_team', 'penalty_drawn_by_onice_index']]


# COMMAND ----------

# MAGIC %md ### Goals

# COMMAND ----------

df_goal = get_goal_detail(df_plays_with_onice)
df_scorer = append_lineup_info(df_stat = df_goal, skater_action = 'goal_scorer', action_label = 'goal_scorer')
df_assist1 = append_lineup_info(df_stat = df_scorer, skater_action = 'primary_assist_player', action_label = 'primary_assist_player')
df_assist2 = append_lineup_info(df_stat = df_assist1, skater_action = 'secondary_assist_player', action_label = 'secondary_assist_player')

df_preunion_goal = df_assist2[df_plays_with_onice.columns+['goal_scorer', 'primary_assist_player', 'secondary_assist_player', 'goal_scorer_shottype', 'goal_scorer_shotzone', 'goal_scorer_shotdist', 'goal_scorer_team', 'goal_scorer_num_goals', 'primary_assistor_num_assists', 'secondary_assistor_num_assists', 'home_goal_scorer', 'goal_scorer_onice_index', 'primary_assist_player_onice_index', 'secondary_assist_player_onice_index']]


# COMMAND ----------

# MAGIC %md ### Shots

# COMMAND ----------

df_shot = get_shot_detail(df_plays_with_onice)
df_shot_final = append_lineup_info(df_stat = df_shot, skater_action = 'shooter', action_label = 'shooter')
df_preunion_shot = df_shot_final[df_plays_with_onice.columns+['shooting_team', 'shooter', 'shot_type', 'shot_distance', 'shooter_team', 'shot_zone', 'shot_distance', 'home_shooter', 'shooter_onice_index']]


# COMMAND ----------

# MAGIC %md ### PSTR/PEND

# COMMAND ----------

spark.read.format("parquet").load("/FileStore/NHL_PLAYS_DB/plays_with_onice").where(f.expr("game_id = '2022020001'")).where(f.expr("type = 'PEND'"))[['detail']] \
    .display()

# COMMAND ----------

spark.read.format("parquet").load("/FileStore/NHL_PLAYS_DB/plays_with_onice").where(f.expr("game_id = '2022020001'")).where(f.expr("type = 'PSTR'"))[['detail']] \
    .display()

# COMMAND ----------

# MAGIC %md ###Takeaways

# COMMAND ----------

df_take = get_takeaway_detail(df_plays_with_onice)
df_take_final = append_lineup_info(df_stat = df_take, skater_action = 'takeaway_player', action_label = 'takeaway_player')

df_preunion_take = df_take_final[df_plays_with_onice.columns+['takeaway_player', 'takeaway_zone', 'takeaway_player_team', 'home_takeaway_player', 'takeaway_player_onice_index']]


# COMMAND ----------

# MAGIC %md ### Stoppages

# COMMAND ----------

spark.read.format("parquet").load("/FileStore/NHL_PLAYS_DB/plays_with_onice").where(f.expr("game_id = '2022020001'")).where(f.expr("type = 'STOP'"))[['detail']] \
    .display()

# COMMAND ----------

# MAGIC %md ### Aggregate Check

# COMMAND ----------

df_plays_with_onice.groupBy("type").agg(f.count(f.lit(1))).display()

# COMMAND ----------

# MAGIC %md ## Union By-Statistic Output

# COMMAND ----------

blocker_cols = ['blocker', 'block_location', 'blocker_team', 'home_blocker', 'blocker_onice_index']
scoring_columns = ['primary_assist_player', 'secondary_assist_player','goal_scorer_num_goals', 'primary_assistor_num_assists', 'secondary_assistor_num_assists', 'primary_assist_player_onice_index', 'secondary_assist_player_onice_index']
out_cols = df_plays_with_onice.columns+['zone', 'shot_type', 'shot_distance', 'shooter', 'shooter_team','home_shooter','shooter_onice_index','shot_result']+blocker_cols+scoring_columns

fo_cols = ['faceoff_taken_by_away','faceoff_taken_by_home','faceoff_taken_by_away_index','faceoff_taken_by_home_index','faceoff_winner_home','faceoff_win_team']
out_cols = out_cols+fo_cols

hit_cols = ['hitter','hittee','hitter_team','hittee_team','home_hitter','hitter_onice_index', 'hittee_onice_index']
out_cols = out_cols+hit_cols

take_cols = ['takeaway_player', 'takeaway_player_team', 'home_takeaway_player', 'takeaway_player_onice_index']
give_cols = ['giveaway_player', 'giveaway_player_team', 'home_giveaway_player', 'giveaway_player_onice_index']
out_cols = out_cols+take_cols+give_cols

penl_cols = ['penalty_drawn_by', 'penalty_committed_by', 'penalty_committed_by_team', 'penalty_drawn_by_team', 'penalty_detail', 'maj_flag', 'penalty_duration', 'home_penalty_committed_by', 'penalty_committed_by_onice_index', 'penalty_drawn_by_onice_index']
out_cols = out_cols+penl_cols

_df_preunion_block = df_preunion_block \
    .withColumn('zone',f.expr("cast(null as string)")) \
    .withColumnRenamed('shot_type','shot_type') \
    .withColumn('shot_distance',f.expr("cast(null as string)")) \
    .withColumn('shot_result',f.expr("'Blocked'")) \
    .withColumn('goal_scorer_num_goals',f.expr("cast(null as int)")) \
    .withColumn('primary_assist_player',f.expr("cast(null as int)")) \
    .withColumn('secondary_assist_player',f.expr("cast(null as int)")) \
    .withColumn('primary_assistor_num_assists',f.expr("cast(null as int)")) \
    .withColumn('secondary_assistor_num_assists',f.expr("cast(null as int)")) \
    .withColumn('primary_assist_player_onice_index',f.expr("cast(null as int)")) \
    .withColumn('secondary_assist_player_onice_index',f.expr("cast(null as int)")) \
    .withColumn('faceoff_win_team',f.expr("cast(null as string)")) \
    .withColumn('faceoff_taken_by_away',f.expr("cast(null as string)")) \
    .withColumn('faceoff_taken_by_home',f.expr("cast(null as string)")) \
    .withColumn('faceoff_taken_by_away_index',f.expr("cast(null as int)")) \
    .withColumn('faceoff_taken_by_home_index',f.expr("cast(null as int)")) \
    .withColumn('faceoff_winner_home',f.expr("cast(null as boolean)")) \
    .withColumn('hitter',f.expr("cast(null as string)")) \
    .withColumn('hittee',f.expr("cast(null as string)")) \
    .withColumn('hitter_team',f.expr("cast(null as string)")) \
    .withColumn('hittee_team',f.expr("cast(null as string)")) \
    .withColumn('home_hitter',f.expr("cast(null as boolean)")) \
    .withColumn('hitter_onice_index',f.expr("cast(null as int)")) \
    .withColumn('hittee_onice_index',f.expr("cast(null as int)")) \
    .withColumn('takeaway_player',f.expr("cast(null as string)")) \
    .withColumn('takeaway_player_team',f.expr("cast(null as string)")) \
    .withColumn('home_takeaway_player',f.expr("cast(null as boolean)")) \
    .withColumn('takeaway_player_onice_index',f.expr("cast(null as int)")) \
    .withColumn('giveaway_player',f.expr("cast(null as string)")) \
    .withColumn('giveaway_player_team',f.expr("cast(null as string)")) \
    .withColumn('home_giveaway_player',f.expr("cast(null as boolean)")) \
    .withColumn('giveaway_player_onice_index',f.expr("cast(null as int)")) \
    .withColumn('penalty_drawn_by',f.expr("cast(null as string)")) \
    .withColumn('penalty_committed_by',f.expr("cast(null as string)")) \
    .withColumn('penalty_committed_by_team',f.expr("cast(null as string)")) \
    .withColumn('penalty_drawn_by_team',f.expr("cast(null as string)")) \
    .withColumn('penalty_detail',f.expr("cast(null as string)")) \
    .withColumn('maj_flag',f.expr("cast(null as string)")) \
    .withColumn('penalty_duration',f.expr("cast(null as string)")) \
    .withColumn('home_penalty_committed_by',f.expr("cast(null as boolean)")) \
    .withColumn('penalty_committed_by_onice_index',f.expr("cast(null as int)")) \
    .withColumn('penalty_drawn_by_onice_index',f.expr("cast(null as int)"))


_df_preunion_goal = df_preunion_goal \
    .withColumnRenamed('goal_scorer_shotzone','zone') \
    .withColumnRenamed('goal_scorer_shottype','shot_type') \
    .withColumnRenamed('goal_scorer_shotdist','shot_distance') \
    .withColumnRenamed('goal_scorer','shooter') \
    .withColumnRenamed('goal_scorer_team','shooter_team') \
    .withColumnRenamed('home_goal_scorer','home_shooter') \
    .withColumnRenamed('goal_scorer_onice_index','shooter_onice_index') \
    .withColumn('shot_result',f.expr("'Goal'")) \
    .withColumn('blocker',f.expr("cast(null as string)")) \
    .withColumn('block_location',f.expr("cast(null as string)")) \
    .withColumn('blocker_team',f.expr("cast(null as string)")) \
    .withColumn('home_blocker',f.expr("cast(null as boolean)")) \
    .withColumn('blocker_onice_index',f.expr("cast(null as string)")) \
    .withColumn('faceoff_win_team',f.expr("cast(null as string)")) \
    .withColumn('faceoff_taken_by_away',f.expr("cast(null as string)")) \
    .withColumn('faceoff_taken_by_home',f.expr("cast(null as string)")) \
    .withColumn('faceoff_taken_by_away_index',f.expr("cast(null as int)")) \
    .withColumn('faceoff_taken_by_home_index',f.expr("cast(null as int)")) \
    .withColumn('faceoff_winner_home',f.expr("cast(null as boolean)")) \
    .withColumn('hitter',f.expr("cast(null as string)")) \
    .withColumn('hittee',f.expr("cast(null as string)")) \
    .withColumn('hitter_team',f.expr("cast(null as string)")) \
    .withColumn('hittee_team',f.expr("cast(null as string)")) \
    .withColumn('home_hitter',f.expr("cast(null as boolean)")) \
    .withColumn('hitter_onice_index',f.expr("cast(null as int)")) \
    .withColumn('hittee_onice_index',f.expr("cast(null as int)")) \
    .withColumn('takeaway_player',f.expr("cast(null as string)")) \
    .withColumn('takeaway_player_team',f.expr("cast(null as string)")) \
    .withColumn('home_takeaway_player',f.expr("cast(null as boolean)")) \
    .withColumn('takeaway_player_onice_index',f.expr("cast(null as int)")) \
    .withColumn('giveaway_player',f.expr("cast(null as string)")) \
    .withColumn('giveaway_player_team',f.expr("cast(null as string)")) \
    .withColumn('home_giveaway_player',f.expr("cast(null as boolean)")) \
    .withColumn('giveaway_player_onice_index',f.expr("cast(null as int)")) \
    .withColumn('penalty_drawn_by',f.expr("cast(null as string)")) \
    .withColumn('penalty_committed_by',f.expr("cast(null as string)")) \
    .withColumn('penalty_committed_by_team',f.expr("cast(null as string)")) \
    .withColumn('penalty_drawn_by_team',f.expr("cast(null as string)")) \
    .withColumn('penalty_detail',f.expr("cast(null as string)")) \
    .withColumn('maj_flag',f.expr("cast(null as string)")) \
    .withColumn('penalty_duration',f.expr("cast(null as string)")) \
    .withColumn('home_penalty_committed_by',f.expr("cast(null as boolean)")) \
    .withColumn('penalty_committed_by_onice_index',f.expr("cast(null as int)")) \
    .withColumn('penalty_drawn_by_onice_index',f.expr("cast(null as int)"))


_df_preunion_miss = df_preunion_miss \
    .withColumnRenamed('shot_zone','zone') \
    .withColumnRenamed('shot_type','shot_type') \
    .withColumn('blocker',f.expr("cast(null as string)")) \
    .withColumn('block_location',f.expr("cast(null as string)")) \
    .withColumn('blocker_team',f.expr("cast(null as string)")) \
    .withColumn('home_blocker',f.expr("cast(null as boolean)")) \
    .withColumn('blocker_onice_index',f.expr("cast(null as string)")) \
    .withColumn('goal_scorer_num_goals',f.expr("cast(null as int)")) \
    .withColumn('primary_assist_player',f.expr("cast(null as int)")) \
    .withColumn('secondary_assist_player',f.expr("cast(null as int)")) \
    .withColumn('primary_assistor_num_assists',f.expr("cast(null as int)")) \
    .withColumn('secondary_assistor_num_assists',f.expr("cast(null as int)")) \
    .withColumn('primary_assist_player_onice_index',f.expr("cast(null as int)")) \
    .withColumn('secondary_assist_player_onice_index',f.expr("cast(null as int)")) \
    .withColumn('faceoff_win_team',f.expr("cast(null as string)")) \
    .withColumn('faceoff_taken_by_away',f.expr("cast(null as string)")) \
    .withColumn('faceoff_taken_by_home',f.expr("cast(null as string)")) \
    .withColumn('faceoff_taken_by_away_index',f.expr("cast(null as int)")) \
    .withColumn('faceoff_taken_by_home_index',f.expr("cast(null as int)")) \
    .withColumn('faceoff_winner_home',f.expr("cast(null as boolean)")) \
    .withColumn('hitter',f.expr("cast(null as string)")) \
    .withColumn('hittee',f.expr("cast(null as string)")) \
    .withColumn('hitter_team',f.expr("cast(null as string)")) \
    .withColumn('hittee_team',f.expr("cast(null as string)")) \
    .withColumn('home_hitter',f.expr("cast(null as boolean)")) \
    .withColumn('hitter_onice_index',f.expr("cast(null as int)")) \
    .withColumn('hittee_onice_index',f.expr("cast(null as int)")) \
    .withColumn('takeaway_player',f.expr("cast(null as string)")) \
    .withColumn('takeaway_player_team',f.expr("cast(null as string)")) \
    .withColumn('home_takeaway_player',f.expr("cast(null as boolean)")) \
    .withColumn('takeaway_player_onice_index',f.expr("cast(null as int)")) \
    .withColumn('giveaway_player',f.expr("cast(null as string)")) \
    .withColumn('giveaway_player_team',f.expr("cast(null as string)")) \
    .withColumn('home_giveaway_player',f.expr("cast(null as boolean)")) \
    .withColumn('giveaway_player_onice_index',f.expr("cast(null as int)")) \
    .withColumn('penalty_drawn_by',f.expr("cast(null as string)")) \
    .withColumn('penalty_committed_by',f.expr("cast(null as string)")) \
    .withColumn('penalty_committed_by_team',f.expr("cast(null as string)")) \
    .withColumn('penalty_drawn_by_team',f.expr("cast(null as string)")) \
    .withColumn('penalty_detail',f.expr("cast(null as string)")) \
    .withColumn('maj_flag',f.expr("cast(null as string)")) \
    .withColumn('penalty_duration',f.expr("cast(null as string)")) \
    .withColumn('home_penalty_committed_by',f.expr("cast(null as boolean)")) \
    .withColumn('penalty_committed_by_onice_index',f.expr("cast(null as int)")) \
    .withColumn('penalty_drawn_by_onice_index',f.expr("cast(null as int)"))


_df_preunion_shot = df_preunion_shot \
    .withColumnRenamed('shot_zone','zone') \
    .withColumnRenamed('shot_type','shot_type')  \
    .withColumn('shot_result',f.expr("'On-Goal'")) \
    .withColumn('blocker',f.expr("cast(null as string)")) \
    .withColumn('block_location',f.expr("cast(null as string)")) \
    .withColumn('blocker_team',f.expr("cast(null as string)")) \
    .withColumn('home_blocker',f.expr("cast(null as boolean)")) \
    .withColumn('blocker_onice_index',f.expr("cast(null as string)")) \
    .withColumn('goal_scorer_num_goals',f.expr("cast(null as int)")) \
    .withColumn('primary_assist_player',f.expr("cast(null as int)")) \
    .withColumn('secondary_assist_player',f.expr("cast(null as int)")) \
    .withColumn('primary_assistor_num_assists',f.expr("cast(null as int)")) \
    .withColumn('secondary_assistor_num_assists',f.expr("cast(null as int)")) \
    .withColumn('primary_assist_player_onice_index',f.expr("cast(null as int)")) \
    .withColumn('secondary_assist_player_onice_index',f.expr("cast(null as int)")) \
    .withColumn('faceoff_win_team',f.expr("cast(null as string)")) \
    .withColumn('faceoff_taken_by_away',f.expr("cast(null as string)")) \
    .withColumn('faceoff_taken_by_home',f.expr("cast(null as string)")) \
    .withColumn('faceoff_taken_by_away_index',f.expr("cast(null as int)")) \
    .withColumn('faceoff_taken_by_home_index',f.expr("cast(null as int)")) \
    .withColumn('faceoff_winner_home',f.expr("cast(null as boolean)")) \
    .withColumn('hitter',f.expr("cast(null as string)")) \
    .withColumn('hittee',f.expr("cast(null as string)")) \
    .withColumn('hitter_team',f.expr("cast(null as string)")) \
    .withColumn('hittee_team',f.expr("cast(null as string)")) \
    .withColumn('home_hitter',f.expr("cast(null as boolean)")) \
    .withColumn('hitter_onice_index',f.expr("cast(null as int)")) \
    .withColumn('hittee_onice_index',f.expr("cast(null as int)")) \
    .withColumn('takeaway_player',f.expr("cast(null as string)")) \
    .withColumn('takeaway_player_team',f.expr("cast(null as string)")) \
    .withColumn('home_takeaway_player',f.expr("cast(null as boolean)")) \
    .withColumn('takeaway_player_onice_index',f.expr("cast(null as int)")) \
    .withColumn('giveaway_player',f.expr("cast(null as string)")) \
    .withColumn('giveaway_player_team',f.expr("cast(null as string)")) \
    .withColumn('home_giveaway_player',f.expr("cast(null as boolean)")) \
    .withColumn('giveaway_player_onice_index',f.expr("cast(null as int)")) \
    .withColumn('penalty_drawn_by',f.expr("cast(null as string)")) \
    .withColumn('penalty_committed_by',f.expr("cast(null as string)")) \
    .withColumn('penalty_committed_by_team',f.expr("cast(null as string)")) \
    .withColumn('penalty_drawn_by_team',f.expr("cast(null as string)")) \
    .withColumn('penalty_detail',f.expr("cast(null as string)")) \
    .withColumn('maj_flag',f.expr("cast(null as string)")) \
    .withColumn('penalty_duration',f.expr("cast(null as string)")) \
    .withColumn('home_penalty_committed_by',f.expr("cast(null as boolean)")) \
    .withColumn('penalty_committed_by_onice_index',f.expr("cast(null as int)")) \
    .withColumn('penalty_drawn_by_onice_index',f.expr("cast(null as int)"))


df_shots_all = _df_preunion_shot[out_cols].unionByName(_df_preunion_miss[out_cols].unionByName(_df_preunion_block[out_cols].unionByName(_df_preunion_goal[out_cols])))


# COMMAND ----------



_df_preunion_face = df_preunion_face \
    .withColumnRenamed('fo_zone','zone') \
    .withColumn('shot_type',f.expr("cast(null as string)")) \
    .withColumn('shot_distance',f.expr("cast(null as string)")) \
    .withColumn('shooter',f.expr("cast(null as string)")) \
    .withColumn('shooter_team',f.expr("cast(null as string)")) \
    .withColumn('home_shooter',f.expr("cast(null as boolean)")) \
    .withColumn('shooter_onice_index',f.expr("cast(null as string)")) \
    .withColumn('shot_result',f.expr("cast(null as string)")) \
    .withColumn('blocker',f.expr("cast(null as string)")) \
    .withColumn('block_location',f.expr("cast(null as string)")) \
    .withColumn('blocker_team',f.expr("cast(null as string)")) \
    .withColumn('home_blocker',f.expr("cast(null as boolean)")) \
    .withColumn('blocker_onice_index',f.expr("cast(null as string)")) \
    .withColumn('goal_scorer_num_goals',f.expr("cast(null as int)")) \
    .withColumn('primary_assist_player',f.expr("cast(null as int)")) \
    .withColumn('secondary_assist_player',f.expr("cast(null as int)")) \
    .withColumn('primary_assistor_num_assists',f.expr("cast(null as int)")) \
    .withColumn('secondary_assistor_num_assists',f.expr("cast(null as int)")) \
    .withColumn('primary_assist_player_onice_index',f.expr("cast(null as int)")) \
    .withColumn('secondary_assist_player_onice_index',f.expr("cast(null as int)")) \
    .withColumnRenamed('fo_winning_team','faceoff_win_team') \
    .withColumnRenamed('fo_away_player','faceoff_taken_by_away') \
    .withColumnRenamed('fo_home_player','faceoff_taken_by_home') \
    .withColumnRenamed('away_onice_index','faceoff_taken_by_away_index') \
    .withColumnRenamed('home_onice_index','faceoff_taken_by_home_index') \
    .withColumn('faceoff_winner_home', f.expr("case when faceoff_win_team = home_team then true else false end")) \
    .withColumn('hitter',f.expr("cast(null as string)")) \
    .withColumn('hittee',f.expr("cast(null as string)")) \
    .withColumn('hitter_team',f.expr("cast(null as string)")) \
    .withColumn('hittee_team',f.expr("cast(null as string)")) \
    .withColumn('home_hitter',f.expr("cast(null as boolean)")) \
    .withColumn('hitter_onice_index',f.expr("cast(null as int)")) \
    .withColumn('hittee_onice_index',f.expr("cast(null as int)")) \
    .withColumn('takeaway_player',f.expr("cast(null as string)")) \
    .withColumn('takeaway_player_team',f.expr("cast(null as string)")) \
    .withColumn('home_takeaway_player',f.expr("cast(null as boolean)")) \
    .withColumn('takeaway_player_onice_index',f.expr("cast(null as int)")) \
    .withColumn('giveaway_player',f.expr("cast(null as string)")) \
    .withColumn('giveaway_player_team',f.expr("cast(null as string)")) \
    .withColumn('home_giveaway_player',f.expr("cast(null as boolean)")) \
    .withColumn('giveaway_player_onice_index',f.expr("cast(null as int)")) \
    .withColumn('penalty_drawn_by',f.expr("cast(null as string)")) \
    .withColumn('penalty_committed_by',f.expr("cast(null as string)")) \
    .withColumn('penalty_committed_by_team',f.expr("cast(null as string)")) \
    .withColumn('penalty_drawn_by_team',f.expr("cast(null as string)")) \
    .withColumn('penalty_detail',f.expr("cast(null as string)")) \
    .withColumn('maj_flag',f.expr("cast(null as string)")) \
    .withColumn('penalty_duration',f.expr("cast(null as string)")) \
    .withColumn('home_penalty_committed_by',f.expr("cast(null as boolean)")) \
    .withColumn('penalty_committed_by_onice_index',f.expr("cast(null as int)")) \
    .withColumn('penalty_drawn_by_onice_index',f.expr("cast(null as int)"))

# COMMAND ----------

_df_preunion_hits = df_preunion_hits \
    .withColumnRenamed('hit_location','zone') \
    .withColumn('shot_type',f.expr("cast(null as string)")) \
    .withColumn('shot_distance',f.expr("cast(null as string)")) \
    .withColumn('shooter',f.expr("cast(null as string)")) \
    .withColumn('shooter_team',f.expr("cast(null as string)")) \
    .withColumn('home_shooter',f.expr("cast(null as boolean)")) \
    .withColumn('shooter_onice_index',f.expr("cast(null as string)")) \
    .withColumn('shot_result',f.expr("cast(null as string)")) \
    .withColumn('blocker',f.expr("cast(null as string)")) \
    .withColumn('block_location',f.expr("cast(null as string)")) \
    .withColumn('blocker_team',f.expr("cast(null as string)")) \
    .withColumn('home_blocker',f.expr("cast(null as boolean)")) \
    .withColumn('blocker_onice_index',f.expr("cast(null as string)")) \
    .withColumn('goal_scorer_num_goals',f.expr("cast(null as int)")) \
    .withColumn('primary_assist_player',f.expr("cast(null as int)")) \
    .withColumn('secondary_assist_player',f.expr("cast(null as int)")) \
    .withColumn('primary_assistor_num_assists',f.expr("cast(null as int)")) \
    .withColumn('secondary_assistor_num_assists',f.expr("cast(null as int)")) \
    .withColumn('primary_assist_player_onice_index',f.expr("cast(null as int)")) \
    .withColumn('secondary_assist_player_onice_index',f.expr("cast(null as int)")) \
    .withColumn('faceoff_win_team',f.expr("cast(null as string)")) \
    .withColumn('faceoff_taken_by_away',f.expr("cast(null as string)")) \
    .withColumn('faceoff_taken_by_home',f.expr("cast(null as string)")) \
    .withColumn('faceoff_taken_by_away_index',f.expr("cast(null as int)")) \
    .withColumn('faceoff_taken_by_home_index',f.expr("cast(null as int)")) \
    .withColumn('faceoff_winner_home',f.expr("cast(null as boolean)")) \
    .withColumn('takeaway_player',f.expr("cast(null as string)")) \
    .withColumn('takeaway_player_team',f.expr("cast(null as string)")) \
    .withColumn('home_takeaway_player',f.expr("cast(null as boolean)")) \
    .withColumn('takeaway_player_onice_index',f.expr("cast(null as int)")) \
    .withColumn('giveaway_player',f.expr("cast(null as string)")) \
    .withColumn('giveaway_player_team',f.expr("cast(null as string)")) \
    .withColumn('home_giveaway_player',f.expr("cast(null as boolean)")) \
    .withColumn('giveaway_player_onice_index',f.expr("cast(null as int)")) \
    .withColumn('penalty_drawn_by',f.expr("cast(null as string)")) \
    .withColumn('penalty_committed_by',f.expr("cast(null as string)")) \
    .withColumn('penalty_committed_by_team',f.expr("cast(null as string)")) \
    .withColumn('penalty_drawn_by_team',f.expr("cast(null as string)")) \
    .withColumn('penalty_detail',f.expr("cast(null as string)")) \
    .withColumn('maj_flag',f.expr("cast(null as string)")) \
    .withColumn('penalty_duration',f.expr("cast(null as string)")) \
    .withColumn('home_penalty_committed_by',f.expr("cast(null as boolean)")) \
    .withColumn('penalty_committed_by_onice_index',f.expr("cast(null as int)")) \
    .withColumn('penalty_drawn_by_onice_index',f.expr("cast(null as int)"))

# COMMAND ----------

_df_preunion_take = df_preunion_take \
    .withColumnRenamed('takeaway_zone','zone') \
    .withColumn('shot_type',f.expr("cast(null as string)")) \
    .withColumn('shot_distance',f.expr("cast(null as string)")) \
    .withColumn('shooter',f.expr("cast(null as string)")) \
    .withColumn('shooter_team',f.expr("cast(null as string)")) \
    .withColumn('home_shooter',f.expr("cast(null as boolean)")) \
    .withColumn('shooter_onice_index',f.expr("cast(null as string)")) \
    .withColumn('shot_result',f.expr("cast(null as string)")) \
    .withColumn('blocker',f.expr("cast(null as string)")) \
    .withColumn('block_location',f.expr("cast(null as string)")) \
    .withColumn('blocker_team',f.expr("cast(null as string)")) \
    .withColumn('home_blocker',f.expr("cast(null as boolean)")) \
    .withColumn('blocker_onice_index',f.expr("cast(null as string)")) \
    .withColumn('goal_scorer_num_goals',f.expr("cast(null as int)")) \
    .withColumn('primary_assist_player',f.expr("cast(null as int)")) \
    .withColumn('secondary_assist_player',f.expr("cast(null as int)")) \
    .withColumn('primary_assistor_num_assists',f.expr("cast(null as int)")) \
    .withColumn('secondary_assistor_num_assists',f.expr("cast(null as int)")) \
    .withColumn('primary_assist_player_onice_index',f.expr("cast(null as int)")) \
    .withColumn('secondary_assist_player_onice_index',f.expr("cast(null as int)")) \
    .withColumn('faceoff_win_team',f.expr("cast(null as string)")) \
    .withColumn('faceoff_taken_by_away',f.expr("cast(null as string)")) \
    .withColumn('faceoff_taken_by_home',f.expr("cast(null as string)")) \
    .withColumn('faceoff_taken_by_away_index',f.expr("cast(null as int)")) \
    .withColumn('faceoff_taken_by_home_index',f.expr("cast(null as int)")) \
    .withColumn('faceoff_winner_home',f.expr("cast(null as boolean)")) \
    .withColumn('hitter',f.expr("cast(null as string)")) \
    .withColumn('hittee',f.expr("cast(null as string)")) \
    .withColumn('hitter_team',f.expr("cast(null as string)")) \
    .withColumn('hittee_team',f.expr("cast(null as string)")) \
    .withColumn('home_hitter',f.expr("cast(null as boolean)")) \
    .withColumn('hitter_onice_index',f.expr("cast(null as int)")) \
    .withColumn('hittee_onice_index',f.expr("cast(null as int)")) \
    .withColumn('giveaway_player',f.expr("cast(null as string)")) \
    .withColumn('giveaway_player_team',f.expr("cast(null as string)")) \
    .withColumn('home_giveaway_player',f.expr("cast(null as boolean)")) \
    .withColumn('giveaway_player_onice_index',f.expr("cast(null as int)")) \
    .withColumn('penalty_drawn_by',f.expr("cast(null as string)")) \
    .withColumn('penalty_committed_by',f.expr("cast(null as string)")) \
    .withColumn('penalty_committed_by_team',f.expr("cast(null as string)")) \
    .withColumn('penalty_drawn_by_team',f.expr("cast(null as string)")) \
    .withColumn('penalty_detail',f.expr("cast(null as string)")) \
    .withColumn('maj_flag',f.expr("cast(null as string)")) \
    .withColumn('penalty_duration',f.expr("cast(null as string)")) \
    .withColumn('home_penalty_committed_by',f.expr("cast(null as boolean)")) \
    .withColumn('penalty_committed_by_onice_index',f.expr("cast(null as int)")) \
    .withColumn('penalty_drawn_by_onice_index',f.expr("cast(null as int)"))




# COMMAND ----------

_df_preunion_give = df_preunion_give \
    .withColumnRenamed('giveaway_zone','zone') \
    .withColumn('shot_type',f.expr("cast(null as string)")) \
    .withColumn('shot_distance',f.expr("cast(null as string)")) \
    .withColumn('shooter',f.expr("cast(null as string)")) \
    .withColumn('shooter_team',f.expr("cast(null as string)")) \
    .withColumn('home_shooter',f.expr("cast(null as boolean)")) \
    .withColumn('shooter_onice_index',f.expr("cast(null as string)")) \
    .withColumn('shot_result',f.expr("cast(null as string)")) \
    .withColumn('blocker',f.expr("cast(null as string)")) \
    .withColumn('block_location',f.expr("cast(null as string)")) \
    .withColumn('blocker_team',f.expr("cast(null as string)")) \
    .withColumn('home_blocker',f.expr("cast(null as boolean)")) \
    .withColumn('blocker_onice_index',f.expr("cast(null as string)")) \
    .withColumn('goal_scorer_num_goals',f.expr("cast(null as int)")) \
    .withColumn('primary_assist_player',f.expr("cast(null as int)")) \
    .withColumn('secondary_assist_player',f.expr("cast(null as int)")) \
    .withColumn('primary_assistor_num_assists',f.expr("cast(null as int)")) \
    .withColumn('secondary_assistor_num_assists',f.expr("cast(null as int)")) \
    .withColumn('primary_assist_player_onice_index',f.expr("cast(null as int)")) \
    .withColumn('secondary_assist_player_onice_index',f.expr("cast(null as int)")) \
    .withColumn('faceoff_win_team',f.expr("cast(null as string)")) \
    .withColumn('faceoff_taken_by_away',f.expr("cast(null as string)")) \
    .withColumn('faceoff_taken_by_home',f.expr("cast(null as string)")) \
    .withColumn('faceoff_taken_by_away_index',f.expr("cast(null as int)")) \
    .withColumn('faceoff_taken_by_home_index',f.expr("cast(null as int)")) \
    .withColumn('faceoff_winner_home',f.expr("cast(null as boolean)")) \
    .withColumn('hitter',f.expr("cast(null as string)")) \
    .withColumn('hittee',f.expr("cast(null as string)")) \
    .withColumn('hitter_team',f.expr("cast(null as string)")) \
    .withColumn('hittee_team',f.expr("cast(null as string)")) \
    .withColumn('home_hitter',f.expr("cast(null as boolean)")) \
    .withColumn('hitter_onice_index',f.expr("cast(null as int)")) \
    .withColumn('hittee_onice_index',f.expr("cast(null as int)")) \
    .withColumn('takeaway_player',f.expr("cast(null as string)")) \
    .withColumn('takeaway_player_team',f.expr("cast(null as string)")) \
    .withColumn('home_takeaway_player',f.expr("cast(null as boolean)")) \
    .withColumn('takeaway_player_onice_index',f.expr("cast(null as int)")) \
    .withColumn('penalty_drawn_by',f.expr("cast(null as string)")) \
    .withColumn('penalty_committed_by',f.expr("cast(null as string)")) \
    .withColumn('penalty_committed_by_team',f.expr("cast(null as string)")) \
    .withColumn('penalty_drawn_by_team',f.expr("cast(null as string)")) \
    .withColumn('penalty_detail',f.expr("cast(null as string)")) \
    .withColumn('maj_flag',f.expr("cast(null as string)")) \
    .withColumn('penalty_duration',f.expr("cast(null as string)")) \
    .withColumn('home_penalty_committed_by',f.expr("cast(null as boolean)")) \
    .withColumn('penalty_committed_by_onice_index',f.expr("cast(null as int)")) \
    .withColumn('penalty_drawn_by_onice_index',f.expr("cast(null as int)"))

# COMMAND ----------



_df_preunion_penl = df_preunion_penl \
    .withColumnRenamed('penalty_zone','zone') \
    .withColumn('shot_type',f.expr("cast(null as string)")) \
    .withColumn('shot_distance',f.expr("cast(null as string)")) \
    .withColumn('shooter',f.expr("cast(null as string)")) \
    .withColumn('shooter_team',f.expr("cast(null as string)")) \
    .withColumn('home_shooter',f.expr("cast(null as boolean)")) \
    .withColumn('shooter_onice_index',f.expr("cast(null as string)")) \
    .withColumn('shot_result',f.expr("cast(null as string)")) \
    .withColumn('blocker',f.expr("cast(null as string)")) \
    .withColumn('block_location',f.expr("cast(null as string)")) \
    .withColumn('blocker_team',f.expr("cast(null as string)")) \
    .withColumn('home_blocker',f.expr("cast(null as boolean)")) \
    .withColumn('blocker_onice_index',f.expr("cast(null as string)")) \
    .withColumn('goal_scorer_num_goals',f.expr("cast(null as int)")) \
    .withColumn('primary_assist_player',f.expr("cast(null as int)")) \
    .withColumn('secondary_assist_player',f.expr("cast(null as int)")) \
    .withColumn('primary_assistor_num_assists',f.expr("cast(null as int)")) \
    .withColumn('secondary_assistor_num_assists',f.expr("cast(null as int)")) \
    .withColumn('primary_assist_player_onice_index',f.expr("cast(null as int)")) \
    .withColumn('secondary_assist_player_onice_index',f.expr("cast(null as int)")) \
    .withColumn('faceoff_win_team',f.expr("cast(null as string)")) \
    .withColumn('faceoff_taken_by_away',f.expr("cast(null as string)")) \
    .withColumn('faceoff_taken_by_home',f.expr("cast(null as string)")) \
    .withColumn('faceoff_taken_by_away_index',f.expr("cast(null as int)")) \
    .withColumn('faceoff_taken_by_home_index',f.expr("cast(null as int)")) \
    .withColumn('faceoff_winner_home',f.expr("cast(null as boolean)")) \
    .withColumn('hitter',f.expr("cast(null as string)")) \
    .withColumn('hittee',f.expr("cast(null as string)")) \
    .withColumn('hitter_team',f.expr("cast(null as string)")) \
    .withColumn('hittee_team',f.expr("cast(null as string)")) \
    .withColumn('home_hitter',f.expr("cast(null as boolean)")) \
    .withColumn('hitter_onice_index',f.expr("cast(null as int)")) \
    .withColumn('hittee_onice_index',f.expr("cast(null as int)")) \
    .withColumn('takeaway_player',f.expr("cast(null as string)")) \
    .withColumn('takeaway_player_team',f.expr("cast(null as string)")) \
    .withColumn('home_takeaway_player',f.expr("cast(null as boolean)")) \
    .withColumn('takeaway_player_onice_index',f.expr("cast(null as int)")) \
    .withColumn('giveaway_player',f.expr("cast(null as string)")) \
    .withColumn('giveaway_player_team',f.expr("cast(null as string)")) \
    .withColumn('home_giveaway_player',f.expr("cast(null as boolean)")) \
    .withColumn('giveaway_player_onice_index',f.expr("cast(null as int)"))



# COMMAND ----------

df_merged_out = _df_preunion_penl[out_cols] \
    .unionByName(_df_preunion_give[out_cols] \
        .unionByName(_df_preunion_take[out_cols] \
            .unionByName(_df_preunion_hits[out_cols] \
                .unionByName(_df_preunion_face[out_cols] \
                    .unionByName(df_shots_all[out_cols])))))

# COMMAND ----------

df_filler = df_plays_with_onice.where(f.expr("type not in ('SHOT','GOAL','FAC','BLOCK','GIVE','HIT','MISS','PENL','TAKE')"))

# COMMAND ----------

missing_columns = set(df_merged_out.columns) - set(df_filler.columns)

# Add missing columns to df_filler with null values and corresponding types
for col in missing_columns:
    # Use schema to get the data type of the missing column in df_merged_out
    data_type = df_merged_out.schema[col].dataType
    
    # Add the missing column to df_filler with null values and the correct data type
    df_filler = df_filler.withColumn(col, f.lit(None).cast(data_type))

# Perform the union with the updated df_filler
unioned_df = df_merged_out.unionByName(df_filler)

# Save the result as a Parquet file
unioned_df.write.format("parquet").save('/FileStore/NHL_PLAYS_DB/plays_sparse_detail', mode = 'overwrite')

# COMMAND ----------

_input = df_plays_with_onice.groupBy("game_id").agg(f.count(f.lit("1")).alias('input_recs'))
_output = spark.read.format("parquet").load('/FileStore/NHL_PLAYS_DB/plays_sparse_detail').orderBy(f.col("game_id").asc(), f.expr("cast(index as int)").asc()).groupBy("game_id").agg(f.count(f.lit("1")).alias('output_recs'))

_input.join(_output,['game_id'],"full").where(f.expr("input_recs <> output_recs")).display()

# COMMAND ----------

_output.display()

# COMMAND ----------


