import requests
import pandas as pd
import argparse
import pyspark

import re
from bs4 import BeautifulSoup
import pyspark.sql.functions as f
from pyspark.sql import Row
from pyspark.sql.types import ArrayType, IntegerType, StringType
from pyspark.sql.types import *
from pyspark.sql.functions import udf

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


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NHL Data Ingestion to GCP")
    parser.add_argument("--gamemin", type=int, help="Starting NHL gameid for data retrieval")
    parser.add_argument("--gamemax", type=int, help="Ending NHL gameid for data retrieval")
    parser.add_argument("--output_bucket", type=str, help="a GCS Bucket")
    parser.add_argument("--output_destination", type=str, help="a location within GCS bucket where output is stored")
    parser.add_argument("--write_mode", type=str, help="overwrite or append, as used in spark.write.*")
    args = parser.parse_args()

    spark = pyspark.sql.SparkSession.builder \
        .appName("NHL Play Ingestion to GCP") \
        .getOrCreate()
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")

    bucket_name = args.output_bucket
    output_destination = args.output_destination
    write_mode = args.write_mode
    game_min = args.gamemin
    game_max = args.gamemax

    errors = []
    for _i,GAMEID in enumerate(range(game_min,game_max)):
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


    df_out.write.format("parquet").mode(write_mode).option("overwriteSchema", "true").save(f"gs://{bucket_name}/{output_destination}/raw_plays")
    df_out_forwards.write.format("parquet").mode(write_mode).option("overwriteSchema", "true").save(f"gs://{bucket_name}/{output_destination}/raw_forwards")
    df_out_defense.write.format("parquet").mode(write_mode).option("overwriteSchema", "true").save(f"gs://{bucket_name}/{output_destination}/raw_defense")
    df_out_goalies.write.format("parquet").mode(write_mode).option("overwriteSchema", "true").save(f"gs://{bucket_name}/{output_destination}/raw_goalies")


    df_plays = spark.read.format("parquet").load(f"gs://{bucket_name}/{output_destination}/raw_plays")
    df_forwards = clean_name_col(spark.read.format("parquet").load(f"gs://{bucket_name}/{output_destination}/raw_forwards"))
    df_defense = clean_name_col(spark.read.format("parquet").load(f"gs://{bucket_name}/{output_destination}/raw_defense"))
    df_goalies = clean_name_col(spark.read.format("parquet").load(f"gs://{bucket_name}/{output_destination}/raw_goalies"))
    df_plays_with_onice = append_nonea_onice_info(df_plays, df_forwards, df_defense, df_goalies)
    df_plays_with_onice.write.format("parquet").save(f"gs://{bucket_name}/{output_destination}/plays_with_onice", mode = write_mode)

    spark.stop()