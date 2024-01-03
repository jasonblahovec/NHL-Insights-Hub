
import pyspark.sql.functions as f
from pyspark.sql.types import IntegerType
from pyspark.sql.types import *
import argparse
import pyspark



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


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NHL Data Ingestion to GCP")
    parser.add_argument("--output_bucket", type=str, help="a GCS Bucket")
    parser.add_argument("--output_destination", type=str, help="a location within GCS bucket where output is stored")
    args = parser.parse_args()

    spark = pyspark.sql.SparkSession.builder \
        .appName("NHL Play Ingestion to GCP") \
        .getOrCreate()
    
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")

    bucket_name = args.output_bucket
    output_destination = args.output_destination

    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
    df_plays_with_onice = spark.read.format("parquet").load(f"gs://{bucket_name}/{output_destination}/plays_with_onice")

    # MAGIC %md ### FaceOff
    df_faceoff_detail = get_faceoff_detail(df_plays_with_onice)
    df_preunion_face = df_faceoff_detail[df_plays_with_onice.columns+['fo_winning_team', 'fo_zone', 'fo_away_player', 'fo_home_player', 'away_onice_index', 'home_onice_index']]

    # MAGIC %md ### Hits
    df_hit_detail = get_hit_detail(df_plays_with_onice)
    df_hitter = append_lineup_info(df_stat = df_hit_detail, skater_action = 'hitter', action_label = 'hitter')
    df_hits_full = append_lineup_info(df_stat = df_hitter, skater_action = 'hittee', action_label = 'hittee')
    df_preunion_hits = df_hits_full[df_plays_with_onice.columns+['hit_location','hitter', 'hittee', 'hitter_team', 'hittee_team', 'home_hitter', 'hitter_onice_index', 'hittee_onice_index']]

    # MAGIC %md ### Blocked Shots
    df_blocks = get_block_detail(df_plays_with_onice)
    df_blocker = append_lineup_info(df_stat = df_blocks, skater_action = 'blocker', action_label = 'blocker')
    df_blocks_full = append_lineup_info(df_stat = df_blocker, skater_action = 'shooter', action_label = 'shooter')
    df_preunion_block = df_blocks_full[df_plays_with_onice.columns+['shooter', 'blocker', 'block_location', 'shot_type', 'blocker_team','shooter_team', 'home_blocker', 'home_shooter', 'shooter_onice_index', 'blocker_onice_index']]

    # MAGIC %md ### Missed Shots
    df_miss = get_miss_detail(df_plays_with_onice)
    df_miss_full = append_lineup_info(df_stat = df_miss, skater_action = 'shooter', action_label = 'shooter')
    df_preunion_miss = df_miss_full[df_plays_with_onice.columns+['shooter', 'shot_type', 'shot_result', 'shot_zone', 'shot_distance', 'shooter_team', 'home_shooter','shooter_onice_index']]

    # MAGIC %md ### Giveaway
    df_give = get_giveaway_detail(df_plays_with_onice)
    df_give_full = append_lineup_info(df_stat = df_give, skater_action = 'giveaway_player', action_label = 'giveaway_player')
    df_preunion_give = df_give_full[df_plays_with_onice.columns+['giveaway_player', 'giveaway_zone', 'giveaway_player_team', 'home_giveaway_player' ,'giveaway_player_onice_index']]

    # MAGIC %md ### Penalty
    df_penl = get_penalty_detail(df_plays_with_onice)
    df_offendor = append_lineup_info(df_stat = df_penl, skater_action = 'penalty_committed_by', action_label = 'penalty_committed_by')
    df_penl_full = append_lineup_info(df_stat = df_offendor, skater_action = 'penalty_drawn_by', action_label = 'penalty_drawn_by')
    df_preunion_penl = df_penl_full[df_plays_with_onice.columns+['penalty_drawn_by', 'penalty_committed_by', 'penalty_committed_by_team','penalty_zone', 'penalty_detail', 'maj_flag', 'penalty_duration', 'home_penalty_committed_by', 'penalty_committed_by_onice_index', 'penalty_drawn_by_team', 'penalty_drawn_by_onice_index']]

    # MAGIC %md ### Goals
    df_goal = get_goal_detail(df_plays_with_onice)
    df_scorer = append_lineup_info(df_stat = df_goal, skater_action = 'goal_scorer', action_label = 'goal_scorer')
    df_assist1 = append_lineup_info(df_stat = df_scorer, skater_action = 'primary_assist_player', action_label = 'primary_assist_player')
    df_assist2 = append_lineup_info(df_stat = df_assist1, skater_action = 'secondary_assist_player', action_label = 'secondary_assist_player')

    df_preunion_goal = df_assist2[df_plays_with_onice.columns+['goal_scorer', 'primary_assist_player', 'secondary_assist_player', 'goal_scorer_shottype', 'goal_scorer_shotzone', 'goal_scorer_shotdist', 'goal_scorer_team', 'goal_scorer_num_goals', 'primary_assistor_num_assists', 'secondary_assistor_num_assists', 'home_goal_scorer', 'goal_scorer_onice_index', 'primary_assist_player_onice_index', 'secondary_assist_player_onice_index']]

    # MAGIC %md ### Shots
    df_shot = get_shot_detail(df_plays_with_onice)
    df_shot_final = append_lineup_info(df_stat = df_shot, skater_action = 'shooter', action_label = 'shooter')
    df_preunion_shot = df_shot_final[df_plays_with_onice.columns+['shooting_team', 'shooter', 'shot_type', 'shot_distance', 'shooter_team', 'shot_zone', 'shot_distance', 'home_shooter', 'shooter_onice_index']]

    # MAGIC %md ###Takeaways
    df_take = get_takeaway_detail(df_plays_with_onice)
    df_take_final = append_lineup_info(df_stat = df_take, skater_action = 'takeaway_player', action_label = 'takeaway_player')
    df_preunion_take = df_take_final[df_plays_with_onice.columns+['takeaway_player', 'takeaway_zone', 'takeaway_player_team', 'home_takeaway_player', 'takeaway_player_onice_index']]


    # MAGIC %md ## Union By-Statistic Output
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

    df_merged_out = _df_preunion_penl[out_cols] \
        .unionByName(_df_preunion_give[out_cols] \
            .unionByName(_df_preunion_take[out_cols] \
                .unionByName(_df_preunion_hits[out_cols] \
                    .unionByName(_df_preunion_face[out_cols] \
                        .unionByName(df_shots_all[out_cols])))))

    df_filler = df_plays_with_onice.where(f.expr("type not in ('SHOT','GOAL','FAC','BLOCK','GIVE','HIT','MISS','PENL','TAKE')"))

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
    unioned_df.write.format("parquet").save(f"gs://{bucket_name}/{output_destination}/plays_sparse_detail", mode = 'overwrite')

    spark.stop()
