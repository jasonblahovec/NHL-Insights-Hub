# Databricks notebook source
# MAGIC %md # Environment

# COMMAND ----------

# MAGIC %pip install Pillow
# MAGIC %pip install openai
# MAGIC %pip install langchain
# MAGIC %pip install tiktoken
# MAGIC %pip install faiss-cpu
# MAGIC %pip install pip install unstructured
# MAGIC %pip install pypdf2
# MAGIC %pip install pypdf
# MAGIC %pip install google-search-results

# COMMAND ----------

import pyspark.sql.functions as f
from pyspark.sql.types import *
import numpy as np
from scipy import stats
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from PIL import Image
from IPython.display import display as IPdisplay
import io
from io import BytesIO
import sys
import os
from langchain.chat_models import ChatOpenAI
from langchain.schema import HumanMessage, SystemMessage, AIMessage
from langchain.schema import Document
from langchain.memory import ChatMessageHistory
import numpy as np
import json
import re
import pandas as pd
import time

os.environ["SERP_API_KEY"] = ''
os.environ["OPENAI_API_KEY"] = ''
openai_api_key = os.environ["OPENAI_API_KEY"]


# COMMAND ----------

# MAGIC %md # Source Code

# COMMAND ----------

# MAGIC %md ## ETL

# COMMAND ----------

class ExtractTeammateData():

    """
    Requires /FileStore/nhl/bq_skater_vs_goaltender_2005_2022, a parquet location containing the BigQuery
    Skater Vs Goaltender Dataset.

    Regular Season Only 

    full_careers = False trims the input games to on;y those between the first and last games played together.  
    (example removal of Crosby rookie year)
    """

    def __init__(self, player_a = 'sidney crosby', player_b = 'evgeni malkin', full_careers = False):
        # Load the dataset with all NHL player-games
        spark.read.format("parquet").load("/FileStore/nhl/bq_skater_vs_goaltender_2005_2022") \
            .createOrReplaceTempView('bq_skater_vs_goaltender_2005_2022')

        self.player_a = player_a
        self.player_b = player_b
        self.full_careers = full_careers

        self.df_all_a_b_games = spark.sql(self.get_sql_all_player_games())

        self.tenure_game_count, self.tenure_first_game, self.tenure_last_game,self.tenure_first_game_ts, self.tenure_last_game_ts =  \
            self.summarize_teammate_tenure()

        if self.full_careers:
            pass
        else:
            self.df_tenure = self.df_all_a_b_games \
                .where(f.expr(f"game_pk between '{self.tenure_first_game}' and '{self.tenure_last_game}'"))

        self.df_tenure = self.append_points_fields()
        self.df_tenure = self.restore_data_types()
        self.df_tenure = self.refine_selected_column()

        self.df_player_a, self.df_player_b = self.player_split_dfs()

    def get_sql_all_player_games(self):
        return f"""
                select 
                    skg.*
                    , gms.player_a_played
                    , gms.player_b_played
                FROM
                    bq_skater_vs_goaltender_2005_2022 skg 
                    left join (
                        select 
                            coalesce(cr.player_a_played,ma.player_b_played) as game_pk
                            ,case when cr.player_a_played is not null then 1 else 0 end as player_a_played
                            ,case when ma.player_b_played is not null then 1 else 0 end as player_b_played
                        from 
                            (select distinct game_pk as player_a_played, player_teamtype from bq_skater_vs_goaltender_2005_2022 where lower(player_fullname) = '{self.player_a}') cr
                            full join (select distinct game_pk as player_b_played, player_teamtype from bq_skater_vs_goaltender_2005_2022 where lower(player_fullname) = '{self.player_b}') ma
                        on cr.player_a_played = ma.player_b_played 
                        and cr.player_teamtype = ma.player_teamtype
                    ) gms 
                    on skg.game_pk = gms.game_pk
                WHERE
                    (lower(skg.player_fullname) like '%{self.player_a}%' or lower(skg.player_fullname) like '%{self.player_b}%')
                    AND skg.game_type = 'R'"""

    def summarize_teammate_tenure(self):
        df_tenure_summary =self.df_all_a_b_games.groupBy("player_fullname", 'player_a_played','player_b_played').agg(
            f.count(f.lit(1)).alias('game_count')
            , f.min('game_pk').alias('first_game')
            , f.max('game_pk').alias('last_game')
            , f.min('game_start').alias('first_game_ts')
            , f.max('game_start').alias('last_game_ts')
            ).where(f.expr("player_a_played = 1 and player_b_played = 1"))[['game_count','first_game','last_game','first_game_ts','last_game_ts']].distinct()
        assert df_tenure_summary.count() == 1

        tenure_game_count = df_tenure_summary.collect()[0].game_count
        tenure_first_game = df_tenure_summary.collect()[0].first_game
        tenure_last_game = df_tenure_summary.collect()[0].last_game
        tenure_first_game_ts = df_tenure_summary.collect()[0].first_game_ts
        tenure_last_game_ts = df_tenure_summary.collect()[0].last_game_ts

        return tenure_game_count, tenure_first_game, tenure_last_game, tenure_first_game_ts, tenure_last_game_ts

    def append_points_fields(self):
        return self.df_tenure \
            .withColumn("points",f.col('goals')+f.col('assists')) \
            .withColumn("standings_points",
                        2*(f.col('regulation_win')
                        +f.col('ot_win')+f.col('shootout_win'))
                        +f.col('ot_loss')
                        +f.col('shootout_loss'))
            
    def restore_data_types(self):
        return self.df_tenure \
            .withColumn("game_number", f.col("game_number").cast("int")) \
            .withColumn("game_pk", f.col("game_pk").cast("int")) \
            .withColumn("player_id", f.col("player_id").cast("int")) \
            .withColumn("assists", f.col("assists").cast("int")) \
            .withColumn("goals", f.col("goals").cast("int")) \
            .withColumn("shots", f.col("shots").cast("int")) \
            .withColumn("hits", f.col("hits").cast("int")) \
            .withColumn("powerPlayGoals", f.col("powerPlayGoals").cast("int")) \
            .withColumn("powerPlayAssists", f.col("powerPlayAssists").cast("int")) \
            .withColumn("penaltyMinutes", f.col("penaltyMinutes").cast("int")) \
            .withColumn("faceoffTaken", f.col("faceoffTaken").cast("int")) \
            .withColumn("faceOffWins", f.col("faceOffWins").cast("int")) \
            .withColumn("takeaways", f.col("takeaways").cast("int")) \
            .withColumn("giveaways", f.col("giveaways").cast("int")) \
            .withColumn("shortHandedGoals", f.col("shortHandedGoals").cast("int")) \
            .withColumn("shortHandedAssists", f.col("shortHandedAssists").cast("int")) \
            .withColumn("blocked", f.col("blocked").cast("int")) \
            .withColumn("plusMinus", f.col("plusMinus").cast("int")) \
            .withColumn("evenTimeOnIce", f.col("evenTimeOnIce").cast("int")) \
            .withColumn("powerPlayTimeOnIce", f.col("powerPlayTimeOnIce").cast("int")) \
            .withColumn("shortHandedTimeOnIce", f.col("shortHandedTimeOnIce").cast("int")) \
            .withColumn("away_goals", f.col("away_goals").cast("int")) \
            .withColumn("home_goals", f.col("home_goals").cast("int")) \
            .withColumn("p1_away_goals", f.col("p1_away_goals").cast("int")) \
            .withColumn("p2_away_goals", f.col("p2_away_goals").cast("int")) \
            .withColumn("p3_away_goals", f.col("p3_away_goals").cast("int")) \
            .withColumn("ot4_away_goals", f.col("ot4_away_goals").cast("int")) \
            .withColumn("so_goals_away", f.col("so_goals_away").cast("int")) \
            .withColumn("p1_home_goals", f.col("p1_home_goals").cast("int")) \
            .withColumn("p2_home_goals", f.col("p2_home_goals").cast("int")) \
            .withColumn("p3_home_goals", f.col("p3_home_goals").cast("int")) \
            .withColumn("ot4_home_goals", f.col("ot4_home_goals").cast("int")) \
            .withColumn("so_goals_home", f.col("so_goals_home").cast("int")) \
            .withColumn("regulation_away", f.col("regulation_away").cast("int")) \
            .withColumn("regulation_home", f.col("regulation_home").cast("int")) \
            .withColumn("regulation_win", f.col("regulation_win").cast("int")) \
            .withColumn("ot_win", f.col("ot_win").cast("int")) \
            .withColumn("shootout_win", f.col("shootout_win").cast("int")) \
            .withColumn("regulation_loss", f.col("regulation_loss").cast("int")) \
            .withColumn("ot_loss", f.col("ot_loss").cast("int")) \
            .withColumn("shootout_loss", f.col("shootout_loss").cast("int")) \
            .withColumn("win", f.col("win").cast("int")) \
            .withColumn("overtime_loss", f.col("overtime_loss").cast("int")) \
            .withColumn("loss", f.col("loss").cast("int")) \
            .withColumn("isFirstStar", f.col("isFirstStar").cast("int")) \
            .withColumn("isSecondStar", f.col("isSecondStar").cast("int")) \
            .withColumn("isThirdStar", f.col("isThirdStar").cast("int")) \
            .withColumn("career_game_number", f.col("career_game_number").cast("int")) \
            .withColumn("season_game_number", f.col("season_game_number").cast("int")) \
            .withColumn("points", f.col("points").cast("int")) \
            .withColumn("standings_points", f.col("standings_points").cast("int"))

    def refine_selected_column(self):

        df_tenure = self.df_tenure[[x.name for x in self.df_tenure.schema if x.dataType == IntegerType()]+
            ['game_start'
            ,'player_teamtype'
            ,'player_fullname'
            ,'player_jersey_number'
            ,'player_position'
            ,'team_away_name'
            ,'team_home_name'
            ,'location'
            ,'head_coach_away'
            ,'head_coach_home']]

        df_tenure = df_tenure \
            .withColumnRenamed("player_b_played","flag_player_b") \
                .withColumnRenamed("player_a_played","flag_player_a")
        
        return df_tenure
    
    def player_split_dfs(self):
        return self.df_tenure.where(f.lower(f.col('player_fullname'))==f.lit(self.player_a)) , \
            self.df_tenure.where(f.lower(f.col('player_fullname'))==f.lit(self.player_b))

    def run_eda(self, include_player_a = True, include_player_b = True):
        if include_player_a and include_player_b:
            print("Performing EDA for both players' data in one batch.")
            dbutils.data.summarize(self.df_tenure)
        elif include_player_a and not include_player_b:
            print(f"Performing EDA for {self.player_a} games played.")
            dbutils.data.summarize(self.df_player_a)
        elif not include_player_a and include_player_b:
            print(f"Performing EDA for {self.player_b} games played.")
            dbutils.data.summarize(self.df_player_b)
        else:
            print("No players selected.")

# COMMAND ----------

# MAGIC %md ## Hypothesis Test

# COMMAND ----------


class TeammateImpact():

    def __init__(self, df_tenure, alpha = .05, player_a = 'sidney crosby', player_b = 'evgeni malkin'):
        self.player_a = player_a
        self.player_b = player_b

        self.a_wo_b = df_tenure.where(f.expr(f"lower(player_fullname) = '{self.player_a}'")).where(f.expr("flag_player_b = 0")).sort("game_pk")
        self.a_w_b = df_tenure.where(f.expr(f"lower(player_fullname) = '{self.player_a}'")).where(f.expr("flag_player_b = 1")).sort("game_pk")
        self.b_wo_a = df_tenure.where(f.expr(f"lower(player_fullname) = '{self.player_b}'")).where(f.expr("flag_player_a = 0")).sort("game_pk")
        self.b_w_a = df_tenure.where(f.expr(f"lower(player_fullname) = '{self.player_b}'")).where(f.expr("flag_player_a = 1")).sort("game_pk")
        self.alpha = alpha

        self.analysis_columns = [
            'assists','goals'
            ,'points','shots'
            ,'hits','powerPlayGoals'
            ,'powerPlayAssists','penaltyMinutes'
            ,'faceoffTaken','faceOffWins'
            ,'takeaways','giveaways','plusMinus'
            ,'evenTimeOnIce','powerPlayTimeOnIce'
            ,'shortHandedTimeOnIce','standings_points']

        self.out_a = []
        self.out_b = []
        self.figures = {}
        for _col in self.analysis_columns:
            
            stat = _col
            a_wo_b_input = [int(x) for x in self.a_wo_b[[stat]].toPandas()[stat].values]
            a_w_b_input = [int(x) for x in self.a_w_b[[stat]].toPandas()[stat].values]

            b_wo_a_input = [int(x) for x in self.b_wo_a[[stat]].toPandas()[stat].values]
            b_w_a_input = [int(x) for x in self.b_w_a[[stat]].toPandas()[stat].values]

            a_wo_b_mean = self.calculate_average(a_wo_b_input)
            a_w_b_mean = self.calculate_average(a_w_b_input)
            a_wo_b_med = self.find_median(a_wo_b_input)
            a_w_b_med = self.find_median(a_w_b_input)

            b_wo_a_mean = self.calculate_average(b_wo_a_input)
            b_w_a_mean = self.calculate_average(b_w_a_input)
            b_wo_a_med = self.find_median(b_wo_a_input)
            b_w_a_med = self.find_median(b_w_a_input)

            U_a, p_a, result_a = self.run_mannwhutneyu(a_w_b_input,a_wo_b_input, alpha = self.alpha, verbose = False, alternative = 'two-sided')
            U_b, p_b, result_b = self.run_mannwhutneyu(b_w_a_input,b_wo_a_input, alpha = self.alpha, verbose = False, alternative = 'two-sided')
            
            self.out_a = self.out_a + [[_col,self.player_a,U_a, p_a, result_a, a_w_b_mean,a_wo_b_mean, a_w_b_med,a_wo_b_med]]
            self.out_b = self.out_b + [[_col,self.player_b,U_b, p_b, result_b, b_w_a_mean,b_wo_a_mean, b_w_a_med,b_wo_a_med]]

            self.figures[_col] = {}
            if result_a[0:len('Reject')]=='Reject':
                bp = self.get_boxplot(a_w_b_input,a_wo_b_input,self.player_a.split(' ')[-1], self.player_b.split(' ')[-1],_col)
                plt.show()
                hst = self.get_histogram(a_w_b_input,a_wo_b_input,self.player_a.split(' ')[-1], self.player_b.split(' ')[-1],_col)
                plt.show()
                self.figures[_col][self.player_a] = {
                    'boxplot':bp
                    ,'histogram':hst
                }
                
            if result_b[0:len('Reject')]=='Reject':
                bp = self.get_boxplot(b_w_a_input,b_wo_a_input,self.player_b.split(' ')[-1], self.player_a.split(' ')[-1],_col)
                plt.show()
                hst = self.get_histogram(b_w_a_input,b_wo_a_input,self.player_b.split(' ')[-1], self.player_a.split(' ')[-1],_col)
                plt.show()
                self.figures[_col][self.player_b] = {
                    'boxplot':bp
                    ,'histogram':hst
                }


        self.df_out_b = spark.createDataFrame(pd.DataFrame(self.out_b, columns = ['statistic','player_last_name','mann_whitney_u','p_value','test_result',f"average_with_{self.player_a.split(' ')[-1].lower()}",f"average_without_{self.player_a.split(' ')[-1].lower()}",f"median_with_{self.player_a.split(' ')[-1].lower()}",f"median_without_{self.player_a.split(' ')[-1].lower()}"])) \
            .withColumn("reject_null",f.expr("case when split(test_result,' ')[0] = 'Reject' then true else false end"))

        self.df_out_a = spark.createDataFrame(pd.DataFrame(self.out_a, columns = ['statistic','player_last_name','mann_whitney_u','p_value','test_result',f"average_with_{self.player_b.split(' ')[-1].lower()}",f"average_without_{self.player_b.split(' ')[-1].lower()}",f"median_with_{self.player_b.split(' ')[-1].lower()}",f"median_without_{self.player_b.split(' ')[-1].lower()}"])) \
            .withColumn("reject_null",f.expr("case when split(test_result,' ')[0] = 'Reject' then true else false end"))

    @staticmethod
    def run_mannwhutneyu(group1,group2, alpha = .05,verbose = False, alternative = 'two-sided'):
        # Perform the Mann-Whitney U test
        # https://www.youtube.com/watch?v=mBYc1SNSEoU
        statistic, p_value = stats.mannwhitneyu(group1, group2, alternative=alternative)

        if verbose:
            # Print the results
            print("Mann-Whitney U Statistic:", statistic)
            print("Two-Sided P-Value:", p_value)

        # Interpret the results
        if p_value < alpha:
            return statistic, p_value, "Reject the null hypothesis: There is a significant difference between the groups."
        else:
            return statistic, p_value, "Fail to reject the null hypothesis: There is no significant difference between the groups."

    @staticmethod
    def get_boxplot(group1,group2,player_name, other_player_name, y_label):
        # Create a list of data for the box plot
        data = [group1, group2]

        # Create a box plot
        plt.boxplot(data, labels=[f"{player_name} with {other_player_name}",f"{player_name} without {other_player_name}"])
        plt.title(f"Box Plot of {player_name} with and without {other_player_name}")
        plt.ylabel(y_label)

        # Capture the plot as a binary image
        image_buffer = BytesIO()
        plt.savefig(image_buffer, format="png")
        image_buffer.seek(0)  # Move the buffer's position to the start

        # Close the plot
        plt.close()

        # Return the binary image as bytes
        return image_buffer.getvalue()

    @staticmethod
    def get_histogram(group1,group2,player_name, other_player_name, x_label):
        plt.hist(group1, bins=range(min(group1), max(group1) + 1), alpha=0.5, label=f"{player_name} with {other_player_name}")
        plt.hist(group2, bins=range(min(group2), max(group2) + 1), alpha=0.5, label=f"{player_name} without {other_player_name}")
        plt.xlabel(x_label)
        plt.ylabel("Frequency")
        plt.legend()
        plt.title(f"{player_name} {x_label} with and without {other_player_name}")
        # Capture the plot as a binary image
        image_buffer = BytesIO()
        plt.savefig(image_buffer, format="png")
        image_buffer.seek(0)  # Move the buffer's position to the start

        # Close the plot
        plt.close()

        # Return the binary image as bytes
        return image_buffer.getvalue()
    
    @staticmethod
    def calculate_average(lst):
        if len(lst) == 0:
            return None  # Avoid division by zero for an empty list
        total = sum(lst)
        average = total / len(lst)
        return (average)

    @staticmethod
    def find_median(lst):
        sorted_lst = sorted(lst)
        n = len(sorted_lst)
        
        if n % 2 == 1:
            # If the list has an odd number of elements
            median = sorted_lst[n // 2]
        else:
            # If the list has an even number of elements
            middle1 = sorted_lst[(n // 2) - 1]
            middle2 = sorted_lst[n // 2]
            median = (middle1 + middle2) / 2

        return (median)
    def display_image_from_bytes(self, image_bytes):
        image = Image.open(io.BytesIO(image_bytes))
        IPdisplay(image)

# COMMAND ----------

# MAGIC %md ## LLM

# COMMAND ----------

class TeammateLLMAnalysis():

    def __init__(self, player_a_filename, player_b_filename, analysis_columns, alpha, input_player_a, input_player_b, first_game, last_game, tenure_game_count):
        self.player_a_filename = player_a_filename
        self.player_b_filename = player_b_filename
        self.analysis_columns = analysis_columns
        self.alpha = alpha
        self.input_player_a = input_player_a
        self.input_player_b = input_player_b
        self.first_game = first_game
        self.last_game = last_game
        self.tenure_game_count = tenure_game_count

        self.df_player_a_results = spark.read \
            .format("delta") \
            .load(self.player_a_filename)

        self.df_player_b_results = spark.read \
            .format("delta") \
            .load(self.player_b_filename)

        # ## An Interpretation of the input data for prompting.
        # self.test_result_schema = self.get_tuple_schema()

        # #populates self.dict_output
        # self.get_statistic_summaries()
        self.player_a_test_results , self.player_b_test_results = self.summarize_test_results()

        self.player_profiles = self.build_player_profiles()


    def summarize_test_results(self):
        player_a_test_results = f"""
        ################# TEST RESULTS FOR {self.input_player_a}  ##############################
        Here are the areas where significant differences were found for {self.input_player_a}:
        """
        for test_result in self.df_player_a_results.where(f.expr("reject_null")).toPandas().itertuples():
            player_a_test_results = player_a_test_results + f"""{self.input_player_a}'s average {test_result.statistic}  changes from {test_result[-5]} to {test_result[-4]} per game when {self.input_player_b} is not in the lineup.  This difference was found to be significant at the {self.alpha} level, with a p-value of {test_result.p_value}.
                """

        no_difference_a = []
        for test_result in self.df_player_a_results.where(f.expr("not reject_null")).toPandas().itertuples():
            no_difference_a = no_difference_a + [test_result.statistic]

        player_a_test_results = player_a_test_results + (f"""
            
            No significant difference was found in the output of {self.input_player_a} in the tests conducted for {str(no_difference_a)}. This suggests that the presence of {self.input_player_b} in the lineup has not historically impacted {self.input_player_a}'s output in these areas of the game.
            ################# END TEST RESULTS FOR {self.input_player_a}  ##############################""")

        player_b_test_results = f"""
        ################# TEST RESULTS FOR {self.input_player_b}  ##############################
        Here are the areas where significant differences were found for {self.input_player_b}:
        """

        for test_result in self.df_player_b_results.where(f.expr("reject_null")).toPandas().itertuples():
            player_b_test_results = player_b_test_results +  f"""{self.input_player_b}'s average {test_result.statistic}  changes from {test_result[-5]} to {test_result[-4]} per game when {self.input_player_a} is not in the lineup.  This difference was found to be significant at the {self.alpha} level, with a p-value of {test_result.p_value}."""

        no_difference_b = []
        for test_result in self.df_player_b_results.where(f.expr("not reject_null")).toPandas().itertuples():
            no_difference_b = no_difference_b + [test_result.statistic]

        player_b_test_results = player_b_test_results + f"""

        No significant difference was found in the output of {self.input_player_b} in the tests conducted for {str(no_difference_b)}. This suggests that the presence of {self.input_player_a} in the lineup has not historically impacted {self.input_player_b}'s output in these areas of the game.
        ################# END TEST RESULTS FOR {self.input_player_b}  ##############################"""

        return player_a_test_results , player_b_test_results

    def build_player_profiles(self):
        prompt_summary = f"""
        You are overly dramatic sports analyst Stephen A. Smith. You've tasked your data analyst cronies with digging up dirt on NHL teammates {self.input_player_a} and {self.input_player_b}.  For each player, they assembled their career games played and split them based on if the other player was in the lineup that game or not, and ran hypothesis tests across several NHL game statistics ('assists','goals','points','shots','hits','powerPlayGoals','powerPlayAssists','penaltyMinutes','faceoffTaken','faceOffWins','takeaways','giveaways','plusMinus','evenTimeOnIce','powerPlayTimeOnIce','shortHandedTimeOnIce','standings_points').  Here are summaries of the findings for each player:
        {self.player_a_test_results}
        {self.player_b_test_results}

        As part of your introduction of the teammates, be sure to reference their first game as teammates, {self.first_game} and their last game in the study, {self.last_game} (there are in yyyy-MM-ddTHH:mm:ss format).  The study ends at the end of the 2022-2023 regular season, or earlier if the two parted ways before that.  Also be sure to include the fact that they have played  {self.tenure_game_count} games as teammates in this span.

        Use these groups of the stats tested in your analysis: 
        Scoring: 'assists', 'goals', 'points', 'powerPlayGoals','powerPlayAssists', shots
        Ice Time: 'evenTimeOnIce','powerPlayTimeOnIce','shortHandedTimeOnIce'
        Face-Offs: 'faceoffTaken','faceOffWins'
        Puck Protection: higher takeaways, lower giveaways
        Other: anything mentioned above and not assigned a group here.

        For reference, assists, goals, points, shots, hits, powerPlayGoals, powerPlayAssists, faceOffTaken, faceOffWins, takeaways, plusMinus, timeOnIce, and standings points can be perceived as better when they are higher. Lower average giveaways are more desirable.


        Please write profiles for each player highlighting differences in their play both with and without each other in the lineup.  Write the outputs in a way that could be presented on a sports news program, and fabricate the drama your on-air personality is well known for.  Cite individual test results to back up your statements, but do not include the p value here.  Do not use the name Stephen A Smith in your ooutput, but otherwise use the same persona."""
        chat = ChatOpenAI(temperature=.5, openai_api_key=openai_api_key)
        history = ChatMessageHistory()

        history.add_user_message(prompt_summary)
        ai_response = chat(history.messages)
        self.profile_prompt = prompt_summary
        return ai_response

# COMMAND ----------



# COMMAND ----------

# MAGIC %md # Main Method

# COMMAND ----------

def create_teammate_profile(input_player_a, input_player_b):
    input_teammate_dataset = ExtractTeammateData(
        player_a = input_player_a
        , player_b = input_player_b
        ,full_careers = False)
    # input_teammate_dataset.run_eda(True, False)

    input_teammate_dataset.df_tenure.write.format("delta").save('/FileStore/nhl/temp_teammate_input', mode = 'overwrite')
    analysis = TeammateImpact( \
        spark.read.format('delta').load('/FileStore/nhl/temp_teammate_input')
        , alpha = .05
        , player_a = input_player_a
        , player_b = input_player_b)

    pl = [input_player_a.split(' ')[-1],input_player_b.split(' ')[-1]]
    pl.sort()

    analysis.df_out_a.write.format("delta") \
        .save(f"/FileStore/nhl/teammate_impact_reports/{'_'.join(pl)}/{input_player_a}/{input_teammate_dataset.tenure_first_game}-{input_teammate_dataset.tenure_last_game}", mode = 'overwrite')
    analysis.df_out_b.write.format("delta") \
        .save(f"/FileStore/nhl/teammate_impact_reports/{'_'.join(pl)}/{input_player_b}/{input_teammate_dataset.tenure_first_game}-{input_teammate_dataset.tenure_last_game}", mode = 'overwrite')

    player_a_filename = f"/FileStore/nhl/teammate_impact_reports/{'_'.join(pl)}/{input_player_a}/{input_teammate_dataset.tenure_first_game}-{input_teammate_dataset.tenure_last_game}"
    player_b_filename = f"/FileStore/nhl/teammate_impact_reports/{'_'.join(pl)}/{input_player_b}/{input_teammate_dataset.tenure_first_game}-{input_teammate_dataset.tenure_last_game}"

    tllm = TeammateLLMAnalysis(player_a_filename, player_b_filename, analysis.analysis_columns, analysis.alpha, input_player_a, input_player_b, input_teammate_dataset.tenure_first_game_ts, input_teammate_dataset.tenure_last_game_ts, input_teammate_dataset.tenure_game_count)

    return input_teammate_dataset, analysis, tllm

# COMMAND ----------

if __name__ == "__main__":
    input_player_a = 'sidney crosby'
    input_player_b = 'evgeni malkin'
    etl, hyp_testing, llm_layer = create_teammate_profile(input_player_a, input_player_b)
    print(llm_layer.player_profiles)

# COMMAND ----------

if __name__ == "__main__":
    input_player_a = 'sidney crosby'
    input_player_b = 'pascal dupuis'
    etl, hyp_testing, llm_layer = create_teammate_profile(input_player_a, input_player_b)
    print(llm_layer.player_profiles)

# COMMAND ----------

if __name__ == "__main__":
    input_player_a = 'alex ovechkin'
    input_player_b = 'nicklas backstrom'
    etl, hyp_testing, llm_layer = create_teammate_profile(input_player_a, input_player_b)
    print(llm_layer.player_profiles)

# COMMAND ----------

if __name__ == "__main__":
    input_player_a = 'connor mcdavid'
    input_player_b = 'leon draisaitl'
    etl, hyp_testing, llm_layer = create_teammate_profile(input_player_a, input_player_b)
    print(llm_layer.player_profiles)

# COMMAND ----------

if __name__ == "__main__":
    input_player_a = 'jonathan toews'
    input_player_b = 'patrick kane'
    etl, hyp_testing, llm_layer = create_teammate_profile(input_player_a, input_player_b)
    print(llm_layer.player_profiles)

# COMMAND ----------

if __name__ == "__main__":
    input_player_a = 'joe thornton'
    input_player_b = 'patrick marleau'
    etl, hyp_testing, llm_layer = create_teammate_profile(input_player_a, input_player_b)
    print(llm_layer.player_profiles)

# COMMAND ----------

# MAGIC %md #Notes

# COMMAND ----------


# import pyspark.sql.functions as f
# df_raw_bq_export = spark.read.format("csv").load('/FileStore/nhl/bq_results_20231110_144115_1699627310459__1_.csv', header ='true').where(f.col('game_season')<=f.lit('20222023'))
# df_raw_bq_export.withColumn('player_fullname', f.expr('lower(player_fullname)')).repartition('player_fullname').write.format("parquet").save("/FileStore/nhl/bq_skater_vs_goaltender_2005_2022", mode = 'overwrite')

# COMMAND ----------

# analysis.display_image_from_bytes(analysis.figures['shots']['leon draisaitl']['boxplot'])
# analysis.display_image_from_bytes(analysis.figures['shots']['leon draisaitl']['histogram'])
